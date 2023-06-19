import os
import pandas as pd
from loguru import logger

import spotify
from table_generator import TableGenerator

# would move to a config.ini file if there were more config parameters like this
CATEGORY_ID = 'latin'

# I usually use a setup() function like this to load environment variables, DB parameters,
# anything from a config file, etc. If anything needed to run the rest of the script
# is not successfully loaded then that can be raised before the script continues
def setup() -> tuple[str]:
    client_id = os.environ.get('SPOTIFY_CLIENT_ID')
    client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')

    # could raise an exception here or return None for control flow but wont worry for now
    if client_id is None or client_secret is None:
        logger.error('Client ID or client secret not set')
    else:
        logger.info('Loaded client_id and client_secret')

    return client_id, client_secret


def extract(client: spotify.SpotifyClient, category_id: str) -> tuple[list[dict]]:
    category_playlist_responses = spotify.get_all_category_simplified_playlists(client, category_id)
    simplified_playlists = spotify.get_simplified_playlists_from_responses(category_playlist_responses)
    logger.info(f'Got {len(simplified_playlists)} simplified playlists from category_id {category_id}')

    playlist_responses = spotify.get_all_playlists(client, simplified_playlists)
    # one-liner so dont really need to move to spotify.py
    playlists = [response.json() for response in playlist_responses]
    spotify.populate_all_tracks_in_playlists(client, playlists)

    return simplified_playlists, playlists


def transform(simplified_playlists: list[dict], playlists: list[dict]) -> dict:
    simplified_playlists_df = pd.DataFrame(simplified_playlists).set_index('id')
    playlists_df = pd.DataFrame(playlists).set_index('id')

    # this is maybe a bit overengineered but I had fun making it.
    # benefit of this mapping dict is that we can assign the names of the tables to the functions
    # that genererate them (which is something we should do anyway because dynamically mapping
    # strings to functions is going to be error-prone), and then loop to avoid a wall of
    # function and .to_csv() calls.
    # the values of this dict are the function objects rather than the returned DataFrames
    # so that we can call the functions one by one and include a log message and potentially
    # other operations like assigning a timestamp column
    table_generator = TableGenerator(simplified_playlists_df, playlists_df)
    table_func_mappings = {
        'category_playlist_records': table_generator.create_category_playlists_records,
        'playlist_records': table_generator.create_playlist_records,
        'tracks_records': table_generator.create_tracks_records,
        'playlist_track_id_records': table_generator.create_playlist_track_id_records,
        'track_artist_id_records': table_generator.create_track_artist_id_records,
        'artists_records': table_generator.create_artists_records
    }

    table_dfs = {}
    for table_name, func in table_func_mappings.items():
        logger.info(f'Creating DataFrame for {table_name}')
        table_dfs[table_name] = func()
    
    return table_dfs
    
def load(table_dfs: dict) -> None:
    # double loop over table names but means we only save csv.gz's after everything before ran successfully
    # tiny number of tables so doesn't really matter anyway
    for table_name, df in table_dfs.items():
        logger.info(f'Saving csv.gz for {table_name}')
        df.to_csv(f'tables/{table_name}.csv.gz', compression='gzip')
    

def main():
    client_id, client_secret = setup()

    # instantiate client and get access token
    client = spotify.SpotifyClient(client_id, client_secret)
    client.get_app_access_token()
    logger.info('Spotify client authenticated successfully')

    simplified_playlists, playlists = extract(client, CATEGORY_ID)
    logger.info('Finished extraction')

    table_dfs = transform(simplified_playlists, playlists)
    logger.info('Finished transformation')

    load(table_dfs)
    logger.info('Finished load')

    logger.info('Finished successfully')


if __name__ == '__main__':
    main()
