import pandas as pd

class TableGenerator:
    """
    Class which contains methods for generating output tables from playlist tables provided by the Spotify API
    """
    def __init__(
        self,
        simplified_playlists_df: pd.DataFrame,
        playlists_df: pd.DataFrame,
        items_normalized: pd.DataFrame=None,
        artists_normalized: pd.DataFrame=None
    ) -> None:
    
        self.simplified_playlists_df = simplified_playlists_df
        self.playlists_df = playlists_df
        self.items_normalized = None
        self.artists_normalized = None
    
    def get_items_normalized(self) -> pd.DataFrame:
        # dont recalculate if already stored
        if self.items_normalized is None:
            # expand tracks dict into columns and reassign playlist_id as index
            tracks_normalized = pd.json_normalize(self.playlists_df['tracks']).set_index(self.playlists_df.index)

            # unroll items list into rows
            items_exploded = tracks_normalized['items'].explode()

            # expand items dict into columns
            items_normalized = pd.json_normalize(items_exploded)

            # reassign playlist_id as column
            items_normalized['playlist_id'] = items_exploded.index

            self.items_normalized = items_normalized

        return self.items_normalized
    

    def get_artists_normalized(self):
        # dont recalculate if already stored
        if self.artists_normalized is None:
            items_normalized = self.get_items_normalized()

            # unroll track.artist into rows, keeping track.id
            artists_exploded = items_normalized[['track.artists', 'track.id']].explode(column='track.artists')

            artists_reindexed = artists_exploded.reset_index()

            # expand track.artist dict into columns
            artists_normalized = pd.json_normalize(artists_reindexed['track.artists'])

            # reassign track_id as column
            artists_normalized['track_id'] = artists_reindexed['track.id']

            self.artists_normalized = artists_normalized
        
        return self.artists_normalized
        

    def create_category_playlists_records(self) -> pd.DataFrame:
        category_playlist_records = self.simplified_playlists_df[[
            'description',
            'name',
            'snapshot_id'
        ]].copy()

        tracks_normalized = pd.json_normalize(self.simplified_playlists_df['tracks'])

        category_playlist_records['tracks_url'] = list(tracks_normalized['href'])
        category_playlist_records['total_tracks'] = list(tracks_normalized['total'])

        # one playlist had a newline in the description
        category_playlist_records['description'] = category_playlist_records['description'].replace(r'\n', '', regex=True)

        return category_playlist_records

    
    def create_playlist_records(self) -> None:
        playlists_followers = self.playlists_df['followers']
        followers_normalized = pd.json_normalize(playlists_followers)
        followers = followers_normalized['total'].rename('followers')

        followers.index = self.playlists_df.index

        return followers


    def create_tracks_records(self) -> pd.DataFrame:
        items_normalized = self.get_items_normalized()
        
        subset = items_normalized[[
            'track.album.album_type',
            'track.id',
            'track.name',
            'track.popularity',
            'track.uri'
        ]]

        tracks_records = subset.rename(
            columns={
                'track.album.album_type': 'album_type',
                'track.id': 'id',
                'track.name': 'name',
                'track.popularity': 'popularity',
                'track.uri': 'uri'
            }
        )
        
        tracks_records_unique = tracks_records.drop_duplicates()
        tracks_records_unique_indexed = tracks_records_unique.set_index('id')
        
        assert tracks_records_unique_indexed.index.is_unique

        return tracks_records_unique_indexed


    def create_playlist_track_id_records(self) -> pd.DataFrame:
        items_normalized = self.get_items_normalized()

        subset = items_normalized[[
            'playlist_id',
            'added_at',
            'track.id'
        ]]
        
        playlist_track_id_records = subset.rename(
            columns={
                'playlist_id': 'playlist_id',
                'added_at': 'playlist_added_at',
                'track.id': 'track_id'
            }
        )

        return playlist_track_id_records


    def create_track_artist_id_records(self) -> pd.DataFrame:
        artists_normalized = self.get_artists_normalized()

        subset = artists_normalized[[
            'id',
            'track_id'
        ]]

        artist_id_records = subset.rename(
            columns={
                'id': 'artist_id',
                'track_id': 'track_id'
            }
        )

        return artist_id_records

    def create_artists_records(self) -> pd.DataFrame:
        artists_normalized = self.get_artists_normalized()

        subset = artists_normalized[[
            'id',
            'name'
        ]]

        artist_records = subset.drop_duplicates()

        return artist_records
