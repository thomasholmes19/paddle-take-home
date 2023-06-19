import urllib
import requests

from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from loguru import logger


class SpotifyClient:
    """
    Client class for accessing the Spotify API
    """

    def __init__(self, client_id: str, client_secret: str, access_token: str=None) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
    
    # have seperated this from the __init__ method to better follow Spotify's client credentials flow
    # https://developer.spotify.com/documentation/web-api/tutorials/client-credentials-flow
    # also from the endpoint docs it seems like an access token can expire, so having this method
    # seperate means we could re-authenticate using the same client instance
    def get_app_access_token(self) -> str:
        """
        Returns an access token to be used in the headers of GET API calls to
        the Get Category's Playlists and Get Playlist endpoints of Spotify.
        Docs to create an app found here: https://developer.spotify.com/documentation/web-api/concepts/apps
        Args:
            client_id (str): your Spotify's app client_id
            client_secret (str): your Spotif's client_secret
        Returns:
            access_token (str): an access token to be used in downstream API calls as Authorization headers
        """
        import base64
        auth_credentials_string = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth_credentials_string.encode('ascii')
        auth_base64 = base64.b64encode(auth_bytes)
        auth_str = auth_base64.decode('ascii')
        auth_url = 'https://accounts.spotify.com/api/token'
        auth_headers = {
            'Authorization': f"Basic {auth_str}",
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        auth_params = {
            'grant_type': 'client_credentials'
        }

        auth_response = requests.post(
            url=auth_url,
            data=auth_params,
            headers=auth_headers
        )

        access_token = auth_response.json()['access_token']
        self.access_token = access_token
        return access_token
    

    def get_headers(self) -> dict:
        if self.access_token is None:
            logger.warning('Spotify access token has not been set')

        bearer_token_headers = {
            'Authorization': f"Bearer {self.access_token}"
        }
        return bearer_token_headers

    
    # couldnt find the actual rate limit so have just set the number of calls to 100
    @on_exception(expo, RateLimitException)
    @limits(calls=100, period=30)
    def get_category_playlists(
        self,
        category_id: str,
        country: str=None,
        limit: int=None,
        offset: int=None
    ) -> requests.models.Response:
        """
        Get a list of Spotify playlists tagged with a particular category.
        https://developer.spotify.com/documentation/web-api/reference/get-a-categories-playlists

        Input:
            category_id (str): The Spotify Category ID for the category (e.g. 'latin')
            country (str): A country: an ISO 3166-1 alpha-2 country code. Provide this parameter to ensure that the category exists for a particular country. (e.g. 'GB')
            limit (int): The maximum number of items to return. Default: 20. Minimum: 1. Maximum: 50.
            offset(int): The index of the first item to return. Default: 0 (the first item). Use with limit to get the next set of items.
        
        Return:
            response (requests.models.Response): response object from the Spotify API
        """
        url = f'https://api.spotify.com/v1/browse/categories/{category_id}/playlists'
        params = {
            'country': country,
            'limit': limit,
            'offset': offset
        }

        # everything below here could be refactored from the methods that call the API into a new generic method.
        # only 3 endpoints defined for now, so little to gain from the abstraction and decoupling is nice
        headers = self.get_headers()

        response = requests.get(
            url=url,
            params=params,
            headers=headers
        )

        # consider control flow to retry or abort in case of error
        if response.status_code != 200:
            logger.error(f'Error {response.status_code}: {response.reason}, {response.json().get("error").get("message")}')

        return response
    
    # couldnt find the actual rate limit so have just set the number of calls to 100
    @on_exception(expo, RateLimitException)
    @limits(calls=100, period=30)
    def get_playlist(
        self,
        playlist_id: str,
        market: str=None,
        fields: str=None,
        additional_types: str=None
    ) -> requests.models.Response:
        """
        Get a playlist owned by a Spotify user.
        https://developer.spotify.com/documentation/web-api/reference/get-playlist

        (not going to paste in the input descriptions again)
        """
        url = f'https://api.spotify.com/v1/playlists/{playlist_id}'

        params = {
            'market': market,
            'fields': fields,
            'additional_types': additional_types
        }

        headers = self.get_headers()

        response = requests.get(
            url=url,
            params=params,
            headers=headers
        )

        # consider control flow to retry or abort in case of error
        if response.status_code != 200:
            logger.error(f'Error {response.status_code}: {response.reason}, {response.json().get("error").get("message")}')

        return response
    
    # couldnt find the actual rate limit so have just set the number of calls to 100
    @on_exception(expo, RateLimitException)
    @limits(calls=100, period=30)
    def get_playlist_items(
        self,
        playlist_id: str,
        market: str=None,
        limit: int=None,
        offset: int=None,
        additional_types: str=None
    ) -> requests.models.Response:
        """
        Get full details of the items of a playlist owned by a Spotify user.
        https://developer.spotify.com/documentation/web-api/reference/get-playlists-tracks
        """
        url = f'https://api.spotify.com/v1/playlists/{playlist_id}/tracks'

        params = {
            'market': market,
            'limit': limit,
            'offset': offset,
            'additional_types': additional_types
        }

        headers = self.get_headers()

        response = requests.get(
            url=url,
            params=params,
            headers=headers
        )

        return response

def get_limit_offset_from_url(url: str) -> tuple[int]:
    parsed_url = urllib.parse.urlparse(url)
    parameters = urllib.parse.parse_qs(parsed_url.query)
    limit = parameters.get('limit')[0]
    offset = parameters.get('offset')[0]
    

    return limit, offset

# default limit to 50 to reduce number of API calls
# dont see any downside to this
def get_all_category_simplified_playlists(client: SpotifyClient, category_id: str) -> list[requests.models.Response]:
    """
    Gets all paginated responses from the get_category_playlists endpoint for the given category_id

    Input:
        client (SpotifyClient): authenticated Spotify client instance
        category_id (str): The Spotify Category ID for the category (e.g. 'latin')
    
    Return:
        responses (list[requests.models.Response]): list of response objects
    """
    responses = []
    offset = 0
    limit = 50
    last_page = False

    while not last_page:
        logger.info(f'Making "Get Category Playlists" GET request for category_id {category_id} (offset={offset})')
        response = client.get_category_playlists(
            category_id=category_id,
            limit=limit,
            offset=offset
        )

        responses.append(response)

        playlists = response.json().get('playlists')
        response_next = playlists.get('next')

        if response_next is None:
            # will cause loop to exit
            last_page = True

        else:
            limit, offset = get_limit_offset_from_url(response_next)
        
    return responses


def get_simplified_playlists_from_responses(responses: list[requests.models.Response]) -> list[dict]:
    """
    Converts a list of responses from get_all_category_playlists() to a list of SimplifiedPlaylistObject
    """
    items = [response.json().get('playlists').get('items') for response in responses]
    playlists = [playlist for item in items for playlist in item]

    return playlists


def get_all_playlists(client: SpotifyClient, simplified_playlists: list[dict]) -> list[requests.models.Response]:
    """
    Gets all detailed playlists from a list of simple playlists

    Input:
        client (SpotifyClient): authenticated Spotify client instance
        simplified_playlists (list[dict]): list of SimplifiedPlayListObjects (as dictionaries)
    
    Return:
        responses (list[requests.models.Response]): list of response objects
    """
    responses = []
    num_playlists = len(simplified_playlists)
    for i, playlist in enumerate(simplified_playlists):
        playlist_id = playlist.get('id')

        logger.info(f'Making "Get Playlist" GET request for playlist_id {playlist_id} ({i+1}/{num_playlists})')
        response = client.get_playlist(playlist_id=playlist_id)
        
        responses.append(response)
    
    return responses


def populate_all_tracks_in_playlists(client: SpotifyClient, playlists: list[dict]) -> list[dict]:
    """
    Given a list of playlists, populates the playlists' tracks items where there are missing tracks
    due to pagination.

    Note: modifies playlists in place

    Input:
        client (SpotifyClient): authenticated Spotify client instance
        playlists (list[dict]): list of PlayListObjects (as dictionaries)
    
    Return:
        playlists (list[dict]): input list of PlayListObjects with populated tracks items
    """
    # loop like this so we dont modify the iterator
    num_playlists = len(playlists)
    for playlist in playlists:
        tracks_next = playlist.get('tracks').get('next')

        if tracks_next is not None:
            playlist_id = playlist.get('id')

            while tracks_next is not None:
                limit, offset = get_limit_offset_from_url(tracks_next)

                logger.info(f'Making "Get Track" GET request for playlist_id {playlist_id} (offset={offset})')
                response = client.get_playlist_items(
                    playlist_id=playlist_id,
                    limit=limit,
                    offset=offset
                )

                tracks = response.json()

                # add new tracks to playlist's tracks items
                tracks_items = tracks.get('items')
                playlist['tracks']['items'] += tracks_items

                # if None, inner loop will exit
                tracks_next = tracks.get('next')

    return playlists
