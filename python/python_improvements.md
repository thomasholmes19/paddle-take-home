# python-improvements

- Add error handling and retry attempts - could be enabled by deploying the script to a scheduler like Airflow or Prefect. Any time a `.get()` method is called, if the specified attribute isn't found this method returns None - can be very useful to catch errors at runtime

- Add testing, both unit and integration tests on the script and data validity tests on the output tables (e.g. primary key uniqueness)

- To track more categories, multiple `extract()` calls could be made with different category IDs. We would then just have to merge the resulting lists and pass that to `load()`. Would probably want to manually impute the category ID into each list before/during merging so that we have that available as a column.

- To track changes in number of playlists, popularity of tracks, etc., we would want to pass the current timestamp to the relevant tables so that we can track these stats at different datetimes. We would also want to store different copies of these tables (e.g. saving as `category_playlists_records_{current_timestamp}`), instead of overwriting as the script does currently

- Store final tables in something like an S3 bucket instead of storing locally

- By using the `snapshot_id` from 'Get Category's Playlists' we can reduce calls to 'Get Playlist' - if the `snapshot_id` is the same as what we already have for a given `playlist_id`, that means the playlist has not changed since our last load and we do not need to re-load it. We could compare `snapshot_id`s by using a key-value store either locally or on something like Redis, or we could query whatever database we eventually store this data in.

- Use tighter rate limits in decorators of API call methods. I couldn't find the exact rate limit nor could I trigger a 429 error with a single thread, but if I were to deploy this script to a production environment I would spend time finding the rate limit. According to the docs, when the rate limit is exceeded a 'Retry-After' field is included in the response, which would be useful but I haven't accounted for this in the current script.

- Remove the transformation step and turn this script from an ETL pipeline to an EL(T) pipeline. We can load all raw data from the API into the warehouse (or an intermediary S3-like service) and use a tool like dbt to build our tables. This way we have all data available to us from when we started loading.