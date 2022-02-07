import asyncio

import ydb
import os

# Query to insert data into tables from session_pool_example.py
import example_data

FillDataQuery = """PRAGMA TablePathPrefix("{}");

DECLARE $seriesData AS List<Struct<
    series_id: Uint64,
    title: Utf8,
    series_info: Utf8,
    release_date: String>>;

DECLARE $seasonsData AS List<Struct<
    series_id: Uint64,
    season_id: Uint64,
    title: Utf8,
    first_aired: String,
    last_aired: String>>;

DECLARE $episodesData AS List<Struct<
    series_id: Uint64,
    season_id: Uint64,
    episode_id: Uint64,
    title: Utf8,
    air_date: String>>;

REPLACE INTO series
SELECT
    series_id,
    title,
    series_info,
    CAST(release_date AS Date) AS release_date
FROM AS_TABLE($seriesData);

REPLACE INTO seasons
SELECT
    series_id,
    season_id,
    title,
    CAST(first_aired AS Date) AS first_aired,
    CAST(last_aired AS Date) AS last_aired
FROM AS_TABLE($seasonsData);

REPLACE INTO episodes
SELECT
    series_id,
    season_id,
    episode_id,
    title,
    CAST(air_date AS Date) AS air_date
FROM AS_TABLE($episodesData);
"""


async def create_tables():  # Creating tables with session_pool_example
    from session_pool_example import main

    await main()


async def main():
    await create_tables()
    endpoint = os.getenv("YDB_ENDPOINT")
    database = os.getenv("YDB_DATABASE")
    driver = ydb.aio.Driver(endpoint=endpoint, database=database)
    pool = ydb.aio.SessionPool(driver, size=10)  # Max number of available session
    session = await pool.acquire()
    prepared_query = await session.prepare(FillDataQuery.format(database))
    await session.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query,
        {
            "$seriesData": example_data.get_series_data(),
            "$seasonsData": example_data.get_seasons_data(),
            "$episodesData": example_data.get_episodes_data(),
        },
        commit_tx=True,
    )
    await pool.release(session)
    await driver.stop()
    await pool.stop()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
