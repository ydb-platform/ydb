CREATE TABLE series (
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Date,
    PRIMARY KEY (series_id)
);

CREATE TABLE seasons (
    series_id Uint64,
    season_id Uint64,
    title Utf8,
    first_aired Date,
    last_aired Date,
    PRIMARY KEY (series_id, season_id)
);

CREATE TABLE episodes (
    series_id Uint64,
    season_id Uint64,
    episode_id Uint64,
    title Utf8,
    air_date Date,
    PRIMARY KEY (series_id, season_id, episode_id)
);

CREATE VIEW view_series WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM series;

CREATE VIEW view_seasons WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM seasons;

CREATE VIEW view_episodes WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM episodes;
