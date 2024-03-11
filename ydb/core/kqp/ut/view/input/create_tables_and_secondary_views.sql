CREATE TABLE `/Root/series` (
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Date,
    PRIMARY KEY (series_id)
);

CREATE TABLE `/Root/seasons` (
    series_id Uint64,
    season_id Uint64,
    title Utf8,
    first_aired Date,
    last_aired Date,
    PRIMARY KEY (series_id, season_id)
);

CREATE TABLE `/Root/episodes` (
    series_id Uint64,
    season_id Uint64,
    episode_id Uint64,
    title Utf8,
    air_date Date,
    PRIMARY KEY (series_id, season_id, episode_id)
);

CREATE VIEW `/Root/view_series` WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM `/Root/series`;

CREATE VIEW `/Root/view_seasons` WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM `/Root/seasons`;

CREATE VIEW `/Root/view_episodes` WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM `/Root/episodes`;
