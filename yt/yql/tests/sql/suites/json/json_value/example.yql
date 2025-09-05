/* syntax version 1 */
/* postgres can not */
USE plato;

-- These examples are taken from [ISO/IEC TR 19075-6:2017] standard (https://www.iso.org/standard/67367.html)
SELECT T.K,
    JSON_VALUE (T.J, 'lax $.who') AS Who
FROM T;

SELECT T.K,
    JSON_VALUE (T.J, 'lax $.who') AS Who,
    JSON_VALUE (T.J, 'lax $.where'
    NULL ON EMPTY) AS Nali
FROM T;

SELECT T.K,
    JSON_VALUE (T.J, 'strict $.who') AS Who,
    JSON_VALUE (T.J, 'strict $.where'
    DEFAULT 'no where there' ON ERROR )
    AS Nali
FROM T;

SELECT T.K,
    JSON_VALUE (T.J, 'lax $.who') AS Who,
    JSON_VALUE (T.J, 'lax $.where' NULL ON EMPTY) AS Nali,
    JSON_VALUE (T.J, 'lax $.friends.name' NULL ON EMPTY
                                        DEFAULT '*** error ***' ON ERROR)
    AS Friend
FROM T;

SELECT T.K,
    JSON_VALUE (T.J, 'strict $.who') AS Who,
    JSON_VALUE (T.J, 'strict $.where' NULL ON EMPTY NULL ON ERROR) AS Nali,

    -- NOTE: output for this particular column differs with standard.
    -- For row with T.K = 106 and T.J = { "who": "Louise", "where": "Iana" } output is "*** error ***", not NULL.
    -- This is because answer in standard (NULL) is not correct. Query "strict $.friends[*].name" is executed in strict mode
    -- where all structural errors are "hard" errors. Row with T.K = 106 does not have "friends" key in T.J. This is structural error
    -- and the result of JSON_VALUE must tolerate ON ERROR section which specifies to return "*** error ***" string.
    --
    -- We can check this in PostgreSQL too (at the moment of writing PostgreSQL does not support JSON_VALUE function so we use jsonb_path_query):
    -- postgres=# select * from jsonb_path_query('{ "who": "Louise", "where": "Iana" }', 'strict $.friends[*].name');
    -- ERROR:  JSON object does not contain key "friends"
    -- PostgreSQL shows us that hard error has happened, as expected.
    JSON_VALUE (T.J, 'strict $.friends[*].name' NULL ON EMPTY
                                            DEFAULT '*** error ***' ON ERROR)
    AS Friend
FROM T;

SELECT T.K,
    JSON_VALUE (T.J, 'lax $.who') AS Who,
    -- NOTE: In the original example INTEGER type was used. YQL does not have INTEGER type, Int64 was used instead
    JSON_VALUE (T.J, 'lax $.friends[0].rank' RETURNING Int64 NULL ON EMPTY)
    AS Rank
FROM T;
