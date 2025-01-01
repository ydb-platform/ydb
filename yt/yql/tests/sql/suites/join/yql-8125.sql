use plato;

PRAGMA yt.JoinCollectColumnarStatistics="async";

INSERT INTO @yang_ids
SELECT
    *
    from Input
    where subkey <= "3"
    LIMIT 100;
commit;

INSERT INTO @yang_ids
    SELECT
        *
        from Input as j
        LEFT ONLY JOIN @yang_ids
        USING(key);
commit;

INSERT INTO @yang_ids
    SELECT
        *
        from Input as j
        LEFT ONLY JOIN @yang_ids
        USING(key);
commit;

select * from @yang_ids;
