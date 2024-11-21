USE plato;

-- Should not generate single MrFill for all content
INSERT INTO Output
SELECT * from(
    select key, sum(cast(subkey as Uint32)) as value
    from Input group by key

    union all

    select "" as key, sum(cast(subkey as Uint32)) as value
    from Input-- group by key
);
