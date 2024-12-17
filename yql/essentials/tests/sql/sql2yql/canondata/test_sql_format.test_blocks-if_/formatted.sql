SELECT
    key,
    subkey,
    if(1 > 0, key, subkey) AS same_as_key,
    if(0 > 100, key, subkey) AS same_as_subkey,
    if(10 > 0, 'scalar1 selected', key) AS sc1,
    if(0 > 11, subkey, 'scalar2 selected') AS sc2,
    if(key > subkey, key, subkey) AS max_of_key_subkey,
    if(key > subkey, key, 'subkey is greater') AS sc3,
    if(subkey >= key, 'subkey is greater or eq', key) AS sc4,
    if(subkey >= key, 'subkey is greater or eq', 'key is greater') AS sc5,
FROM
    plato.Input
ORDER BY
    key,
    subkey
;
