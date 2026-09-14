select
    key,
    subkey,
    if (1 > 0, key, subkey) as same_as_key,
    if (0 > 100, key, subkey) as same_as_subkey,
    if (10 > 0, 'scalar1 selected', key) as sc1,
    if (0 > 11, subkey, 'scalar2 selected') as sc2,


    if(key > subkey, key, subkey) as max_of_key_subkey,
    if(key > subkey, key, 'subkey is greater') as sc3,
    if(subkey >= key, 'subkey is greater or eq', key) as sc4,
    if(subkey >= key, 'subkey is greater or eq', 'key is greater') as sc5,

from plato.Input
order by key, subkey;
