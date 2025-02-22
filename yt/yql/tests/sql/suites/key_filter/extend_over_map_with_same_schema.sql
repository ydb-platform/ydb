select * from (
    SELECT "x023" as key, subkey, value FROM plato.Input WHERE key == "023"
    union all
    SELECT "x037" as key, subkey, value FROM plato.Input WHERE key == "037"
    union all
    SELECT "x075" as key, subkey, value FROM plato.Input WHERE key == "075"
    union all
    SELECT "x150" as key, subkey, value FROM plato.Input WHERE key == "150"
) as x
order by key, subkey;