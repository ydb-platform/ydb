use plato;

select key, subkey, value
from (
    select TablePath() as tbl, key, subkey, value
    from concat(Input1, Input2)
)
where tbl = "Input" and value != "";
