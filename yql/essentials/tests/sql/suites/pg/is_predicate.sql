--!syntax_pg
select c,
    c is true as "true", c is not true  as "~true",
    c is false as "false", c is not false as "~false",
    c is unknown as "unk", c is not unknown as "~unk"
from (values (true), (false), (null)) as t(c)

