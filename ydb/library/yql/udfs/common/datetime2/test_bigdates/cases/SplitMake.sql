/* syntax version 1 */
select
    DateTime::MakeDate32(DateTime::Split64(d)) as d
from Input;
