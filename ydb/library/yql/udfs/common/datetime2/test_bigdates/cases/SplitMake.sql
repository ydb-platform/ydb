/* syntax version 1 */
select
    DateTime::MakeDate32(DateTime::Split(d)) as d
from Input;
