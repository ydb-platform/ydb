select
    value,
    case when
    value like "abc"
    then "true"
    else "false"
    end as is_abc
from plato.Input
order by is_abc desc, value asc;