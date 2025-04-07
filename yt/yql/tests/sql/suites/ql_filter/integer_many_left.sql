pragma yt.UseQLFilter;

select a, c, d, e
from plato.Input
where
    a > 5
    and
    c > 5
    and
    d > 5
    and
    e > 5;