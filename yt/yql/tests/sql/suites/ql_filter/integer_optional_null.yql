pragma yt.UseQLFilter;

select *
from plato.Input
where
    (
        a > null
        or b > nothing(Int32?)
        or c is null
    )
    and (c > 0) is null;