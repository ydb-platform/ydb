pragma yt.UseQLFilter;
PRAGMA yt.UseSkiff='false';

select c
from plato.Input
where 
    c > -1
    AND a < 18446744073709551615;
