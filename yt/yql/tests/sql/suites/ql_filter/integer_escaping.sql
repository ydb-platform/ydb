pragma yt.UseQLFilter;
PRAGMA yt.UseSkiff='false';

select 
    `escaping []\``,
    `escaping []\`` as renamed
from plato.Input
where `escaping []\`` > 5;
