pragma yt.UseQLFilter;

select 
    `escaping []\``,
    `escaping []\`` as renamed
from plato.Input
where `escaping []\`` > 5;
