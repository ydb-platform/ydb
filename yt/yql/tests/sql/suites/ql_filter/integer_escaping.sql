/* yt can not */
/* waiting for update YT-24048 */

pragma yt.UseQLFilter;

select 
    `escaping []\``,
    `escaping []\`` as renamed
from plato.Input
where `escaping []\`` > 5;
