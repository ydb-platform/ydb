/* yt can not */
/* waiting for update YT-24048 */

pragma yt.UseQLFilter;

select c
from plato.Input
where 
    c > -1
    AND a < 18446744073709551615;
