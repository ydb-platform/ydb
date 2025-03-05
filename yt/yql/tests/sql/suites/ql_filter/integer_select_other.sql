/* yt can not */
/* waiting for update YT-24048 */

pragma yt.UseQLFilter;

select b
from plato.Input
where
    a > 5;