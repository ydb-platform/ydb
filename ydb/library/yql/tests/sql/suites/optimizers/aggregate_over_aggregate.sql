/* syntax version 1 */

use plato;

select distinct * from Input group by value, subkey, key order by subkey;
