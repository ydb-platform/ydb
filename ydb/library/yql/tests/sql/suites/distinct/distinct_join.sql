/* syntax version 1 */

use plato;

select distinct * from (select Unwrap(cast(key as Int32)) as key, value from Input2) as a 
join                   (select Just(1ul) as key, 123 as subkey) as b
using(key) order by value;

