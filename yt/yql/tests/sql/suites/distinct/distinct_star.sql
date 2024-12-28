/* syntax version 1 */

use plato;

select distinct * from Input2 order by key, subkey;

select distinct * without subkey from Input2 order by key, value;

select distinct a.*, TableName() as tn, without subkey from Input2 as a order by key, value;
