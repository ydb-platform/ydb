/* postgres can not */
/* syntax version 1 */
-- in v1 substring returns Null as a result for missing value
select
    substring(key, 1, 1) as char,
    substring(value, 1) as tail
from plato.Input;
