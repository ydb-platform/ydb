/* postgres can not */
/* dq can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;
pragma yt.MaxRowWeight='32M';

insert into Input
select key, subkey, value from Input;

