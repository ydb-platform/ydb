/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

evaluate for $i in ["1", "2", "3"] do begin
    select * from Input where subkey = $i;
end do;
