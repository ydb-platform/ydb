/* postgres can not */
USE plato;

SELECT key, ROW_NUMBER() OVER w AS row_num
FROM (select * from Input where key != "020")
WINDOW w AS ();
