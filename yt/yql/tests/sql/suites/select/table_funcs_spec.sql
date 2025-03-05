/* syntax version 1 */
/* postgres can not */
/* kikimr can not */
use plato;

--insert into Output
select
  TablePath() as table_path,
  TableRecordIndex() as table_rec,
  TableName("foo/bar") as table_name1,
  TableName("baz") as table_name2,
  TableName() as table_name3
from Input
where key = '800'
;
