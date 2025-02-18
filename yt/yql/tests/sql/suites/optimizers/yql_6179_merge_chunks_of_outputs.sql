/* syntax version 1 */
/* postgres can not */
/* kikimr can not - yt pragma */

PRAGMA yt.MinPublishedAvgChunkSize="0";
PRAGMA yt.MinTempAvgChunkSize="0";

USE plato;

$i = (select subkey as s from Input where key = "112" limit 1);
$j = (select subkey as s from Input where key = "113" limit 1);

select * from Input where cast(TableRecordIndex() as String) == $i or
                          cast(TableRecordIndex() as String) == $j;

