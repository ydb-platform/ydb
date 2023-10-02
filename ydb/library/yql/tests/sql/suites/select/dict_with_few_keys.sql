/* syntax version 1 */
/* postgres can not */
USE plato;

$dict = (
SELECT
  AsDict(
    AsTuple("key", cast(key as uint32) ?? 0),
    AsTuple("sk", cast(subkey as uint32) ?? 1),
    AsTuple("str",  Cast(ByteAt(value, 0) as uint32) ?? 256)
  ) as dd
FROM Input);

--INSERT INTO Output
SELECT dd['key'] as key, dd['str'] as zz from $dict as d ORDER BY key, zz
