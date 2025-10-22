/* postgres can not */
USE plato;

$input=(SELECT
  Cast(key as int32) / 100 as key_hundred,
  AsStruct(
    Cast(key as int32) as key,
    Cast(subkey as int32) as subkey
  ) as `struct`,
  value
FROM Input as inSrc);

--INSERT INTO Output
SELECT
  key_hundred as a_part,
  `struct`.key - lead(outSrc.`struct`.key, 1) over w as keyDiff,
  value
FROM $input as outSrc
WINDOW w as (partition by key_hundred ORDER BY `struct`.key, value)
ORDER by a_part, value
;
