/* postgres can not */

USE plato;

$r4_20  = (SELECT optkey FROM Input WHERE optkey BETWEEN 4  AND 20);
$r6_20  = (SELECT key    FROM Input WHERE optkey BETWEEN 6  AND 20);
$r8_20  = (SELECT key    FROM Input WHERE optkey BETWEEN 8  AND 20);
$r10_20 = (SELECT optkey FROM Input WHERE optkey BETWEEN 10 AND 20);

$r1_16  = (SELECT optkey FROM Input WHERE optkey BETWEEN 1  AND 16);
$r1_12  = (SELECT key    FROM Input WHERE optkey BETWEEN 1  AND 12);

SELECT key FROM Input WHERE
   (key + 1)      IN         $r4_20  AND      -- key = [3, 19]       -- 2 joinable
   optkey         IN         $r6_20  AND      -- key = [6, 19]

   key            IN /*+ COMPACT() */ $r8_20  AND      -- key = [8, 19]       -- 1 nonjoinable (due to COMPACT)

   (optkey + 3)   IN         $r10_20 AND      -- key = [8, 17]       -- 3 joinable
   (key + 4)      NOT IN      $r1_12 AND      -- key = [9, 17]
   key            IN         $r10_20 AND      -- key = [10, 17]

   (optkey      IN $r1_16 OR key IN $r1_16) AND   -- key = [10, 16]  -- 1 nonjoinable (not SqlIn)

   (key - 1)    IN $r1_12          AND            -- key = [10, 13]  -- 2 joinable
   (key - 3)    NOT IN $r8_20                     -- key = [10]

