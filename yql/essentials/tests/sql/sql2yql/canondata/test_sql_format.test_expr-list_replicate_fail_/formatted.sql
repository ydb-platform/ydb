/* postgres can not */
/* custom error:Second argument in ListReplicate = 18446744073709551615 exceeds maximum value = 4294967296*/
SELECT
    ListReplicate(1, -1)
;
