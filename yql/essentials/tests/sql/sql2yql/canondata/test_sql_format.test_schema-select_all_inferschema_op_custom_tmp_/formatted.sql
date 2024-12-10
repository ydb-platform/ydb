/* postgres can not */
/* kikimr can not */
PRAGMA yt.InferSchemaTableCountThreshold = "0";
PRAGMA yt.TmpFolder = "custom";

SELECT
    *
FROM
    plato.Input WITH inferscheme
;
