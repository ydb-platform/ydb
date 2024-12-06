/* postgres can not */
/* kikimr can not */
USE plato;
PRAGMA yt.InferSchema;
PRAGMA yt.InferSchemaTableCountThreshold = "0";

INSERT INTO Output WITH truncate
SELECT
    *
FROM
    Output
;
