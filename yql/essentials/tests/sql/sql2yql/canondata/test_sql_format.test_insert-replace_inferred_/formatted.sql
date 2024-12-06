/* postgres can not */
/* kikimr can not */
USE plato;
PRAGMA yt.InferSchema;
PRAGMA yt.InferSchemaTableCountThreshold = "100000";

INSERT INTO Output WITH truncate
SELECT
    *
FROM
    Output
;
