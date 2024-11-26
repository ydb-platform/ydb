/* postgres can not */
/* kikimr can not */
PRAGMA yt.InferSchemaTableCountThreshold = "0";

SELECT
    *
FROM plato.Input
    WITH inferscheme;
