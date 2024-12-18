/* custom error:Cannot infer schema for table Input2, first 1 row(s) has no columns*/
PRAGMA yt.InferSchemaTableCountThreshold = '0';

SELECT
    *
FROM
    plato.range(``, Input1, Input3) WITH inferscheme
;
