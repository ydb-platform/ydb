PRAGMA yt.InferSchemaTableCountThreshold = "0";

SELECT
    *
FROM plato.range(``, Input1, Input3)
    WITH inferscheme;
