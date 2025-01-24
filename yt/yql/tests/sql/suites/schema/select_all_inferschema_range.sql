pragma yt.InferSchemaTableCountThreshold="0";
select * from plato.range(``, Input1, Input3) with inferscheme;
