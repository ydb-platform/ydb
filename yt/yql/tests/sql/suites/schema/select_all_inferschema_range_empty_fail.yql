/* custom error:Cannot infer schema for table Input2, first 1 row(s) has no columns*/
pragma yt.InferSchemaTableCountThreshold="0";
select * from plato.range(``, Input1, Input3) with inferscheme;
