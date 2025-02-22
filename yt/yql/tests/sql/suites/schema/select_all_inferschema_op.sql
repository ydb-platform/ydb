/* postgres can not */
/* kikimr can not */
pragma yt.InferSchemaTableCountThreshold="0";

select * from plato.Input with inferscheme;
