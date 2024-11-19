/* postgres can not */
/* kikimr can not */
use plato;

pragma yt.InferSchema;
pragma yt.InferSchemaTableCountThreshold="100000";

insert into Output with truncate
select * from Output;
