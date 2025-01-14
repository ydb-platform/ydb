/* syntax version 1 */
use plato;
pragma yt.InferSchema;

select * from Input with schema Struct<key:String>;