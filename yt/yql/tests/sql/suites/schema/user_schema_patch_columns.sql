/* syntax version 1 */
use plato;
pragma yt.InferSchema;

select ListSort(DictItems(_other)) as _other,key,subkey from Input with columns Struct<key:Utf8, subkey:String?>;