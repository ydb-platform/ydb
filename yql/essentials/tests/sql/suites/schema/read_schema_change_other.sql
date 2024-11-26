/* custom error:It is forbidden to specify the column '_other'*/
select * from plato.Input with schema Struct<_other:Yson>;
