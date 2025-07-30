PRAGMA warning('disable', '4510');

SELECT
    YQL::FlatMap(Opaque(Just(Just(AsStruct(1 AS x)))), ($optS) -> (Just(AsStruct($optS.x AS x)))).x
;
