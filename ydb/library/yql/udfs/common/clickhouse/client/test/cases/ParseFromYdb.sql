/* syntax version 1 */
$callable = ($input)->{
    $parse = YQL::Udf(AsAtom("ClickHouseClient.ParseFromYdb"), Void(), Struct<'release_date':Uint64?,'series_id':Uint64?,'series_info':String?,'title':Utf8?>);
    return $parse(YQL::Map($input, ($i)->($i.value)) )
};

select * from ( process Input using $callable(TableRows()));
