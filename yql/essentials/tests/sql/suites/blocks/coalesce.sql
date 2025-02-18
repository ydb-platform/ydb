$data1 = [
    <|x:nothing(int32?),y:10|>,
    <|x:just(1),y:10|>,
];

$data2 = [
    <|x:nothing(int32?),y:just(10)|>,
    <|x:just(1),y:just(10)|>,
    <|x:just(1),y:nothing(int32?)|>,
];

select x ?? y from as_table($data1);

select x ?? y from as_table($data2);
