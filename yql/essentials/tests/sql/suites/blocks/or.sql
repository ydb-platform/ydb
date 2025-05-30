$data = [
    <|x:false,y:false|>,
    <|x:false,y:true|>,
    <|x:true,y:false|>,
    <|x:true,y:true|>,
];

select x or y from as_table($data);
