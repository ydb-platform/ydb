$data = [
    <|x:false,y:false|>,
    <|x:false,y:true|>,
    <|x:true,y:false|>,
    <|x:true,y:true|>,
];

select x and y from as_table($data);
