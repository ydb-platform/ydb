$data = [
    <|x:false,y:false|>,
    <|x:false,y:true|>,
    <|x:true,y:false|>,
    <|x:true,y:true|>,
];

select x,y,x and Opaque(false),x and Opaque(true) from as_table($data);
select x,y,Opaque(false) and y,Opaque(true) and y from as_table($data);
