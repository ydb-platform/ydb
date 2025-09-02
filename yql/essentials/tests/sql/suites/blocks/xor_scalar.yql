$data = [
    <|x:false,y:false|>,
    <|x:false,y:true|>,
    <|x:true,y:false|>,
    <|x:true,y:true|>,
];

select x,y,x xor Opaque(false),x xor Opaque(true) from as_table($data);
select x,y,Opaque(false) xor y,Opaque(true) xor y from as_table($data);
