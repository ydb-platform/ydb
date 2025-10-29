$data = [
    <|x:false,y:false|>,
    <|x:false,y:true|>,
    <|x:false,y:null|>,
    <|x:true,y:false|>,
    <|x:true,y:true|>,
    <|x:true,y:null|>,
    <|x:null,y:false|>,
    <|x:null,y:true|>,
    <|x:null,y:null|>,
];

select x,y,Opaque(false) xor y,Opaque(true) xor y,Opaque(Nothing(bool?)) xor y from as_table($data);
select x,y,x xor Opaque(false),x xor Opaque(true),x xor Opaque(Nothing(bool?)) from as_table($data);
