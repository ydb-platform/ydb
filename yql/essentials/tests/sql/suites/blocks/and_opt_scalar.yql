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

select x,y,Opaque(false) and y,Opaque(true) and y,Opaque(Nothing(bool?)) and y from as_table($data);
select x,y,x and Opaque(false),x and Opaque(true),x and Opaque(Nothing(bool?)) from as_table($data);
