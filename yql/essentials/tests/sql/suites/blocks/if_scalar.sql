$data = [
    <|x:false,y:1,z:2|>,
    <|x:true,y:3,z:4|>,
];

select x,if(Opaque(false),y,z),if(Opaque(true),y,z),if(x,Opaque(5),z),if(x,5,Opaque(6)) from as_table($data);
select x,if(Opaque(false),Opaque(5),z),if(Opaque(true),y,Opaque(6)) from as_table($data);
