$data = [
    <|x:false,y:false|>,
    <|x:false,y:true|>,
    <|x:true,y:false|>,
    <|x:true,y:true|>,
];

select x,y,x or Opaque(false),x or Opaque(true) from as_table($data);
select x,y,Opaque(false) or y,Opaque(true) or y from as_table($data);
