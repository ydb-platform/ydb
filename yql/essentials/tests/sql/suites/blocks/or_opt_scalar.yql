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

select x,y,Opaque(false) or y,Opaque(true) or y,Opaque(Nothing(bool?)) or y from as_table($data);
select x,y,x or Opaque(false),x or Opaque(true),x or Opaque(Nothing(bool?)) from as_table($data);
