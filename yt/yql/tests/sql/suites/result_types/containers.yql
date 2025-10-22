$f = ($value) -> {
   return <|
       "plain":$value,
       "opt_1+":just($value),
       "opt_1-":nothing(optionaltype(typeof($value))),
       "opt_2++":just(just($value)),
       "opt_2+-":just(nothing(optionaltype(typeof($value)))),
       "opt_2--":nothing(optionaltype(optionaltype(typeof($value)))),
       "tuple":($value,),
    |>;
};

$data = <|
    "list0": ListCreate(int32),
    "list1": [1],
    "list2": [1,2],
    "tuple0": (),
    "tuple1": (1,),
    "tuple2": (1,"foo"),
    "struct0": <||>,
    "struct1": <|a:1|>,
    "struct2": <|a:1,b:"foo"|>,
    "vartuple1": Variant(1,"0",Variant<Int32>),
    "vartuple2a": Variant(1,"0",Variant<Int32,String>),
    "vartuple2b": Variant("foo","1",Variant<Int32,String>),
    "varstruct1": Variant(1,"a",Variant<a:Int32>),
    "varstruct2a": Variant(1,"a",Variant<a:Int32,b:String>),
    "varstruct2b": Variant("foo","b",Variant<a:Int32,b:String>),
    "enum1": AsEnum("foo"),
    "enum2a": Enum("foo",Enum<"foo","bar">),
    "enum2b": Enum("bar",Enum<"foo","bar">),
    "tag": AsTagged(1,'foo'),
    "dict0": DictCreate(int32, string),
    "dict1": {1:"foo"},
    "set0": SetCreate(int32),
    "set1": {1},
|>;

evaluate for $name in StructMembers($data) do begin 
select * from (
    select $f($data.$name)
) flatten columns into result $name;
end do;
