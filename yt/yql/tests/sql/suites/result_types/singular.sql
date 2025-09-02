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
    "void": Void(),
    "null": null,
    "emptylist": [],
    "emptydict": {},
|>;

evaluate for $name in StructMembers($data) do begin 
select * from (
    select $f($data.$name)
) flatten columns into result $name;
end do;
