$f = ($value) -> {
   return <|
       "plain":$value,
       "missing":nothing(typeof($value)),
       "opt_1+":just($value),
       "opt_1-":nothing(optionaltype(typeof($value))),
       "opt_2++":just(just($value)),
       "opt_2+-":just(nothing(optionaltype(typeof($value)))),
       "opt_2--":nothing(optionaltype(optionaltype(typeof($value)))),
       "tuple":($value,),
    |>;
};

$data = <|
    "pgint2": pgint2("1"),
    "pgint4": pgint2("2"),
    "pgint8": pgint2("3"),
    "pgfloat4": pgfloat4("4"),
    "pgfloat8": pgfloat8("5"),
    "pgtext": pgtext("a"),
    "pgvarchar": pgtext("b"),
    "pgbytea": pgbytea("c"),
    "pgcstring": pgbytea("d"),
    "pgdate": pgdate("2001-02-03"),
    "pgname": pgname("e"),
|>;

evaluate for $name in StructMembers($data) do begin 
select * from (
    select $f($data.$name)
) flatten columns into result $name;
end do;

