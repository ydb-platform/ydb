/* postgres can not */
$id = ($x)->{
    $f = Yql::Callable(CallableType(0, TypeOf($x)), ()->{return $x});
    return $f();
};

$vt1 = ParseType("Variant<a:Int32,b:Uint32?>");
$vt2 = ParseType("Variant<b:Int64,c:Uint32>");

select AsTuple(
    Variant(1,"a",$vt1) == Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) == Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) == Variant($id(2u),"b",$vt2),
    Variant(1,"b",$vt1) == Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) == Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) == Variant($id(1u),"b",$vt2)
);

select AsTuple(
    Variant(1,"a",$vt1) != Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) != Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) != Variant($id(2u),"b",$vt2),
    Variant(1,"b",$vt1) != Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) != Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) != Variant($id(1u),"b",$vt2)
);

select AsTuple(
    Variant(1,"a",$vt1) < Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) < Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) < Variant($id(2u),"b",$vt2),
    Variant(1,"b",$vt1) < Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) < Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) < Variant($id(1u),"b",$vt2)
);

select AsTuple(
    Variant(1,"a",$vt1) <= Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) <= Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) <= Variant($id(2u),"b",$vt2),
    Variant(1,"b",$vt1) <= Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) <= Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) <= Variant($id(1u),"b",$vt2)
);

select AsTuple(
    Variant(1,"a",$vt1) > Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) > Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) > Variant($id(2u),"b",$vt2),
    Variant(1,"b",$vt1) > Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) > Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) > Variant($id(1u),"b",$vt2)
);

select AsTuple(
    Variant(1,"a",$vt1) >= Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) >= Variant($id(1u),"b",$vt2),
    Variant(1,"b",$vt1) >= Variant($id(2u),"b",$vt2),
    Variant(1,"b",$vt1) >= Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) >= Variant($id(1u),"c",$vt2),
    Variant(1u/0u,"b",$vt1) >= Variant($id(1u),"b",$vt2)
);

