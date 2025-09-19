/* postgres can not */
$id = ($x)->{ 
    $f = Yql::Callable(CallableType(0, TypeOf($x)), ()->{return $x});
    return $f();
};

$vt1 = ParseType("Variant<Int32,Uint32?>");
$vt2 = ParseType("Variant<Int64,Uint32>");

select AsTuple(
    Variant(1,"0",$vt1) < Variant($id(1u),"0",$vt2),
    Variant(1,"0",$vt1) < Variant(2u,"0",$vt2),
    Variant(1,"0",$vt1) < Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) < Variant($id(1u),"1",$vt2),
    Variant(1,"1",$vt1) < Variant(2u,"1",$vt2),
    Variant(2,"1",$vt1) < Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) < Variant($id(1u),"0",$vt2),
    Variant(2u/0u,"1",$vt1) < Variant(1u,"1",$vt2)
);                   

select AsTuple(
    Variant(1,"0",$vt1) <= Variant($id(1u),"0",$vt2),
    Variant(1,"0",$vt1) <= Variant(2u,"0",$vt2),
    Variant(1,"0",$vt1) <= Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) <= Variant($id(1u),"1",$vt2),
    Variant(1,"1",$vt1) <= Variant(2u,"1",$vt2),
    Variant(2,"1",$vt1) <= Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) <= Variant($id(1u),"0",$vt2),
    Variant(2u/0u,"1",$vt1) <= Variant(1u,"1",$vt2)
);

select AsTuple(
    Variant(1,"0",$vt1) == Variant($id(1u),"0",$vt2),
    Variant(1,"0",$vt1) == Variant(2u,"0",$vt2),
    Variant(1,"0",$vt1) == Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) == Variant($id(1u),"1",$vt2),
    Variant(1,"1",$vt1) == Variant(2u,"1",$vt2),
    Variant(2,"1",$vt1) == Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) == Variant($id(1u),"0",$vt2),
    Variant(2u/0u,"1",$vt1) == Variant(1u,"1",$vt2)
);

select AsTuple(
    Variant(1,"0",$vt1) != Variant($id(1u),"0",$vt2),
    Variant(1,"0",$vt1) != Variant(2u,"0",$vt2),
    Variant(1,"0",$vt1) != Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) != Variant($id(1u),"1",$vt2),
    Variant(1,"1",$vt1) != Variant(2u,"1",$vt2),
    Variant(2,"1",$vt1) != Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) != Variant($id(1u),"0",$vt2),
    Variant(2u/0u,"1",$vt1) != Variant(1u,"1",$vt2)
);

select AsTuple(
    Variant(1,"0",$vt1) > Variant($id(1u),"0",$vt2),
    Variant(1,"0",$vt1) > Variant(2u,"0",$vt2),
    Variant(1,"0",$vt1) > Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) > Variant($id(1u),"1",$vt2),
    Variant(1,"1",$vt1) > Variant(2u,"1",$vt2),
    Variant(2,"1",$vt1) > Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) > Variant($id(1u),"0",$vt2),
    Variant(2u/0u,"1",$vt1) > Variant(1u,"1",$vt2)
);

select AsTuple(
    Variant(1,"0",$vt1) >= Variant($id(1u),"0",$vt2),
    Variant(1,"0",$vt1) >= Variant(2u,"0",$vt2),
    Variant(1,"0",$vt1) >= Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) >= Variant($id(1u),"1",$vt2),
    Variant(1,"1",$vt1) >= Variant(2u,"1",$vt2),
    Variant(2,"1",$vt1) >= Variant(1u,"1",$vt2),
    Variant(1,"1",$vt1) >= Variant($id(1u),"0",$vt2),
    Variant(2u/0u,"1",$vt1) >= Variant(1u,"1",$vt2)
);
