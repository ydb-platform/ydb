/* postgres can not */
$energy = ($m,$v)->{
    return AsTagged(Untag($m,"G") * Untag($v,"V") * Untag($v,"V") / 2, "E");
};

select $energy(AsTagged(5.0,"G"), AsTagged(10.0,"V"));
