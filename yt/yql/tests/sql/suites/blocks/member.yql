USE plato;

SELECT
    val.a as a,
    <|qq:key,qkrq:"QKRQ"|> as q,
    /* XXX: <AddMember> callable always expands to <AsStruct>. */
    AddMember(val, "k", key) as wik,
    /* XXX: <RemoveMember> callable always expands to <AsStruct>. */
    RemoveMember(val, "x") as wox,
FROM Input;
