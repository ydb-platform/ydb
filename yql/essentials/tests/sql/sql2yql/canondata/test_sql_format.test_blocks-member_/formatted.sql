USE plato;

SELECT
    val.a AS a,
    <|qq: key, qkrq: 'QKRQ'|> AS q,

    /* XXX: <AddMember> callable always expands to <AsStruct>. */
    AddMember(val, 'k', key) AS wik,

    /* XXX: <RemoveMember> callable always expands to <AsStruct>. */
    RemoveMember(val, 'x') AS wox,
FROM
    Input
;
