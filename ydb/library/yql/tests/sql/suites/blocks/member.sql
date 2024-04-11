USE plato;
/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
pragma UseBlocks;

SELECT
    val.a as a,
    RemoveMember(val, "x") as wox,
FROM Input;
