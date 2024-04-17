USE plato;
/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
pragma UseBlocks;

SELECT
    val.a as a,
    AddMember(val, "c", key) as wic,
FROM Input;
