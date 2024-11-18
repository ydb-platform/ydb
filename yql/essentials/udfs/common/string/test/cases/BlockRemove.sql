/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
PRAGMA UseBlocks;

SELECT
    value,
    String::RemoveAll(value, "as") AS all,
    String::RemoveFirst(value, "a") AS first,
    String::RemoveLast(value, "a") AS last,
    String::RemoveFirst(value, "as") AS first2,
    String::RemoveLast(value, "as") AS last2,
    String::RemoveFirst(value, "") AS first3,
    String::RemoveLast(value, "") AS last3,
    String::RemoveAll(value, "`") AS hwruall,
    String::RemoveFirst(value, "`") AS hwrufirst,
    String::RemoveLast(value, "`") AS hwrulast,
FROM Input;
