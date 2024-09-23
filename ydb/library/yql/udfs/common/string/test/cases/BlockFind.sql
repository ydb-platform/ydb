/* syntax version 1 */
pragma UseBlocks;
SELECT
    value,
    String::Contains(value, "as") AS contains,
    String::LevensteinDistance(value, "as") AS levenstein
FROM Input;
