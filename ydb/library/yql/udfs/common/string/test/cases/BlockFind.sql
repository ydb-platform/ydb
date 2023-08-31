/* syntax version 1 */
pragma UseBlocks;
SELECT
    value,
    String::LevensteinDistance(value, "as") AS levenstein
FROM Input;
