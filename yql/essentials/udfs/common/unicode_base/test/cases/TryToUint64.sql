/* syntax version 1 */
SELECT
    value as value,
    Unicode::TryToUint64(value, 10),
    Unicode::TryToUint64(value, 1),
    Unicode::TryToUint64(value, 4),
    Unicode::TryToUint64(value, 8),
    Unicode::TryToUint64(value, 16)
From Input
