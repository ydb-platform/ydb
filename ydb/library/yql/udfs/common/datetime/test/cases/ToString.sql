/* syntax version 0 */
SELECT DateTime::ToString(COALESCE(CAST(value AS Uint64), CAST(0 as Uint64))) FROM Input;
