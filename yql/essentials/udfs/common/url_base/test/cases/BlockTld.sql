/* syntax version 1 */
PRAGMA UseBlocks;
SELECT
    Url::GetTLD(value) AS tld,
    Url::IsKnownTLD(value) AS known,
    Url::IsWellKnownTLD(value) AS well_known
FROM Input;
