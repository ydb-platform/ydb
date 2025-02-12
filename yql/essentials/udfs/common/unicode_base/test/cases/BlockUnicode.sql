/* syntax version 1 */

pragma UseBlocks;

SELECT
    value AS value,
    Unicode::IsUtf(value) AS is,
    Unicode::GetLength(value) AS length,
    Unicode::Substring(value, 1) AS one_end_substring,
    Unicode::Substring(value, 0, 2) AS two_end_substring,
    Unicode::RemoveAll(value, "\xD1\x87пr") AS remove_all,
    Unicode::LevensteinDistance(value, value || Unicode::Substring(value, 0, 5)) AS levenstein,
    Unicode::Reverse(value) AS reverse,
    Unicode::Find(value, "ет"u) AS find,
    Unicode::RFind(value, "ет"u) AS rfind,
    Unicode::Find(value, "ет"u, 7ul) AS find_from,
    Unicode::RFind(value, "ет"u, 7ul) AS rfind_from
FROM Input
