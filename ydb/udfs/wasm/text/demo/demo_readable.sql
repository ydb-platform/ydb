/* syntax version 1 */
-- First rows of `text_200kb` with counters. PreferWasm on.
PRAGMA ydb.EnableWasmUdfResidentStringColumns = "true";

SELECT
    id,
    SUBSTRING(txt, 0u, 48u) AS head,
    Text::count_letters(txt) AS letters,
    Text::count_digits(txt) AS digits,
    Text::count_upper(txt) AS upper,
    Text::text_length(txt) AS len,
    Text::byte_at(txt, 0) AS b0
FROM `text_200kb`
WHERE id <= 5ul
ORDER BY id;
