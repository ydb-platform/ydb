/* syntax version 1 */
-- Expected: letters=3, digits=3, upper=1, len=7, b0=65 ('A'), b1=98 ('b')
SELECT
    Text::count_letters("Abc 123") AS letters,
    Text::count_digits("Abc 123") AS digits,
    Text::count_upper("Abc 123") AS upper,
    Text::text_length("Abc 123") AS len,
    Text::byte_at("Abc 123", 0) AS b0,
    Text::byte_at("Abc 123", 1) AS b1;
