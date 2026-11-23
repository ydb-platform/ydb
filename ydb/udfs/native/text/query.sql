/* syntax version 1 */
-- Expected: letters=3, digits=3, upper=1, len=7, b0=65 ('A'), b1=98 ('b')
SELECT
    TextNative::count_letters("Abc 123") AS letters,
    TextNative::count_digits("Abc 123") AS digits,
    TextNative::count_upper("Abc 123") AS upper,
    TextNative::text_length("Abc 123") AS len,
    TextNative::byte_at("Abc 123", 0l) AS b0,
    TextNative::byte_at("Abc 123", 1l) AS b1;
