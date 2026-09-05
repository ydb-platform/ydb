/* syntax version 1 */
-- Full scan of `text_1mb`. SUM without ORDER BY keeps the UDF in the same
-- stage as the table read so CollectWasmUdfStringColumns marks `txt`.
SELECT SUM(Text::count_letters(txt) + Text::count_digits(txt) + Text::count_upper(txt)) AS checksum
FROM `text_1mb`;
