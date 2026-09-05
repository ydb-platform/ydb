/* syntax version 1 */
-- Full scan of `text_200kb`. SUM without ORDER BY keeps the UDF in the same
-- stage as the table read so CollectWasmUdfStringColumns marks `txt`.
SELECT SUM(Text::byte_at(txt, 0) + Text::byte_at(txt, 1) + Text::byte_at(txt, 2) + Text::byte_at(txt, 3) + Text::byte_at(txt, 4) + Text::byte_at(txt, 5) + Text::byte_at(txt, 6) + Text::byte_at(txt, 7) + Text::byte_at(txt, 8) + Text::byte_at(txt, 9) + Text::byte_at(txt, 10) + Text::byte_at(txt, 11) + Text::byte_at(txt, 12) + Text::byte_at(txt, 13) + Text::byte_at(txt, 14) + Text::byte_at(txt, 15)) AS checksum
FROM `text_200kb`;
