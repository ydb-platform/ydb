/* syntax version 1 */
-- Full scan of `text_2mb`. SUM without ORDER BY; TextNative loaded via --udfs-dir.
SELECT SUM(TextNative::byte_at(txt, 0) + TextNative::byte_at(txt, 1) + TextNative::byte_at(txt, 2) + TextNative::byte_at(txt, 3) + TextNative::byte_at(txt, 4) + TextNative::byte_at(txt, 5) + TextNative::byte_at(txt, 6) + TextNative::byte_at(txt, 7) + TextNative::byte_at(txt, 8) + TextNative::byte_at(txt, 9) + TextNative::byte_at(txt, 10) + TextNative::byte_at(txt, 11) + TextNative::byte_at(txt, 12) + TextNative::byte_at(txt, 13) + TextNative::byte_at(txt, 14) + TextNative::byte_at(txt, 15)) AS checksum
FROM `text_2mb`;
