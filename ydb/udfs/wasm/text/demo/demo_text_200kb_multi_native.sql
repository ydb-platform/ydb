/* syntax version 1 */
-- Full scan of `text_200kb`. SUM without ORDER BY; TextNative loaded via --udfs-dir.
SELECT SUM(TextNative::count_letters(txt) + TextNative::count_digits(txt) + TextNative::count_upper(txt)) AS checksum
FROM `text_200kb`;
