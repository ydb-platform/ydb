/* syntax version 1 */
-- Full scan of `text_2mb`. SUM without ORDER BY; TextNative loaded via --udfs-dir.
SELECT SUM(TextNative::count_letters(txt)) AS checksum
FROM `text_2mb`;
