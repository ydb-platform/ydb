/* syntax version 1 */
-- Full scan of `text_200kb`. SUM without ORDER BY; TextNative loaded via --udfs-dir.
SELECT SUM(TextNative::text_length(txt)) AS checksum
FROM `text_200kb`;
