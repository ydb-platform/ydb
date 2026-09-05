/* syntax version 1 */
-- Full scan of `text_2mb`. SUM without ORDER BY keeps the UDF in the same
-- stage as the table read so CollectWasmUdfStringColumns marks `txt`.
SELECT SUM(Text::text_length(txt)) AS checksum
FROM `text_2mb`;
