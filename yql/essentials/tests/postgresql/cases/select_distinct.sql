--
-- SELECT_DISTINCT
--
--
-- awk '{print $3;}' onek.data | sort -n | uniq
--
SELECT DISTINCT two FROM tmp ORDER BY 1;
--
-- awk '{print $5;}' onek.data | sort -n | uniq
--
SELECT DISTINCT ten FROM tmp ORDER BY 1;
--
-- awk '{print $16;}' onek.data | sort -d | uniq
--
SELECT DISTINCT string4 FROM tmp ORDER BY 1;
--
-- Also, some tests of IS DISTINCT FROM, which doesn't quite deserve its
-- very own regression file.
--
CREATE TEMP TABLE disttable (f1 integer);
