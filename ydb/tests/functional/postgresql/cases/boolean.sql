--
-- BOOLEAN
--

--
-- sanity check - if this fails go insane!
--
SELECT 1 AS one;


-- ******************testing built-in type bool********************

-- check bool input syntax

SELECT true AS true;

SELECT false AS false;

SELECT bool 't' AS true;

SELECT bool '   f           ' AS false;

SELECT bool 'true' AS true;

SELECT bool 'test' AS error;

SELECT bool 'false' AS false;

SELECT bool 'foo' AS error;

SELECT bool 'y' AS true;

SELECT bool 'yes' AS true;

SELECT bool 'yeah' AS error;

SELECT bool 'n' AS false;

SELECT bool 'no' AS false;

SELECT bool 'nay' AS error;

SELECT bool 'on' AS true;

SELECT bool 'off' AS false;

SELECT bool 'of' AS false;

SELECT bool 'o' AS error;

SELECT bool 'on_' AS error;

SELECT bool 'off_' AS error;

SELECT bool '1' AS true;

SELECT bool '11' AS error;

SELECT bool '0' AS false;

SELECT bool '000' AS error;

SELECT bool '' AS error;

-- and, or, not in qualifications

SELECT bool 't' or bool 'f' AS true;

SELECT bool 't' and bool 'f' AS false;

SELECT not bool 'f' AS true;

SELECT bool 't' = bool 'f' AS false;

SELECT bool 't' <> bool 'f' AS true;

SELECT bool 't' > bool 'f' AS true;

SELECT bool 't' >= bool 'f' AS true;

SELECT bool 'f' < bool 't' AS true;

SELECT bool 'f' <= bool 't' AS true;

-- explicit casts to/from text
SELECT 'TrUe'::text::boolean AS true, 'fAlse'::text::boolean AS false;
SELECT '    true   '::text::boolean AS true,
       '     FALSE'::text::boolean AS false;
SELECT true::boolean::text AS true, false::boolean::text AS false;

SELECT '  tru e '::text::boolean AS invalid;    -- error
SELECT ''::text::boolean AS invalid;            -- error
