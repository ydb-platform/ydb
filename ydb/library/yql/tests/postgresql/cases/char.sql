--
-- CHAR
--

-- fixed-length by value
-- internally passed by value if <= 4 bytes in storage

SELECT char 'c' = char 'c' AS true;

--
-- Build a table for testing
--

CREATE TABLE CHAR_TBL(f1 char);
