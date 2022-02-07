const char* tests[] = {
  "SELECT 1",
  "SELECT $1",
	"SELECT $1, 1",
	"SELECT $1, $2",
  "ALTER ROLE postgres LOGIN SUPERUSER PASSWORD 'xyz'",
  "ALTER ROLE postgres LOGIN SUPERUSER PASSWORD $1",
  "CREATE ROLE postgres ENCRYPTED PASSWORD 'xyz'",
  "CREATE ROLE postgres ENCRYPTED PASSWORD $1",
  "ALTER ROLE foo WITH PASSWORD 'bar' VALID UNTIL 'infinity'",
  "ALTER ROLE foo WITH PASSWORD $1 VALID UNTIL $2",
  "SELECT a, SUM(b) FROM tbl WHERE c = 'foo' GROUP BY 1, 'bar' ORDER BY 1, 'cafe'",
  "SELECT a, SUM(b) FROM tbl WHERE c = $1 GROUP BY 1, $2 ORDER BY 1, $3",
  "select date_trunc($1, created_at at time zone $2), count(*) from users group by date_trunc('day', created_at at time zone 'US/Pacific')",
  "select date_trunc($1, created_at at time zone $2), count(*) from users group by date_trunc($1, created_at at time zone $2)",
  "select count(1), date_trunc('day', created_at at time zone 'US/Pacific'), 'something', 'somethingelse' from users group by date_trunc('day', created_at at time zone 'US/Pacific'), date_trunc('day', created_at), 'foobar', 'abcdef'",
  "select count($1), date_trunc($2, created_at at time zone $3), $4, $5 from users group by date_trunc($2, created_at at time zone $3), date_trunc($6, created_at), $4, $5",
  "SELECT CAST('abc' as varchar(50))",
  "SELECT CAST($1 as varchar(50))",
  // These below are as expected, though questionable if upstream shouldn't be
  // fixed as this could bloat pg_stat_statements
  "DECLARE cursor_b CURSOR FOR SELECT * FROM x WHERE id = 123",
  "DECLARE cursor_b CURSOR FOR SELECT * FROM x WHERE id = $1",
  "FETCH 1000 FROM cursor_a",
  "FETCH 1000 FROM cursor_a",
  "CLOSE cursor_a",
  "CLOSE cursor_a",
};

size_t testsLength = __LINE__ - 6;
