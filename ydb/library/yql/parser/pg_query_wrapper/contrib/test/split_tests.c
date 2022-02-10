const char* tests[] = {
  "",
  "",
  "SELECT 1",
  "loc=0,len=8",
  "SELECT 1; SELECT 2",
  "loc=0,len=8;loc=9,len=9",
  "SELECT 1; SELECT 2; SELECT 3",
  "loc=0,len=8;loc=9,len=9;loc=19,len=9",
  "SELECT /* comment with ; */ 1; SELECT 2",
  "loc=0,len=29;loc=30,len=9",
  "SELECT --othercomment with ;\n 1; SELECT 2",
  "loc=0,len=31;loc=32,len=9",
  "CREATE RULE x AS ON SELECT TO tbl DO (SELECT 1; SELECT 2)",
  "loc=0,len=57",
  "SELECT 1;\n;\n-- comment\nSELECT 2;\n;",
  "loc=0,len=8;loc=11,len=20"
};

size_t testsLength = __LINE__ - 4;
