import os
from chdb import session

tmp = "/tmp/.query_sqlite"
if not os.path.exists(tmp):
    os.mkdir(tmp)

# Touch lite.db file
# open(f"{tmp}/lite.db", "w").close()

chs = session.Session(tmp)
# chs.query(
#     f"CREATE DATABASE IF NOT EXISTS sqlite_db EGNINE = SQLite('{tmp}/lite.db'); USE sqlite_db;"
# )
chs.query("CREATE DATABASE IF NOT EXISTS sqlite_db;")
chs.query("SHOW DATABASES;").show()
chs.query("USE sqlite_db;")
chs.query(
    f"CREATE TABLE IF NOT EXISTS lite (c1 String, c2 int) ENGINE = SQLite('lite.sqlite3', 'lite2');"
).show()

chs.query("SHOW TABLES;").show()
chs.query("SHOW CREATE TABLE lite;").show()
# chs.query("SELECT * FROM sqlite_db.lite;", "Debug").show()

chs.query("INSERT INTO lite VALUES ('a', 1);")
chs.query("INSERT INTO lite VALUES ('b', 2);")
ret = chs.query("SELECT * FROM lite;", "Debug")
print(ret)

# simple aggregation
ret = chs.query("SELECT c1, SUM(c2) FROM lite GROUP BY c1;")
print(ret)
