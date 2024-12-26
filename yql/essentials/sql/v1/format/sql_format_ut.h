Y_UNIT_TEST(Pragma) {
    TCases cases = {
        {"pragma user = user;","PRAGMA user = user;\n"},
        {"pragma user = default;","PRAGMA user = default;\n"},
        {"pragma user.user = user;","PRAGMA user.user = user;\n"},
        {"pragma user.user(user);","PRAGMA user.user(user);\n"},
        {"pragma user.user(user, user);","PRAGMA user.user(user, user);\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(DotAfterDigits) {
    TCases cases = {
        {"select a.1 .b from plato.foo;","SELECT\n\ta.1 .b\nFROM\n\tplato.foo\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(GrantPermissions) {
    TCases cases {
        {"use plato;grant connect, modify tables, list on `/Root` to user;", "USE plato;\n\nGRANT CONNECT, MODIFY TABLES, LIST ON `/Root` TO user;\n"},
        {"use plato;grant select , select tables, select attributes on `/Root` to user;", "USE plato;\n\nGRANT SELECT, SELECT TABLES, SELECT ATTRIBUTES ON `/Root` TO user;\n"},
        {"use plato;grant insert, modify attributes on `/Root` to user;", "USE plato;\n\nGRANT INSERT, MODIFY ATTRIBUTES ON `/Root` TO user;\n"},
        {"use plato;grant use legacy, use on `/Root` to user1, user2;", "USE plato;\n\nGRANT USE LEGACY, USE ON `/Root` TO user1, user2;\n"},
        {"use plato;grant manage, full legacy, full, create on `/Root` to user;", "USE plato;\n\nGRANT MANAGE, FULL LEGACY, FULL, CREATE ON `/Root` TO user;\n"},
        {"use plato;grant drop, grant, select row, update row on `/Root` to user;", "USE plato;\n\nGRANT DROP, GRANT, SELECT ROW, UPDATE ROW ON `/Root` TO user;\n"},
        {"use plato;grant erase row, create directory on `/Root` to user;", "USE plato;\n\nGRANT ERASE ROW, CREATE DIRECTORY ON `/Root` TO user;\n"},
        {"use plato;grant create table, create queue, remove schema on `/Root` to user;", "USE plato;\n\nGRANT CREATE TABLE, CREATE QUEUE, REMOVE SCHEMA ON `/Root` TO user;\n"},
        {"use plato;grant describe schema, alter schema on `/Root` to user;", "USE plato;\n\nGRANT DESCRIBE SCHEMA, ALTER SCHEMA ON `/Root` TO user;\n"},
        {"use plato;grant select, on `/Root` to user, with grant option;", "USE plato;\n\nGRANT SELECT, ON `/Root` TO user, WITH GRANT OPTION;\n"},
        {"use plato;grant all privileges on `/Root` to user;", "USE plato;\n\nGRANT ALL PRIVILEGES ON `/Root` TO user;\n"},
        {"use plato;grant list on `/Root/db1`, `/Root/db2` to user;", "USE plato;\n\nGRANT LIST ON `/Root/db1`, `/Root/db2` TO user;\n"}
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(RevokePermissions) {
    TCases cases {
        {"use plato;revoke connect, modify tables, list on `/Root` from user;", "USE plato;\n\nREVOKE CONNECT, MODIFY TABLES, LIST ON `/Root` FROM user;\n"},
        {"use plato;revoke select , select tables, select attributes on `/Root` from user;", "USE plato;\n\nREVOKE SELECT, SELECT TABLES, SELECT ATTRIBUTES ON `/Root` FROM user;\n"},
        {"use plato;revoke insert, modify attributes on `/Root` from user;", "USE plato;\n\nREVOKE INSERT, MODIFY ATTRIBUTES ON `/Root` FROM user;\n"},
        {"use plato;revoke use legacy, use on `/Root` from user1, user2;", "USE plato;\n\nREVOKE USE LEGACY, USE ON `/Root` FROM user1, user2;\n"},
        {"use plato;revoke manage, full legacy, full, create on `/Root` from user;", "USE plato;\n\nREVOKE MANAGE, FULL LEGACY, FULL, CREATE ON `/Root` FROM user;\n"},
        {"use plato;revoke drop, grant, select row, update row on `/Root` from user;", "USE plato;\n\nREVOKE DROP, GRANT, SELECT ROW, UPDATE ROW ON `/Root` FROM user;\n"},
        {"use plato;revoke erase row, create directory on `/Root` from user;", "USE plato;\n\nREVOKE ERASE ROW, CREATE DIRECTORY ON `/Root` FROM user;\n"},
        {"use plato;revoke create table, create queue, remove schema on `/Root` from user;", "USE plato;\n\nREVOKE CREATE TABLE, CREATE QUEUE, REMOVE SCHEMA ON `/Root` FROM user;\n"},
        {"use plato;revoke describe schema, alter schema on `/Root` from user;", "USE plato;\n\nREVOKE DESCRIBE SCHEMA, ALTER SCHEMA ON `/Root` FROM user;\n"},
        {"use plato;revoke grant option for insert, on `/Root` from user;", "USE plato;\n\nREVOKE GRANT OPTION FOR INSERT, ON `/Root` FROM user;\n"},
        {"use plato;revoke all privileges on `/Root` from user;", "USE plato;\n\nREVOKE ALL PRIVILEGES ON `/Root` FROM user;\n"},
        {"use plato;revoke list on `/Root/db1`, `/Root/db2` from user;", "USE plato;\n\nREVOKE LIST ON `/Root/db1`, `/Root/db2` FROM user;\n"}
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(DropRole) {
    TCases cases = {
        {"use plato;drop user user,user,user;","USE plato;\n\nDROP USER user, user, user;\n"},
        {"use plato;drop group if exists user;","USE plato;\n\nDROP GROUP IF EXISTS user;\n"},
        {"use plato;drop group user,;","USE plato;\n\nDROP GROUP user,;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(CreateUser) {
    TCases cases = {
        {"use plato;create user user;", "USE plato;\n\nCREATE USER user;\n"},
        {"use plato;create user user encrypted password 'foo';", "USE plato;\n\nCREATE USER user ENCRYPTED PASSWORD 'foo';\n"},
        {"use plato;CREATE USER user1;", "USE plato;\n\nCREATE USER user1;\n"},
        {"use plato;create user user1 encrypted password '123' login;", "USE plato;\n\nCREATE USER user1 ENCRYPTED PASSWORD '123' LOGIN;\n"},
        {"use plato;cREATE USER user1 PASSWORD '123' NOLOGIN;", "USE plato;\n\nCREATE USER user1 PASSWORD '123' NOLOGIN;\n"},
        {"use plato;CREATE USER user1 LOGIN;", "USE plato;\n\nCREATE USER user1 LOGIN;\n"},
        {"use plato;CREATE USER user1 NOLOGIN;", "USE plato;\n\nCREATE USER user1 NOLOGIN;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(CreateGroup) {
    TCases cases = {
        {"use plato;create group user;","USE plato;\n\nCREATE GROUP user;\n"},
        {"use plato;create group user with user user;","USE plato;\n\nCREATE GROUP user WITH USER user;\n"},
        {"use plato;create group user with user user, user,;","USE plato;\n\nCREATE GROUP user WITH USER user, user,;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(AlterUser) {
    TCases cases = {
        {"use plato;alter user user rename to user;","USE plato;\n\nALTER USER user RENAME TO user;\n"},
        {"use plato;alter user user encrypted password 'foo';","USE plato;\n\nALTER USER user ENCRYPTED PASSWORD 'foo';\n"},
        {"use plato;alter user user with encrypted password 'foo';","USE plato;\n\nALTER USER user WITH ENCRYPTED PASSWORD 'foo';\n"},
        {"use plato;ALTER USER user1 NOLOGIN;", "USE plato;\n\nALTER USER user1 NOLOGIN;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(AlterGroup) {
    TCases cases = {
        {"use plato;alter group user add user user;","USE plato;\n\nALTER GROUP user ADD USER user;\n"},
        {"use plato;alter group user drop user user;","USE plato;\n\nALTER GROUP user DROP USER user;\n"},
        {"use plato;alter group user add user user, user,;","USE plato;\n\nALTER GROUP user ADD USER user, user,;\n"},
        {"use plato;alter group user rename to user;","USE plato;\n\nALTER GROUP user RENAME TO user;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(AlterSequence) {
    TCases cases = {
        {"use plato;alter sequence sequence start with 10 increment 2 restart with 5;","USE plato;\n\nALTER SEQUENCE sequence START WITH 10 INCREMENT 2 RESTART WITH 5;\n"},
        {"use plato;alter sequence if exists sequence increment 1000 start 100 restart;","USE plato;\n\nALTER SEQUENCE IF EXISTS sequence INCREMENT 1000 START 100 RESTART;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}


Y_UNIT_TEST(Use) {
    TCases cases = {
        {"use user;","USE user;\n"},
        {"use user:user;","USE user: user;\n"},
        {"use user:*;","USE user: *;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Commit) {
    TCases cases = {
        {"commit;","COMMIT;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Rollback) {
    TCases cases = {
        {"rollback;","ROLLBACK;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Export) {
    TCases cases = {
        {"export $foo;","EXPORT\n\t$foo\n;\n"},
        {"export $foo, $bar;","EXPORT\n\t$foo,\n\t$bar\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Import) {
    TCases cases = {
        {"import user symbols $foo;","IMPORT user SYMBOLS $foo;\n"},
        {"import user symbols $foo,$bar;","IMPORT user SYMBOLS $foo, $bar;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Values) {
    TCases cases = {
        {"values (1);","VALUES\n\t(1)\n;\n"},
        {"values (1,2),(3,4);","VALUES\n\t(1, 2),\n\t(3, 4)\n;\n"},
        {"values ('a\nb');","VALUES\n\t('a\nb')\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Declare) {
    TCases cases = {
        {"declare $foo as int32;","DECLARE $foo AS int32;\n"},
        {"declare $foo as bool ?","DECLARE $foo AS bool?;\n"},
        {"declare $foo as bool ? ?","DECLARE $foo AS bool??;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(NamedNode) {
    TCases cases = {
        {"$x=1",
            "$x = 1;\n"},
        {"$x,$y=(2,3)",
            "$x, $y = (2, 3);\n"},
        {"$a = select 1 union all select 2",
            "$a = (\n\tSELECT\n\t\t1\n\tUNION ALL\n\tSELECT\n\t\t2\n);\n"},
        {"$a = select 1 from $as;",
            "$a = (\n\tSELECT\n\t\t1\n\tFROM\n\t\t$as\n);\n"},
        {"$a = select * from $t -- comment",
            "$a = (\n\tSELECT\n\t\t*\n\tFROM\n\t\t$t -- comment\n);\n"},
        {"-- comment\r\r\r$a=1;",
            "-- comment\r\n$a = 1;\n"},
        {"$a=1;-- comment\n$b=2;/* comment */ /* comment */\n$c = 3;/* comment */ -- comment",
            "$a = 1; -- comment\n$b = 2; /* comment */ /* comment */\n$c = 3; /* comment */ -- comment\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(DropTable) {
    TCases cases = {
        {"drop table user","DROP TABLE user;\n"},
        {"drop table if exists user","DROP TABLE IF EXISTS user;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(CreateTable) {
    TCases cases = {
        {"create table user(user int32)","CREATE TABLE user (\n\tuser int32\n);\n"},
        {"create table user(user int32,user bool ?)","CREATE TABLE user (\n\tuser int32,\n\tuser bool?\n);\n"},
        {"create table user(user int32) with (user=user)","CREATE TABLE user (\n\tuser int32\n)\nWITH (user = user);\n"},
        {"create table user(primary key (user))","CREATE TABLE user (\n\tPRIMARY KEY (user)\n);\n"},
        {"create table user(primary key (user,user))","CREATE TABLE user (\n\tPRIMARY KEY (user, user)\n);\n"},
        {"create table user(partition by (user))","CREATE TABLE user (\n\tPARTITION BY (user)\n);\n"},
        {"create table user(partition by (user,user))","CREATE TABLE user (\n\tPARTITION BY (user, user)\n);\n"},
        {"create table user(order by (user asc))","CREATE TABLE user (\n\tORDER BY (user ASC)\n);\n"},
        {"create table user(order by (user desc,user))","CREATE TABLE user (\n\tORDER BY (user DESC, user)\n);\n"},
        {"create table user(user int32) with (ttl=interval('P1D') on user as seconds)",
            "CREATE TABLE user (\n\tuser int32\n)\nWITH (ttl = interval('P1D') ON user AS SECONDS);\n"},
        {"create table user(user int32) with (ttl=interval('P1D') on user as MilliSeconds)",
            "CREATE TABLE user (\n\tuser int32\n)\nWITH (ttl = interval('P1D') ON user AS MILLISECONDS);\n"},
        {"create table user(user int32) with (ttl=interval('P1D') on user as microSeconds)",
            "CREATE TABLE user (\n\tuser int32\n)\nWITH (ttl = interval('P1D') ON user AS MICROSECONDS);\n"},
        {"create table user(user int32) with (ttl=interval('P1D') on user as nAnOsEcOnDs)",
            "CREATE TABLE user (\n\tuser int32\n)\nWITH (ttl = interval('P1D') ON user AS NANOSECONDS);\n"},
        {"create table user(user int32) with (ttl=interval('P1D') delete on user as nAnOsEcOnDs)",
            "CREATE TABLE user (\n\tuser int32\n)\nWITH (ttl = interval('P1D') DELETE ON user AS NANOSECONDS);\n"},
        {"create table user(user int32) with (ttl=interval('P1D')to external data source tier1 ,interval('P10D')delete on user as seconds)",
            "CREATE TABLE user (\n"
            "\tuser int32\n"
            ")\n"
            "WITH (ttl =\n"
            "\tinterval('P1D') TO EXTERNAL DATA SOURCE tier1,\n"
            "\tinterval('P10D') DELETE\n"
            "ON user AS SECONDS);\n"},
        {"create table user(index user global unique sync on (user,user) with (user=user,user=user))",
            "CREATE TABLE user (\n\tINDEX user GLOBAL UNIQUE SYNC ON (user, user) WITH (user = user, user = user)\n);\n"},
        {"create table user(index user global async on (user) with (user=user,))",
            "CREATE TABLE user (\n\tINDEX user GLOBAL ASYNC ON (user) WITH (user = user,)\n);\n"},
        {"create table user(index user local on (user) cover (user))",
            "CREATE TABLE user (\n\tINDEX user LOCAL ON (user) COVER (user)\n);\n"},
        {"create table user(index user local on (user) cover (user,user))",
            "CREATE TABLE user (\n\tINDEX user LOCAL ON (user) COVER (user, user)\n);\n"},
        {"create table user(index idx global using subtype on (col) cover (col) with (setting = foo, another_setting = bar));",
            "CREATE TABLE user (\n\tINDEX idx GLOBAL USING subtype ON (col) COVER (col) WITH (setting = foo, another_setting = bar)\n);\n"},
        {"create table user(family user (user='foo'))",
            "CREATE TABLE user (\n\tFAMILY user (user = 'foo')\n);\n"},
        {"create table user(family user (user='foo',user='bar'))",
            "CREATE TABLE user (\n\tFAMILY user (user = 'foo', user = 'bar')\n);\n"},
        {"create table user(changefeed user with (user='foo'))",
            "CREATE TABLE user (\n\tCHANGEFEED user WITH (user = 'foo')\n);\n"},
        {"create table user(changefeed user with (user='foo',user='bar'))",
            "CREATE TABLE user (\n\tCHANGEFEED user WITH (user = 'foo', user = 'bar')\n);\n"},
        {"create table user(user) AS SELECT 1","CREATE TABLE user (\n\tuser\n)\nAS\nSELECT\n\t1\n;\n"},
        {"create table user(user) AS VALUES (1), (2)","CREATE TABLE user (\n\tuser\n)\nAS\nVALUES\n\t(1),\n\t(2)\n;\n"},
        {"create table user(foo int32, bar bool ?) inherits (s3:$cluster.xxx) partition by hash(a,b,hash) with (inherits=interval('PT1D') ON logical_time) tablestore tablestore",
            "CREATE TABLE user (\n"
            "\tfoo int32,\n"
            "\tbar bool?\n"
            ")\n"
            "INHERITS (s3: $cluster.xxx)\n"
            "PARTITION BY HASH (a, b, hash)\n"
            "WITH (inherits = interval('PT1D') ON logical_time)\n"
            "TABLESTORE tablestore;\n"},
        {"create table user(foo int32, bar bool ?) partition by hash(a,b,hash) with (tiering='some')",
            "CREATE TABLE user (\n"
            "\tfoo int32,\n"
            "\tbar bool?\n"
            ")\n"
            "PARTITION BY HASH (a, b, hash)\n"
            "WITH (tiering = 'some');\n"},
        {"create table if not  exists user(user int32)", "CREATE TABLE IF NOT EXISTS user (\n\tuser int32\n);\n"},
        {"create temp   table    user(user int32)", "CREATE TEMP TABLE user (\n\tuser int32\n);\n"},
        {"create   temporary   table    user(user int32)", "CREATE TEMPORARY TABLE user (\n\tuser int32\n);\n"}
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(ObjectOperations) {
    TCases cases = {
        {"alter oBject usEr (TYpe abcde) Set (a = b)",
            "ALTER OBJECT usEr (TYPE abcde) SET (a = b);\n"},
        {"creAte oBject usEr (tYpe abcde) With (a = b)",
            "CREATE OBJECT usEr (TYPE abcde) WITH (a = b);\n"},
            {"creAte oBject if not exIstS usEr (tYpe abcde) With (a = b)",
            "CREATE OBJECT IF NOT EXISTS usEr (TYPE abcde) WITH (a = b);\n"},
        {"creAte oBject usEr (tYpe abcde) With a = b",
            "CREATE OBJECT usEr (TYPE abcde) WITH a = b;\n"},
        {"dRop oBject usEr (tYpe abcde) With (aeEE)",
            "DROP OBJECT usEr (TYPE abcde) WITH (aeEE);\n"},
            {"dRop oBject If ExistS usEr (tYpe abcde) With (aeEE)",
            "DROP OBJECT IF EXISTS usEr (TYPE abcde) WITH (aeEE);\n"},
        {"dRop oBject usEr (tYpe abcde) With aeEE",
            "DROP OBJECT usEr (TYPE abcde) WITH aeEE;\n"}
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(TableStoreOperations) {
    TCases cases = {
        {"alter tableStore uSer aDd column usEr int32",
            "ALTER TABLESTORE uSer ADD COLUMN usEr int32;\n"},
            {"alter tableStore uSer drOp column usEr",
            "ALTER TABLESTORE uSer DROP COLUMN usEr;\n"}
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(ExternalDataSourceOperations) {
    TCases cases = {
        {"creAte exTernAl daTa SouRce usEr With (a = \"b\")",
            "CREATE EXTERNAL DATA SOURCE usEr WITH (a = 'b');\n"},
        {"creAte exTernAl daTa SouRce if not exists usEr With (a = \"b\")",
            "CREATE EXTERNAL DATA SOURCE IF NOT EXISTS usEr WITH (a = 'b');\n"},
        {"creAte oR rePlaCe exTernAl daTa SouRce usEr With (a = \"b\")",
            "CREATE OR REPLACE EXTERNAL DATA SOURCE usEr WITH (a = 'b');\n"},
        {"create external data source eds with (a=\"a\",b=\"b\",c = true)",
            "CREATE EXTERNAL DATA SOURCE eds WITH (\n\ta = 'a',\n\tb = 'b',\n\tc = TRUE\n);\n"},
        {"alter external data source eds set a true, reset (b, c), set (x=y, z=false)",
            "ALTER EXTERNAL DATA SOURCE eds\n\tSET a TRUE,\n\tRESET (b, c),\n\tSET (x = y, z = FALSE)\n;\n"},
        {"alter external data source eds reset (a), set (x=y)",
            "ALTER EXTERNAL DATA SOURCE eds\n\tRESET (a),\n\tSET (x = y)\n;\n"},
        {"dRop exTerNal Data SouRce usEr",
            "DROP EXTERNAL DATA SOURCE usEr;\n"},
        {"dRop exTerNal Data SouRce if exists usEr",
            "DROP EXTERNAL DATA SOURCE IF EXISTS usEr;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(AsyncReplication) {
    TCases cases = {
        {"create async replication user for table1 AS table2 with (user='foo')",
            "CREATE ASYNC REPLICATION user FOR table1 AS table2 WITH (user = 'foo');\n"},
        {"alter async replication user set (user='foo')",
            "ALTER ASYNC REPLICATION user SET (user = 'foo');\n"},
        {"drop async replication user",
            "DROP ASYNC REPLICATION user;\n"},
        {"drop async replication user cascade",
            "DROP ASYNC REPLICATION user CASCADE;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Transfer) {
    TCases cases = {
        {"create transfer user for topic1 to table1 with (user='foo')",
            "CREATE TRANSFER user FOR topic1 TO table1 WITH (user = 'foo');\n"},
        {"alter transfer user set (user='foo')",
            "ALTER TRANSFER user SET (user = 'foo');\n"},
        {"drop transfer user",
            "DROP TRANSFER user;\n"},
        {"drop transfer user cascade",
            "DROP TRANSFER user CASCADE;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(ExternalTableOperations) {
    TCases cases = {
        {"creAte exTernAl TabLe usEr (a int) With (a = \"b\")",
            "CREATE EXTERNAL TABLE usEr (\n\ta int\n)\nWITH (a = 'b');\n"},
        {"creAte oR rePlaCe exTernAl TabLe usEr (a int) With (a = \"b\")",
            "CREATE OR REPLACE EXTERNAL TABLE usEr (\n\ta int\n)\nWITH (a = 'b');\n"},
        {"creAte exTernAl TabLe iF NOt Exists usEr (a int) With (a = \"b\")",
            "CREATE EXTERNAL TABLE IF NOT EXISTS usEr (\n\ta int\n)\nWITH (a = 'b');\n"},
        {"create external table user (a int) with (a=\"b\",c=\"d\")",
            "CREATE EXTERNAL TABLE user (\n\ta int\n)\nWITH (\n\ta = 'b',\n\tc = 'd'\n);\n"},
        {"alter  external table user add column col1 int32, drop column col2, reset(prop), set (prop2 = 42, x=y), set a true",
            "ALTER EXTERNAL TABLE user\n\tADD COLUMN col1 int32,\n\tDROP COLUMN col2,\n\tRESET (prop),\n\tSET (prop2 = 42, x = y),\n\tSET a TRUE\n;\n"},
        {"dRop exTerNal taBlE usEr",
            "DROP EXTERNAL TABLE usEr;\n"},
        {"dRop exTerNal taBlE iF eXiStS usEr",
            "DROP EXTERNAL TABLE IF EXISTS usEr;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(TypeSelection) {
    TCases cases = {
        {"Select tYpe.* frOm Table tYpe",
            "SELECT\n\ttYpe.*\nFROM\n\tTable tYpe\n;\n"}
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(AlterTable) {
    TCases cases = {
        {"alter table user add user int32",
            "ALTER TABLE user\n\tADD user int32\n;\n"},
        {"alter table user add user int32, add user bool ?",
            "ALTER TABLE user\n\tADD user int32,\n\tADD user bool?\n;\n"},
        {"alter table user add column user int32",
            "ALTER TABLE user\n\tADD COLUMN user int32\n;\n"},
        {"alter table user drop user",
            "ALTER TABLE user\n\tDROP user\n;\n"},
        {"alter table user drop column user",
            "ALTER TABLE user\n\tDROP COLUMN user\n;\n"},
        {"alter table user alter column user set family user",
            "ALTER TABLE user\n\tALTER COLUMN user SET FAMILY user\n;\n"},
        {"alter table t alter column c drop not null",
            "ALTER TABLE t\n\tALTER COLUMN c DROP NOT NULL\n;\n"},
        {"alter table user add family user(user='foo')",
            "ALTER TABLE user\n\tADD FAMILY user (user = 'foo')\n;\n"},
        {"alter table user alter family user set user 'foo'",
            "ALTER TABLE user\n\tALTER FAMILY user SET user 'foo'\n;\n"},
        {"alter table user set user user",
            "ALTER TABLE user\n\tSET user user\n;\n"},
        {"alter table user set (user=user)",
            "ALTER TABLE user\n\tSET (user = user)\n;\n"},
        {"alter table user set (user=user,user=user)",
            "ALTER TABLE user\n\tSET (user = user, user = user)\n;\n"},
        {"alter table user reset(user)",
            "ALTER TABLE user\n\tRESET (user)\n;\n"},
        {"alter table user reset(user, user)",
            "ALTER TABLE user\n\tRESET (user, user)\n;\n"},
        {"alter table user add index user local on (user)",
            "ALTER TABLE user\n\tADD INDEX user LOCAL ON (user)\n;\n"},
        {"alter table user alter index idx set setting 'foo'",
            "ALTER TABLE user\n\tALTER INDEX idx SET setting 'foo'\n;\n"},
        {"alter table user alter index idx set (setting = 'foo', another_setting = 'bar')",
            "ALTER TABLE user\n\tALTER INDEX idx SET (setting = 'foo', another_setting = 'bar')\n;\n"},
        {"alter table user alter index idx reset (setting, another_setting)",
            "ALTER TABLE user\n\tALTER INDEX idx RESET (setting, another_setting)\n;\n"},
        {"alter table user add index idx global using subtype on (col) cover (col) with (setting = foo, another_setting = 'bar');",
            "ALTER TABLE user\n\tADD INDEX idx GLOBAL USING subtype ON (col) COVER (col) WITH (setting = foo, another_setting = 'bar')\n;\n"},
        {"alter table user drop index user",
            "ALTER TABLE user\n\tDROP INDEX user\n;\n"},
        {"alter table user rename to user",
            "ALTER TABLE user\n\tRENAME TO user\n;\n"},
        {"alter table user add changefeed user with (user = 'foo')",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (user = 'foo')\n;\n"},
        {"alter table user alter changefeed user disable",
            "ALTER TABLE user\n\tALTER CHANGEFEED user DISABLE\n;\n"},
        {"alter table user alter changefeed user set(user='foo')",
            "ALTER TABLE user\n\tALTER CHANGEFEED user SET (user = 'foo')\n;\n"},
        {"alter table user drop changefeed user",
            "ALTER TABLE user\n\tDROP CHANGEFEED user\n;\n"},
        {"alter table user add changefeed user with (initial_scan = tRUe)",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (initial_scan = TRUE)\n;\n"},
        {"alter table user add changefeed user with (initial_scan = FaLsE)",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (initial_scan = FALSE)\n;\n"},
        {"alter table user add changefeed user with (retention_period = Interval(\"P1D\"))",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (retention_period = Interval('P1D'))\n;\n"},
        {"alter table user add changefeed user with (virtual_timestamps = TruE)",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (virtual_timestamps = TRUE)\n;\n"},
        {"alter table user add changefeed user with (virtual_timestamps = fAlSe)",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (virtual_timestamps = FALSE)\n;\n"},
        {"alter table user add changefeed user with (barriers_interval = Interval(\"PT1S\"))",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (barriers_interval = Interval('PT1S'))\n;\n"},
        {"alter table user add changefeed user with (topic_min_active_partitions = 1)",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (topic_min_active_partitions = 1)\n;\n"},
        {"alter table user add changefeed user with (topic_auto_partitioning = 'ENABLED', topic_min_active_partitions = 1, topic_max_active_partitions = 7)",
            "ALTER TABLE user\n\tADD CHANGEFEED user WITH (topic_auto_partitioning = 'ENABLED', topic_min_active_partitions = 1, topic_max_active_partitions = 7)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(CreateTopic) {
    TCases cases = {
        {"create topic topic1",
            "CREATE TOPIC topic1;\n"},
        {"create topic topic1 (consumer c1)",
            "CREATE TOPIC topic1 (\n\tCONSUMER c1\n);\n"},
        {"create topic topic1 (consumer c1, consumer c2 with (important = True))",
            "CREATE TOPIC topic1 (\n\tCONSUMER c1,\n\tCONSUMER c2 WITH (important = TRUE)\n);\n"},
        {"create topic topic1 (consumer c1) with (partition_count_limit = 5)",
            "CREATE TOPIC topic1 (\n\tCONSUMER c1\n) WITH (\n\tpartition_count_limit = 5\n);\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(AlterTopic) {
    TCases cases = {
        {"alter topic topic1 alter consumer c1 set (important = false)",
            "ALTER TOPIC topic1\n\tALTER CONSUMER c1 SET (important = FALSE)\n;\n"},
        {"alter topic topic1 alter consumer c1 set (important = false), alter consumer c2 reset (read_from)",
            "ALTER TOPIC topic1\n\tALTER CONSUMER c1 SET (important = FALSE),\n\tALTER CONSUMER c2 RESET (read_from)\n;\n"},
        {"alter topic topic1 add consumer c1, drop consumer c2",
            "ALTER TOPIC topic1\n\tADD CONSUMER c1,\n\tDROP CONSUMER c2\n;\n"},
        {"alter topic topic1 set (supported_codecs = 'RAW'), RESET (retention_period)",
            "ALTER TOPIC topic1\n\tSET (supported_codecs = 'RAW'),\n\tRESET (retention_period)\n;\n"},

    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(DropTopic) {
    TCases cases = {
        {"drop topic topic1",
            "DROP TOPIC topic1;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(TopicExistsStatement) {
    TCases cases = {
        {"drop topic if exists topic1",
            "DROP TOPIC IF EXISTS topic1;\n"},
        {"create topic if not exists topic1 with (partition_count_limit = 5)",
            "CREATE TOPIC IF NOT EXISTS topic1 WITH (\n\tpartition_count_limit = 5\n);\n"},
        {"alter topic if exists topic1 alter consumer c1 set (important = false)",
            "ALTER TOPIC IF EXISTS topic1\n\tALTER CONSUMER c1 SET (important = FALSE)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Do) {
    TCases cases = {
        {"do $a(1,2,3)",
            "DO\n\t$a(1, 2, 3)\n;\n"},
        {"do begin values(1); end do;",
            "DO BEGIN\n\tVALUES\n\t\t(1)\n\t;\nEND DO;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(DefineActionOrSubquery) {
    TCases cases = {
        {"define action $a() as "
            "define action $b() as "
            "values(1); "
            "end define; "
            "define subquery $c() as "
            "select 1; "
            "end define; "
            "do $b(); "
            "process $c(); "
            "end define",
            "DEFINE ACTION $a() AS\n\tDEFINE ACTION $b() AS\n\t\t"
            "VALUES\n\t\t\t(1)\n\t\t;\n\tEND DEFINE;\n\n\t"
            "DEFINE SUBQUERY $c() AS\n\t\tSELECT\n\t\t\t1\n\t\t;\n\t"
            "END DEFINE;\n\tDO\n\t\t$b()\n\t;\n\n\tPROCESS $c();\nEND DEFINE;\n"},
        {"define action $foo($bar) as;"
            "$a = 10;; "
            "$b = 20;;; "
            "$c = $a + $b "
            "end define",
            "DEFINE ACTION $foo($bar) AS\n\t"
            "$a = 10;\n\t"
            "$b = 20;\n\t"
            "$c = $a + $b;\n"
            "END DEFINE;\n"},
        {"define subquery $s() as;"
            "select * from $t1 "
            "union all select * from $t2 "
            "end define",
            "DEFINE SUBQUERY $s() AS\n\t"
            "SELECT\n\t\t*\n\tFROM\n\t\t$t1\n\t"
            "UNION ALL\n\t"
            "SELECT\n\t\t*\n\tFROM\n\t\t$t2\n\t;\n"
            "END DEFINE;\n"},
        {"define subquery $s() as $t = select * from $a end define",
            "DEFINE SUBQUERY $s() AS\n\t"
            "$t = (\n\t\tSELECT\n\t\t\t*\n\t\tFROM\n\t\t\t$a\n\t);\n"
            "END DEFINE;\n"},
        {"define subquery $s() as; $t = select * from $a; end define",
            "DEFINE SUBQUERY $s() AS\n\t"
            "$t = (\n\t\tSELECT\n\t\t\t*\n\t\tFROM\n\t\t\t$a\n\t);\n"
            "END DEFINE;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(If) {
    TCases cases = {
        {"evaluate if 1=1 do $a()",
            "EVALUATE IF 1 == 1 DO\n\t$a()\n;\n"},
        {"evaluate if 1=1 do $a() else do $b()",
            "EVALUATE IF 1 == 1 DO\n\t$a()\nELSE DO\n\t$b()\n;\n"},
        {"evaluate if 1=1 do begin select 1; end do",
            "EVALUATE IF 1 == 1 DO BEGIN\n\tSELECT\n\t\t1\n\t;\nEND DO;\n"},
        {"evaluate if 1=1 do begin select 1; end do else do begin select 2; end do",
            "EVALUATE IF 1 == 1 DO BEGIN\n\tSELECT\n\t\t1\n\t;\nEND DO "
            "ELSE DO BEGIN\n\tSELECT\n\t\t2\n\t;\nEND DO;\n"},
        {"evaluate if 1=1 do begin; select 1 end do else do begin select 2;; select 3 end do",
            "EVALUATE IF 1 == 1 DO BEGIN\n\tSELECT\n\t\t1\n\t;\nEND DO ELSE DO BEGIN\n\t"
            "SELECT\n\t\t2\n\t;\n\n\tSELECT\n\t\t3\n\t;\nEND DO;\n"},
        {"evaluate if 1=1 do begin (select 1) end do",
            "EVALUATE IF 1 == 1 DO BEGIN\n\t(\n\t\tSELECT\n\t\t\t1\n\t);\nEND DO;\n"},
        {"evaluate if 1=1 do begin $a = select * from $begin; $end = 1; end do",
            "EVALUATE IF 1 == 1 DO BEGIN\n\t$a = (\n\t\tSELECT\n\t\t\t*\n\t\tFROM\n\t\t\t$begin\n\t);\n\t$end = 1;\nEND DO;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(For) {
    TCases cases = {
        {"evaluate for $x in [] do $a($x)",
            "EVALUATE FOR $x IN [] DO\n\t$a($x)\n;\n"},
        {"evaluate for $x in [] do $a($x) else do $b()",
            "EVALUATE FOR $x IN [] DO\n\t$a($x)\nELSE DO\n\t$b()\n;\n"},
        {"evaluate for $x in [] do begin select $x; end do",
            "EVALUATE FOR $x IN [] DO BEGIN\n\tSELECT\n\t\t$x\n\t;\nEND DO;\n"},
        {"evaluate for $x in [] do begin select $x; end do else do begin select 2; end do",
            "EVALUATE FOR $x IN [] DO BEGIN\n\tSELECT\n\t\t$x\n\t;\nEND DO ELSE DO BEGIN\n\tSELECT\n\t\t2\n\t;\nEND DO;\n"},
        {"evaluate parallel for $x in [] do $a($x)",
            "EVALUATE PARALLEL FOR $x IN [] DO\n\t$a($x)\n;\n"},
        {"evaluate for $x in [] do begin; select $x;; select $y end do",
            "EVALUATE FOR $x IN [] DO BEGIN\n\tSELECT\n\t\t$x\n\t;\n\n\tSELECT\n\t\t$y\n\t;\nEND DO;\n"},
        {"evaluate for $x in [] do begin (select 1) end do",
            "EVALUATE FOR $x IN [] DO BEGIN\n\t(\n\t\tSELECT\n\t\t\t1\n\t);\nEND DO;\n"},
        {"evaluate for $x in [] do begin $a = select * from $begin; $end = 1; end do",
            "EVALUATE FOR $x IN [] DO BEGIN\n\t$a = (\n\t\tSELECT\n\t\t\t*\n\t\tFROM\n\t\t\t$begin\n\t);\n\t$end = 1;\nEND DO;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Update) {
    TCases cases = {
        {"update user on default values",
            "UPDATE user\nON DEFAULT VALUES;\n"},
        {"update user on values (1),(2)",
            "UPDATE user\nON\nVALUES\n\t(1),\n\t(2)\n;\n"},
        {"update user on select 1 as x, 2 as y",
            "UPDATE user\nON\nSELECT\n\t1 AS x,\n\t2 AS y\n;\n"},
        {"update user on (x) values (1),(2),(3)",
            "UPDATE user\nON (\n\tx\n)\nVALUES\n\t(1),\n\t(2),\n\t(3)\n;\n"},
        {"update user on (x,y) values (1,2),(2,3),(3,4)",
            "UPDATE user\nON (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2),\n\t(2, 3),\n\t(3, 4)\n;\n"},
        {"update user on (x) select 1",
            "UPDATE user\nON (\n\tx\n)\nSELECT\n\t1\n;\n"},
        {"update user on (x,y) select 1,2",
            "UPDATE user\nON (\n\tx,\n\ty\n)\nSELECT\n\t1,\n\t2\n;\n"},
        {"update user set x=1",
            "UPDATE user\nSET\n\tx = 1\n;\n"},
        {"update user set (x)=(1)",
            "UPDATE user\nSET\n(\n\tx\n) = (\n\t1\n);\n"},
        {"update user set (x,y)=(1,2)",
            "UPDATE user\nSET\n(\n\tx,\n\ty\n) = (\n\t1,\n\t2\n);\n"},
        {"update user set (x,y)=(select 1,2)",
            "UPDATE user\nSET\n(\n\tx,\n\ty\n) = (\n\tSELECT\n\t\t1,\n\t\t2\n);\n"},
        {"update user set x=1,y=2 where z=3",
            "UPDATE user\nSET\n\tx = 1,\n\ty = 2\nWHERE z == 3;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Delete) {
    TCases cases = {
        {"delete from user",
            "DELETE FROM user;\n"},
        {"delete from user where 1=1",
            "DELETE FROM user\nWHERE 1 == 1;\n"},
        {"delete from user on select 1 as x, 2 as y",
            "DELETE FROM user\nON\nSELECT\n\t1 AS x,\n\t2 AS y\n;\n"},
        {"delete from user on (x) values (1)",
            "DELETE FROM user\nON (\n\tx\n)\nVALUES\n\t(1)\n;\n"},
        {"delete from user on (x,y) values (1,2), (3,4)",
            "DELETE FROM user\nON (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2),\n\t(3, 4)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Into) {
    TCases cases = {
        {"insert into user select 1 as x",
            "INSERT INTO user\nSELECT\n\t1 AS x\n;\n"},
        {"insert or abort into user select 1 as x",
            "INSERT OR ABORT INTO user\nSELECT\n\t1 AS x\n;\n"},
        {"insert or revert into user select 1 as x",
            "INSERT OR REVERT INTO user\nSELECT\n\t1 AS x\n;\n"},
        {"insert or ignore into user select 1 as x",
            "INSERT OR IGNORE INTO user\nSELECT\n\t1 AS x\n;\n"},
        {"upsert into user select 1 as x",
            "UPSERT INTO user\nSELECT\n\t1 AS x\n;\n"},
        {"replace into user select 1 as x",
            "REPLACE INTO user\nSELECT\n\t1 AS x\n;\n"},
        {"insert into user(x) values (1)",
            "INSERT INTO user (\n\tx\n)\nVALUES\n\t(1)\n;\n"},
        {"insert into user(x,y) values (1,2)",
            "INSERT INTO user (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2)\n;\n"},
        {"insert into plato.user select 1 as x",
            "INSERT INTO plato.user\nSELECT\n\t1 AS x\n;\n"},
        {"insert into @user select 1 as x",
            "INSERT INTO @user\nSELECT\n\t1 AS x\n;\n"},
        {"insert into $user select 1 as x",
            "INSERT INTO $user\nSELECT\n\t1 AS x\n;\n"},
        {"insert into @$user select 1 as x",
            "INSERT INTO @$user\nSELECT\n\t1 AS x\n;\n"},
        {"upsert into user erase by (x,y) values (1)",
            "UPSERT INTO user\n\tERASE BY (\n\t\tx,\n\t\ty\n\t)\nVALUES\n\t(1)\n;\n"},
        {"insert into user with truncate select 1 as x",
            "INSERT INTO user WITH truncate\nSELECT\n\t1 AS x\n;\n"},
        {"insert into user with (truncate,inferscheme='1') select 1 as x",
            "INSERT INTO user WITH (\n\ttruncate,\n\tinferscheme = '1'\n)\nSELECT\n\t1 AS x\n;\n"},
        {"insert into user with schema Struct<user:int32> select 1 as user",
            "INSERT INTO user WITH SCHEMA Struct<user: int32>\nSELECT\n\t1 AS user\n;\n"},
        {"insert into user with schema (int32 as user) select 1 as user",
            "INSERT INTO user WITH SCHEMA (int32 AS user)\nSELECT\n\t1 AS user\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Process) {
    TCases cases = {
        {"process user",
            "PROCESS user;\n"},
        {"process user using $f() as user",
            "PROCESS user\nUSING $f() AS user;\n"},
        {"process user,user using $f()",
            "PROCESS user, user\nUSING $f();\n"},
        {"process user using $f() where 1=1 having 1=1 assume order by user",
            "PROCESS user\nUSING $f()\nWHERE\n\t1 == 1\nHAVING\n\t1 == 1\nASSUME ORDER BY\n\tuser\n;\n"},
        {"process user using $f() union all process user using $f()",
            "PROCESS user\nUSING $f()\nUNION ALL\nPROCESS user\nUSING $f();\n"},
        {"process user using $f() with foo=bar",
            "PROCESS user\nUSING $f()\nWITH foo = bar;\n"},
        {"discard process user using $f()",
            "DISCARD PROCESS user\nUSING $f();\n"},
        {"process user using $f() into result user",
            "PROCESS user\nUSING $f()\nINTO RESULT user;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Reduce) {
    TCases cases = {
        {"reduce user on user using $f()",
            "REDUCE user\nON\n\tuser\nUSING $f();\n"},
        {"reduce user on user, using $f()",
            "REDUCE user\nON\n\tuser,\nUSING $f();\n"},
        {"discard reduce user on user using $f();",
            "DISCARD REDUCE user\nON\n\tuser\nUSING $f();\n"},
        {"reduce user on user using $f() into result user",
            "REDUCE user\nON\n\tuser\nUSING $f()\nINTO RESULT user;\n"},
        {"reduce user on user using all $f()",
            "REDUCE user\nON\n\tuser\nUSING ALL $f();\n"},
        {"reduce user on user using $f() as user",
            "REDUCE user\nON\n\tuser\nUSING $f() AS user;\n"},
        {"reduce user,user on user using $f()",
            "REDUCE user, user\nON\n\tuser\nUSING $f();\n"},
        {"reduce user on user,user using $f()",
            "REDUCE user\nON\n\tuser,\n\tuser\nUSING $f();\n"},
        {"reduce user on user using $f() where 1=1 having 1=1 assume order by user",
            "REDUCE user\nON\n\tuser\nUSING $f()\nWHERE\n\t1 == 1\nHAVING\n\t1 == 1\nASSUME ORDER BY\n\tuser\n;\n"},
        {"reduce user presort user,user on user using $f();",
            "REDUCE user\nPRESORT\n\tuser,\n\tuser\nON\n\tuser\nUSING $f();\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Select) {
    TCases cases = {
        {"select 1",
            "SELECT\n\t1\n;\n"},
        {"select 1,",
            "SELECT\n\t1,\n;\n"},
        {"select 1 as x",
            "SELECT\n\t1 AS x\n;\n"},
        {"select *",
            "SELECT\n\t*\n;\n"},
        {"select a.*",
            "SELECT\n\ta.*\n;\n"},
        {"select * without a",
            "SELECT\n\t*\nWITHOUT\n\ta\n;\n"},
        {"select * without a,b",
            "SELECT\n\t*\nWITHOUT\n\ta,\n\tb\n;\n"},
        {"select * without a,",
            "SELECT\n\t*\nWITHOUT\n\ta,\n;\n"},
        {"select 1 from user",
            "SELECT\n\t1\nFROM\n\tuser\n;\n"},
        {"select 1 from plato.user",
            "SELECT\n\t1\nFROM\n\tplato.user\n;\n"},
        {"select 1 from $user",
            "SELECT\n\t1\nFROM\n\t$user\n;\n"},
        {"select 1 from @user",
            "SELECT\n\t1\nFROM\n\t@user\n;\n"},
        {"select 1 from @$user",
            "SELECT\n\t1\nFROM\n\t@$user\n;\n"},
        {"select 1 from user view user",
            "SELECT\n\t1\nFROM\n\tuser VIEW user\n;\n"},
        {"select 1 from user as user",
            "SELECT\n\t1\nFROM\n\tuser AS user\n;\n"},
        {"select 1 from user as user(user)",
            "SELECT\n\t1\nFROM\n\tuser AS user (\n\t\tuser\n\t)\n;\n"},
        {"select 1 from user as user(user, user)",
            "SELECT\n\t1\nFROM\n\tuser AS user (\n\t\tuser,\n\t\tuser\n\t)\n;\n"},
        {"select 1 from user with user=user",
            "SELECT\n\t1\nFROM\n\tuser WITH user = user\n;\n"},
        {"select 1 from user with (user=user, user=user)",
            "SELECT\n\t1\nFROM\n\tuser WITH (\n\t\tuser = user,\n\t\tuser = user\n\t)\n;\n"},
        {"select 1 from user sample 0.1",
            "SELECT\n\t1\nFROM\n\tuser\n\tSAMPLE 0.1\n;\n"},
        {"select 1 from user tablesample system(0.1)",
            "SELECT\n\t1\nFROM\n\tuser\n\tTABLESAMPLE SYSTEM (0.1)\n;\n"},
        {"select 1 from user tablesample bernoulli(0.1) repeatable(10)",
            "SELECT\n\t1\nFROM\n\tuser\n\tTABLESAMPLE BERNOULLI (0.1) REPEATABLE (10)\n;\n"},
        {"select 1 from user flatten columns",
            "SELECT\n\t1\nFROM\n\tuser\n\tFLATTEN COLUMNS\n;\n"},
        {"select 1 from user flatten list by user",
            "SELECT\n\t1\nFROM\n\tuser\n\tFLATTEN LIST BY user\n;\n"},
        {"select 1 from user flatten list by (user,user)",
            "SELECT\n\t1\nFROM\n\tuser\n\tFLATTEN LIST BY (\n\t\tuser,\n\t\tuser\n\t)\n;\n"},
        {"select 1 from $user(1,2)",
            "SELECT\n\t1\nFROM\n\t$user(1, 2)\n;\n"},
        {"select 1 from $user(1,2) view user",
            "SELECT\n\t1\nFROM\n\t$user(1, 2) VIEW user\n;\n"},
        {"select 1 from range('a','b')",
            "SELECT\n\t1\nFROM\n\trange('a', 'b')\n;\n"},
        {"from user select 1",
            "FROM\n\tuser\nSELECT\n\t1\n;\n"},
        {"select * from user as a join user as b on a.x=b.y",
            "SELECT\n\t*\nFROM\n\tuser AS a\nJOIN\n\tuser AS b\nON\n\ta.x == b.y\n;\n"},
        {"select * from user as a join user as b using(x)",
            "SELECT\n\t*\nFROM\n\tuser AS a\nJOIN\n\tuser AS b\nUSING (x);\n"},
        {"select * from any user as a full join user as b on a.x=b.y",
            "SELECT\n\t*\nFROM ANY\n\tuser AS a\nFULL JOIN\n\tuser AS b\nON\n\ta.x == b.y\n;\n"},
        {"select * from user as a left join any user as b on a.x=b.y",
            "SELECT\n\t*\nFROM\n\tuser AS a\nLEFT JOIN ANY\n\tuser AS b\nON\n\ta.x == b.y\n;\n"},
        {"select * from any user as a right join any user as b on a.x=b.y",
            "SELECT\n\t*\nFROM ANY\n\tuser AS a\nRIGHT JOIN ANY\n\tuser AS b\nON\n\ta.x == b.y\n;\n"},
        {"select * from user as a cross join user as b",
            "SELECT\n\t*\nFROM\n\tuser AS a\nCROSS JOIN\n\tuser AS b\n;\n"},
        {"select 1 from user where key = 1",
            "SELECT\n\t1\nFROM\n\tuser\nWHERE\n\tkey == 1\n;\n"},
        {"select 1 from user having count(*) = 1",
            "SELECT\n\t1\nFROM\n\tuser\nHAVING\n\tcount(*) == 1\n;\n"},
        {"select 1 from user group by key",
            "SELECT\n\t1\nFROM\n\tuser\nGROUP BY\n\tkey\n;\n"},
        {"select 1 from user group compact by key, value as v",
            "SELECT\n\t1\nFROM\n\tuser\nGROUP COMPACT BY\n\tkey,\n\tvalue AS v\n;\n"},
        {"select 1 from user group by key with combine",
            "SELECT\n\t1\nFROM\n\tuser\nGROUP BY\n\tkey\n\tWITH combine\n;\n"},
        {"select 1 from user order by key asc",
            "SELECT\n\t1\nFROM\n\tuser\nORDER BY\n\tkey ASC\n;\n"},
        {"select 1 from user order by key, value desc",
            "SELECT\n\t1\nFROM\n\tuser\nORDER BY\n\tkey,\n\tvalue DESC\n;\n"},
        {"select 1 from user assume order by key",
            "SELECT\n\t1\nFROM\n\tuser\nASSUME ORDER BY\n\tkey\n;\n"},
        {"select 1 from user window w1 as (), w2 as ()",
            "SELECT\n\t1\nFROM\n\tuser\nWINDOW\n\tw1 AS (),\n\tw2 AS ()\n;\n"},
        {"select 1 from user window w1 as (user)",
            "SELECT\n\t1\nFROM\n\tuser\nWINDOW\n\tw1 AS (\n\t\tuser\n\t)\n;\n"},
        {"select 1 from user window w1 as (partition by user)",
            "SELECT\n\t1\nFROM\n\tuser\nWINDOW\n\tw1 AS (\n\t\tPARTITION BY\n\t\t\tuser\n\t)\n;\n"},
        {"select 1 from user window w1 as (partition by user, user)",
            "SELECT\n\t1\nFROM\n\tuser\nWINDOW\n\tw1 AS (\n\t\tPARTITION BY\n\t\t\tuser,\n\t\t\tuser\n\t)\n;\n"},
        {"select 1 from user window w1 as (order by user asc)",
            "SELECT\n\t1\nFROM\n\tuser\nWINDOW\n\tw1 AS (\n\t\tORDER BY\n\t\t\tuser ASC\n\t)\n;\n"},
        {"select 1 from user window w1 as (order by user, user desc)",
            "SELECT\n\t1\nFROM\n\tuser\nWINDOW\n\tw1 AS (\n\t\tORDER BY\n\t\t\tuser,\n\t\t\tuser DESC\n\t)\n;\n"},
        {"select 1 from user window w1 as (rows between 1 preceding and 1 following)",
            "SELECT\n\t1\nFROM\n\tuser\nWINDOW\n\tw1 AS (\n\t\tROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING\n\t)\n;\n"},
        {"select 1 limit 10",
            "SELECT\n\t1\nLIMIT 10;\n"},
        {"select 1 limit 10 offset 5",
            "SELECT\n\t1\nLIMIT 10 OFFSET 5;\n"},
        {"select 1 union all select 2",
            "SELECT\n\t1\nUNION ALL\nSELECT\n\t2\n;\n" },
        {"select * from $user where key == 1 -- comment",
            "SELECT\n\t*\nFROM\n\t$user\nWHERE\n\tkey == 1 -- comment\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(CompositeTypesAndQuestions) {
    TCases cases = {
        {"declare $_x AS list<int32>??;declare $_y AS int32 ? ? ;select 1<>2, 1??2,"
            "formattype(list<int32>), formattype(resource<user>),formattype(tuple<>), formattype(tuple<  >), formattype(int32 ? ? )",
            "DECLARE $_x AS list<int32>??;\nDECLARE $_y AS int32??;\n\nSELECT\n\t1 != 2,\n\t1 ?? 2,\n\tformattype(list<int32>),"
        "\n\tformattype(resource<user>),\n\tformattype(tuple<>),\n\tformattype(tuple< >),\n\tformattype(int32??" ")\n;\n"
        },
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Lambda) {
    TCases cases = {
        {"$f=($a,$b)->{$x=$a+$b;return $a*$x};$g=($a,$b?)->($a+$b??0);select $f(10,4),$g(1,2);",
            "$f = ($a, $b) -> {\n\t$x = $a + $b;\n\tRETURN $a * $x;\n};\n\n"
            "$g = ($a, $b?) -> ($a + $b ?? 0);\n\n"
            "SELECT\n\t$f(10, 4),\n\t$g(1, 2)\n;\n"},
        {"$f=($arg)->{;$a=10;;$b=20;;;RETURN $a+$b}",
            "$f = ($arg) -> {\n\t$a = 10;\n\t$b = 20;\n\tRETURN $a + $b;\n};\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(NestedSelect) {
    TCases cases = {
        {"$x=select 1",
            "$x = (\n\tSELECT\n\t\t1\n);\n"},
        {"$x=(select 1)",
            "$x = (\n\tSELECT\n\t\t1\n);\n"},
        {"$x=((select 1))",
            "$x = (\n\t(\n\t\tSELECT\n\t\t\t1\n\t)\n);\n"},
        {"select 1 in (select 1)",
            "SELECT\n\t1 IN (\n\t\tSELECT\n\t\t\t1\n\t)\n;\n"},
        {"select 1 in ((select 1))",
            "SELECT\n\t1 IN (\n\t\t(\n\t\t\tSELECT\n\t\t\t\t1\n\t\t)\n\t)\n;\n"},
        {"select 1 in (\nselect 1)",
            "SELECT\n\t1 IN (\n\t\tSELECT\n\t\t\t1\n\t)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Cast) {
    TCases cases = {
        {"select cast(1 as string)","SELECT\n\tCAST(1 AS string)\n;\n"},
        {"select bitcast(1 as int32)","SELECT\n\tBITCAST(1 AS int32)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(StructLiteral) {
    TCases cases = {
        {"select <||>","SELECT\n\t<||>\n;\n"},
        {"select <|a:1|>","SELECT\n\t<|a: 1|>\n;\n"},
        {"select <|a:1,b:2|>","SELECT\n\t<|a: 1, b: 2|>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(TableHints) {
    TCases cases = {
        {"select * from plato.T with schema(foo int32, bar list<string>) where key is not null",
            "SELECT\n\t*\nFROM\n\tplato.T WITH SCHEMA (foo int32, bar list<string>)\nWHERE\n\tkey IS NOT NULL\n;\n"},
        {"select * from plato.T with schema struct<foo:integer, Bar:list<string?>> where key<0",
            "SELECT\n\t*\nFROM\n\tplato.T WITH SCHEMA struct<foo: integer, Bar: list<string?>>\nWHERE\n\tkey < 0\n;\n"},
        {"select * from plato.T with (foo=bar, x=$y, a=(a, b, c), u='aaa', schema (foo int32, bar list<string>))",
            "SELECT\n\t*\nFROM\n\tplato.T WITH (\n\t\tfoo = bar,\n\t\tx = $y,\n\t\ta = (a, b, c),\n\t\tu = 'aaa',\n\t\tSCHEMA (foo int32, bar list<string>)\n\t)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(BoolAsVariableName) {
    TCases cases = {
        {"$ False = True; select $ False;",
            "$False = TRUE;\n\nSELECT\n\t$False\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(WithSchemaEquals) {
    TCases cases = {
        {"select * from plato.T with (format= csv_with_names, schema=(year int32 Null, month String, day String not   null, a Utf8, b Uint16));",
            "SELECT\n\t*\nFROM\n\tplato.T WITH (\n\t\tformat = csv_with_names,\n\t\tSCHEMA = (year int32 NULL, month String, day String NOT NULL, a Utf8, b Uint16)\n\t)\n;\n"},
            };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(SquareBrackets) {
    TCases cases = {
        {"select a[0]",
            "SELECT\n\ta[0]\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineList) {
    TCases cases = {
        {"select [\n]",
            "SELECT\n\t[\n\t]\n;\n"},
        {"select [1\n]",
            "SELECT\n\t[\n\t\t1\n\t]\n;\n"},
        {"select [\n1]",
            "SELECT\n\t[\n\t\t1\n\t]\n;\n"},
        {"select [1,\n]",
            "SELECT\n\t[\n\t\t1,\n\t]\n;\n"},
        {"select [1\n,]",
            "SELECT\n\t[\n\t\t1,\n\t]\n;\n"},
        {"select [\n1,]",
            "SELECT\n\t[\n\t\t1,\n\t]\n;\n"},
        {"select [1,2,\n3,4]",
            "SELECT\n\t[\n\t\t1, 2,\n\t\t3, 4\n\t]\n;\n"},
        {"select [1,2,\n3,4,]",
            "SELECT\n\t[\n\t\t1, 2,\n\t\t3, 4,\n\t]\n;\n"},
        {"select [1,2\n,3,\n4\n,5]",
            "SELECT\n\t[\n\t\t1, 2,\n\t\t3,\n\t\t4,\n\t\t5\n\t]\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineTuple) {
    TCases cases = {
        {"select (\n)",
            "SELECT\n\t(\n\t)\n;\n"},
        {"select (1,\n)",
            "SELECT\n\t(\n\t\t1,\n\t)\n;\n"},
        {"select (1\n,)",
            "SELECT\n\t(\n\t\t1,\n\t)\n;\n"},
        {"select (\n1,)",
            "SELECT\n\t(\n\t\t1,\n\t)\n;\n"},
        {"select (1,2,\n3,4)",
            "SELECT\n\t(\n\t\t1, 2,\n\t\t3, 4\n\t)\n;\n"},
        {"select (1,2,\n3,4,)",
            "SELECT\n\t(\n\t\t1, 2,\n\t\t3, 4,\n\t)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineSet) {
    TCases cases = {
        {"select {\n}",
            "SELECT\n\t{\n\t}\n;\n"},
        {"select {1\n}",
            "SELECT\n\t{\n\t\t1\n\t}\n;\n"},
        {"select {\n1}",
            "SELECT\n\t{\n\t\t1\n\t}\n;\n"},
        {"select {1,\n}",
            "SELECT\n\t{\n\t\t1,\n\t}\n;\n"},
        {"select {1\n,}",
            "SELECT\n\t{\n\t\t1,\n\t}\n;\n"},
        {"select {\n1,}",
            "SELECT\n\t{\n\t\t1,\n\t}\n;\n"},
        {"select {1,2,\n3,4}",
            "SELECT\n\t{\n\t\t1, 2,\n\t\t3, 4\n\t}\n;\n"},
        {"select {1,2,\n3,4,}",
            "SELECT\n\t{\n\t\t1, 2,\n\t\t3, 4,\n\t}\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineDict) {
    TCases cases = {
        {"select {0:1\n}",
            "SELECT\n\t{\n\t\t0: 1\n\t}\n;\n"},
        {"select {\n0:1}",
            "SELECT\n\t{\n\t\t0: 1\n\t}\n;\n"},
        {"select {0:1,\n}",
            "SELECT\n\t{\n\t\t0: 1,\n\t}\n;\n"},
        {"select {0:1\n,}",
            "SELECT\n\t{\n\t\t0: 1,\n\t}\n;\n"},
        {"select {\n0:1,}",
            "SELECT\n\t{\n\t\t0: 1,\n\t}\n;\n"},
        {"select {10:1,20:2,\n30:3,40:4}",
            "SELECT\n\t{\n\t\t10: 1, 20: 2,\n\t\t30: 3, 40: 4\n\t}\n;\n"},
        {"select {10:1,20:2,\n30:3,40:4,}",
            "SELECT\n\t{\n\t\t10: 1, 20: 2,\n\t\t30: 3, 40: 4,\n\t}\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineFuncCall) {
    TCases cases = {
        {"select f(\n)",
            "SELECT\n\tf(\n\t)\n;\n"},
        {"select f(1\n)",
            "SELECT\n\tf(\n\t\t1\n\t)\n;\n"},
        {"select f(\n1)",
            "SELECT\n\tf(\n\t\t1\n\t)\n;\n"},
        {"select f(1,\n)",
            "SELECT\n\tf(\n\t\t1,\n\t)\n;\n"},
        {"select f(1\n,)",
            "SELECT\n\tf(\n\t\t1,\n\t)\n;\n"},
        {"select f(\n1,)",
            "SELECT\n\tf(\n\t\t1,\n\t)\n;\n"},
        {"select f(1,2,\n3,4)",
            "SELECT\n\tf(\n\t\t1, 2,\n\t\t3, 4\n\t)\n;\n"},
        {"select f(1,2,\n3,4,)",
            "SELECT\n\tf(\n\t\t1, 2,\n\t\t3, 4,\n\t)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineStruct) {
    TCases cases = {
        {"select <|\n|>",
            "SELECT\n\t<|\n\t|>\n;\n"},
        {"select <|a:1\n|>",
            "SELECT\n\t<|\n\t\ta: 1\n\t|>\n;\n"},
        {"select <|\na:1|>",
            "SELECT\n\t<|\n\t\ta: 1\n\t|>\n;\n"},
        {"select <|a:1,\n|>",
            "SELECT\n\t<|\n\t\ta: 1,\n\t|>\n;\n"},
        {"select <|a:1\n,|>",
            "SELECT\n\t<|\n\t\ta: 1,\n\t|>\n;\n"},
        {"select <|\na:1,|>",
            "SELECT\n\t<|\n\t\ta: 1,\n\t|>\n;\n"},
        {"select <|a:1,b:2,\nc:3,d:4|>",
            "SELECT\n\t<|\n\t\ta: 1, b: 2,\n\t\tc: 3, d: 4\n\t|>\n;\n"},
        {"select <|a:1,b:2,\nc:3,d:4,|>",
            "SELECT\n\t<|\n\t\ta: 1, b: 2,\n\t\tc: 3, d: 4,\n\t|>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineListType) {
    TCases cases = {
        {"select list<int32\n>",
            "SELECT\n\tlist<\n\t\tint32\n\t>\n;\n"},
        {"select list<\nint32>",
            "SELECT\n\tlist<\n\t\tint32\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineOptionalType) {
    TCases cases = {
        {"select optional<int32\n>",
            "SELECT\n\toptional<\n\t\tint32\n\t>\n;\n"},
        {"select optional<\nint32>",
            "SELECT\n\toptional<\n\t\tint32\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineStreamType) {
    TCases cases = {
        {"select stream<int32\n>",
            "SELECT\n\tstream<\n\t\tint32\n\t>\n;\n"},
        {"select stream<\nint32>",
            "SELECT\n\tstream<\n\t\tint32\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineFlowType) {
    TCases cases = {
        {"select flow<int32\n>",
            "SELECT\n\tflow<\n\t\tint32\n\t>\n;\n"},
        {"select flow<\nint32>",
            "SELECT\n\tflow<\n\t\tint32\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineSetType) {
    TCases cases = {
        {"select set<int32\n>",
            "SELECT\n\tset<\n\t\tint32\n\t>\n;\n"},
        {"select set<\nint32>",
            "SELECT\n\tset<\n\t\tint32\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineTupleType) {
    TCases cases = {
        {"select tuple<\n>",
            "SELECT\n\ttuple<\n\t\t \n\t>\n;\n"},
        {"select tuple<int32\n>",
            "SELECT\n\ttuple<\n\t\tint32\n\t>\n;\n"},
        {"select tuple<\nint32>",
            "SELECT\n\ttuple<\n\t\tint32\n\t>\n;\n"},
        {"select tuple<int32,\n>",
            "SELECT\n\ttuple<\n\t\tint32,\n\t>\n;\n"},
        {"select tuple<int32\n,>",
            "SELECT\n\ttuple<\n\t\tint32,\n\t>\n;\n"},
        {"select tuple<\nint32,>",
            "SELECT\n\ttuple<\n\t\tint32,\n\t>\n;\n"},
        {"select tuple<\nint32,string,\ndouble,bool>",
            "SELECT\n\ttuple<\n\t\tint32, string,\n\t\tdouble, bool\n\t>\n;\n"},
        {"select tuple<\nint32,string,\ndouble,bool,>",
            "SELECT\n\ttuple<\n\t\tint32, string,\n\t\tdouble, bool,\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineStructType) {
    TCases cases = {
        {"select struct<\n>",
            "SELECT\n\tstruct<\n\t\t \n\t>\n;\n"},
        {"select struct<a:int32\n>",
            "SELECT\n\tstruct<\n\t\ta: int32\n\t>\n;\n"},
        {"select struct<\na:int32>",
            "SELECT\n\tstruct<\n\t\ta: int32\n\t>\n;\n"},
        {"select struct<a:int32,\n>",
            "SELECT\n\tstruct<\n\t\ta: int32,\n\t>\n;\n"},
        {"select struct<a:int32\n,>",
            "SELECT\n\tstruct<\n\t\ta: int32,\n\t>\n;\n"},
        {"select struct<\na:int32,>",
            "SELECT\n\tstruct<\n\t\ta: int32,\n\t>\n;\n"},
        {"select struct<\na:int32,b:string,\nc:double,d:bool>",
            "SELECT\n\tstruct<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool\n\t>\n;\n"},
        {"select struct<\na:int32,b:string,\nc:double,d:bool,>",
            "SELECT\n\tstruct<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool,\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineVariantOverTupleType) {
    TCases cases = {
        {"select variant<int32\n>",
            "SELECT\n\tvariant<\n\t\tint32\n\t>\n;\n"},
        {"select variant<\nint32>",
            "SELECT\n\tvariant<\n\t\tint32\n\t>\n;\n"},
        {"select variant<int32,\n>",
            "SELECT\n\tvariant<\n\t\tint32,\n\t>\n;\n"},
        {"select variant<int32\n,>",
            "SELECT\n\tvariant<\n\t\tint32,\n\t>\n;\n"},
        {"select variant<\nint32,>",
            "SELECT\n\tvariant<\n\t\tint32,\n\t>\n;\n"},
        {"select variant<\nint32,string,\ndouble,bool>",
            "SELECT\n\tvariant<\n\t\tint32, string,\n\t\tdouble, bool\n\t>\n;\n"},
        {"select variant<\nint32,string,\ndouble,bool,>",
            "SELECT\n\tvariant<\n\t\tint32, string,\n\t\tdouble, bool,\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineVariantOverStructType) {
    TCases cases = {
        {"select variant<a:int32\n>",
            "SELECT\n\tvariant<\n\t\ta: int32\n\t>\n;\n"},
        {"select variant<\na:int32>",
            "SELECT\n\tvariant<\n\t\ta: int32\n\t>\n;\n"},
        {"select variant<a:int32,\n>",
            "SELECT\n\tvariant<\n\t\ta: int32,\n\t>\n;\n"},
        {"select variant<a:int32\n,>",
            "SELECT\n\tvariant<\n\t\ta: int32,\n\t>\n;\n"},
        {"select variant<\na:int32,>",
            "SELECT\n\tvariant<\n\t\ta: int32,\n\t>\n;\n"},
        {"select variant<\na:int32,b:string,\nc:double,d:bool>",
            "SELECT\n\tvariant<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool\n\t>\n;\n"},
        {"select variant<\na:int32,b:string,\nc:double,d:bool,>",
            "SELECT\n\tvariant<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool,\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineEnum) {
    TCases cases = {
        {"select enum<a\n>",
            "SELECT\n\tenum<\n\t\ta\n\t>\n;\n"},
        {"select enum<\na>",
            "SELECT\n\tenum<\n\t\ta\n\t>\n;\n"},
        {"select enum<a,\n>",
            "SELECT\n\tenum<\n\t\ta,\n\t>\n;\n"},
        {"select enum<a\n,>",
            "SELECT\n\tenum<\n\t\ta,\n\t>\n;\n"},
        {"select enum<\na,>",
            "SELECT\n\tenum<\n\t\ta,\n\t>\n;\n"},
        {"select enum<\na,b,\nc,d>",
            "SELECT\n\tenum<\n\t\ta, b,\n\t\tc, d\n\t>\n;\n"},
        {"select enum<\na,b,\nc,d,>",
            "SELECT\n\tenum<\n\t\ta, b,\n\t\tc, d,\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineResourceType) {
    TCases cases = {
        {"select resource<foo\n>",
            "SELECT\n\tresource<\n\t\tfoo\n\t>\n;\n"},
        {"select resource<\nfoo>",
            "SELECT\n\tresource<\n\t\tfoo\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineTaggedType) {
    TCases cases = {
        {"select tagged<int32,foo\n>",
            "SELECT\n\ttagged<\n\t\tint32, foo\n\t>\n;\n"},
        {"select tagged<int32,\nfoo>",
            "SELECT\n\ttagged<\n\t\tint32,\n\t\tfoo\n\t>\n;\n"},
        {"select tagged<int32\n,foo>",
            "SELECT\n\ttagged<\n\t\tint32,\n\t\tfoo\n\t>\n;\n"},
        {"select tagged<\nint32,foo>",
            "SELECT\n\ttagged<\n\t\tint32, foo\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineDictType) {
    TCases cases = {
        {"select dict<int32,string\n>",
            "SELECT\n\tdict<\n\t\tint32, string\n\t>\n;\n"},
        {"select dict<int32,\nstring>",
            "SELECT\n\tdict<\n\t\tint32,\n\t\tstring\n\t>\n;\n"},
        {"select dict<int32\n,string>",
            "SELECT\n\tdict<\n\t\tint32,\n\t\tstring\n\t>\n;\n"},
        {"select dict<\nint32,string>",
            "SELECT\n\tdict<\n\t\tint32, string\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiLineCallableType) {
    TCases cases = {
        {"select callable<()->int32\n>",
            "SELECT\n\tcallable<\n\t\t() -> int32\n\t>\n;\n"},
        {"select callable<\n()->int32>",
            "SELECT\n\tcallable<\n\t\t() -> int32\n\t>\n;\n"},
        {"select callable<\n(int32)->int32>",
            "SELECT\n\tcallable<\n\t\t(int32) -> int32\n\t>\n;\n"},
        {"select callable<\n(int32,\ndouble)->int32>",
            "SELECT\n\tcallable<\n\t\t(\n\t\t\tint32,\n\t\t\tdouble\n\t\t) -> int32\n\t>\n;\n"},
        {"select callable<\n(int32\n,double)->int32>",
            "SELECT\n\tcallable<\n\t\t(\n\t\t\tint32,\n\t\t\tdouble\n\t\t) -> int32\n\t>\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(UnaryOp) {
    TCases cases = {
        {"select -x,+x,~x,-1,-1.0,+1,+1.0,~1u",
            "SELECT\n\t-x,\n\t+x,\n\t~x,\n\t-1,\n\t-1.0,\n\t+1,\n\t+1.0,\n\t~1u\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MatchRecognize) {
    TCases cases = {{R"(
pragma FeatureR010="prototype";
USE plato;
SELECT
    *
FROM Input MATCH_RECOGNIZE(
    PARTITION BY a, b, c
    ORDER BY ts
    MEASURES LAST(B1.ts) AS b1, LAST(B3.ts) AS b3
    ONE ROW PER MATCH AFTER MATCH SKIP TO NEXT ROW INITIAL
    PATTERN ( A B2 + B3 )
    SUBSET U = (C, D), W = (Q, P)
    DEFINE A as A, B as B
);
)",
R"(PRAGMA FeatureR010 = 'prototype';

USE plato;

SELECT
    *
FROM
    Input MATCH_RECOGNIZE (
        PARTITION BY
            a,
            b,
            c
        ORDER BY
            ts
        MEASURES
            LAST(B1.ts) AS b1,
            LAST(B3.ts) AS b3
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        INITIAL PATTERN (A B2 + B3)
        SUBSET
            U = (C, D),
            W = (Q, P)
        DEFINE
            A AS A,
            B AS B
    )
;
)"
    }};
    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(CreateTableTrailingComma) {
    TCases cases = {
        {"CREATE TABLE tableName (Key Uint32, PRIMARY KEY (Key),);",
            "CREATE TABLE tableName (\n\tKey Uint32,\n\tPRIMARY KEY (Key),\n);\n"},
        {"CREATE TABLE tableName (Key Uint32,);",
            "CREATE TABLE tableName (\n\tKey Uint32,\n);\n"},
    };
    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Union) {
    TCases cases = {
        {"select 1 union all select 2 union select 3 union all select 4 union select 5",
            "SELECT\n\t1\nUNION ALL\nSELECT\n\t2\nUNION\nSELECT\n\t3\nUNION ALL\nSELECT\n\t4\nUNION\nSELECT\n\t5\n;\n"},
        {"select 1 union all (select 2)",
            "SELECT\n\t1\nUNION ALL\n(\n\tSELECT\n\t\t2\n);\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(CommentAfterLastSelect) {
    TCases cases = {
        {"SELECT 1--comment\n",
            "SELECT\n\t1 --comment\n;\n"},
        {"SELECT 1\n\n--comment\n",
            "SELECT\n\t1\n\n--comment\n;\n"},
        {"SELECT 1\n\n--comment",
            "SELECT\n\t1\n\n--comment\n;\n"},
        {"SELECT * FROM Input /* comment */\n\n\n",
            "SELECT\n\t*\nFROM\n\tInput /* comment */\n;\n"},
        {"SELECT * FROM Input\n\n\n\n/* comment */\n\n\n",
            "SELECT\n\t*\nFROM\n\tInput\n\n/* comment */;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(WindowFunctionInsideExpr) {
    TCases cases = {
        {"SELECT CAST(ROW_NUMBER() OVER () AS String) AS x,\nFROM Input;",
            "SELECT\n\tCAST(ROW_NUMBER() OVER () AS String) AS x,\nFROM\n\tInput\n;\n"},
        {"SELECT CAST(ROW_NUMBER() OVER (PARTITION BY key) AS String) AS x,\nFROM Input;",
            "SELECT\n\tCAST(\n\t\tROW_NUMBER() OVER (\n\t\t\tPARTITION BY\n\t\t\t\tkey\n\t\t) AS String\n\t) AS x,\nFROM\n\tInput\n;\n"},
        {"SELECT CAST(ROW_NUMBER() OVER (users) AS String) AS x,\nFROM Input;",
            "SELECT\n\tCAST(\n\t\tROW_NUMBER() OVER (\n\t\t\tusers\n\t\t) AS String\n\t) AS x,\nFROM\n\tInput\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(ExistsExpr) {
    TCases cases = {
        {"SELECT EXISTS (SELECT 1);",
            "SELECT\n\tEXISTS (\n\t\tSELECT\n\t\t\t1\n\t)\n;\n"},
        {"SELECT CAST(EXISTS(SELECT 1) AS Int) AS x,\nFROM Input;",
            "SELECT\n\tCAST(\n\t\tEXISTS (\n\t\t\tSELECT\n\t\t\t\t1\n\t\t) AS Int\n\t) AS x,\nFROM\n\tInput\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(LambdaInsideExpr) {
    TCases cases = {
        {"SELECT ListMap(AsList(1,2),($x)->{return $x+1});",
            "SELECT\n\tListMap(\n\t\tAsList(1, 2), ($x) -> {\n\t\t\tRETURN $x + 1;\n\t\t}\n\t)\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(CaseExpr) {
    TCases cases = {
        {"SELECT CASE WHEN 1 == 2 THEN 3 WHEN 4 == 5 THEN 6 WHEN 7 == 8 THEN 9 ELSE 10 END;",
            "SELECT\n\tCASE\n\t\tWHEN 1 == 2 THEN 3\n\t\tWHEN 4 == 5 THEN 6\n\t\tWHEN 7 == 8 THEN 9\n\t\tELSE 10\n\tEND\n;\n"},
        {"SELECT CAST(CASE WHEN 1 == 2 THEN 3 WHEN 4 == 5 THEN 6 ELSE 10 END AS String);",
            "SELECT\n\tCAST(\n\t\tCASE\n\t\t\tWHEN 1 == 2 THEN 3\n\t\t\tWHEN 4 == 5 THEN 6\n\t\t\tELSE 10\n\t\tEND AS String\n\t)\n;\n"},
        {"SELECT CASE x WHEN 1 THEN 2 WHEN 3 THEN 4 WHEN 5 THEN 6 ELSE 10 END;",
            "SELECT\n\tCASE x\n\t\tWHEN 1 THEN 2\n\t\tWHEN 3 THEN 4\n\t\tWHEN 5 THEN 6\n\t\tELSE 10\n\tEND\n;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(MultiTokenOperations) {
    TCases cases = {
        {"$x = 1 >>| 2;",
            "$x = 1 >>| 2;\n"},
        {"$x = 1 >> 2;",
            "$x = 1 >> 2;\n"},
        {"$x = 1 ?? 2;",
            "$x = 1 ?? 2;\n"},
        {"$x = 1 >  /*comment*/  >  /*comment*/  | 2;",
            "$x = 1 > /*comment*/> /*comment*/| 2;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(OperatorNewlines) {
    TCases cases = {
        {"$x = TRUE\nOR\nFALSE;",
            "$x = TRUE\n\tOR\n\tFALSE;\n"},
        {"$x = TRUE OR\nFALSE;",
            "$x = TRUE\n\tOR FALSE;\n"},
        {"$x = TRUE\nOR FALSE;",
            "$x = TRUE\n\tOR FALSE;\n"},
        {"$x = 1+\n2*\n3;",
            "$x = 1\n\t+ 2\n\t* 3;\n"},
        {"$x = 1\n+\n2\n*3\n*5\n+\n4;",
            "$x = 1\n\t+\n\t2\n\t* 3\n\t* 5\n\t+\n\t4;\n"},
        {"$x = 1\n+2+3+4\n+5+6+7+\n\n8+9+10;",
            "$x = 1\n\t+ 2 + 3 + 4\n\t+ 5 + 6 + 7\n\t+ 8 + 9 + 10;\n"},
        {"$x = TRUE\nAND\nTRUE OR\nFALSE\nAND TRUE\nOR FALSE\nAND TRUE\nOR FALSE;",
            "$x = TRUE\n\tAND\n\tTRUE\n\tOR FALSE\n\tAND TRUE\n\tOR FALSE\n\tAND TRUE\n\tOR FALSE;\n"},
        {"$x = 1 -- comment\n+ 2;",
            "$x = 1 -- comment\n\t+ 2;\n"},
        {"$x = 1 -- comment\n+ -- comment\n2;",
            "$x = 1 -- comment\n\t+ -- comment\n\t2;\n"},
        {"$x = 1 + -- comment\n2;",
            "$x = 1\n\t+ -- comment\n\t2;\n"},
        {"$x = 1\n>\n>\n|\n2;",
            "$x = 1\n\t>>|\n\t2;\n"},
        {"$x = 1\n?? 2 ??\n3\n??\n4 +\n5\n*\n6 +\n7 ??\n8;",
            "$x = 1 ??\n\t2 ??\n\t3\n\t??\n\t4\n\t+ 5\n\t*\n\t6\n\t+ 7 ??\n\t8;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(ObfuscateSelect) {
    TCases cases = {
        {"select 1;",
            "SELECT\n\t0\n;\n"},
        {"select true;",
            "SELECT\n\tFALSE\n;\n"},
        {"select 'foo';",
            "SELECT\n\t'str'\n;\n"},
        {"select 3.0;",
            "SELECT\n\t0.0\n;\n"},
        {"select col;",
            "SELECT\n\tid\n;\n"},
        {"select * from tab;",
            "SELECT\n\t*\nFROM\n\tid\n;\n"},
        {"select cast(col as int32);",
            "SELECT\n\tCAST(id AS int32)\n;\n"},
        {"select func(col);",
            "SELECT\n\tfunc(id)\n;\n"},
        {"select mod::func(col);",
            "SELECT\n\tmod::func(id)\n;\n"},
        {"declare $a as int32;",
            "DECLARE $id AS int32;\n"},
        {"select * from `logs/of/bob` where pwd='foo';",
            "SELECT\n\t*\nFROM\n\tid\nWHERE\n\tid == 'str'\n;\n"},
        {"select $f();",
            "SELECT\n\t$id()\n;\n"},
    };

    TSetup setup;
    setup.Run(cases, NSQLFormat::EFormatMode::Obfuscate);
}

Y_UNIT_TEST(ObfuscatePragma) {
    TCases cases = {
        {"pragma a=1",
            "PRAGMA id = 0;\n"},
        {"pragma a='foo';",
            "PRAGMA id = 'str';\n"},
        {"pragma a=true;",
            "PRAGMA id = FALSE;\n"},
        {"pragma a=$foo;",
            "PRAGMA id = $id;\n"},
        {"pragma a=foo;",
            "PRAGMA id = id;\n"},
    };

    TSetup setup;
    setup.Run(cases, NSQLFormat::EFormatMode::Obfuscate);
}

Y_UNIT_TEST(CreateView) {
    TCases cases = {{
            "creAte vIEw TheView As SELect 1",
            "CREATE VIEW TheView AS\nSELECT\n\t1\n;\n"
        }, {
            "creAte vIEw If Not ExIsTs TheView As SELect 1",
            "CREATE VIEW IF NOT EXISTS TheView AS\nSELECT\n\t1\n;\n"
        }, {
            "creAte vIEw TheView wiTh (option = tRuE) As SELect 1",
            "CREATE VIEW TheView WITH (option = TRUE) AS\nSELECT\n\t1\n;\n"
        }
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(DropView) {
    TCases cases = {{
            "dRop viEW theVIEW",
            "DROP VIEW theVIEW;\n"
        }, {
            "dRop viEW iF EXistS theVIEW",
            "DROP VIEW IF EXISTS theVIEW;\n"
        }
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(ResourcePoolOperations) {
    TCases cases = {
        {"creAte reSourCe poOl naMe With (a = \"b\")",
            "CREATE RESOURCE POOL naMe WITH (a = 'b');\n"},
        {"create resource pool eds with (a=\"a\",b=\"b\",c = true)",
            "CREATE RESOURCE POOL eds WITH (\n\ta = 'a',\n\tb = 'b',\n\tc = TRUE\n);\n"},
        {"alTer reSOurcE poOl naMe resEt (b, c), seT (x=y, z=false)",
            "ALTER RESOURCE POOL naMe\n\tRESET (b, c),\n\tSET (x = y, z = FALSE)\n;\n"},
        {"alter resource pool eds reset (a), set (x=y)",
            "ALTER RESOURCE POOL eds\n\tRESET (a),\n\tSET (x = y)\n;\n"},
        {"dRop reSourCe poOl naMe",
            "DROP RESOURCE POOL naMe;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(BackupCollectionOperations) {
    TCases cases = {
        {"creAte  BackuP colLection `-naMe` wIth (a = \"b\")",
            "CREATE BACKUP COLLECTION `-naMe` WITH (a = 'b');\n"},
        {"creAte  BackuP colLection `-naMe`     DATabase wIth (a = 'b')",
            "CREATE BACKUP COLLECTION `-naMe` DATABASE WITH (a = 'b');\n"},
        {"creAte  BackuP colLection    `-naMe`   (   tabLe      `tbl1`      , TablE `tbl2`) wIth (a = \"b\")",
            "CREATE BACKUP COLLECTION `-naMe` (TABLE `tbl1`, TABLE `tbl2`) WITH (a = 'b');\n"},
        {"alTer bACKuP coLLECTION naMe resEt (b, c), seT (x=y, z=false)",
            "ALTER BACKUP COLLECTION naMe\n\tRESET (b, c),\n\tSET (x = y, z = FALSE)\n;\n"},
        {"alTer bACKuP coLLECTION naMe aDD         DATAbase",
            "ALTER BACKUP COLLECTION naMe\n\tADD DATABASE\n;\n"},
        {"alTer bACKuP coLLECTION naMe DRoP    \n\n    DaTAbase",
            "ALTER BACKUP COLLECTION naMe\n\tDROP DATABASE\n;\n"},
        {"alTer bACKuP coLLECTION naMe add    \n\n    tablE\n\tsometable,drOp TABle `other`",
            "ALTER BACKUP COLLECTION naMe\n\tADD TABLE sometable,\n\tDROP TABLE `other`\n;\n"},
        {"DROP backup collectiOn       `/some/path`",
            "DROP BACKUP COLLECTION `/some/path`;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Analyze) {
    TCases cases = {
        {"analyze table (col1, col2, col3)",
            "ANALYZE table (col1, col2, col3);\n"},
        {"analyze table",
            "ANALYZE table;\n"}
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(ResourcePoolClassifierOperations) {
    TCases cases = {
        {"creAte reSourCe poOl ClaSsiFIer naMe With (a = \"b\")",
            "CREATE RESOURCE POOL CLASSIFIER naMe WITH (a = 'b');\n"},
        {"create resource pool classifier eds with (a=\"a\",b=\"b\",c = true)",
            "CREATE RESOURCE POOL CLASSIFIER eds WITH (\n\ta = 'a',\n\tb = 'b',\n\tc = TRUE\n);\n"},
        {"alTer reSOurcE poOl ClaSsiFIer naMe resEt (b, c), seT (x=y, z=false)",
            "ALTER RESOURCE POOL CLASSIFIER naMe\n\tRESET (b, c),\n\tSET (x = y, z = FALSE)\n;\n"},
        {"alter resource pool classifier eds reset (a), set (x=y)",
            "ALTER RESOURCE POOL CLASSIFIER eds\n\tRESET (a),\n\tSET (x = y)\n;\n"},
        {"dRop reSourCe poOl ClaSsiFIer naMe",
            "DROP RESOURCE POOL CLASSIFIER naMe;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Backup) {
    TCases cases = {
        {"\tBaCKup\n\n TestCollection      incremENTAl",
            "BACKUP TestCollection INCREMENTAL;\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(Restore) {
    TCases cases = {
        {"resToRe\n\n\n TestCollection       aT\n  \t \n     '2024-06-16_20-14-02'",
            "RESTORE TestCollection AT '2024-06-16_20-14-02';\n"},
    };

    TSetup setup;
    setup.Run(cases);
}

Y_UNIT_TEST(AnsiLexer) {
    TCases cases = {
        {"select 'a', \"a\" from (select 1 as \"a\")",
            "SELECT\n\t'a',\n\t\"a\"\nFROM (\n\tSELECT\n\t\t1 AS \"a\"\n);\n"},
    };

    TSetup setup(/* ansiLexer = */ true);
    setup.Run(cases);
}
