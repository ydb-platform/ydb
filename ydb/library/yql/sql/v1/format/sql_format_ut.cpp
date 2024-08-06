#include <library/cpp/testing/unittest/registar.h>

#include "sql_format.h"

#include <google/protobuf/arena.h>
#include <util/string/subst.h>
#include <util/string/join.h>

namespace {

using TCases = TVector<std::pair<TString, TString>>;

struct TSetup {
    TSetup() {
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &Arena;
        Formatter = NSQLFormat::MakeSqlFormatter(settings);
    }

    void Run(const TCases& cases, NSQLFormat::EFormatMode mode = NSQLFormat::EFormatMode::Pretty) {
        for (const auto& c : cases) {
            NYql::TIssues issues;
            TString formatted;
            auto res = Formatter->Format(c.first, formatted, issues, mode);
            UNIT_ASSERT_C(res, issues.ToString());
            auto expected = c.second;
            SubstGlobal(expected, "\t", TString(NSQLFormat::OneIndent, ' '));
            UNIT_ASSERT_NO_DIFF(formatted, expected);

            TString formatted2;
            auto res2 = Formatter->Format(formatted, formatted2, issues);
            UNIT_ASSERT_C(res2, issues.ToString());
            UNIT_ASSERT_NO_DIFF(formatted, formatted2);

            if (mode == NSQLFormat::EFormatMode::Pretty) {
                auto mutatedQuery = NSQLFormat::MutateQuery(c.first);
                auto res3 = Formatter->Format(mutatedQuery, formatted, issues);
                UNIT_ASSERT_C(res3, issues.ToString());
            }
        }
    }

    google::protobuf::Arena Arena;
    NSQLFormat::ISqlFormatter::TPtr Formatter;
};

}

Y_UNIT_TEST_SUITE(CheckSqlFormatter) {
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
            {"select a.1 .b from plato.foo;","SELECT\n\ta.1 .b\nFROM plato.foo;\n"},
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
            {"use plato;create user user;","USE plato;\n\nCREATE USER user;\n"},
            {"use plato;create user user encrypted password 'foo';","USE plato;\n\nCREATE USER user ENCRYPTED PASSWORD 'foo';\n"},
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
            {"export $foo;","EXPORT $foo;\n"},
            {"export $foo, $bar;","EXPORT $foo, $bar;\n"},
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
            {"values (1);","VALUES\n\t(1);\n"},
            {"values (1,2),(3,4);","VALUES\n\t(1, 2),\n\t(3, 4);\n"},
            {"values ('a\nb');","VALUES\n\t('a\nb');\n"},
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
            {"$x=1","$x = 1;\n"},
            {"$x,$y=(2,3)","$x, $y = (2, 3);\n"},
            {"$a = select 1 union all select 2","$a =\n\tSELECT\n\t\t1\n\tUNION ALL\n\tSELECT\n\t\t2;\n"},
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
            {"create table user(user) AS SELECT 1","CREATE TABLE user (\n\tuser\n)\nAS\nSELECT\n    1;\n"},
            {"create table user(user) AS VALUES (1), (2)","CREATE TABLE user (\n\tuser\n)\nAS\nVALUES\n    (1),\n    (2);\n"},
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
             "CREATE EXTERNAL DATA SOURCE usEr WITH (a = \"b\");\n"},
             {"creAte exTernAl daTa SouRce if not exists usEr With (a = \"b\")",
             "CREATE EXTERNAL DATA SOURCE IF NOT EXISTS usEr WITH (a = \"b\");\n"},
             {"creAte oR rePlaCe exTernAl daTa SouRce usEr With (a = \"b\")",
             "CREATE OR REPLACE EXTERNAL DATA SOURCE usEr WITH (a = \"b\");\n"},
             {"create external data source eds with (a=\"a\",b=\"b\",c = true)",
             "CREATE EXTERNAL DATA SOURCE eds WITH (\n\ta = \"a\",\n\tb = \"b\",\n\tc = TRUE\n);\n"},
             {"alter external data source eds set a true, reset (b, c), set (x=y, z=false)",
             "ALTER EXTERNAL DATA SOURCE eds\n\tSET a TRUE,\n\tRESET (b, c),\n\tSET (x = y, z = FALSE);\n"},
             {"alter external data source eds reset (a), set (x=y)",
             "ALTER EXTERNAL DATA SOURCE eds\n\tRESET (a),\n\tSET (x = y);\n"},
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

    Y_UNIT_TEST(ExternalTableOperations) {
        TCases cases = {
            {"creAte exTernAl TabLe usEr (a int) With (a = \"b\")",
             "CREATE EXTERNAL TABLE usEr (\n\ta int\n)\nWITH (a = \"b\");\n"},
             {"creAte oR rePlaCe exTernAl TabLe usEr (a int) With (a = \"b\")",
             "CREATE OR REPLACE EXTERNAL TABLE usEr (\n\ta int\n)\nWITH (a = \"b\");\n"},
            {"creAte exTernAl TabLe iF NOt Exists usEr (a int) With (a = \"b\")",
             "CREATE EXTERNAL TABLE IF NOT EXISTS usEr (\n\ta int\n)\nWITH (a = \"b\");\n"},
            {"create external table user (a int) with (a=\"b\",c=\"d\")",
             "CREATE EXTERNAL TABLE user (\n\ta int\n)\nWITH (\n\ta = \"b\",\n\tc = \"d\"\n);\n"},
            {"alter  external table user add column col1 int32, drop column col2, reset(prop), set (prop2 = 42, x=y), set a true",
            "ALTER EXTERNAL TABLE user\n\tADD COLUMN col1 int32,\n\tDROP COLUMN col2,\n\tRESET (prop),\n\tSET (prop2 = 42, x = y),\n\tSET a TRUE;\n"},
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
             "SELECT\n\ttYpe.*\nFROM Table\n\ttYpe;\n"}
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(AlterTable) {
        TCases cases = {
            {"alter table user add user int32",
             "ALTER TABLE user\n\tADD user int32;\n"},
            {"alter table user add user int32, add user bool ?",
             "ALTER TABLE user\n\tADD user int32,\n\tADD user bool?;\n"},
            {"alter table user add column user int32",
             "ALTER TABLE user\n\tADD COLUMN user int32;\n"},
            {"alter table user drop user",
             "ALTER TABLE user\n\tDROP user;\n"},
            {"alter table user drop column user",
             "ALTER TABLE user\n\tDROP COLUMN user;\n"},
            {"alter table user alter column user set family user",
             "ALTER TABLE user\n\tALTER COLUMN user SET FAMILY user;\n"},
            {"alter table t alter column c drop not null",
             "ALTER TABLE t\n\tALTER COLUMN c DROP NOT NULL;\n"},
            {"alter table user add family user(user='foo')",
             "ALTER TABLE user\n\tADD FAMILY user (user = 'foo');\n"},
            {"alter table user alter family user set user 'foo'",
             "ALTER TABLE user\n\tALTER FAMILY user SET user 'foo';\n"},
            {"alter table user set user user",
             "ALTER TABLE user\n\tSET user user;\n"},
            {"alter table user set (user=user)",
             "ALTER TABLE user\n\tSET (user = user);\n"},
            {"alter table user set (user=user,user=user)",
             "ALTER TABLE user\n\tSET (user = user, user = user);\n"},
            {"alter table user reset(user)",
             "ALTER TABLE user\n\tRESET (user);\n"},
            {"alter table user reset(user, user)",
             "ALTER TABLE user\n\tRESET (user, user);\n"},
            {"alter table user add index user local on (user)",
             "ALTER TABLE user\n\tADD INDEX user LOCAL ON (user);\n"},
            {"alter table user alter index idx set setting 'foo'",
             "ALTER TABLE user\n\tALTER INDEX idx SET setting 'foo';\n"},
            {"alter table user alter index idx set (setting = 'foo', another_setting = 'bar')",
             "ALTER TABLE user\n\tALTER INDEX idx SET (setting = 'foo', another_setting = 'bar');\n"},
            {"alter table user alter index idx reset (setting, another_setting)",
             "ALTER TABLE user\n\tALTER INDEX idx RESET (setting, another_setting);\n"},
            {"alter table user add index idx global using subtype on (col) cover (col) with (setting = foo, another_setting = 'bar');",
             "ALTER TABLE user\n\tADD INDEX idx GLOBAL USING subtype ON (col) COVER (col) WITH (setting = foo, another_setting = 'bar');\n"}, 
            {"alter table user drop index user",
             "ALTER TABLE user\n\tDROP INDEX user;\n"},
            {"alter table user rename to user",
             "ALTER TABLE user\n\tRENAME TO user;\n"},
            {"alter table user add changefeed user with (user = 'foo')",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (user = 'foo');\n"},
            {"alter table user alter changefeed user disable",
             "ALTER TABLE user\n\tALTER CHANGEFEED user DISABLE;\n"},
            {"alter table user alter changefeed user set(user='foo')",
             "ALTER TABLE user\n\tALTER CHANGEFEED user SET (user = 'foo');\n"},
            {"alter table user drop changefeed user",
             "ALTER TABLE user\n\tDROP CHANGEFEED user;\n"},
            {"alter table user add changefeed user with (initial_scan = tRUe)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (initial_scan = TRUE);\n"},
            {"alter table user add changefeed user with (initial_scan = FaLsE)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (initial_scan = FALSE);\n"},
            {"alter table user add changefeed user with (retention_period = Interval(\"P1D\"))",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (retention_period = Interval(\"P1D\"));\n"},
            {"alter table user add changefeed user with (virtual_timestamps = TruE)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (virtual_timestamps = TRUE);\n"},
            {"alter table user add changefeed user with (virtual_timestamps = fAlSe)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (virtual_timestamps = FALSE);\n"},
            {"alter table user add changefeed user with (resolved_timestamps = Interval(\"PT1S\"))",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (resolved_timestamps = Interval(\"PT1S\"));\n"},
            {"alter table user add changefeed user with (topic_min_active_partitions = 1)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (topic_min_active_partitions = 1);\n"},
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
             "ALTER TOPIC topic1\n\tALTER CONSUMER c1 SET (important = FALSE);\n"},
             {"alter topic topic1 alter consumer c1 set (important = false), alter consumer c2 reset (read_from)",
              "ALTER TOPIC topic1\n\tALTER CONSUMER c1 SET (important = FALSE),\n\tALTER CONSUMER c2 RESET (read_from);\n"},
             {"alter topic topic1 add consumer c1, drop consumer c2",
              "ALTER TOPIC topic1\n\tADD CONSUMER c1,\n\tDROP CONSUMER c2;\n"},
             {"alter topic topic1 set (supported_codecs = 'RAW'), RESET (retention_period)",
              "ALTER TOPIC topic1\n\tSET (supported_codecs = 'RAW'),\n\tRESET (retention_period);\n"},

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

    Y_UNIT_TEST(Do) {
        TCases cases = {
            {"do $a(1,2,3)",
             "DO $a(1, 2, 3);\n"},
            {"do begin values(1); end do;",
             "DO BEGIN\n\tVALUES\n\t\t(1);\nEND DO;\n"},
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
             "VALUES\n\t\t\t(1);\n\tEND DEFINE;\n\n\t"
             "DEFINE SUBQUERY $c() AS\n\t\tSELECT\n\t\t\t1;\n\t"
             "END DEFINE;\n\tDO $b();\n\n\tPROCESS $c();\nEND DEFINE;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(If) {
        TCases cases = {
            {"evaluate if 1=1 do $a()",
             "EVALUATE IF 1 = 1\n\tDO $a();\n"},
            {"evaluate if 1=1 do $a() else do $b()",
             "EVALUATE IF 1 = 1\n\tDO $a()\nELSE\n\tDO $b();\n"},
            {"evaluate if 1=1 do begin select 1; end do",
             "EVALUATE IF 1 = 1\n\tDO BEGIN\n\t\tSELECT\n\t\t\t1;\n\tEND DO;\n"},
            {"evaluate if 1=1 do begin select 1; end do else do begin select 2; end do",
             "EVALUATE IF 1 = 1\n\tDO BEGIN\n\t\tSELECT\n\t\t\t1;\n\tEND DO\n"
             "ELSE\n\tDO BEGIN\n\t\tSELECT\n\t\t\t2;\n\tEND DO;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(For) {
        TCases cases = {
            {"evaluate for $x in [] do $a($x)",
             "EVALUATE FOR $x IN []\n\tDO $a($x);\n"},
            {"evaluate for $x in [] do $a($x) else do $b()",
             "EVALUATE FOR $x IN []\n\tDO $a($x)\nELSE\n\tDO $b();\n"},
            {"evaluate for $x in [] do begin select $x; end do",
             "EVALUATE FOR $x IN []\n\tDO BEGIN\n\t\tSELECT\n\t\t\t$x;\n\tEND DO;\n"},
            {"evaluate for $x in [] do begin select $x; end do else do begin select 2; end do",
             "EVALUATE FOR $x IN []\n\tDO BEGIN\n\t\tSELECT\n\t\t\t$x;\n\tEND DO\nELSE\n\tDO BEGIN\n\t\tSELECT\n\t\t\t2;\n\tEND DO;\n"},
            {"evaluate parallel for $x in [] do $a($x)",
             "EVALUATE PARALLEL FOR $x IN []\n\tDO $a($x);\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Update) {
        TCases cases = {
            {"update user on default values",
             "UPDATE user\nON DEFAULT VALUES;\n"},
            {"update user on values (1),(2)",
             "UPDATE user\nON\nVALUES\n\t(1),\n\t(2);\n"},
            {"update user on select 1 as x, 2 as y",
             "UPDATE user\nON\nSELECT\n\t1 AS x,\n\t2 AS y;\n"},
            {"update user on (x) values (1),(2),(3)",
             "UPDATE user\nON (\n\tx\n)\nVALUES\n\t(1),\n\t(2),\n\t(3);\n"},
            {"update user on (x,y) values (1,2),(2,3),(3,4)",
             "UPDATE user\nON (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2),\n\t(2, 3),\n\t(3, 4);\n"},
            {"update user on (x) select 1",
             "UPDATE user\nON (\n\tx\n)\nSELECT\n\t1;\n"},
            {"update user on (x,y) select 1,2",
             "UPDATE user\nON (\n\tx,\n\ty\n)\nSELECT\n\t1,\n\t2;\n"},
            {"update user set x=1",
             "UPDATE user\nSET\n\tx = 1;\n"},
            {"update user set (x)=(1)",
             "UPDATE user\nSET\n(\n\tx\n) = (\n\t1\n);\n"},
            {"update user set (x,y)=(1,2)",
             "UPDATE user\nSET\n(\n\tx,\n\ty\n) = (\n\t1,\n\t2\n);\n"},
            {"update user set (x,y)=(select 1,2)",
             "UPDATE user\nSET\n(\n\tx,\n\ty\n) = (\n\tSELECT\n\t\t1,\n\t\t2\n);\n"},
            {"update user set x=1,y=2 where z=3",
             "UPDATE user\nSET\n\tx = 1,\n\ty = 2\nWHERE z = 3;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Delete) {
        TCases cases = {
            {"delete from user",
             "DELETE FROM user;\n"},
            {"delete from user where 1=1",
             "DELETE FROM user\nWHERE 1 = 1;\n"},
            {"delete from user on select 1 as x, 2 as y",
             "DELETE FROM user\nON\nSELECT\n\t1 AS x,\n\t2 AS y;\n"},
            {"delete from user on (x) values (1)",
             "DELETE FROM user\nON (\n\tx\n)\nVALUES\n\t(1);\n"},
            {"delete from user on (x,y) values (1,2), (3,4)",
             "DELETE FROM user\nON (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2),\n\t(3, 4);\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Into) {
        TCases cases = {
            {"insert into user select 1 as x",
             "INSERT INTO user\nSELECT\n\t1 AS x;\n"},
            {"insert or abort into user select 1 as x",
             "INSERT OR ABORT INTO user\nSELECT\n\t1 AS x;\n"},
            {"insert or revert into user select 1 as x",
             "INSERT OR REVERT INTO user\nSELECT\n\t1 AS x;\n"},
            {"insert or ignore into user select 1 as x",
             "INSERT OR IGNORE INTO user\nSELECT\n\t1 AS x;\n"},
            {"upsert into user select 1 as x",
             "UPSERT INTO user\nSELECT\n\t1 AS x;\n"},
            {"replace into user select 1 as x",
             "REPLACE INTO user\nSELECT\n\t1 AS x;\n"},
            {"insert into user(x) values (1)",
             "INSERT INTO user (\n\tx\n)\nVALUES\n\t(1);\n"},
            {"insert into user(x,y) values (1,2)",
             "INSERT INTO user (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2);\n"},
            {"insert into plato.user select 1 as x",
             "INSERT INTO plato.user\nSELECT\n\t1 AS x;\n"},
            {"insert into @user select 1 as x",
             "INSERT INTO @user\nSELECT\n\t1 AS x;\n"},
            {"insert into $user select 1 as x",
             "INSERT INTO $user\nSELECT\n\t1 AS x;\n"},
            {"insert into @$user select 1 as x",
             "INSERT INTO @$user\nSELECT\n\t1 AS x;\n"},
            {"upsert into user erase by (x,y) values (1)",
             "UPSERT INTO user\n\tERASE BY (\n\t\tx,\n\t\ty\n\t)\nVALUES\n\t(1);\n"},
            {"insert into user with truncate select 1 as x",
             "INSERT INTO user\n\tWITH truncate\nSELECT\n\t1 AS x;\n"},
            {"insert into user with (truncate,inferscheme='1') select 1 as x",
             "INSERT INTO user\n\tWITH (truncate, inferscheme = '1')\nSELECT\n\t1 AS x;\n"},
            {"insert into user with schema Struct<user:int32> select 1 as user",
             "INSERT INTO user\n\tWITH SCHEMA Struct<user: int32>\nSELECT\n\t1 AS user;\n"},
            {"insert into user with schema (int32 as user) select 1 as user",
             "INSERT INTO user\n\tWITH SCHEMA (int32 AS user)\nSELECT\n\t1 AS user;\n"},
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
             "PROCESS user\nUSING $f()\nWHERE 1 = 1\nHAVING 1 = 1\nASSUME ORDER BY\n\tuser;\n"},
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
             "REDUCE user\nON\n\tuser\nUSING $f()\nWHERE 1 = 1\nHAVING 1 = 1\nASSUME ORDER BY\n\tuser;\n"},
            {"reduce user presort user,user on user using $f();",
             "REDUCE user\nPRESORT\n\tuser,\n\tuser\nON\n\tuser\nUSING $f();\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Select) {
        TCases cases = {
            {"select 1",
             "SELECT\n\t1;\n"},
            {"select 1,",
             "SELECT\n\t1,;\n"},
            {"select 1 as x",
             "SELECT\n\t1 AS x;\n"},
            {"select *",
             "SELECT\n\t*;\n"},
            {"select a.*",
             "SELECT\n\ta.*;\n"},
            {"select * without a",
             "SELECT\n\t*\n\tWITHOUT\n\t\ta;\n"},
            {"select * without a,b",
             "SELECT\n\t*\n\tWITHOUT\n\t\ta,\n\t\tb;\n"},
            {"select * without a,",
             "SELECT\n\t*\n\tWITHOUT\n\t\ta,;\n"},
            {"select 1 from user",
             "SELECT\n\t1\nFROM user;\n"},
            {"select 1 from plato.user",
             "SELECT\n\t1\nFROM plato.user;\n"},
            {"select 1 from $user",
             "SELECT\n\t1\nFROM $user;\n"},
            {"select 1 from @user",
             "SELECT\n\t1\nFROM @user;\n"},
            {"select 1 from @$user",
             "SELECT\n\t1\nFROM @$user;\n"},
            {"select 1 from user view user",
             "SELECT\n\t1\nFROM user\n\tVIEW user;\n"},
            {"select 1 from user as user",
             "SELECT\n\t1\nFROM user\n\tAS user;\n"},
            {"select 1 from user as user(user)",
             "SELECT\n\t1\nFROM user\n\tAS user (\n\t\tuser\n\t);\n"},
            {"select 1 from user as user(user, user)",
             "SELECT\n\t1\nFROM user\n\tAS user (\n\t\tuser,\n\t\tuser\n\t);\n"},
            {"select 1 from user with user=user",
             "SELECT\n\t1\nFROM user\n\tWITH user = user;\n"},
            {"select 1 from user with (user=user, user=user)",
             "SELECT\n\t1\nFROM user\n\tWITH (user = user, user = user);\n"},
            {"select 1 from user sample 0.1",
             "SELECT\n\t1\nFROM user\n\tSAMPLE 0.1;\n"},
            {"select 1 from user tablesample system(0.1)",
             "SELECT\n\t1\nFROM user\n\tTABLESAMPLE SYSTEM (0.1);\n"},
            {"select 1 from user tablesample bernoulli(0.1) repeatable(10)",
             "SELECT\n\t1\nFROM user\n\tTABLESAMPLE BERNOULLI (0.1) REPEATABLE (10);\n"},
            {"select 1 from user flatten columns",
             "SELECT\n\t1\nFROM user\n\tFLATTEN COLUMNS;\n"},
            {"select 1 from user flatten list by user",
             "SELECT\n\t1\nFROM user\n\tFLATTEN LIST BY\n\t\tuser;\n"},
            {"select 1 from user flatten list by (user,user)",
             "SELECT\n\t1\nFROM user\n\tFLATTEN LIST BY (\n\t\tuser,\n\t\tuser\n\t);\n"},
            {"select 1 from $user(1,2)",
             "SELECT\n\t1\nFROM $user(1, 2);\n"},
            {"select 1 from $user(1,2) view user",
             "SELECT\n\t1\nFROM $user(1, 2)\n\tVIEW user;\n"},
            {"select 1 from range('a','b')",
             "SELECT\n\t1\nFROM range('a', 'b');\n"},
            {"from user select 1",
             "FROM user\nSELECT\n\t1;\n"},
            {"select * from user as a join user as b on a.x=b.y",
             "SELECT\n\t*\nFROM user\n\tAS a\nJOIN user\n\tAS b\nON a.x = b.y;\n"},
            {"select * from user as a join user as b using(x)",
             "SELECT\n\t*\nFROM user\n\tAS a\nJOIN user\n\tAS b\nUSING (x);\n"},
            {"select * from any user as a full join user as b on a.x=b.y",
             "SELECT\n\t*\nFROM ANY user\n\tAS a\nFULL JOIN user\n\tAS b\nON a.x = b.y;\n"},
            {"select * from user as a left join any user as b on a.x=b.y",
             "SELECT\n\t*\nFROM user\n\tAS a\nLEFT JOIN ANY user\n\tAS b\nON a.x = b.y;\n"},
            {"select * from any user as a right join any user as b on a.x=b.y",
             "SELECT\n\t*\nFROM ANY user\n\tAS a\nRIGHT JOIN ANY user\n\tAS b\nON a.x = b.y;\n"},
            {"select * from user as a cross join user as b",
             "SELECT\n\t*\nFROM user\n\tAS a\nCROSS JOIN user\n\tAS b;\n"},
            {"select 1 from user where key = 1",
             "SELECT\n\t1\nFROM user\nWHERE key = 1;\n"},
            {"select 1 from user having count(*) = 1",
             "SELECT\n\t1\nFROM user\nHAVING count(*) = 1;\n"},
            {"select 1 from user group by key",
             "SELECT\n\t1\nFROM user\nGROUP BY\n\tkey;\n"},
            {"select 1 from user group compact by key, value as v",
             "SELECT\n\t1\nFROM user\nGROUP COMPACT BY\n\tkey,\n\tvalue AS v;\n"},
            {"select 1 from user group by key with combine",
             "SELECT\n\t1\nFROM user\nGROUP BY\n\tkey\n\tWITH combine;\n"},
            {"select 1 from user order by key asc",
             "SELECT\n\t1\nFROM user\nORDER BY\n\tkey ASC;\n"},
            {"select 1 from user order by key, value desc",
             "SELECT\n\t1\nFROM user\nORDER BY\n\tkey,\n\tvalue DESC;\n"},
            {"select 1 from user assume order by key",
             "SELECT\n\t1\nFROM user\nASSUME ORDER BY\n\tkey;\n"},
            {"select 1 from user window w1 as (), w2 as ()",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (),\n\tw2 AS ();\n"},
            {"select 1 from user window w1 as (user)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tuser\n\t);\n"},
            {"select 1 from user window w1 as (partition by user)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tPARTITION BY\n\t\t\tuser\n\t);\n"},
            {"select 1 from user window w1 as (partition by user, user)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tPARTITION BY\n\t\t\tuser,\n\t\t\tuser\n\t);\n"},
            {"select 1 from user window w1 as (order by user asc)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tORDER BY\n\t\t\tuser ASC\n\t);\n"},
            {"select 1 from user window w1 as (order by user, user desc)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tORDER BY\n\t\t\tuser,\n\t\t\tuser DESC\n\t);\n"},
            {"select 1 from user window w1 as (rows between 1 preceding and 1 following)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING\n\t);\n"},
            {"select 1 limit 10",
             "SELECT\n\t1\nLIMIT 10;\n"},
            {"select 1 limit 10 offset 5",
             "SELECT\n\t1\nLIMIT 10 OFFSET 5;\n"},
            { "select 1 union all select 2",
             "SELECT\n\t1\nUNION ALL\nSELECT\n\t2;\n" },
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CompositeTypesAndQuestions) {
        TCases cases = {
            {"declare $_x AS list<int32>??;declare $_y AS int32 ? ? ;select 1<>2, 1??2,"
             "formattype(list<int32>), formattype(resource<user>),formattype(tuple<>), formattype(tuple<  >), formattype(int32 ? ? )",
             "DECLARE $_x AS list<int32>??;\nDECLARE $_y AS int32??;\n\nSELECT\n\t1 <> 2,\n\t1 ?? 2,\n\tformattype(list<int32>),"
            "\n\tformattype(resource<user>),\n\tformattype(tuple<>),\n\tformattype(tuple< >),\n\tformattype(int32??" ");\n"
            },
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Lambda) {
        TCases cases = {
            {"$f=($a,$b)->{$x=$a+$b;return $a*$x};$g=($a,$b?)->($a+$b??0);select $f(10,4),$g(1,2);",
             "$f = ($a, $b) -> {\n\t$x = $a + $b;\n\tRETURN $a * $x\n};\n"
             "$g = ($a, $b?) -> ($a + $b ?? 0);\n\n"
            "SELECT\n\t$f(10, 4),\n\t$g(1, 2);\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(NestedSelect) {
        TCases cases = {
            {"$x=select 1",
             "$x =\n\tSELECT\n\t\t1;\n"},
            {"$x=(select 1)",
             "$x = (\n\tSELECT\n\t\t1\n);\n"},
            {"select 1 in (select 1)",
             "SELECT\n\t1 IN (\n\t\tSELECT\n\t\t\t1\n\t);\n"},
            {"select 1 in ((select 1))",
             "SELECT\n\t1 IN (\n\t\t(\n\t\t\tSELECT\n\t\t\t\t1\n\t\t)\n\t);\n"},
            {"select 1 in (\nselect 1)",
             "SELECT\n\t1 IN (\n\t\tSELECT\n\t\t\t1\n\t);\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Cast) {
        TCases cases = {
            {"select cast(1 as string)","SELECT\n\tCAST(1 AS string);\n"},
            {"select bitcast(1 as int32)","SELECT\n\tBITCAST(1 AS int32);\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(StructLiteral) {
        TCases cases = {
            {"select <||>","SELECT\n\t<||>;\n"},
            {"select <|a:1|>","SELECT\n\t<|a: 1|>;\n"},
            {"select <|a:1,b:2|>","SELECT\n\t<|a: 1, b: 2|>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(TableHints) {
        TCases cases = {
            {"select * from plato.T with schema(foo int32, bar list<string>) where key is not null",
             "SELECT\n\t*\nFROM plato.T\n\tWITH SCHEMA (foo int32, bar list<string>)\nWHERE key IS NOT NULL;\n"},
            {"select * from plato.T with schema struct<foo:integer, Bar:list<string?>> where key<0",
             "SELECT\n\t*\nFROM plato.T\n\tWITH SCHEMA struct<foo: integer, Bar: list<string?>>\nWHERE key < 0;\n"},
            {"select * from plato.T with (foo=bar, x=$y, a=(a, b, c), u='aaa', schema (foo int32, bar list<string>))",
             "SELECT\n\t*\nFROM plato.T\n\tWITH (foo = bar, x = $y, a = (a, b, c), u = 'aaa', SCHEMA (foo int32, bar list<string>));\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(BoolAsVariableName) {
        TCases cases = {
            {"$ False = True; select $ False;",
             "$False = TRUE;\n\nSELECT\n\t$False;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(WithSchemaEquals) {
        TCases cases = {
            {"select * from plato.T with (format= csv_with_names, schema=(year int32 Null, month String, day String not   null, a Utf8, b Uint16));",
             "SELECT\n\t*\nFROM plato.T\n\tWITH (format = csv_with_names, SCHEMA = (year int32 NULL, month String, day String NOT NULL, a Utf8, b Uint16));\n"},
             };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(SquareBrackets) {
        TCases cases = {
            {"select a[0]",
             "SELECT\n\ta[0];\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineList) {
        TCases cases = {
            {"select [\n]",
             "SELECT\n\t[\n\t];\n"},
            {"select [1\n]",
             "SELECT\n\t[\n\t\t1\n\t];\n"},
            {"select [\n1]",
             "SELECT\n\t[\n\t\t1\n\t];\n"},
            {"select [1,\n]",
             "SELECT\n\t[\n\t\t1,\n\t];\n"},
            {"select [1\n,]",
             "SELECT\n\t[\n\t\t1,\n\t];\n"},
            {"select [\n1,]",
             "SELECT\n\t[\n\t\t1,\n\t];\n"},
            {"select [1,2,\n3,4]",
             "SELECT\n\t[\n\t\t1, 2,\n\t\t3, 4\n\t];\n"},
            {"select [1,2,\n3,4,]",
             "SELECT\n\t[\n\t\t1, 2,\n\t\t3, 4,\n\t];\n"},
            {"select [1,2\n,3,\n4\n,5]",
             "SELECT\n\t[\n\t\t1, 2,\n\t\t3,\n\t\t4,\n\t\t5\n\t];\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineTuple) {
        TCases cases = {
            {"select (\n)",
             "SELECT\n\t(\n\t);\n"},
            {"select (1,\n)",
             "SELECT\n\t(\n\t\t1,\n\t);\n"},
            {"select (1\n,)",
             "SELECT\n\t(\n\t\t1,\n\t);\n"},
            {"select (\n1,)",
             "SELECT\n\t(\n\t\t1,\n\t);\n"},
            {"select (1,2,\n3,4)",
             "SELECT\n\t(\n\t\t1, 2,\n\t\t3, 4\n\t);\n"},
            {"select (1,2,\n3,4,)",
             "SELECT\n\t(\n\t\t1, 2,\n\t\t3, 4,\n\t);\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineSet) {
        TCases cases = {
            {"select {\n}",
             "SELECT\n\t{\n\t};\n"},
            {"select {1\n}",
             "SELECT\n\t{\n\t\t1\n\t};\n"},
            {"select {\n1}",
             "SELECT\n\t{\n\t\t1\n\t};\n"},
            {"select {1,\n}",
             "SELECT\n\t{\n\t\t1,\n\t};\n"},
            {"select {1\n,}",
             "SELECT\n\t{\n\t\t1,\n\t};\n"},
            {"select {\n1,}",
             "SELECT\n\t{\n\t\t1,\n\t};\n"},
            {"select {1,2,\n3,4}",
             "SELECT\n\t{\n\t\t1, 2,\n\t\t3, 4\n\t};\n"},
            {"select {1,2,\n3,4,}",
             "SELECT\n\t{\n\t\t1, 2,\n\t\t3, 4,\n\t};\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineDict) {
        TCases cases = {
            {"select {0:1\n}",
             "SELECT\n\t{\n\t\t0: 1\n\t};\n"},
            {"select {\n0:1}",
             "SELECT\n\t{\n\t\t0: 1\n\t};\n"},
            {"select {0:1,\n}",
             "SELECT\n\t{\n\t\t0: 1,\n\t};\n"},
            {"select {0:1\n,}",
             "SELECT\n\t{\n\t\t0: 1,\n\t};\n"},
            {"select {\n0:1,}",
             "SELECT\n\t{\n\t\t0: 1,\n\t};\n"},
            {"select {10:1,20:2,\n30:3,40:4}",
             "SELECT\n\t{\n\t\t10: 1, 20: 2,\n\t\t30: 3, 40: 4\n\t};\n"},
            {"select {10:1,20:2,\n30:3,40:4,}",
             "SELECT\n\t{\n\t\t10: 1, 20: 2,\n\t\t30: 3, 40: 4,\n\t};\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineFuncCall) {
        TCases cases = {
            {"select f(\n)",
             "SELECT\n\tf(\n\t);\n"},
            {"select f(1\n)",
             "SELECT\n\tf(\n\t\t1\n\t);\n"},
            {"select f(\n1)",
             "SELECT\n\tf(\n\t\t1\n\t);\n"},
            {"select f(1,\n)",
             "SELECT\n\tf(\n\t\t1,\n\t);\n"},
            {"select f(1\n,)",
             "SELECT\n\tf(\n\t\t1,\n\t);\n"},
            {"select f(\n1,)",
             "SELECT\n\tf(\n\t\t1,\n\t);\n"},
            {"select f(1,2,\n3,4)",
             "SELECT\n\tf(\n\t\t1, 2,\n\t\t3, 4\n\t);\n"},
            {"select f(1,2,\n3,4,)",
             "SELECT\n\tf(\n\t\t1, 2,\n\t\t3, 4,\n\t);\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineStruct) {
        TCases cases = {
            {"select <|\n|>",
             "SELECT\n\t<|\n\t|>;\n"},
            {"select <|a:1\n|>",
             "SELECT\n\t<|\n\t\ta: 1\n\t|>;\n"},
            {"select <|\na:1|>",
             "SELECT\n\t<|\n\t\ta: 1\n\t|>;\n"},
            {"select <|a:1,\n|>",
             "SELECT\n\t<|\n\t\ta: 1,\n\t|>;\n"},
            {"select <|a:1\n,|>",
             "SELECT\n\t<|\n\t\ta: 1,\n\t|>;\n"},
            {"select <|\na:1,|>",
             "SELECT\n\t<|\n\t\ta: 1,\n\t|>;\n"},
            {"select <|a:1,b:2,\nc:3,d:4|>",
             "SELECT\n\t<|\n\t\ta: 1, b: 2,\n\t\tc: 3, d: 4\n\t|>;\n"},
            {"select <|a:1,b:2,\nc:3,d:4,|>",
             "SELECT\n\t<|\n\t\ta: 1, b: 2,\n\t\tc: 3, d: 4,\n\t|>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineListType) {
        TCases cases = {
            {"select list<int32\n>",
             "SELECT\n\tlist<\n\t\tint32\n\t>;\n"},
            {"select list<\nint32>",
             "SELECT\n\tlist<\n\t\tint32\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineOptionalType) {
        TCases cases = {
            {"select optional<int32\n>",
             "SELECT\n\toptional<\n\t\tint32\n\t>;\n"},
            {"select optional<\nint32>",
             "SELECT\n\toptional<\n\t\tint32\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineStreamType) {
        TCases cases = {
            {"select stream<int32\n>",
             "SELECT\n\tstream<\n\t\tint32\n\t>;\n"},
            {"select stream<\nint32>",
             "SELECT\n\tstream<\n\t\tint32\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineFlowType) {
        TCases cases = {
            {"select flow<int32\n>",
             "SELECT\n\tflow<\n\t\tint32\n\t>;\n"},
            {"select flow<\nint32>",
             "SELECT\n\tflow<\n\t\tint32\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineSetType) {
        TCases cases = {
            {"select set<int32\n>",
             "SELECT\n\tset<\n\t\tint32\n\t>;\n"},
            {"select set<\nint32>",
             "SELECT\n\tset<\n\t\tint32\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineTupleType) {
        TCases cases = {
            {"select tuple<\n>",
             "SELECT\n\ttuple<\n\t\t \n\t>;\n"},
            {"select tuple<int32\n>",
             "SELECT\n\ttuple<\n\t\tint32\n\t>;\n"},
            {"select tuple<\nint32>",
             "SELECT\n\ttuple<\n\t\tint32\n\t>;\n"},
            {"select tuple<int32,\n>",
             "SELECT\n\ttuple<\n\t\tint32,\n\t>;\n"},
            {"select tuple<int32\n,>",
             "SELECT\n\ttuple<\n\t\tint32,\n\t>;\n"},
            {"select tuple<\nint32,>",
             "SELECT\n\ttuple<\n\t\tint32,\n\t>;\n"},
            {"select tuple<\nint32,string,\ndouble,bool>",
             "SELECT\n\ttuple<\n\t\tint32, string,\n\t\tdouble, bool\n\t>;\n"},
            {"select tuple<\nint32,string,\ndouble,bool,>",
             "SELECT\n\ttuple<\n\t\tint32, string,\n\t\tdouble, bool,\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineStructType) {
        TCases cases = {
            {"select struct<\n>",
             "SELECT\n\tstruct<\n\t\t \n\t>;\n"},
            {"select struct<a:int32\n>",
             "SELECT\n\tstruct<\n\t\ta: int32\n\t>;\n"},
            {"select struct<\na:int32>",
             "SELECT\n\tstruct<\n\t\ta: int32\n\t>;\n"},
            {"select struct<a:int32,\n>",
             "SELECT\n\tstruct<\n\t\ta: int32,\n\t>;\n"},
            {"select struct<a:int32\n,>",
             "SELECT\n\tstruct<\n\t\ta: int32,\n\t>;\n"},
            {"select struct<\na:int32,>",
             "SELECT\n\tstruct<\n\t\ta: int32,\n\t>;\n"},
            {"select struct<\na:int32,b:string,\nc:double,d:bool>",
             "SELECT\n\tstruct<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool\n\t>;\n"},
            {"select struct<\na:int32,b:string,\nc:double,d:bool,>",
             "SELECT\n\tstruct<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool,\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineVariantOverTupleType) {
        TCases cases = {
            {"select variant<int32\n>",
             "SELECT\n\tvariant<\n\t\tint32\n\t>;\n"},
            {"select variant<\nint32>",
             "SELECT\n\tvariant<\n\t\tint32\n\t>;\n"},
            {"select variant<int32,\n>",
             "SELECT\n\tvariant<\n\t\tint32,\n\t>;\n"},
            {"select variant<int32\n,>",
             "SELECT\n\tvariant<\n\t\tint32,\n\t>;\n"},
            {"select variant<\nint32,>",
             "SELECT\n\tvariant<\n\t\tint32,\n\t>;\n"},
            {"select variant<\nint32,string,\ndouble,bool>",
             "SELECT\n\tvariant<\n\t\tint32, string,\n\t\tdouble, bool\n\t>;\n"},
            {"select variant<\nint32,string,\ndouble,bool,>",
             "SELECT\n\tvariant<\n\t\tint32, string,\n\t\tdouble, bool,\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineVariantOverStructType) {
        TCases cases = {
            {"select variant<a:int32\n>",
             "SELECT\n\tvariant<\n\t\ta: int32\n\t>;\n"},
            {"select variant<\na:int32>",
             "SELECT\n\tvariant<\n\t\ta: int32\n\t>;\n"},
            {"select variant<a:int32,\n>",
             "SELECT\n\tvariant<\n\t\ta: int32,\n\t>;\n"},
            {"select variant<a:int32\n,>",
             "SELECT\n\tvariant<\n\t\ta: int32,\n\t>;\n"},
            {"select variant<\na:int32,>",
             "SELECT\n\tvariant<\n\t\ta: int32,\n\t>;\n"},
            {"select variant<\na:int32,b:string,\nc:double,d:bool>",
             "SELECT\n\tvariant<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool\n\t>;\n"},
            {"select variant<\na:int32,b:string,\nc:double,d:bool,>",
             "SELECT\n\tvariant<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool,\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineEnum) {
        TCases cases = {
            {"select enum<a\n>",
             "SELECT\n\tenum<\n\t\ta\n\t>;\n"},
            {"select enum<\na>",
             "SELECT\n\tenum<\n\t\ta\n\t>;\n"},
            {"select enum<a,\n>",
             "SELECT\n\tenum<\n\t\ta,\n\t>;\n"},
            {"select enum<a\n,>",
             "SELECT\n\tenum<\n\t\ta,\n\t>;\n"},
            {"select enum<\na,>",
             "SELECT\n\tenum<\n\t\ta,\n\t>;\n"},
            {"select enum<\na,b,\nc,d>",
             "SELECT\n\tenum<\n\t\ta, b,\n\t\tc, d\n\t>;\n"},
            {"select enum<\na,b,\nc,d,>",
             "SELECT\n\tenum<\n\t\ta, b,\n\t\tc, d,\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineResourceType) {
        TCases cases = {
            {"select resource<foo\n>",
             "SELECT\n\tresource<\n\t\tfoo\n\t>;\n"},
            {"select resource<\nfoo>",
             "SELECT\n\tresource<\n\t\tfoo\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineTaggedType) {
        TCases cases = {
            {"select tagged<int32,foo\n>",
             "SELECT\n\ttagged<\n\t\tint32, foo\n\t>;\n"},
            {"select tagged<int32,\nfoo>",
             "SELECT\n\ttagged<\n\t\tint32,\n\t\tfoo\n\t>;\n"},
            {"select tagged<int32\n,foo>",
             "SELECT\n\ttagged<\n\t\tint32,\n\t\tfoo\n\t>;\n"},
            {"select tagged<\nint32,foo>",
             "SELECT\n\ttagged<\n\t\tint32, foo\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineDictType) {
        TCases cases = {
            {"select dict<int32,string\n>",
             "SELECT\n\tdict<\n\t\tint32, string\n\t>;\n"},
            {"select dict<int32,\nstring>",
             "SELECT\n\tdict<\n\t\tint32,\n\t\tstring\n\t>;\n"},
            {"select dict<int32\n,string>",
             "SELECT\n\tdict<\n\t\tint32,\n\t\tstring\n\t>;\n"},
            {"select dict<\nint32,string>",
             "SELECT\n\tdict<\n\t\tint32, string\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineCallableType) {
        TCases cases = {
            {"select callable<()->int32\n>",
             "SELECT\n\tcallable<\n\t\t() -> int32\n\t>;\n"},
            {"select callable<\n()->int32>",
             "SELECT\n\tcallable<\n\t\t() -> int32\n\t>;\n"},
            {"select callable<\n(int32)->int32>",
             "SELECT\n\tcallable<\n\t\t(int32) -> int32\n\t>;\n"},
            {"select callable<\n(int32,\ndouble)->int32>",
             "SELECT\n\tcallable<\n\t\t(\n\t\t\tint32,\n\t\t\tdouble\n\t\t) -> int32\n\t>;\n"},
            {"select callable<\n(int32\n,double)->int32>",
             "SELECT\n\tcallable<\n\t\t(\n\t\t\tint32,\n\t\t\tdouble\n\t\t) -> int32\n\t>;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(UnaryOp) {
        TCases cases = {
            {"select -x,+x,~x,-1,-1.0,+1,+1.0,~1u",
             "SELECT\n\t-x,\n\t+x,\n\t~x,\n\t-1,\n\t-1.0,\n\t+1,\n\t+1.0,\n\t~1u;\n"},
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
    PATTERN ( A )
    DEFINE A as A
);
)",
R"(PRAGMA FeatureR010 = "prototype";
USE plato;

SELECT
    *
FROM Input MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS A);
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
             "SELECT\n\t1\nUNION ALL\nSELECT\n\t2\nUNION\nSELECT\n\t3\nUNION ALL\nSELECT\n\t4\nUNION\nSELECT\n\t5;\n"},
             };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CommentAfterLastSelect) {
        TCases cases = {
            {"SELECT 1--comment\n",
             "SELECT\n\t1--comment\n;\n"},
            {"SELECT 1\n\n--comment\n",
             "SELECT\n\t1--comment\n;\n"},
            {"SELECT 1\n\n--comment",
             "SELECT\n\t1--comment\n;\n"},
            {"SELECT * FROM Input\n\n\n\n/* comment */\n\n\n",
             "SELECT\n\t*\nFROM Input/* comment */;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(WindowFunctionInsideExpr) {
        TCases cases = {
            {"SELECT CAST(ROW_NUMBER() OVER () AS String) AS x,\nFROM Input;",
             "SELECT\n\tCAST(ROW_NUMBER() OVER () AS String) AS x,\nFROM Input;\n"},
            {"SELECT CAST(ROW_NUMBER() OVER (PARTITION BY key) AS String) AS x,\nFROM Input;",
             "SELECT\n\tCAST(\n\t\tROW_NUMBER() OVER (\n\t\t\tPARTITION BY\n\t\t\t\tkey\n\t\t) AS String\n\t) AS x,\nFROM Input;\n"},
            {"SELECT CAST(ROW_NUMBER() OVER (users) AS String) AS x,\nFROM Input;",
            "SELECT\n\tCAST(\n\t\tROW_NUMBER() OVER (\n\t\t\tusers\n\t\t) AS String\n\t) AS x,\nFROM Input;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(ExistsExpr) {
        TCases cases = {
            {"SELECT EXISTS (SELECT 1);",
             "SELECT\n\tEXISTS (\n\t\tSELECT\n\t\t\t1\n\t);\n"},
            {"SELECT CAST(EXISTS(SELECT 1) AS Int) AS x,\nFROM Input;",
             "SELECT\n\tCAST(\n\t\tEXISTS (\n\t\t\tSELECT\n\t\t\t\t1\n\t\t) AS Int\n\t) AS x,\nFROM Input;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(LambdaInsideExpr) {
        TCases cases = {
            {"SELECT ListMap(AsList(1,2),($x)->{return $x+1});",
             "SELECT\n\tListMap(\n\t\tAsList(1, 2), ($x) -> {\n\t\t\tRETURN $x + 1\n\t\t}\n\t);\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CaseExpr) {
        TCases cases = {
            {"SELECT CASE WHEN 1 == 2 THEN 3 WHEN 4 == 5 THEN 6 WHEN 7 == 8 THEN 9 ELSE 10 END;",
             "SELECT\n\tCASE\n\t\tWHEN 1 == 2\n\t\t\tTHEN 3\n\t\tWHEN 4 == 5\n\t\t\tTHEN 6\n\t\tWHEN 7 == 8\n\t\t\tTHEN 9\n\t\tELSE 10\n\tEND;\n"},
            {"SELECT CAST(CASE WHEN 1 == 2 THEN 3 WHEN 4 == 5 THEN 6 ELSE 10 END AS String);",
             "SELECT\n\tCAST(\n\t\tCASE\n\t\t\tWHEN 1 == 2\n\t\t\t\tTHEN 3\n\t\t\tWHEN 4 == 5\n\t\t\t\tTHEN 6\n\t\t\tELSE 10\n\t\tEND AS String\n\t);\n"},
            {"SELECT CASE x WHEN 1 THEN 2 WHEN 3 THEN 4 WHEN 5 THEN 6 ELSE 10 END;",
             "SELECT\n\tCASE x\n\t\tWHEN 1\n\t\t\tTHEN 2\n\t\tWHEN 3\n\t\t\tTHEN 4\n\t\tWHEN 5\n\t\t\tTHEN 6\n\t\tELSE 10\n\tEND;\n"},
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
             "$x = 1 >/*comment*/>/*comment*/| 2;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(OperatorNewlines) {
        TCases cases = {
            {"$x = TRUE\nOR\nFALSE;",
             "$x = TRUE\n\tOR\n\tFALSE;\n"},
            {"$x = TRUE OR\nFALSE;",
             "$x = TRUE OR\n\tFALSE;\n"},
            {"$x = TRUE\nOR FALSE;",
             "$x = TRUE OR\n\tFALSE;\n"},
            {"$x = 1\n+2\n*3;",
             "$x = 1 +\n\t2 *\n\t\t3;\n"},
            {"$x = 1\n+\n2\n*3\n*5\n+\n4;",
             "$x = 1\n\t+\n\t2 *\n\t\t3 *\n\t\t5\n\t+\n\t4;\n"},
            {"$x = 1\n+2+3+4\n+5+6+7+\n\n8+9+10;",
             "$x = 1 +\n\t2 + 3 + 4 +\n\t5 + 6 + 7 +\n\t8 + 9 + 10;\n"},
            {"$x = TRUE\nAND\nTRUE OR\nFALSE\nAND TRUE\nOR FALSE\nAND TRUE\nOR FALSE;",
             "$x = TRUE\n\tAND\n\tTRUE OR\n\tFALSE AND\n\t\tTRUE OR\n\tFALSE AND\n\t\tTRUE OR\n\tFALSE;\n"},
            {"$x = 1 -- comment\n+ 2;",
             "$x = 1-- comment\n\t+\n\t2;\n"},
             {"$x = 1 -- comment\n+ -- comment\n2;",
             "$x = 1-- comment\n\t+-- comment\n\t2;\n"},
             {"$x = 1 + -- comment\n2;",
             "$x = 1 +-- comment\n\t2;\n"},
             {"$x = 1\n>\n>\n|\n2;",
             "$x = 1\n\t>>|\n\t2;\n"},
             {"$x = 1\n?? 2 ??\n3\n??\n4 +\n5\n*\n6 +\n7 ??\n8;",
             "$x = 1 ??\n\t2 ??\n\t3\n\t??\n\t4 +\n\t\t5\n\t\t\t*\n\t\t\t6 +\n\t\t7 ??\n\t8;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(ObfuscateSelect) {
        TCases cases = {
            {"select 1;",
             "SELECT\n\t0;\n"},
            {"select true;",
             "SELECT\n\tFALSE;\n"},
            {"select 'foo';",
             "SELECT\n\t'str';\n"},
            {"select 3.0;",
             "SELECT\n\t0.0;\n"},
            {"select col;",
             "SELECT\n\tid;\n"},
            {"select * from tab;",
             "SELECT\n\t*\nFROM id;\n"},
            {"select cast(col as int32);",
             "SELECT\n\tCAST(id AS int32);\n"},
            {"select func(col);",
             "SELECT\n\tfunc(id);\n"},
            {"select mod::func(col);",
             "SELECT\n\tmod::func(id);\n"},
            {"declare $a as int32;",
             "DECLARE $id AS int32;\n"},
            {"select * from `logs/of/bob` where pwd='foo';",
             "SELECT\n\t*\nFROM id\nWHERE id = 'str';\n"},
            {"select $f();",
             "SELECT\n\t$id();\n"},
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
        TCases cases = {
            {"creAte vIEw TheView wiTh (security_invoker = trUE) As SELect 1",
             "CREATE VIEW TheView WITH (security_invoker = TRUE) AS\nSELECT\n\t1;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(DropView) {
        TCases cases = {
            {"dRop viEW theVIEW",
             "DROP VIEW theVIEW;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(ResourcePoolOperations) {
        TCases cases = {
            {"creAte reSourCe poOl naMe With (a = \"b\")",
             "CREATE RESOURCE POOL naMe WITH (a = \"b\");\n"},
             {"create resource pool eds with (a=\"a\",b=\"b\",c = true)",
             "CREATE RESOURCE POOL eds WITH (\n\ta = \"a\",\n\tb = \"b\",\n\tc = TRUE\n);\n"},
             {"alTer reSOurcE poOl naMe resEt (b, c), seT (x=y, z=false)",
             "ALTER RESOURCE POOL naMe\n\tRESET (b, c),\n\tSET (x = y, z = FALSE);\n"},
             {"alter resource pool eds reset (a), set (x=y)",
             "ALTER RESOURCE POOL eds\n\tRESET (a),\n\tSET (x = y);\n"},
            {"dRop reSourCe poOl naMe",
             "DROP RESOURCE POOL naMe;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(BackupCollectionOperations) {
        TCases cases = {
            {"creAte  BackuP colLection `-naMe` wIth (a = \"b\")",
             "CREATE BACKUP COLLECTION `-naMe` WITH (a = \"b\");\n"},
             {"alTer bACKuP coLLECTION naMe resEt (b, c), seT (x=y, z=false)",
             "ALTER BACKUP COLLECTION naMe\n\tRESET (b, c),\n\tSET (x = y, z = FALSE);\n"},
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
             "CREATE RESOURCE POOL CLASSIFIER naMe WITH (a = \"b\");\n"},
             {"create resource pool classifier eds with (a=\"a\",b=\"b\",c = true)",
             "CREATE RESOURCE POOL CLASSIFIER eds WITH (\n\ta = \"a\",\n\tb = \"b\",\n\tc = TRUE\n);\n"},
             {"alTer reSOurcE poOl ClaSsiFIer naMe resEt (b, c), seT (x=y, z=false)",
             "ALTER RESOURCE POOL CLASSIFIER naMe\n\tRESET (b, c),\n\tSET (x = y, z = FALSE);\n"},
             {"alter resource pool classifier eds reset (a), set (x=y)",
             "ALTER RESOURCE POOL CLASSIFIER eds\n\tRESET (a),\n\tSET (x = y);\n"},
            {"dRop reSourCe poOl ClaSsiFIer naMe",
             "DROP RESOURCE POOL CLASSIFIER naMe;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }
}
