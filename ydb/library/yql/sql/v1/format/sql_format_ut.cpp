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

    void Run(const TCases& cases) {
        for (const auto& c : cases) {
            NYql::TIssues issues;
            TString formatted;
            auto res = Formatter->Format(c.first, formatted, issues);
            UNIT_ASSERT_C(res, issues.ToString());
            auto expected = c.second;
            SubstGlobal(expected, "\t", TString(NSQLFormat::OneIndent, ' '));
            UNIT_ASSERT_NO_DIFF(formatted, expected);
            auto mutatedQuery = NSQLFormat::MutateQuery(c.first);
            auto res2 = Formatter->Format(mutatedQuery, formatted, issues);
            UNIT_ASSERT_C(res2, issues.ToString());
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
            {"select a.1 .b from plato.foo;","SELECT\n\ta.1 .b\nFROM plato.foo;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(GrantPermissions) {
        TCases cases {
            {"use plato;grant connect, modify tables, list on `/Root` to user;", "USE plato;\nGRANT CONNECT, MODIFY TABLES, LIST ON `/Root` TO user;\n\n"},
            {"use plato;grant select , select tables, select attributes on `/Root` to user;", "USE plato;\nGRANT SELECT, SELECT TABLES, SELECT ATTRIBUTES ON `/Root` TO user;\n\n"},
            {"use plato;grant insert, modify attributes on `/Root` to user;", "USE plato;\nGRANT INSERT, MODIFY ATTRIBUTES ON `/Root` TO user;\n\n"},
            {"use plato;grant use legacy, use on `/Root` to user1, user2;", "USE plato;\nGRANT USE LEGACY, USE ON `/Root` TO user1, user2;\n\n"},
            {"use plato;grant manage, full legacy, full, create on `/Root` to user;", "USE plato;\nGRANT MANAGE, FULL LEGACY, FULL, CREATE ON `/Root` TO user;\n\n"},
            {"use plato;grant drop, grant, select row, update row on `/Root` to user;", "USE plato;\nGRANT DROP, GRANT, SELECT ROW, UPDATE ROW ON `/Root` TO user;\n\n"},
            {"use plato;grant erase row, create directory on `/Root` to user;", "USE plato;\nGRANT ERASE ROW, CREATE DIRECTORY ON `/Root` TO user;\n\n"},
            {"use plato;grant create table, create queue, remove schema on `/Root` to user;", "USE plato;\nGRANT CREATE TABLE, CREATE QUEUE, REMOVE SCHEMA ON `/Root` TO user;\n\n"},
            {"use plato;grant describe schema, alter schema on `/Root` to user;", "USE plato;\nGRANT DESCRIBE SCHEMA, ALTER SCHEMA ON `/Root` TO user;\n\n"},
            {"use plato;grant select, on `/Root` to user, with grant option;", "USE plato;\nGRANT SELECT, ON `/Root` TO user, WITH GRANT OPTION;\n\n"},
            {"use plato;grant all privileges on `/Root` to user;", "USE plato;\nGRANT ALL PRIVILEGES ON `/Root` TO user;\n\n"},
            {"use plato;grant list on `/Root/db1`, `/Root/db2` to user;", "USE plato;\nGRANT LIST ON `/Root/db1`, `/Root/db2` TO user;\n\n"}
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(RevokePermissions) {
        TCases cases {
            {"use plato;revoke connect, modify tables, list on `/Root` from user;", "USE plato;\nREVOKE CONNECT, MODIFY TABLES, LIST ON `/Root` FROM user;\n\n"},
            {"use plato;revoke select , select tables, select attributes on `/Root` from user;", "USE plato;\nREVOKE SELECT, SELECT TABLES, SELECT ATTRIBUTES ON `/Root` FROM user;\n\n"},
            {"use plato;revoke insert, modify attributes on `/Root` from user;", "USE plato;\nREVOKE INSERT, MODIFY ATTRIBUTES ON `/Root` FROM user;\n\n"},
            {"use plato;revoke use legacy, use on `/Root` from user1, user2;", "USE plato;\nREVOKE USE LEGACY, USE ON `/Root` FROM user1, user2;\n\n"},
            {"use plato;revoke manage, full legacy, full, create on `/Root` from user;", "USE plato;\nREVOKE MANAGE, FULL LEGACY, FULL, CREATE ON `/Root` FROM user;\n\n"},
            {"use plato;revoke drop, grant, select row, update row on `/Root` from user;", "USE plato;\nREVOKE DROP, GRANT, SELECT ROW, UPDATE ROW ON `/Root` FROM user;\n\n"},
            {"use plato;revoke erase row, create directory on `/Root` from user;", "USE plato;\nREVOKE ERASE ROW, CREATE DIRECTORY ON `/Root` FROM user;\n\n"},
            {"use plato;revoke create table, create queue, remove schema on `/Root` from user;", "USE plato;\nREVOKE CREATE TABLE, CREATE QUEUE, REMOVE SCHEMA ON `/Root` FROM user;\n\n"},
            {"use plato;revoke describe schema, alter schema on `/Root` from user;", "USE plato;\nREVOKE DESCRIBE SCHEMA, ALTER SCHEMA ON `/Root` FROM user;\n\n"},
            {"use plato;revoke grant option for insert, on `/Root` from user;", "USE plato;\nREVOKE GRANT OPTION FOR INSERT, ON `/Root` FROM user;\n\n"},
            {"use plato;revoke all privileges on `/Root` from user;", "USE plato;\nREVOKE ALL PRIVILEGES ON `/Root` FROM user;\n\n"},
            {"use plato;revoke list on `/Root/db1`, `/Root/db2` from user;", "USE plato;\nREVOKE LIST ON `/Root/db1`, `/Root/db2` FROM user;\n\n"}
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(DropRole) {
        TCases cases = {
            {"use plato;drop user user,user,user;","USE plato;\nDROP USER user, user, user;\n\n"},
            {"use plato;drop group if exists user;","USE plato;\nDROP GROUP IF EXISTS user;\n\n"},
            {"use plato;drop group user,;","USE plato;\nDROP GROUP user,;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CreateUser) {
        TCases cases = {
            {"use plato;create user user;","USE plato;\nCREATE USER user;\n\n"},
            {"use plato;create user user encrypted password 'foo';","USE plato;\nCREATE USER user ENCRYPTED PASSWORD 'foo';\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CreateGroup) {
        TCases cases = {
            {"use plato;create group user;","USE plato;\nCREATE GROUP user;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(AlterUser) {
        TCases cases = {
            {"use plato;alter user user rename to user;","USE plato;\nALTER USER user RENAME TO user;\n\n"},
            {"use plato;alter user user encrypted password 'foo';","USE plato;\nALTER USER user ENCRYPTED PASSWORD 'foo';\n\n"},
            {"use plato;alter user user with encrypted password 'foo';","USE plato;\nALTER USER user WITH ENCRYPTED PASSWORD 'foo';\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(AlterGroup) {
        TCases cases = {
            {"use plato;alter group user add user user;","USE plato;\nALTER GROUP user ADD USER user;\n\n"},
            {"use plato;alter group user drop user user;","USE plato;\nALTER GROUP user DROP USER user;\n\n"},
            {"use plato;alter group user add user user, user,;","USE plato;\nALTER GROUP user ADD USER user, user,;\n\n"},
            {"use plato;alter group user rename to user;","USE plato;\nALTER GROUP user RENAME TO user;\n\n"},
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
            {"values (1);","VALUES\n\t(1);\n\n"},
            {"values (1,2),(3,4);","VALUES\n\t(1, 2),\n\t(3, 4);\n\n"},
            {"values ('a\nb');","VALUES\n\t(\n\t\t'a\nb'\n\t);\n\n"},
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
            {"$a = select 1 union all select 2","$a =\n\tSELECT\n\t\t1\n\tUNION ALL\n\tSELECT\n\t\t2;\n\n"},
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
            {"create table user(user int32)","CREATE TABLE user (\n\tuser int32\n);\n\n"},
            {"create table user(user int32,user bool ?)","CREATE TABLE user (\n\tuser int32,\n\tuser bool?\n);\n\n"},
            {"create table user(user int32) with (user=user)","CREATE TABLE user (\n\tuser int32\n)\nWITH (user = user);\n\n"},
            {"create table user(primary key (user))","CREATE TABLE user (\n\tPRIMARY KEY (user)\n);\n\n"},
            {"create table user(primary key (user,user))","CREATE TABLE user (\n\tPRIMARY KEY (user, user)\n);\n\n"},
            {"create table user(partition by (user))","CREATE TABLE user (\n\tPARTITION BY (user)\n);\n\n"},
            {"create table user(partition by (user,user))","CREATE TABLE user (\n\tPARTITION BY (user, user)\n);\n\n"},
            {"create table user(order by (user asc))","CREATE TABLE user (\n\tORDER BY (user ASC)\n);\n\n"},
            {"create table user(order by (user desc,user))","CREATE TABLE user (\n\tORDER BY (user DESC, user)\n);\n\n"},
            {"create table user(index user global unique sync with (user=user,user=user) on (user,user))",
             "CREATE TABLE user (\n\tINDEX user GLOBAL UNIQUE SYNC WITH (user = user, user = user) ON (user, user)\n);\n\n"},
            {"create table user(index user global async with (user=user,) on (user))",
             "CREATE TABLE user (\n\tINDEX user GLOBAL ASYNC WITH (user = user,) ON (user)\n);\n\n"},
            {"create table user(index user local on (user) cover (user))",
             "CREATE TABLE user (\n\tINDEX user LOCAL ON (user) COVER (user)\n);\n\n"},
            {"create table user(index user local on (user) cover (user,user))",
             "CREATE TABLE user (\n\tINDEX user LOCAL ON (user) COVER (user, user)\n);\n\n"},
            {"create table user(family user (user='foo'))",
             "CREATE TABLE user (\n\tFAMILY user (user = 'foo')\n);\n\n"},
            {"create table user(family user (user='foo',user='bar'))",
             "CREATE TABLE user (\n\tFAMILY user (user = 'foo', user = 'bar')\n);\n\n"},
            {"create table user(changefeed user with (user='foo'))",
             "CREATE TABLE user (\n\tCHANGEFEED user WITH (user = 'foo')\n);\n\n"},
            {"create table user(changefeed user with (user='foo',user='bar'))",
             "CREATE TABLE user (\n\tCHANGEFEED user WITH (user = 'foo', user = 'bar')\n);\n\n"},
            {"create table user(foo int32, bar bool ?) inherits (s3:$cluster.xxx) partition by hash(a,b,hash) with (inherits=interval('PT1D') ON logical_time) tablestore tablestore",
              "CREATE TABLE user (\n"
              "\tfoo int32,\n"
              "\tbar bool?\n"
              ")\n"
              "INHERITS (s3: $cluster.xxx)\n"
              "PARTITION BY HASH (a, b, hash)\n"
              "WITH (inherits = interval('PT1D') ON logical_time)\n"
              "TABLESTORE tablestore;\n\n"},
            {"create table user(foo int32, bar bool ?) partition by hash(a,b,hash) with (tiering='some')",
              "CREATE TABLE user (\n"
              "\tfoo int32,\n"
              "\tbar bool?\n"
              ")\n"
              "PARTITION BY HASH (a, b, hash)\n"
              "WITH (tiering = 'some');\n\n"}
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(ObjectOperations) {
        TCases cases = {
            {"alter oBject usEr (TYpe abcde) Set (a = b)",
             "ALTER OBJECT usEr (TYPE abcde) SET (a = b);\n\n"},
            {"creAte oBject usEr (tYpe abcde) With (a = b)",
             "CREATE OBJECT usEr (TYPE abcde) WITH (a = b);\n\n"},
            {"creAte oBject usEr (tYpe abcde) With a = b",
             "CREATE OBJECT usEr (TYPE abcde) WITH a = b;\n\n"},
            {"dRop oBject usEr (tYpe abcde) With (aeEE)",
             "DROP OBJECT usEr (TYPE abcde) WITH (aeEE);\n\n"},
            {"dRop oBject usEr (tYpe abcde) With aeEE",
             "DROP OBJECT usEr (TYPE abcde) WITH aeEE;\n\n"}
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(ExternalDataSourceOperations) {
        TCases cases = {
            {"creAte exTernAl daTa SouRce usEr With (a = \"b\")",
             "CREATE EXTERNAL DATA SOURCE usEr WITH (a = \"b\");\n\n"},
            {"dRop exTerNal Data SouRce usEr",
             "DROP EXTERNAL DATA SOURCE usEr;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(AsyncReplication) {
        TCases cases = {
            {"create async replication user for table1 AS table2 with (user='foo')",
             "CREATE ASYNC REPLICATION user FOR table1 AS table2 WITH (user = 'foo');\n\n"},
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
             "CREATE EXTERNAL TABLE usEr (\n\ta int\n)\nWITH (a = \"b\");\n\n"},
            {"dRop exTerNal taBlE usEr",
             "DROP EXTERNAL TABLE usEr;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(TypeSelection) {
        TCases cases = {
            {"Select tYpe.* frOm Table tYpe",
             "SELECT\n\ttYpe.*\nFROM Table\n\ttYpe;\n\n"}
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(AlterTable) {
        TCases cases = {
            {"alter table user add user int32",
             "ALTER TABLE user\n\tADD user int32;\n\n"},
            {"alter table user add user int32, add user bool ?",
             "ALTER TABLE user\n\tADD user int32,\n\tADD user bool?;\n\n"},
            {"alter table user add column user int32",
             "ALTER TABLE user\n\tADD COLUMN user int32;\n\n"},
            {"alter table user drop user",
             "ALTER TABLE user\n\tDROP user;\n\n"},
            {"alter table user drop column user",
             "ALTER TABLE user\n\tDROP COLUMN user;\n\n"},
            {"alter table user alter column user set family user",
             "ALTER TABLE user\n\tALTER COLUMN user SET FAMILY user;\n\n"},
            {"alter table user add family user(user='foo')",
             "ALTER TABLE user\n\tADD FAMILY user (user = 'foo');\n\n"},
            {"alter table user alter family user set user 'foo'",
             "ALTER TABLE user\n\tALTER FAMILY user SET user 'foo';\n\n"},
            {"alter table user set user user",
             "ALTER TABLE user\n\tSET user user;\n\n"},
            {"alter table user set (user=user)",
             "ALTER TABLE user\n\tSET (user = user);\n\n"},
            {"alter table user set (user=user,user=user)",
             "ALTER TABLE user\n\tSET (user = user, user = user);\n\n"},
            {"alter table user reset(user)",
             "ALTER TABLE user\n\tRESET (user);\n\n"},
            {"alter table user reset(user, user)",
             "ALTER TABLE user\n\tRESET (user, user);\n\n"},
            {"alter table user add index user local on (user)",
             "ALTER TABLE user\n\tADD INDEX user LOCAL ON (user);\n\n"},
            {"alter table user drop index user",
             "ALTER TABLE user\n\tDROP INDEX user;\n\n"},
            {"alter table user rename to user",
             "ALTER TABLE user\n\tRENAME TO user;\n\n"},
            {"alter table user add changefeed user with (user = 'foo')",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (user = 'foo');\n\n"},
            {"alter table user alter changefeed user disable",
             "ALTER TABLE user\n\tALTER CHANGEFEED user DISABLE;\n\n"},
            {"alter table user alter changefeed user set(user='foo')",
             "ALTER TABLE user\n\tALTER CHANGEFEED user SET (user = 'foo');\n\n"},
            {"alter table user drop changefeed user",
             "ALTER TABLE user\n\tDROP CHANGEFEED user;\n\n"},
            {"alter table user add changefeed user with (initial_scan = tRUe)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (initial_scan = TRUE);\n\n"},
            {"alter table user add changefeed user with (initial_scan = FaLsE)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (initial_scan = FALSE);\n\n"},
            {"alter table user add changefeed user with (retention_period = Interval(\"P1D\"))",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (retention_period = Interval(\"P1D\"));\n\n"},
            {"alter table user add changefeed user with (virtual_timestamps = TruE)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (virtual_timestamps = TRUE);\n\n"},
            {"alter table user add changefeed user with (virtual_timestamps = fAlSe)",
             "ALTER TABLE user\n\tADD CHANGEFEED user WITH (virtual_timestamps = FALSE);\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CreateTopic) {
        TCases cases = {
            {"create topic topic1",
             "CREATE TOPIC topic1;\n\n"},
             {"create topic topic1 (consumer c1)",
             "CREATE TOPIC topic1 (\n\tCONSUMER c1\n);\n\n"},
             {"create topic topic1 (consumer c1, consumer c2 with (important = True))",
             "CREATE TOPIC topic1 (\n\tCONSUMER c1,\n\tCONSUMER c2 WITH (important = TRUE)\n);\n\n"},
             {"create topic topic1 (consumer c1) with (partition_count_limit = 5)",
             "CREATE TOPIC topic1 (\n\tCONSUMER c1\n) WITH (\n\tpartition_count_limit = 5\n);\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }
    Y_UNIT_TEST(AlterTopic) {
        TCases cases = {
             {"alter topic topic1 alter consumer c1 set (important = false)",
             "ALTER TOPIC topic1\n\tALTER CONSUMER c1 SET (important = FALSE);\n\n"},
             {"alter topic topic1 alter consumer c1 set (important = false), alter consumer c2 reset (read_from)",
              "ALTER TOPIC topic1\n\tALTER CONSUMER c1 SET (important = FALSE),\n\tALTER CONSUMER c2 RESET (read_from);\n\n"},
             {"alter topic topic1 add consumer c1, drop consumer c2",
              "ALTER TOPIC topic1\n\tADD CONSUMER c1,\n\tDROP CONSUMER c2;\n\n"},
             {"alter topic topic1 set (supported_codecs = 'RAW'), RESET (retention_period)",
              "ALTER TOPIC topic1\n\tSET (supported_codecs = 'RAW'),\n\tRESET (retention_period);\n\n"},

        };

        TSetup setup;
        setup.Run(cases);
    }
    Y_UNIT_TEST(DropTopic) {
        TCases cases = {
            {"drop topic topic1",
             "DROP TOPIC topic1;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Do) {
        TCases cases = {
            {"do $a(1,2,3)",
             "DO $a(1, 2, 3);\n"},
            {"do begin values(1); end do;",
             "DO BEGIN\n\tVALUES\n\t\t(1);\nEND DO;\n\n"},
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
             "END DEFINE;\n\n\tDO $b();\n\tPROCESS $c();\nEND DEFINE;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(If) {
        TCases cases = {
            {"evaluate if 1=1 do $a()",
             "EVALUATE IF 1 = 1\n\tDO $a();\n\n"},
            {"evaluate if 1=1 do $a() else do $b()",
             "EVALUATE IF 1 = 1\n\tDO $a()\nELSE\n\tDO $b();\n\n"},
            {"evaluate if 1=1 do begin select 1; end do",
             "EVALUATE IF 1 = 1\n\tDO BEGIN\n\t\tSELECT\n\t\t\t1;\n\tEND DO;\n\n"},
            {"evaluate if 1=1 do begin select 1; end do else do begin select 2; end do",
             "EVALUATE IF 1 = 1\n\tDO BEGIN\n\t\tSELECT\n\t\t\t1;\n\tEND DO\n"
             "ELSE\n\tDO BEGIN\n\t\tSELECT\n\t\t\t2;\n\tEND DO;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(For) {
        TCases cases = {
            {"evaluate for $x in [] do $a($x)",
             "EVALUATE FOR $x IN []\n\tDO $a($x);\n\n"},
            {"evaluate for $x in [] do $a($x) else do $b()",
             "EVALUATE FOR $x IN []\n\tDO $a($x)\nELSE\n\tDO $b();\n\n"},
            {"evaluate for $x in [] do begin select $x; end do",
             "EVALUATE FOR $x IN []\n\tDO BEGIN\n\t\tSELECT\n\t\t\t$x;\n\tEND DO;\n\n"},
            {"evaluate for $x in [] do begin select $x; end do else do begin select 2; end do",
             "EVALUATE FOR $x IN []\n\tDO BEGIN\n\t\tSELECT\n\t\t\t$x;\n\tEND DO\nELSE\n\tDO BEGIN\n\t\tSELECT\n\t\t\t2;\n\tEND DO;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Update) {
        TCases cases = {
            {"update user on default values",
             "UPDATE user\nON DEFAULT VALUES;\n\n"},
            {"update user on values (1),(2)",
             "UPDATE user\nON\nVALUES\n\t(1),\n\t(2);\n\n"},
            {"update user on select 1 as x, 2 as y",
             "UPDATE user\nON\nSELECT\n\t1 AS x,\n\t2 AS y;\n\n"},
            {"update user on (x) values (1),(2),(3)",
             "UPDATE user\nON (\n\tx\n)\nVALUES\n\t(1),\n\t(2),\n\t(3);\n\n"},
            {"update user on (x,y) values (1,2),(2,3),(3,4)",
             "UPDATE user\nON (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2),\n\t(2, 3),\n\t(3, 4);\n\n"},
            {"update user on (x) select 1",
             "UPDATE user\nON (\n\tx\n)\nSELECT\n\t1;\n\n"},
            {"update user on (x,y) select 1,2",
             "UPDATE user\nON (\n\tx,\n\ty\n)\nSELECT\n\t1,\n\t2;\n\n"},
            {"update user set x=1",
             "UPDATE user\nSET\n\tx = 1;\n\n"},
            {"update user set (x)=(1)",
             "UPDATE user\nSET\n(\n\tx\n) = (\n\t1\n);\n\n"},
            {"update user set (x,y)=(1,2)",
             "UPDATE user\nSET\n(\n\tx,\n\ty\n) = (\n\t1,\n\t2\n);\n\n"},
            {"update user set (x,y)=(select 1,2)",
             "UPDATE user\nSET\n(\n\tx,\n\ty\n) = (\n\tSELECT\n\t\t1,\n\t\t2\n);\n\n"},
            {"update user set x=1,y=2 where z=3",
             "UPDATE user\nSET\n\tx = 1,\n\ty = 2\nWHERE z = 3;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Delete) {
        TCases cases = {
            {"delete from user",
             "DELETE FROM user;\n\n"},
            {"delete from user where 1=1",
             "DELETE FROM user\nWHERE 1 = 1;\n\n"},
            {"delete from user on select 1 as x, 2 as y",
             "DELETE FROM user\nON\nSELECT\n\t1 AS x,\n\t2 AS y;\n\n"},
            {"delete from user on (x) values (1)",
             "DELETE FROM user\nON (\n\tx\n)\nVALUES\n\t(1);\n\n"},
            {"delete from user on (x,y) values (1,2), (3,4)",
             "DELETE FROM user\nON (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2),\n\t(3, 4);\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Into) {
        TCases cases = {
            {"insert into user select 1 as x",
             "INSERT INTO user\nSELECT\n\t1 AS x;\n\n"},
            {"insert or abort into user select 1 as x",
             "INSERT OR ABORT INTO user\nSELECT\n\t1 AS x;\n\n"},
            {"insert or revert into user select 1 as x",
             "INSERT OR REVERT INTO user\nSELECT\n\t1 AS x;\n\n"},
            {"insert or ignore into user select 1 as x",
             "INSERT OR IGNORE INTO user\nSELECT\n\t1 AS x;\n\n"},
            {"upsert into user select 1 as x",
             "UPSERT INTO user\nSELECT\n\t1 AS x;\n\n"},
            {"replace into user select 1 as x",
             "REPLACE INTO user\nSELECT\n\t1 AS x;\n\n"},
            {"insert into user(x) values (1)",
             "INSERT INTO user (\n\tx\n)\nVALUES\n\t(1);\n\n"},
            {"insert into user(x,y) values (1,2)",
             "INSERT INTO user (\n\tx,\n\ty\n)\nVALUES\n\t(1, 2);\n\n"},
            {"insert into plato.user select 1 as x",
             "INSERT INTO plato.user\nSELECT\n\t1 AS x;\n\n"},
            {"insert into @user select 1 as x",
             "INSERT INTO @user\nSELECT\n\t1 AS x;\n\n"},
            {"insert into $user select 1 as x",
             "INSERT INTO $user\nSELECT\n\t1 AS x;\n\n"},
            {"insert into @$user select 1 as x",
             "INSERT INTO @$user\nSELECT\n\t1 AS x;\n\n"},
            {"upsert into user erase by (x,y) values (1)",
             "UPSERT INTO user\n\tERASE BY (\n\t\tx,\n\t\ty\n\t)\nVALUES\n\t(1);\n\n"},
            {"insert into user with truncate select 1 as x",
             "INSERT INTO user\n\tWITH truncate\nSELECT\n\t1 AS x;\n\n"},
            {"insert into user with (truncate,inferscheme='1') select 1 as x",
             "INSERT INTO user\n\tWITH (truncate, inferscheme = '1')\nSELECT\n\t1 AS x;\n\n"},
            {"insert into user with schema Struct<user:int32> select 1 as user",
             "INSERT INTO user\n\tWITH SCHEMA Struct<user: int32>\nSELECT\n\t1 AS user;\n\n"},
            {"insert into user with schema (int32 as user) select 1 as user",
             "INSERT INTO user\n\tWITH SCHEMA (int32 AS user)\nSELECT\n\t1 AS user;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Process) {
        TCases cases = {
            {"process user",
             "PROCESS user;\n\n"},
            {"process user using $f() as user",
             "PROCESS user\nUSING $f() AS user;\n\n"},
            {"process user,user using $f()",
             "PROCESS user, user\nUSING $f();\n\n"},
            {"process user using $f() where 1=1 having 1=1 assume order by user",
             "PROCESS user\nUSING $f()\nWHERE 1 = 1\nHAVING 1 = 1\nASSUME ORDER BY\n\tuser;\n\n"},
            {"process user using $f() union all process user using $f()",
             "PROCESS user\nUSING $f()\nUNION ALL\nPROCESS user\nUSING $f();\n\n"},
            {"process user using $f() with foo=bar",
             "PROCESS user\nUSING $f()\nWITH foo = bar;\n\n"},
            {"discard process user using $f()",
             "DISCARD PROCESS user\nUSING $f();\n\n"},
            {"process user using $f() into result user",
             "PROCESS user\nUSING $f()\nINTO RESULT user;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Reduce) {
        TCases cases = {
            {"reduce user on user using $f()",
             "REDUCE user\nON\n\tuser\nUSING $f();\n\n"},
            {"reduce user on user, using $f()",
             "REDUCE user\nON\n\tuser,\nUSING $f();\n\n"},
            {"discard reduce user on user using $f();",
             "DISCARD REDUCE user\nON\n\tuser\nUSING $f();\n\n"},
            {"reduce user on user using $f() into result user",
             "REDUCE user\nON\n\tuser\nUSING $f()\nINTO RESULT user;\n\n"},
            {"reduce user on user using all $f()",
             "REDUCE user\nON\n\tuser\nUSING ALL $f();\n\n"},
            {"reduce user on user using $f() as user",
             "REDUCE user\nON\n\tuser\nUSING $f() AS user;\n\n"},
            {"reduce user,user on user using $f()",
             "REDUCE user, user\nON\n\tuser\nUSING $f();\n\n"},
            {"reduce user on user,user using $f()",
             "REDUCE user\nON\n\tuser,\n\tuser\nUSING $f();\n\n"},
            {"reduce user on user using $f() where 1=1 having 1=1 assume order by user",
             "REDUCE user\nON\n\tuser\nUSING $f()\nWHERE 1 = 1\nHAVING 1 = 1\nASSUME ORDER BY\n\tuser;\n\n"},
            {"reduce user presort user,user on user using $f();",
             "REDUCE user\nPRESORT\n\tuser,\n\tuser\nON\n\tuser\nUSING $f();\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Select) {
        TCases cases = {
            {"select 1",
             "SELECT\n\t1;\n\n"},
            {"select 1,",
             "SELECT\n\t1,;\n\n"},
            {"select 1 as x",
             "SELECT\n\t1 AS x;\n\n"},
            {"select *",
             "SELECT\n\t*;\n\n"},
            {"select a.*",
             "SELECT\n\ta.*;\n\n"},
            {"select * without a",
             "SELECT\n\t*\n\tWITHOUT\n\t\ta;\n\n"},
            {"select * without a,b",
             "SELECT\n\t*\n\tWITHOUT\n\t\ta,\n\t\tb;\n\n"},
            {"select * without a,",
             "SELECT\n\t*\n\tWITHOUT\n\t\ta,;\n\n"},
            {"select 1 from user",
             "SELECT\n\t1\nFROM user;\n\n"},
            {"select 1 from plato.user",
             "SELECT\n\t1\nFROM plato.user;\n\n"},
            {"select 1 from $user",
             "SELECT\n\t1\nFROM $user;\n\n"},
            {"select 1 from @user",
             "SELECT\n\t1\nFROM @user;\n\n"},
            {"select 1 from @$user",
             "SELECT\n\t1\nFROM @$user;\n\n"},
            {"select 1 from user view user",
             "SELECT\n\t1\nFROM user\n\tVIEW user;\n\n"},
            {"select 1 from user as user",
             "SELECT\n\t1\nFROM user\n\tAS user;\n\n"},
            {"select 1 from user as user(user)",
             "SELECT\n\t1\nFROM user\n\tAS user (\n\t\tuser\n\t);\n\n"},
            {"select 1 from user as user(user, user)",
             "SELECT\n\t1\nFROM user\n\tAS user (\n\t\tuser,\n\t\tuser\n\t);\n\n"},
            {"select 1 from user with user=user",
             "SELECT\n\t1\nFROM user\n\tWITH user = user;\n\n"},
            {"select 1 from user with (user=user, user=user)",
             "SELECT\n\t1\nFROM user\n\tWITH (user = user, user = user);\n\n"},
            {"select 1 from user sample 0.1",
             "SELECT\n\t1\nFROM user\n\tSAMPLE 0.1;\n\n"},
            {"select 1 from user tablesample system(0.1)",
             "SELECT\n\t1\nFROM user\n\tTABLESAMPLE SYSTEM (0.1);\n\n"},
            {"select 1 from user tablesample bernoulli(0.1) repeatable(10)",
             "SELECT\n\t1\nFROM user\n\tTABLESAMPLE BERNOULLI (0.1) REPEATABLE (10);\n\n"},
            {"select 1 from user flatten columns",
             "SELECT\n\t1\nFROM user\n\tFLATTEN COLUMNS;\n\n"},
            {"select 1 from user flatten list by user",
             "SELECT\n\t1\nFROM user\n\tFLATTEN LIST BY\n\t\tuser;\n\n"},
            {"select 1 from user flatten list by (user,user)",
             "SELECT\n\t1\nFROM user\n\tFLATTEN LIST BY (\n\t\tuser,\n\t\tuser\n\t);\n\n"},
            {"select 1 from $user(1,2)",
             "SELECT\n\t1\nFROM $user(1, 2);\n\n"},
            {"select 1 from $user(1,2) view user",
             "SELECT\n\t1\nFROM $user(1, 2)\n\tVIEW user;\n\n"},
            {"select 1 from range('a','b')",
             "SELECT\n\t1\nFROM range('a', 'b');\n\n"},
            {"from user select 1",
             "FROM user\nSELECT\n\t1;\n\n"},
            {"select * from user as a join user as b on a.x=b.y",
             "SELECT\n\t*\nFROM user\n\tAS a\nJOIN user\n\tAS b\nON a.x = b.y;\n\n"},
            {"select * from user as a join user as b using(x)",
             "SELECT\n\t*\nFROM user\n\tAS a\nJOIN user\n\tAS b\nUSING (x);\n\n"},
            {"select * from any user as a full join user as b on a.x=b.y",
             "SELECT\n\t*\nFROM ANY user\n\tAS a\nFULL JOIN user\n\tAS b\nON a.x = b.y;\n\n"},
            {"select * from user as a left join any user as b on a.x=b.y",
             "SELECT\n\t*\nFROM user\n\tAS a\nLEFT JOIN ANY user\n\tAS b\nON a.x = b.y;\n\n"},
            {"select * from any user as a right join any user as b on a.x=b.y",
             "SELECT\n\t*\nFROM ANY user\n\tAS a\nRIGHT JOIN ANY user\n\tAS b\nON a.x = b.y;\n\n"},
            {"select * from user as a cross join user as b",
             "SELECT\n\t*\nFROM user\n\tAS a\nCROSS JOIN user\n\tAS b;\n\n"},
            {"select 1 from user where key = 1",
             "SELECT\n\t1\nFROM user\nWHERE key = 1;\n\n"},
            {"select 1 from user having count(*) = 1",
             "SELECT\n\t1\nFROM user\nHAVING count(*) = 1;\n\n"},
            {"select 1 from user group by key",
             "SELECT\n\t1\nFROM user\nGROUP BY\n\tkey;\n\n"},
            {"select 1 from user group compact by key, value as v",
             "SELECT\n\t1\nFROM user\nGROUP COMPACT BY\n\tkey,\n\tvalue AS v;\n\n"},
            {"select 1 from user group by key with combine",
             "SELECT\n\t1\nFROM user\nGROUP BY\n\tkey\n\tWITH combine;\n\n"},
            {"select 1 from user order by key asc",
             "SELECT\n\t1\nFROM user\nORDER BY\n\tkey ASC;\n\n"},
            {"select 1 from user order by key, value desc",
             "SELECT\n\t1\nFROM user\nORDER BY\n\tkey,\n\tvalue DESC;\n\n"},
            {"select 1 from user assume order by key",
             "SELECT\n\t1\nFROM user\nASSUME ORDER BY\n\tkey;\n\n"},
            {"select 1 from user window w1 as (), w2 as ()",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t),\n\tw2 AS (\n\t);\n\n"},
            {"select 1 from user window w1 as (user)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tuser\n\t);\n\n"},
            {"select 1 from user window w1 as (partition by user)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tPARTITION BY\n\t\t\tuser\n\t);\n\n"},
            {"select 1 from user window w1 as (partition by user, user)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tPARTITION BY\n\t\t\tuser,\n\t\t\tuser\n\t);\n\n"},
            {"select 1 from user window w1 as (order by user asc)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tORDER BY\n\t\t\tuser ASC\n\t);\n\n"},
            {"select 1 from user window w1 as (order by user, user desc)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tORDER BY\n\t\t\tuser,\n\t\t\tuser DESC\n\t);\n\n"},
            {"select 1 from user window w1 as (rows between 1 preceding and 1 following)",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t\tROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING\n\t);\n\n"},
            {"select 1 limit 10",
             "SELECT\n\t1\nLIMIT 10;\n\n"},
            {"select 1 limit 10 offset 5",
             "SELECT\n\t1\nLIMIT 10 OFFSET 5;\n\n"},
            { "select 1 union all select 2",
             "SELECT\n\t1\nUNION ALL\nSELECT\n\t2;\n\n" },
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CompositeTypesAndQuestions) {
        TCases cases = {
            {"declare $_x AS list<int32>??;declare $_y AS int32 ? ? ;select 1<>2, 1??2,"
             "formattype(list<int32>), formattype(resource<user>),formattype(tuple<>), formattype(tuple<  >), formattype(int32 ? ? )",
             "DECLARE $_x AS list<int32>??;\nDECLARE $_y AS int32??;\nSELECT\n\t1 <> 2,\n\t1 ?? 2,\n\tformattype(list<int32>),"
            "\n\tformattype(resource<user>),\n\tformattype(tuple<>),\n\tformattype(tuple< >),\n\tformattype(int32??" ");\n\n"
            },
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Lambda) {
        TCases cases = {
            {"$f=($a,$b)->{$x=$a+$b;return $a*$x};$g=($a,$b?)->($a+$b??0);select $f(10,4),$g(1,2);",
             "$f = ($a, $b) -> {\n\t$x = $a + $b;\n\tRETURN $a * $x\n};\n"
             "$g = ($a, $b?) -> ($a + $b ?? 0);\n"
            "SELECT\n\t$f(10, 4),\n\t$g(1, 2);\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(NestedSelect) {
        TCases cases = {
            {"$x=select 1",
             "$x =\n\tSELECT\n\t\t1;\n\n"},
            {"$x=(select 1)",
             "$x = (\n\tSELECT\n\t\t1\n);\n\n"},
            {"select 1 in (select 1)",
             "SELECT\n\t1 IN (\n\t\tSELECT\n\t\t\t1\n\t);\n\n"},
            {"select 1 in ((select 1))",
             "SELECT\n\t1 IN (\n\t\t(\n\t\t\tSELECT\n\t\t\t\t1\n\t\t)\n\t);\n\n"},
            {"select 1 in (\nselect 1)",
             "SELECT\n\t1 IN (\n\t\tSELECT\n\t\t\t1\n\t);\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(Cast) {
        TCases cases = {
            {"select cast(1 as string)","SELECT\n\tCAST(1 AS string);\n\n"},
            {"select bitcast(1 as int32)","SELECT\n\tBITCAST(1 AS int32);\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(StructLiteral) {
        TCases cases = {
            {"select <||>","SELECT\n\t<||>;\n\n"},
            {"select <|a:1|>","SELECT\n\t<|a: 1|>;\n\n"},
            {"select <|a:1,b:2|>","SELECT\n\t<|a: 1, b: 2|>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(TableHints) {
        TCases cases = {
            {"select * from plato.T with schema(foo int32, bar list<string>) where key is not null",
             "SELECT\n\t*\nFROM plato.T\n\tWITH SCHEMA (foo int32, bar list<string>)\nWHERE key IS NOT NULL;\n\n"},
            {"select * from plato.T with schema struct<foo:integer, Bar:list<string?>> where key<0",
             "SELECT\n\t*\nFROM plato.T\n\tWITH SCHEMA struct<foo: integer, Bar: list<string?>>\nWHERE key < 0;\n\n"},
            {"select * from plato.T with (foo=bar, x=$y, a=(a, b, c), u='aaa', schema (foo int32, bar list<string>))",
             "SELECT\n\t*\nFROM plato.T\n\tWITH (foo = bar, x = $y, a = (a, b, c), u = 'aaa', SCHEMA (foo int32, bar list<string>));\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(BoolAsVariableName) {
        TCases cases = {
            {"$ False = True; select $ False;",
             "$False = TRUE;\nSELECT\n\t$False;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(WithSchemaEquals) {
        TCases cases = {
            {"select * from plato.T with (format= csv_with_names, schema=(year int32 Null, month String, day String not   null, a Utf8, b Uint16));",
             "SELECT\n\t*\nFROM plato.T\n\tWITH (format = csv_with_names, SCHEMA = (year int32 NULL, month String, day String NOT NULL, a Utf8, b Uint16));\n\n"},
             };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(SquareBrackets) {
        TCases cases = {
            {"select a[0]",
             "SELECT\n\ta[0];\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineList) {
        TCases cases = {
            {"select [\n]",
             "SELECT\n\t[\n\t];\n\n"},
            {"select [1\n]",
             "SELECT\n\t[\n\t\t1\n\t];\n\n"},
            {"select [\n1]",
             "SELECT\n\t[\n\t\t1\n\t];\n\n"},
            {"select [1,\n]",
             "SELECT\n\t[\n\t\t1,\n\t];\n\n"},
            {"select [1\n,]",
             "SELECT\n\t[\n\t\t1,\n\t];\n\n"},
            {"select [\n1,]",
             "SELECT\n\t[\n\t\t1,\n\t];\n\n"},
            {"select [1,2,\n3,4]",
             "SELECT\n\t[\n\t\t1, 2,\n\t\t3, 4\n\t];\n\n"},
            {"select [1,2,\n3,4,]",
             "SELECT\n\t[\n\t\t1, 2,\n\t\t3, 4,\n\t];\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineTuple) {
        TCases cases = {
            {"select (\n)",
             "SELECT\n\t(\n\t);\n\n"},
            {"select (1,\n)",
             "SELECT\n\t(\n\t\t1,\n\t);\n\n"},
            {"select (1\n,)",
             "SELECT\n\t(\n\t\t1,\n\t);\n\n"},
            {"select (\n1,)",
             "SELECT\n\t(\n\t\t1,\n\t);\n\n"},
            {"select (1,2,\n3,4)",
             "SELECT\n\t(\n\t\t1, 2,\n\t\t3, 4\n\t);\n\n"},
            {"select (1,2,\n3,4,)",
             "SELECT\n\t(\n\t\t1, 2,\n\t\t3, 4,\n\t);\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineSet) {
        TCases cases = {
            {"select {\n}",
             "SELECT\n\t{\n\t};\n\n"},
            {"select {1\n}",
             "SELECT\n\t{\n\t\t1\n\t};\n\n"},
            {"select {\n1}",
             "SELECT\n\t{\n\t\t1\n\t};\n\n"},
            {"select {1,\n}",
             "SELECT\n\t{\n\t\t1,\n\t};\n\n"},
            {"select {1\n,}",
             "SELECT\n\t{\n\t\t1,\n\t};\n\n"},
            {"select {\n1,}",
             "SELECT\n\t{\n\t\t1,\n\t};\n\n"},
            {"select {1,2,\n3,4}",
             "SELECT\n\t{\n\t\t1, 2,\n\t\t3, 4\n\t};\n\n"},
            {"select {1,2,\n3,4,}",
             "SELECT\n\t{\n\t\t1, 2,\n\t\t3, 4,\n\t};\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineDict) {
        TCases cases = {
            {"select {0:1\n}",
             "SELECT\n\t{\n\t\t0: 1\n\t};\n\n"},
            {"select {\n0:1}",
             "SELECT\n\t{\n\t\t0: 1\n\t};\n\n"},
            {"select {0:1,\n}",
             "SELECT\n\t{\n\t\t0: 1,\n\t};\n\n"},
            {"select {0:1\n,}",
             "SELECT\n\t{\n\t\t0: 1,\n\t};\n\n"},
            {"select {\n0:1,}",
             "SELECT\n\t{\n\t\t0: 1,\n\t};\n\n"},
            {"select {10:1,20:2,\n30:3,40:4}",
             "SELECT\n\t{\n\t\t10: 1, 20: 2,\n\t\t30: 3, 40: 4\n\t};\n\n"},
            {"select {10:1,20:2,\n30:3,40:4,}",
             "SELECT\n\t{\n\t\t10: 1, 20: 2,\n\t\t30: 3, 40: 4,\n\t};\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineFuncCall) {
        TCases cases = {
            {"select f(\n)",
             "SELECT\n\tf(\n\t);\n\n"},
            {"select f(1\n)",
             "SELECT\n\tf(\n\t\t1\n\t);\n\n"},
            {"select f(\n1)",
             "SELECT\n\tf(\n\t\t1\n\t);\n\n"},
            {"select f(1,\n)",
             "SELECT\n\tf(\n\t\t1,\n\t);\n\n"},
            {"select f(1\n,)",
             "SELECT\n\tf(\n\t\t1,\n\t);\n\n"},
            {"select f(\n1,)",
             "SELECT\n\tf(\n\t\t1,\n\t);\n\n"},
            {"select f(1,2,\n3,4)",
             "SELECT\n\tf(\n\t\t1, 2,\n\t\t3, 4\n\t);\n\n"},
            {"select f(1,2,\n3,4,)",
             "SELECT\n\tf(\n\t\t1, 2,\n\t\t3, 4,\n\t);\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineStruct) {
        TCases cases = {
            {"select <|\n|>",
             "SELECT\n\t<|\n\t|>;\n\n"},
            {"select <|a:1\n|>",
             "SELECT\n\t<|\n\t\ta: 1\n\t|>;\n\n"},
            {"select <|\na:1|>",
             "SELECT\n\t<|\n\t\ta: 1\n\t|>;\n\n"},
            {"select <|a:1,\n|>",
             "SELECT\n\t<|\n\t\ta: 1,\n\t|>;\n\n"},
            {"select <|a:1\n,|>",
             "SELECT\n\t<|\n\t\ta: 1,\n\t|>;\n\n"},
            {"select <|\na:1,|>",
             "SELECT\n\t<|\n\t\ta: 1,\n\t|>;\n\n"},
            {"select <|a:1,b:2,\nc:3,d:4|>",
             "SELECT\n\t<|\n\t\ta: 1, b: 2,\n\t\tc: 3, d: 4\n\t|>;\n\n"},
            {"select <|a:1,b:2,\nc:3,d:4,|>",
             "SELECT\n\t<|\n\t\ta: 1, b: 2,\n\t\tc: 3, d: 4,\n\t|>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineListType) {
        TCases cases = {
            {"select list<int32\n>",
             "SELECT\n\tlist<\n\t\tint32\n\t>;\n\n"},
            {"select list<\nint32>",
             "SELECT\n\tlist<\n\t\tint32\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineOptionalType) {
        TCases cases = {
            {"select optional<int32\n>",
             "SELECT\n\toptional<\n\t\tint32\n\t>;\n\n"},
            {"select optional<\nint32>",
             "SELECT\n\toptional<\n\t\tint32\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineStreamType) {
        TCases cases = {
            {"select stream<int32\n>",
             "SELECT\n\tstream<\n\t\tint32\n\t>;\n\n"},
            {"select stream<\nint32>",
             "SELECT\n\tstream<\n\t\tint32\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineFlowType) {
        TCases cases = {
            {"select flow<int32\n>",
             "SELECT\n\tflow<\n\t\tint32\n\t>;\n\n"},
            {"select flow<\nint32>",
             "SELECT\n\tflow<\n\t\tint32\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineSetType) {
        TCases cases = {
            {"select set<int32\n>",
             "SELECT\n\tset<\n\t\tint32\n\t>;\n\n"},
            {"select set<\nint32>",
             "SELECT\n\tset<\n\t\tint32\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineTupleType) {
        TCases cases = {
            {"select tuple<\n>",
             "SELECT\n\ttuple<\n\t\t \n\t>;\n\n"},
            {"select tuple<int32\n>",
             "SELECT\n\ttuple<\n\t\tint32\n\t>;\n\n"},
            {"select tuple<\nint32>",
             "SELECT\n\ttuple<\n\t\tint32\n\t>;\n\n"},
            {"select tuple<int32,\n>",
             "SELECT\n\ttuple<\n\t\tint32,\n\t>;\n\n"},
            {"select tuple<int32\n,>",
             "SELECT\n\ttuple<\n\t\tint32,\n\t>;\n\n"},
            {"select tuple<\nint32,>",
             "SELECT\n\ttuple<\n\t\tint32,\n\t>;\n\n"},
            {"select tuple<\nint32,string,\ndouble,bool>",
             "SELECT\n\ttuple<\n\t\tint32, string,\n\t\tdouble, bool\n\t>;\n\n"},
            {"select tuple<\nint32,string,\ndouble,bool,>",
             "SELECT\n\ttuple<\n\t\tint32, string,\n\t\tdouble, bool,\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineStructType) {
        TCases cases = {
            {"select struct<\n>",
             "SELECT\n\tstruct<\n\t\t \n\t>;\n\n"},
            {"select struct<a:int32\n>",
             "SELECT\n\tstruct<\n\t\ta: int32\n\t>;\n\n"},
            {"select struct<\na:int32>",
             "SELECT\n\tstruct<\n\t\ta: int32\n\t>;\n\n"},
            {"select struct<a:int32,\n>",
             "SELECT\n\tstruct<\n\t\ta: int32,\n\t>;\n\n"},
            {"select struct<a:int32\n,>",
             "SELECT\n\tstruct<\n\t\ta: int32,\n\t>;\n\n"},
            {"select struct<\na:int32,>",
             "SELECT\n\tstruct<\n\t\ta: int32,\n\t>;\n\n"},
            {"select struct<\na:int32,b:string,\nc:double,d:bool>",
             "SELECT\n\tstruct<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool\n\t>;\n\n"},
            {"select struct<\na:int32,b:string,\nc:double,d:bool,>",
             "SELECT\n\tstruct<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool,\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineVariantOverTupleType) {
        TCases cases = {
            {"select variant<int32\n>",
             "SELECT\n\tvariant<\n\t\tint32\n\t>;\n\n"},
            {"select variant<\nint32>",
             "SELECT\n\tvariant<\n\t\tint32\n\t>;\n\n"},
            {"select variant<int32,\n>",
             "SELECT\n\tvariant<\n\t\tint32,\n\t>;\n\n"},
            {"select variant<int32\n,>",
             "SELECT\n\tvariant<\n\t\tint32,\n\t>;\n\n"},
            {"select variant<\nint32,>",
             "SELECT\n\tvariant<\n\t\tint32,\n\t>;\n\n"},
            {"select variant<\nint32,string,\ndouble,bool>",
             "SELECT\n\tvariant<\n\t\tint32, string,\n\t\tdouble, bool\n\t>;\n\n"},
            {"select variant<\nint32,string,\ndouble,bool,>",
             "SELECT\n\tvariant<\n\t\tint32, string,\n\t\tdouble, bool,\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineVariantOverStructType) {
        TCases cases = {
            {"select variant<a:int32\n>",
             "SELECT\n\tvariant<\n\t\ta: int32\n\t>;\n\n"},
            {"select variant<\na:int32>",
             "SELECT\n\tvariant<\n\t\ta: int32\n\t>;\n\n"},
            {"select variant<a:int32,\n>",
             "SELECT\n\tvariant<\n\t\ta: int32,\n\t>;\n\n"},
            {"select variant<a:int32\n,>",
             "SELECT\n\tvariant<\n\t\ta: int32,\n\t>;\n\n"},
            {"select variant<\na:int32,>",
             "SELECT\n\tvariant<\n\t\ta: int32,\n\t>;\n\n"},
            {"select variant<\na:int32,b:string,\nc:double,d:bool>",
             "SELECT\n\tvariant<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool\n\t>;\n\n"},
            {"select variant<\na:int32,b:string,\nc:double,d:bool,>",
             "SELECT\n\tvariant<\n\t\ta: int32, b: string,\n\t\tc: double, d: bool,\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineEnum) {
        TCases cases = {
            {"select enum<a\n>",
             "SELECT\n\tenum<\n\t\ta\n\t>;\n\n"},
            {"select enum<\na>",
             "SELECT\n\tenum<\n\t\ta\n\t>;\n\n"},
            {"select enum<a,\n>",
             "SELECT\n\tenum<\n\t\ta,\n\t>;\n\n"},
            {"select enum<a\n,>",
             "SELECT\n\tenum<\n\t\ta,\n\t>;\n\n"},
            {"select enum<\na,>",
             "SELECT\n\tenum<\n\t\ta,\n\t>;\n\n"},
            {"select enum<\na,b,\nc,d>",
             "SELECT\n\tenum<\n\t\ta, b,\n\t\tc, d\n\t>;\n\n"},
            {"select enum<\na,b,\nc,d,>",
             "SELECT\n\tenum<\n\t\ta, b,\n\t\tc, d,\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineResourceType) {
        TCases cases = {
            {"select resource<foo\n>",
             "SELECT\n\tresource<\n\t\tfoo\n\t>;\n\n"},
            {"select resource<\nfoo>",
             "SELECT\n\tresource<\n\t\tfoo\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineTaggedType) {
        TCases cases = {
            {"select tagged<int32,foo\n>",
             "SELECT\n\ttagged<\n\t\tint32, foo\n\t>;\n\n"},
            {"select tagged<int32,\nfoo>",
             "SELECT\n\ttagged<\n\t\tint32,\n\t\tfoo\n\t>;\n\n"},
            {"select tagged<int32\n,foo>",
             "SELECT\n\ttagged<\n\t\tint32, foo\n\t>;\n\n"},
            {"select tagged<\nint32,foo>",
             "SELECT\n\ttagged<\n\t\tint32, foo\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineDictType) {
        TCases cases = {
            {"select dict<int32,string\n>",
             "SELECT\n\tdict<\n\t\tint32, string\n\t>;\n\n"},
            {"select dict<int32,\nstring>",
             "SELECT\n\tdict<\n\t\tint32,\n\t\tstring\n\t>;\n\n"},
            {"select dict<int32\n,string>",
             "SELECT\n\tdict<\n\t\tint32, string\n\t>;\n\n"},
            {"select dict<\nint32,string>",
             "SELECT\n\tdict<\n\t\tint32, string\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(MultiLineCallableType) {
        TCases cases = {
            {"select callable<()->int32\n>",
             "SELECT\n\tcallable<\n\t\t() -> int32\n\t>;\n\n"},
            {"select callable<\n()->int32>",
             "SELECT\n\tcallable<\n\t\t() -> int32\n\t>;\n\n"},
            {"select callable<\n(int32)->int32>",
             "SELECT\n\tcallable<\n\t\t(int32) -> int32\n\t>;\n\n"},
            {"select callable<\n(int32,\ndouble)->int32>",
             "SELECT\n\tcallable<\n\t\t(\n\t\t\tint32,\n\t\t\tdouble\n\t\t) -> int32\n\t>;\n\n"},
            {"select callable<\n(int32\n,double)->int32>",
             "SELECT\n\tcallable<\n\t\t(\n\t\t\tint32, double\n\t\t) -> int32\n\t>;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(UnaryOp) {
        TCases cases = {
            {"select -x,+x,~x,-1,-1.0,+1,+1.0,~1u",
             "SELECT\n\t-x,\n\t+x,\n\t~x,\n\t-1,\n\t-1.0,\n\t+1,\n\t+1.0,\n\t~1u;\n\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }


}
