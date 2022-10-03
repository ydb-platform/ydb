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
            {"select a.1 .b from plato.foo;","SELECT\n\ta.1 .b\nFROM plato.foo;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(DropRole) {
        TCases cases = {
            {"use plato;drop user user,user,user;","USE plato;\nDROP USER user, user, user;\n"},
            {"use plato;drop group if exists user;","USE plato;\nDROP GROUP IF EXISTS user;\n"},
            {"use plato;drop group user,;","USE plato;\nDROP GROUP user,;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CreateUser) {
        TCases cases = {
            {"use plato;create user user;","USE plato;\nCREATE USER user;\n"},
            {"use plato;create user user encrypted password 'foo';","USE plato;\nCREATE USER user ENCRYPTED PASSWORD 'foo';\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(CreateGroup) {
        TCases cases = {
            {"use plato;create group user;","USE plato;\nCREATE GROUP user;\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(AlterUser) {
        TCases cases = {
            {"use plato;alter user user rename to user;","USE plato;\nALTER USER user RENAME TO user;\n"},
            {"use plato;alter user user encrypted password 'foo';","USE plato;\nALTER USER user ENCRYPTED PASSWORD 'foo';\n"},
            {"use plato;alter user user with encrypted password 'foo';","USE plato;\nALTER USER user WITH ENCRYPTED PASSWORD 'foo';\n"},
        };

        TSetup setup;
        setup.Run(cases);
    }

    Y_UNIT_TEST(AlterGroup) {
        TCases cases = {
            {"use plato;alter group user add user user;","USE plato;\nALTER GROUP user ADD USER user;\n"},
            {"use plato;alter group user drop user user;","USE plato;\nALTER GROUP user DROP USER user;\n"},
            {"use plato;alter group user add user user, user,;","USE plato;\nALTER GROUP user ADD USER user, user,;\n"},
            {"use plato;alter group user rename to user;","USE plato;\nALTER GROUP user RENAME TO user;\n"},
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
            {"create table user(index user global unique sync with (user=user,user=user) on (user,user))",
             "CREATE TABLE user (\n\tINDEX user GLOBAL UNIQUE SYNC WITH (user = user, user = user) ON (user, user)\n);\n"},
            {"create table user(index user global async with (user=user,) on (user))",
             "CREATE TABLE user (\n\tINDEX user GLOBAL ASYNC WITH (user = user,) ON (user)\n);\n"},
            {"create table user(index user local on (user) cover (user))",
             "CREATE TABLE user (\n\tINDEX user LOCAL ON (user) COVER (user)\n);\n"},
            {"create table user(index user local on (user) cover (user,user))",
             "CREATE TABLE user (\n\tINDEX user LOCAL ON (user) COVER (user, user)\n);\n"},
            {"create table user(family user (user='foo'))",
             "CREATE TABLE user (\n\tFAMILY user (user = 'foo')\n);\n"},
            {"create table user(family user (user='foo',user='bar'))",
             "CREATE TABLE user (\n\tFAMILY user (user = 'foo', user = 'bar')\n);\n"},
            {"create table user(changefeed user with (user='foo'))",
             "CREATE TABLE user (\n\tCHANGEFEED user WITH (user = 'foo')\n);\n"},
            {"create table user(changefeed user with (user='foo',user='bar'))",
             "CREATE TABLE user (\n\tCHANGEFEED user WITH (user = 'foo', user = 'bar')\n);\n"},
             {"create table user(foo int32, bar bool ?) inherits (s3:$cluster.xxx) partition by hash(a,b,hash) with (inherits=interval('PT1D') ON logical_time) tablestore tablestore",
              "CREATE TABLE user (\n"
              "\tfoo int32,\n"
              "\tbar bool?\n"
              ")\n"
              "INHERITS (s3: $cluster.xxx)\n"
              "PARTITION BY HASH (a, b, hash)\n"
              "WITH (inherits = interval('PT1D') ON logical_time)\n"
              "TABLESTORE tablestore;\n"}
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
             "VALUES\n\t\t\t(1);\n\tEND DEFINE;\n\t"
             "DEFINE SUBQUERY $c() AS\n\t\tSELECT\n\t\t\t1;\n\t"
             "END DEFINE;\n\tDO $b();\n\tPROCESS $c();\nEND DEFINE;\n"},
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
            {"select 1 from user order by key asc",
             "SELECT\n\t1\nFROM user\nORDER BY\n\tkey ASC;\n"},
            {"select 1 from user order by key, value desc",
             "SELECT\n\t1\nFROM user\nORDER BY\n\tkey,\n\tvalue DESC;\n"},
            {"select 1 from user assume order by key",
             "SELECT\n\t1\nFROM user\nASSUME ORDER BY\n\tkey;\n"},
            {"select 1 from user window w1 as (), w2 as ()",
             "SELECT\n\t1\nFROM user\nWINDOW\n\tw1 AS (\n\t),\n\tw2 AS (\n\t);\n"},
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
             "DECLARE $_x AS list<int32>??;\nDECLARE $_y AS int32??;\nSELECT\n\t1 <> 2,\n\t1 ?? 2,\n\tformattype(list<int32>),"
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
             "$g = ($a, $b?) -> ($a + $b ?? 0);\n"
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
             "$False = TRUE;\nSELECT\n\t$False;\n"},
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
}
