#include <ydb/library/yql/public/purecalc/purecalc.h>

#include "fake_spec.h"

#include <ydb/library/yql/public/purecalc/ut/protos/test_structs.pb.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TestPg) {
    using namespace NYql::NPureCalc;

    Y_UNIT_TEST(TestPgCompile) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            SELECT * FROM "Input";
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(1,true), FakeOS(true), sql, ETranslationMode::PG);
        }());

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullStreamProgram(FakeIS(1,true), FakeOS(true), sql, ETranslationMode::PG);
        }(), TCompileError, "PullList mode");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePushStreamProgram(FakeIS(1, true), FakeOS(true), sql, ETranslationMode::PG);
        }(), TCompileError, "PullList mode");
    }

    Y_UNIT_TEST(TestSqlWrongTableName) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            SELECT * FROM WrongTable;
        )");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullListProgram(FakeIS(1, true), FakeOS(true), sql, ETranslationMode::PG);
        }(), TCompileError, "Failed to optimize");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullStreamProgram(FakeIS(1, true), FakeOS(true), sql, ETranslationMode::PG);
        }(), TCompileError, "PullList mode");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePushStreamProgram(FakeIS(1, true), FakeOS(true), sql, ETranslationMode::PG);
        }(), TCompileError, "PullList mode");
    }

    Y_UNIT_TEST(TestInvalidSql) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            Just some invalid SQL;
        )");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullListProgram(FakeIS(1, true), FakeOS(true), sql, ETranslationMode::PG);
        }(), TCompileError, "failed to parse PG");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullStreamProgram(FakeIS(1, true), FakeOS(true), sql, ETranslationMode::PG);
        }(), TCompileError, "PullList mode");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePushStreamProgram(FakeIS(1, true), FakeOS(true), sql, ETranslationMode::PG);
        }(), TCompileError, "PullList mode");
    }
}
