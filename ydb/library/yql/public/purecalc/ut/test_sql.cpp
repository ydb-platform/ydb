#include <ydb/library/yql/public/purecalc/purecalc.h>

#include "fake_spec.h"

#include <ydb/library/yql/public/purecalc/ut/protos/test_structs.pb.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TestSql) {
    using namespace NYql::NPureCalc;

    Y_UNIT_TEST(TestSqlCompile) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            SELECT * FROM Input;
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());

        auto program = factory->MakePushStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        auto expectedIssues = TString(R"(<main>: Warning: Type annotation, code: 1030
    generated.sql:2:13: Warning: At function: PersistableRepr
        generated.sql:2:13: Warning: Persistable required. Atom, key, world, datasink, datasource, callable, resource, stream and lambda are not persistable, code: 1104
)");

        UNIT_ASSERT_VALUES_EQUAL(expectedIssues, program->GetIssues().ToString());
    }

    Y_UNIT_TEST(TestSqlCompileSingleUnnamedInput) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            SELECT * FROM TABLES()
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePushStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());
    }

    Y_UNIT_TEST(TestSqlCompileNamedMultiinputs) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            SELECT * FROM Input0
            UNION ALL
            SELECT * FROM Input1
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(2), FakeOS(), sql, ETranslationMode::SQL);
        }());
    }

    Y_UNIT_TEST(TestSqlCompileUnnamedMultiinputs) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            $t0, $t1, $t2 = PROCESS TABLES();
            SELECT * FROM $t0
            UNION ALL
            SELECT * FROM $t1
            UNION ALL
            SELECT * FROM $t2
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(3), FakeOS(), sql, ETranslationMode::SQL);
        }());
    }

    Y_UNIT_TEST(TestSqlCompileWithWarning) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            $x = 1;
            $y = 2;
            SELECT $x as Name FROM Input;
        )");

        auto expectedIssues = TString(R"(generated.sql:3:13: Warning: Symbol $y is not used, code: 4527
<main>: Warning: Type annotation, code: 1030
    generated.sql:4:13: Warning: At function: PersistableRepr
        generated.sql:4:13: Warning: Persistable required. Atom, key, world, datasink, datasource, callable, resource, stream and lambda are not persistable, code: 1104
)");

        auto program = factory->MakePushStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        UNIT_ASSERT_VALUES_EQUAL(expectedIssues, program->GetIssues().ToString());
    }

    Y_UNIT_TEST(TestSqlWrongTableName) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            SELECT * FROM WrongTable;
        )");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }(), TCompileError, "Failed to optimize");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }(), TCompileError, "Failed to optimize");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePushStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }(), TCompileError, "Failed to optimize");
    }

    Y_UNIT_TEST(TestAllocateLargeStringOnEvaluate) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            $data = Length(EvaluateExpr("long string" || " very loooong string"));
            SELECT $data as Name FROM Input;
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePushStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());
    }

    Y_UNIT_TEST(TestInvalidSql) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            Just some invalid SQL;
        )");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }(), TCompileError, "failed to parse SQL");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }(), TCompileError, "failed to parse SQL");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePushStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }(), TCompileError, "failed to parse SQL");
    }

    Y_UNIT_TEST(TestUseProcess) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            $processor = ($row) -> ($row);

            PROCESS Input using $processor(TableRow());
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePushStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());
    }

    Y_UNIT_TEST(TestUseCodegen) {
        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            $processor = ($row) -> {
                $lambda = EvaluateCode(LambdaCode(($row) -> ($row)));
                return $lambda($row);
            };

            PROCESS Input using $processor(TableRow());
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
        }());
    }
}
