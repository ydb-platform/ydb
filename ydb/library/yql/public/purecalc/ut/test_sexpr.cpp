#include <ydb/library/yql/public/purecalc/purecalc.h>

#include "fake_spec.h"

#include <ydb/library/yql/public/purecalc/ut/protos/test_structs.pb.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TestSExpr) {
    Y_UNIT_TEST(TestSExprCompile) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        auto expr = TString(R"(
            (
                (return (Self '0))
            )
        )");

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullStreamProgram(FakeIS(), FakeOS(), expr, ETranslationMode::SExpr);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), expr, ETranslationMode::SExpr);
        }());

        UNIT_ASSERT_NO_EXCEPTION([&](){
            factory->MakePushStreamProgram(FakeIS(), FakeOS(), expr, ETranslationMode::SExpr);
        }());
    }

    Y_UNIT_TEST(TestInvalidSExpr) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        auto sql = TString(R"(
            Some totally invalid SExpr
        )");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SExpr);
        }(), TCompileError, "failed to parse s-expression");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullListProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SExpr);
        }(), TCompileError, "failed to parse s-expression");

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePushStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SExpr);
        }(), TCompileError, "failed to parse s-expression");
    }
}
