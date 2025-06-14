#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/io_specs/protobuf/spec.h>
#include <yql/essentials/public/purecalc/ut/protos/test_structs.pb.h>
#include <yql/essentials/public/purecalc/ut/empty_stream.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TestEval) {
    Y_UNIT_TEST(TestEvalExpr) {
        using namespace NYql::NPureCalc;

        auto options = TProgramFactoryOptions();
        auto factory = MakeProgramFactory(options);

        auto program = factory->MakePullListProgram(
            TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
            TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
            "SELECT Unwrap(cast(EvaluateExpr('foo' || 'bar') as Utf8)) AS X",
            ETranslationMode::SQL
        );

        auto stream = program->Apply(EmptyStream<NPureCalcProto::TStringMessage*>());

        NPureCalcProto::TStringMessage* message;

        UNIT_ASSERT(message = stream->Fetch());
        UNIT_ASSERT_EQUAL(message->GetX(), "foobar");
        UNIT_ASSERT(!stream->Fetch());
    }

    Y_UNIT_TEST(TestSelfType) {
        using namespace NYql::NPureCalc;

        auto options = TProgramFactoryOptions();
        auto factory = MakeProgramFactory(options);

        try {
            auto program = factory->MakePullListProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
                "$input = PROCESS Input;select unwrap(cast(FormatType(EvaluateType(TypeHandle(TypeOf($input)))) AS Utf8)) AS X",
                ETranslationMode::SQL
            );

            auto stream = program->Apply(EmptyStream<NPureCalcProto::TStringMessage*>());

            NPureCalcProto::TStringMessage* message;

            UNIT_ASSERT(message = stream->Fetch());
            UNIT_ASSERT_VALUES_EQUAL(message->GetX(), "List<Struct<'X':Utf8>>");
            UNIT_ASSERT(!stream->Fetch());
        } catch (const TCompileError& e) {
            UNIT_FAIL(e.GetIssues());
        }
    }

    Y_UNIT_TEST(CantUseSelfInsideEvaluation) {
        using namespace NYql::NPureCalc;

        auto options = TProgramFactoryOptions();
        auto factory = MakeProgramFactory(options);

        try {
            auto program = factory->MakePullListProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
                "$x = SELECT count(*) FROM Input;select unwrap(cast(EvaluateExpr($x) AS Utf8)) AS X",
                ETranslationMode::SQL
            );

            program->Apply(EmptyStream<NPureCalcProto::TStringMessage*>());
            UNIT_FAIL("Exception is expected");
        } catch (const TCompileError& e) {
            UNIT_ASSERT_C(TString(e.GetIssues()).Contains("Inputs aren't available during evaluation"), e.GetIssues());
        }
    }
}
