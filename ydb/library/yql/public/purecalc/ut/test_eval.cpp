#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/protobuf/spec.h>
#include <ydb/library/yql/public/purecalc/ut/protos/test_structs.pb.h>
#include <ydb/library/yql/public/purecalc/ut/empty_stream.h>

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
}
