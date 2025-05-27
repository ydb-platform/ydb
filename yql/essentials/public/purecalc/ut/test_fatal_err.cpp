#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/io_specs/protobuf/spec.h>
#include <yql/essentials/public/purecalc/ut/protos/test_structs.pb.h>
#include <yql/essentials/public/purecalc/ut/empty_stream.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TestFatalError) {
    Y_UNIT_TEST(TestFailType) {
        using namespace NYql::NPureCalc;

        auto options = TProgramFactoryOptions();
        auto factory = MakeProgramFactory(options);

        try {
            factory->MakePullListProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
                "pragma warning(\"disable\",\"4510\");select unwrap(cast(Yql::FailMe(AsAtom('type')) as Utf8)) as X;",
                ETranslationMode::SQL
            );
            UNIT_FAIL("Exception is expected");
        } catch (const TCompileError& e) {
            UNIT_ASSERT_C(e.GetIssues().Contains("abnormal"), e.GetIssues());
        }
    }
}
