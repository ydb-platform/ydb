#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/io_specs/protobuf/spec.h>
#include <yql/essentials/public/purecalc/ut/protos/test_structs.pb.h>
#include <yql/essentials/public/purecalc/ut/empty_stream.h>

#include <library/cpp/testing/unittest/registar.h>

#include "fake_spec.h"

Y_UNIT_TEST_SUITE(TestLangVer) {
    Y_UNIT_TEST(TooHighLangVer) {
        using namespace NYql::NPureCalc;

        auto options = TProgramFactoryOptions();
        options.SetLanguageVersion(NYql::GetMaxLangVersion());
        auto factory = MakeProgramFactory(options);

        try {
            auto sql = TString(R"(
                SELECT * FROM Input;
            )");

            factory->MakePullStreamProgram(FakeIS(), FakeOS(), sql, ETranslationMode::SQL);
            UNIT_FAIL("Exception is expected");
        } catch (const TCompileError& e) {
            UNIT_ASSERT_C(e.GetIssues().Contains("version"), e.GetIssues());
        }
    }
}
