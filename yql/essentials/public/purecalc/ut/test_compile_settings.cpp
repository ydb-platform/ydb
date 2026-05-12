#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/io_specs/protobuf/spec.h>
#include <yql/essentials/public/purecalc/ut/protos/test_structs.pb.h>
#include <yql/essentials/public/purecalc/ut/empty_stream.h>

#include <library/cpp/testing/unittest/registar.h>

#include "fake_spec.h"

Y_UNIT_TEST_SUITE(TestCompileSettings) {

TString MakeBigSql() {
    TStringBuilder b;
    b << "SELECT * FROM Input WHERE ListLength([";
    for (ui32 i = 0; i < 1000; ++i) {
        b << i << ",";
    }

    b << "]) > 0";
    return b;
}

Y_UNIT_TEST(NodesAllocationLimit) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.InternalSettings.NodesAllocationLimit = 10;
    auto factory = MakeProgramFactory(options);

    try {
        factory->MakePullStreamProgram(FakeIS(), FakeOS(), MakeBigSql(), ETranslationMode::SQL);
        UNIT_FAIL("Exception is expected");
    } catch (const TCompileError& e) {
        UNIT_ASSERT_C(e.GetIssues().Contains("Too many allocated nodes"), e.GetIssues());
    }
}

Y_UNIT_TEST(StringsAllocationLimit) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.InternalSettings.StringsAllocationLimit = 10;
    auto factory = MakeProgramFactory(options);

    try {
        factory->MakePullStreamProgram(FakeIS(), FakeOS(), MakeBigSql(), ETranslationMode::SQL);
        UNIT_FAIL("Exception is expected");
    } catch (const TCompileError& e) {
        UNIT_ASSERT_C(e.GetIssues().Contains("Too large string pool"), e.GetIssues());
    }
}

Y_UNIT_TEST(RepeatTransformLimit) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.InternalSettings.RepeatTransformLimit = 10;
    auto factory = MakeProgramFactory(options);

    try {
        factory->MakePullStreamProgram(FakeIS(), FakeOS(), MakeBigSql(), ETranslationMode::SQL);
        UNIT_FAIL("Exception is expected");
    } catch (const TCompileError& e) {
        UNIT_ASSERT_C(e.GetIssues().Contains("SyncTransform takes too much iterations"), e.GetIssues());
    }
}

} // Y_UNIT_TEST_SUITE(TestCompileSettings)
