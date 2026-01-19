#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/io_specs/protobuf/spec.h>
#include <yql/essentials/public/purecalc/ut/protos/test_structs.pb.h>
#include <yql/essentials/public/purecalc/ut/empty_stream.h>
#include <yql/essentials/public/purecalc/helpers/stream/stream_from_vector.h>

#include <library/cpp/testing/unittest/registar.h>

#include "fake_spec.h"

Y_UNIT_TEST_SUITE(TestLinear) {
Y_UNIT_TEST(DynamicPullListUsedTwice) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.SetLanguageVersion(NYql::GetMaxReleasedLangVersion());
    auto factory = MakeProgramFactory(options);

    auto sql = TString(R"(
            $d = ToDynamicLinear(ToMutDict({"foo"},1));
            $t = Opaque(($d, $d));
            SELECT If(DictHasItems(FromMutDict(FromDynamicLinear($t.0))) AND
                      DictHasItems(FromMutDict(FromDynamicLinear($t.1))), "HAS"u, "NOT"u) AS X;
        )");

    try {
        auto program = factory->MakePullListProgram(TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                                                    TProtobufOutputSpec<NPureCalcProto::TStringMessage>(), sql, ETranslationMode::SQL);
        try {
            auto stream = program->Apply(EmptyStream<NPureCalcProto::TStringMessage*>());
        } catch (const yexception& e) {
            UNIT_ASSERT_C(TStringBuf(e.what()).Contains("The linear value has already been used"), e.what());
        }
    } catch (const TCompileError& e) {
        UNIT_FAIL(e.GetIssues());
    }
}

Y_UNIT_TEST(DynamicPullListNotUsed) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.SetLanguageVersion(NYql::GetMaxReleasedLangVersion());
    auto factory = MakeProgramFactory(options);

    auto sql = TString(R"(
            $d1 = ToMutDict({1},1);
            $d2 = ToMutDict({2},2);
            SELECT If(DictHasItems(FromMutDict(FromDynamicLinear(Unwrap(Opaque([
                ToDynamicLinear($d1),
                ToDynamicLinear($d2)])[0])))), "HAS"u, "NOT"u) AS X;
        )");

    try {
        auto program = factory->MakePullListProgram(TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                                                    TProtobufOutputSpec<NPureCalcProto::TStringMessage>(), sql, ETranslationMode::SQL);
        auto stream = program->Apply(EmptyStream<NPureCalcProto::TStringMessage*>());
        try {
            stream->Fetch();
            stream->Fetch();
            UNIT_FAIL("Expected exception");
        } catch (const yexception& e) {
            UNIT_ASSERT_C(TStringBuf(e.what()).Contains("Linear value is not consumed"), e.what());
        }
    } catch (const TCompileError& e) {
        UNIT_FAIL(e.GetIssues());
    }
}

Y_UNIT_TEST(DynamicPullStreamUsedTwice) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.SetLanguageVersion(NYql::GetMaxReleasedLangVersion());
    auto factory = MakeProgramFactory(options);

    auto sql = TString(R"(
            $d = ToDynamicLinear(ToMutDict({"foo"},1));
            $t = Opaque(($d, $d));
            SELECT If(DictHasItems(FromMutDict(FromDynamicLinear($t.0))) AND
                      DictHasItems(FromMutDict(FromDynamicLinear($t.1))), "HAS"u, "NOT"u) AS X
            FROM Input
        )");

    try {
        auto program = factory->MakePullStreamProgram(TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                                                      TProtobufOutputSpec<NPureCalcProto::TStringMessage>(), sql, ETranslationMode::SQL);
        try {
            NPureCalcProto::TStringMessage msg;
            msg.SetX("foo");
            auto stream = program->Apply(StreamFromVector<NPureCalcProto::TStringMessage>({msg}));
            stream->Fetch();
            UNIT_FAIL("Expected exception");
        } catch (const yexception& e) {
            UNIT_ASSERT_C(TStringBuf(e.what()).Contains("The linear value has already been used"), e.what());
        }
    } catch (const TCompileError& e) {
        UNIT_FAIL(e.GetIssues());
    }
}

Y_UNIT_TEST(DynamicPullStreamNotUsed) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.SetLanguageVersion(NYql::GetMaxReleasedLangVersion());
    auto factory = MakeProgramFactory(options);

    auto sql = TString(R"(
            $d1 = ToMutDict({1},1);
            $d2 = ToMutDict({2},2);
            SELECT If(DictHasItems(FromMutDict(FromDynamicLinear(Unwrap(Opaque([
                ToDynamicLinear($d1),
                ToDynamicLinear($d2)])[0])))), "HAS"u, "NOT"u) AS X
            FROM Input;
        )");

    try {
        auto program = factory->MakePullStreamProgram(TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                                                      TProtobufOutputSpec<NPureCalcProto::TStringMessage>(), sql, ETranslationMode::SQL);
        try {
            NPureCalcProto::TStringMessage msg;
            msg.SetX("foo");
            auto stream = program->Apply(StreamFromVector<NPureCalcProto::TStringMessage>({msg}));
            stream->Fetch();
            stream->Fetch();
            UNIT_FAIL("Expected exception");
        } catch (const yexception& e) {
            UNIT_ASSERT_C(TStringBuf(e.what()).Contains("Linear value is not consumed"), e.what());
        }
    } catch (const TCompileError& e) {
        UNIT_FAIL(e.GetIssues());
    }
}

template <typename T>
class TEmptyConsumer: public NYql::NPureCalc::IConsumer<T> {
public:
    void OnObject(T) override {
    }

    void OnFinish() override {
    }
};

Y_UNIT_TEST(DynamicPushStreamUsedTwice) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.SetLanguageVersion(NYql::GetMaxReleasedLangVersion());
    auto factory = MakeProgramFactory(options);

    auto sql = TString(R"(
            $d = ToDynamicLinear(ToMutDict({"foo"},1));
            $t = Opaque(($d, $d));
            SELECT If(DictHasItems(FromMutDict(FromDynamicLinear($t.0))) AND
                      DictHasItems(FromMutDict(FromDynamicLinear($t.1))), "HAS"u, "NOT"u) AS X
            FROM Input
        )");

    try {
        auto program = factory->MakePushStreamProgram(TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                                                      TProtobufOutputSpec<NPureCalcProto::TStringMessage>(), sql, ETranslationMode::SQL);
        try {
            NPureCalcProto::TStringMessage msg;
            msg.SetX("foo");
            auto consumer = program->Apply(MakeHolder<TEmptyConsumer<NPureCalcProto::TStringMessage*>>());
            consumer->OnObject(&msg);
            UNIT_FAIL("Expected exception");
        } catch (const yexception& e) {
            UNIT_ASSERT_C(TStringBuf(e.what()).Contains("The linear value has already been used"), e.what());
        }
    } catch (const TCompileError& e) {
        UNIT_FAIL(e.GetIssues());
    }
}

Y_UNIT_TEST(DynamicPushStreamNotUsed) {
    using namespace NYql::NPureCalc;

    auto options = TProgramFactoryOptions();
    options.SetLanguageVersion(NYql::GetMaxReleasedLangVersion());
    auto factory = MakeProgramFactory(options);

    auto sql = TString(R"(
            $d1 = ToMutDict({1},1);
            $d2 = ToMutDict({2},2);
            SELECT If(DictHasItems(FromMutDict(FromDynamicLinear(Unwrap(Opaque([
                ToDynamicLinear($d1),
                ToDynamicLinear($d2)])[0])))), "HAS"u, "NOT"u) AS X
            FROM Input;
        )");

    try {
        auto program = factory->MakePushStreamProgram(TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                                                      TProtobufOutputSpec<NPureCalcProto::TStringMessage>(), sql, ETranslationMode::SQL);
        try {
            NPureCalcProto::TStringMessage msg;
            msg.SetX("foo");
            auto consumer = program->Apply(MakeHolder<TEmptyConsumer<NPureCalcProto::TStringMessage*>>());
            consumer->OnObject(&msg);
            consumer->OnFinish();
            UNIT_FAIL("Expected exception");
        } catch (const yexception& e) {
            UNIT_ASSERT_C(TStringBuf(e.what()).Contains("Linear value is not consumed"), e.what());
        }
    } catch (const TCompileError& e) {
        UNIT_FAIL(e.GetIssues());
    }
}

} // Y_UNIT_TEST_SUITE(TestLinear)
