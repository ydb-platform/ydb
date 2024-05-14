#include "proto_builder.h"
#include "value_builder.h"

#include <ydb/library/yql/minikql/protobuf_udf/ut/protobuf_ut.pb.h>

#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_buf.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/public/udf/udf_types.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/string/strip.h>
#include <util/memory/tempbuf.h>


using namespace NYql;
using namespace NKikimr::NMiniKQL;

namespace {

struct TSetup {
    TSetup()
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , FunctionRegistry(CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr()))
        , TypeInfoHelper(new TTypeInfoHelper())
        , FunctionTypeInfoBuilder(Env, TypeInfoHelper, "", nullptr, {})
        , PgmBuilder(Env, *FunctionRegistry)
        , MemInfo("Test")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , ValueBuilder(HolderFactory, NUdf::EValidatePolicy::Exception)
    {
    }

    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    IFunctionRegistry::TPtr FunctionRegistry;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    TFunctionTypeInfoBuilder FunctionTypeInfoBuilder;
    TProgramBuilder PgmBuilder;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder ValueBuilder;
};

template <typename TProto>
TString YsonToProtoText(TSetup& setup, NUdf::TProtoInfo& info, TStringBuf yson) {
    TStringStream err;
    auto val = NCommon::ParseYsonValue(
        setup.HolderFactory,
        NYT::NodeToYsonString(NYT::NodeFromYsonString(yson), ::NYson::EYsonFormat::Binary),
        static_cast<NKikimr::NMiniKQL::TStructType*>(info.StructType),
        0,
        &err, true);
    if (!val) {
        throw yexception() << err.Str();
    }
    TProto proto;
    FillProtoFromValue(*val, proto, info);

    TString res;
    NProtoBuf::TextFormat::Printer printer;
    printer.SetSingleLineMode(true);
    printer.SetUseShortRepeatedPrimitives(true);
    if (!printer.PrintToString(proto, &res)) {
        throw yexception() << "Proto printer failed";
    }
    return StripString(res);
}

class TTestWriter : public NCommon::IBlockWriter {
public:
    TTestWriter()
        : Buffer_(1024)
    {
    }

    void SetRecordBoundaryCallback(std::function<void()> /*callback*/) override {
    }

    std::pair<char*, char*> NextEmptyBlock() override {
        auto ptr = Buffer_.Current();
        return {ptr, ptr + Buffer_.Left()};
    }

    void ReturnBlock(size_t avail, std::optional<size_t> /*lastRecordBoundary*/) override {
        Buffer_.Proceed(avail);
    }

    void Finish() override {
    }

    TStringBuf Str() const {
        return {Buffer_.Data(), Buffer_.Filled()};
    }

private:
    TTempBuf Buffer_;
};

template <typename TProto>
TString ProtoTextToYson(TSetup& setup, NUdf::TProtoInfo& info, TStringBuf protoText) {
    TProto proto;
    if (!NProtoBuf::TextFormat::ParseFromString(TString{protoText}, &proto)) {
        throw yexception() << "Failed to parse proto";
    }

    auto value = FillValueFromProto(proto, &setup.ValueBuilder, info);
    TTestWriter out;
    NCommon::TOutputBuf buf(out, nullptr);
    NCommon::WriteYsonValueInTableFormat(buf, static_cast<NKikimr::NMiniKQL::TStructType*>(info.StructType), 0, value, true);
    buf.Finish();

    return NYT::NodeToYsonString(NYT::NodeFromYsonString(out.Str()), ::NYson::EYsonFormat::Text);
}

} // unnamed

Y_UNIT_TEST_SUITE(TProtobufValueBuilderTests) {

    template <bool YTMODE>
    void TestFloatSingle() {
        TSetup setup;
        NUdf::TProtoInfo info;
        info.YtMode = YTMODE;
        NUdf::ProtoTypeBuild<NYql::NProtoTest::TIntegral>(setup.FunctionTypeInfoBuilder, &info);

        UNIT_ASSERT_VALUES_EQUAL(
            YsonToProtoText<NYql::NProtoTest::TIntegral>(setup, info, "{FloatField=0.5}"),
            "FloatField: 0.5"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            YsonToProtoText<NYql::NProtoTest::TIntegral>(setup, info, "{FloatField=0.}"),
            "FloatField: 0"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            YsonToProtoText<NYql::NProtoTest::TIntegral>(setup, info, "{FloatField=-0.33333333}"),
            "FloatField: -0.333333343"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            YsonToProtoText<NYql::NProtoTest::TIntegral>(setup, info, "{FloatField=%-inf}"),
            "FloatField: -inf"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            YsonToProtoText<NYql::NProtoTest::TIntegral>(setup, info, "{FloatField=%nan}"),
            "FloatField: nan"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            ProtoTextToYson<NYql::NProtoTest::TIntegral>(setup, info, "FloatField: 0.5"),
            "[#;#;#;#;#;[0.5];#;#;#;#;#;#;#;#]"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            ProtoTextToYson<NYql::NProtoTest::TIntegral>(setup, info, "FloatField: 0"),
            "[#;#;#;#;#;[0.];#;#;#;#;#;#;#;#]"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            ProtoTextToYson<NYql::NProtoTest::TIntegral>(setup, info, "FloatField: -0.33333333"),
            "[#;#;#;#;#;[-0.3333333433];#;#;#;#;#;#;#;#]"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            ProtoTextToYson<NYql::NProtoTest::TIntegral>(setup, info, "FloatField: -inf"),
            "[#;#;#;#;#;[%-inf];#;#;#;#;#;#;#;#]"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            ProtoTextToYson<NYql::NProtoTest::TIntegral>(setup, info, "FloatField: nan"),
            "[#;#;#;#;#;[%nan];#;#;#;#;#;#;#;#]"
        );
    }

    Y_UNIT_TEST(FloatSingleYtMode) {
        TestFloatSingle<true>();
    }

    Y_UNIT_TEST(FloatSingle) {
        TestFloatSingle<false>();
    }

    template <bool YTMODE>
    void TestFloatRepeated() {
        TSetup setup;
        NUdf::TProtoInfo info;
        info.YtMode = YTMODE;
        info.OptionalLists = true;
        NUdf::ProtoTypeBuild<NYql::NProtoTest::TRepeated>(setup.FunctionTypeInfoBuilder, &info);

        UNIT_ASSERT_VALUES_EQUAL(
            YsonToProtoText<NYql::NProtoTest::TRepeated>(setup, info, "{FloatField=[0.; -0.33333333; 0.5; 1.0; %inf; %nan]}"),
            "FloatField: [0, -0.333333343, 0.5, 1, inf, nan]"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            ProtoTextToYson<NYql::NProtoTest::TRepeated>(setup, info, "FloatField: [0, -0.33333333, 0.5, 1, inf, nan]"),
            "[[[0.;-0.3333333433;0.5;1.;%inf;%nan]];#]"
        );
    }

    Y_UNIT_TEST(FloatRepeatedYtMode) {
        TestFloatRepeated<true>();
    }

    Y_UNIT_TEST(FloatRepeated) {
        TestFloatRepeated<false>();
    }

    template <bool YTMODE>
    void TestFloatDefaults() {
        TSetup setup;
        NUdf::TProtoInfo info;
        info.YtMode = YTMODE;
        info.OptionalLists = true;
        NUdf::ProtoTypeBuild<NYql::NProtoTest::TWithDefaults>(setup.FunctionTypeInfoBuilder, &info);

        UNIT_ASSERT_VALUES_EQUAL(
            ProtoTextToYson<NYql::NProtoTest::TWithDefaults>(setup, info, " "),
            "[[0.];[0.5];[%inf];[%nan]]"
        );
    }

    Y_UNIT_TEST(FloatDefaultsYtMode) {
        TestFloatDefaults<true>();
    }

    Y_UNIT_TEST(FloatDefaults) {
        TestFloatDefaults<false>();
    }
};
