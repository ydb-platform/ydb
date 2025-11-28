#include "type_builder.h"

#include <yql/essentials/minikql/protobuf_udf/ut/protobuf_ut.pb.h>
#include <yt/yql/providers/yt/lib/schema/schema.h>
#include <yt/yql/providers/yt/common/yql_names.h>

#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yt/cpp/mapreduce/interface/format.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <algorithm>

using namespace NYql;
using namespace NKikimr::NMiniKQL;

namespace {

struct TSetup {
    TSetup()
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , FunctionRegistry(CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr()))
        , TypeInfoHelper(new TTypeInfoHelper())
        , FunctionTypeInfoBuilder(UnknownLangVersion, Env, TypeInfoHelper, "", nullptr, {})
        , PgmBuilder(Env, *FunctionRegistry)
    {
    }

    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    IFunctionRegistry::TPtr FunctionRegistry;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    TFunctionTypeInfoBuilder FunctionTypeInfoBuilder;
    TProgramBuilder PgmBuilder;
};

} // namespace

Y_UNIT_TEST_SUITE(TProtobufTypeBuilderTests) {

template <typename TProto, EEnumFormat EnumFormat = EEnumFormat::Number, ERecursionTraits Recursion = ERecursionTraits::Fail>
void CheckYtSchemaCompatibility() {
    TSetup setup;

    NUdf::TProtoInfo info;
    info.YtMode = true;
    info.EnumFormat = EnumFormat;
    info.Recursion = Recursion;

    NUdf::ProtoTypeBuild<TProto>(setup.FunctionTypeInfoBuilder, &info);
    auto protoType = NCommon::TypeToYsonNode(static_cast<const NKikimr::NMiniKQL::TStructType*>(info.StructType));

    auto ytSchema = NYT::CreateTableSchema(*TProto::GetDescriptor(), true);
    auto ytSchemaType = YTSchemaToRowSpec(ytSchema.ToNode())[RowSpecAttrType];
    // Yt schema has struct fields in protobuf-order. Make them in name ascending order
    ytSchemaType = NCommon::TypeToYsonNode(NCommon::ParseTypeFromYson(ytSchemaType, setup.PgmBuilder, Cerr));

    UNIT_ASSERT_EQUAL_C(
        NYT::NodeToYsonString(protoType, ::NYson::EYsonFormat::Text),
        NYT::NodeToYsonString(ytSchemaType, ::NYson::EYsonFormat::Text),
        "\nUdfType: " << NYT::NodeToYsonString(protoType, ::NYson::EYsonFormat::Pretty)
                      << "\nYtSchemaType: " << NYT::NodeToYsonString(ytSchemaType, ::NYson::EYsonFormat::Pretty));
}

Y_UNIT_TEST(YtMode_Map) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TWithMap>();
}

Y_UNIT_TEST(YtMode_OneOf) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TWithOneof>();
}

Y_UNIT_TEST(YtMode_OptionalList) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TOptionalList>();
}

Y_UNIT_TEST(YtMode_NoOptionInheritance) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TNoOptionInheritance>();
}

Y_UNIT_TEST(YtMode_RowMixedSerializationOptions_ColumnNames) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TRowMixedSerializationOptions_ColumnNames>();
}

Y_UNIT_TEST(YtMode_RowSerializedRepeatedFields) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TRowSerializedRepeatedFields>();
}

Y_UNIT_TEST(YtMode_RowMixedSerializationOptions) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TRowMixedSerializationOptions>();
}

Y_UNIT_TEST(YtMode_RowMessageSerializationOption) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TRowMessageSerializationOption>();
}

Y_UNIT_TEST(YtMode_RowFieldSerializationOption) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TRowFieldSerializationOption>();
}

Y_UNIT_TEST(YtMode_WithTypeOptions) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TWithTypeOptions>();
}

Y_UNIT_TEST(YtMode_Aggregated) {
    CheckYtSchemaCompatibility<NYql::NProtoTest::TAggregated>();
}

Y_UNIT_TEST(FieldTypeParsing) {
    // Test SeparateOptional.
    UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(NYql::NProtoTest::TIntegral::descriptor()->FindFieldByName("DoubleField"), /*ytMode=*/true), NUdf::EFieldContext::SeparateOptional);

    // Test SeparateRequired.
    UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(NYql::NProtoTest::TWithTypeOptions::descriptor()->FindFieldByName("RequiredAnyField"), /*ytMode=*/true), NUdf::EFieldContext::SeparateRequired);

    // Test SeparateRepeated.
    UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(NYql::NProtoTest::TRepeated::descriptor()->FindFieldByName("Int32Field"), /*ytMode=*/true), NUdf::EFieldContext::SeparateRepeated);

    // Test MapKey and MapValue.
    auto mapDescriptor = NYql::NProtoTest::TWithMap::descriptor()->FindFieldByName("MapDefault")->message_type();
    UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(mapDescriptor->map_key(), /*ytMode=*/true), NUdf::EFieldContext::MapKey);
    UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(mapDescriptor->map_value(), /*ytMode=*/true), NUdf::EFieldContext::MapValue);

    // Test InsideOneofVariant fields in TDefaultSeparateFields.
    auto defaultSeparateFields = NYql::NProtoTest::TWithOneof::TDefaultSeparateFields::descriptor();
    auto oneof2 = defaultSeparateFields->FindOneofByName("Oneof2");
    UNIT_ASSERT(oneof2);

    // Test all fields in the Oneof2 oneof.
    for (int i = 0; i < oneof2->field_count(); ++i) {
        auto field = oneof2->field(i);
        UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(field, /*ytMode=*/true), NUdf::EFieldContext::InsideOneofYtVariant);
        UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(field, /*ytMode=*/false), NUdf::EFieldContext::InsideOneofProtobufSeparateFields);
    }

    // Test InsideOneofYtSeparateFields fields in TNoDefault.
    auto noDefault = NYql::NProtoTest::TWithOneof::TNoDefault::descriptor();
    auto oneof1 = noDefault->FindOneofByName("Oneof1");
    UNIT_ASSERT(oneof1);

    // Test all fields in the Oneof1 oneof.
    for (int i = 0; i < oneof1->field_count(); ++i) {
        auto field = oneof1->field(i);
        UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(field, /*ytMode=*/true), NUdf::EFieldContext::InsideOneofYtSeparateFields);
        UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(field, /*ytMode=*/false), NUdf::EFieldContext::InsideOneofProtobufSeparateFields);
    }

    // Test InsideOneofYtSeparateFields fields in TSerializationProtobuf.
    auto serializationProtobuf = NYql::NProtoTest::TWithOneof::TSerializationProtobuf::descriptor();
    auto oneof = serializationProtobuf->FindOneofByName("Oneof");
    UNIT_ASSERT(oneof);

    // Test all fields in the Oneof oneof.
    for (int i = 0; i < oneof->field_count(); ++i) {
        auto field = oneof->field(i);
        UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(field, /*ytMode=*/true), NUdf::EFieldContext::InsideOneofYtSeparateFields);
        UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(field, /*ytMode=*/false), NUdf::EFieldContext::InsideOneofProtobufSeparateFields);
    }

    // Test TopLevelOneof in TWithOneof.
    auto withOneof = NYql::NProtoTest::TWithOneof::descriptor();
    auto topLevelOneof = withOneof->FindOneofByName("TopLevelOneof");
    UNIT_ASSERT(topLevelOneof);

    // Test the field in TopLevelOneof.
    auto memberField = topLevelOneof->field(0);
    UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(memberField, /*ytMode=*/true), NUdf::EFieldContext::InsideOneofYtVariant);
    UNIT_ASSERT_EQUAL(NUdf::GetFieldContext(memberField, /*ytMode=*/false), NUdf::EFieldContext::InsideOneofProtobufSeparateFields);
}
}; // Y_UNIT_TEST_SUITE(TProtobufTypeBuilderTests)
