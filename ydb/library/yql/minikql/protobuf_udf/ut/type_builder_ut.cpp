#include "type_builder.h"

//#include <alice/wonderlogs/protos/wonderlogs.pb.h>
#include <ydb/library/yql/minikql/protobuf_udf/ut/protobuf_ut.pb.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>

#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/public/udf/udf_types.h>
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
        , FunctionTypeInfoBuilder(Env, TypeInfoHelper, "", nullptr, {})
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

} // unnamed

Y_UNIT_TEST_SUITE(TProtobufTypeBuilderTests) {

    template <typename TProto, EEnumFormat EnumFormat = EEnumFormat::Number, ERecursionTraits Recursion = ERecursionTraits::Fail>
    void CheckYtSchemaCompatibility () {
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
                << "\nYtSchemaType: " << NYT::NodeToYsonString(ytSchemaType, ::NYson::EYsonFormat::Pretty)
        );
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

/*
    Y_UNIT_TEST(YtMode_Wonderlog) {
        CheckYtSchemaCompatibility<NAlice::NWonderlogs::TWonderlog, EEnumFormat::Name, ERecursionTraits::Bytes>();
    }
*/
};
