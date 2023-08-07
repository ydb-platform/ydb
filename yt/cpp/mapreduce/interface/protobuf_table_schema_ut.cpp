#include "common.h"
#include "errors.h"
#include "common_ut.h"
#include "util/generic/fwd.h"

#include <yt/cpp/mapreduce/interface/protobuf_table_schema_ut.pb.h>
#include <yt/cpp/mapreduce/interface/proto3_ut.pb.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

using namespace NYT;

bool IsFieldPresent(const TTableSchema& schema, TStringBuf name)
{
    for (const auto& field : schema.Columns()) {
        if (field.Name() == name) {
            return true;
        }
    }
    return false;
}

Y_UNIT_TEST_SUITE(ProtoSchemaTest_Simple)
{
    Y_UNIT_TEST(TIntegral)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TIntegral>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("DoubleField").Type(ToTypeV3(EValueType::VT_DOUBLE, false)))
            .AddColumn(TColumnSchema().Name("FloatField").Type(ToTypeV3(EValueType::VT_DOUBLE, false)))
            .AddColumn(TColumnSchema().Name("Int32Field").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("Int64Field").Type(ToTypeV3(EValueType::VT_INT64, false)))
            .AddColumn(TColumnSchema().Name("Uint32Field").Type(ToTypeV3(EValueType::VT_UINT32, false)))
            .AddColumn(TColumnSchema().Name("Uint64Field").Type(ToTypeV3(EValueType::VT_UINT64, false)))
            .AddColumn(TColumnSchema().Name("Sint32Field").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("Sint64Field").Type(ToTypeV3(EValueType::VT_INT64, false)))
            .AddColumn(TColumnSchema().Name("Fixed32Field").Type(ToTypeV3(EValueType::VT_UINT32, false)))
            .AddColumn(TColumnSchema().Name("Fixed64Field").Type(ToTypeV3(EValueType::VT_UINT64, false)))
            .AddColumn(TColumnSchema().Name("Sfixed32Field").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("Sfixed64Field").Type(ToTypeV3(EValueType::VT_INT64, false)))
            .AddColumn(TColumnSchema().Name("BoolField").Type(ToTypeV3(EValueType::VT_BOOLEAN, false)))
            .AddColumn(TColumnSchema().Name("EnumField").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(TOneOf)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TOneOf>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("DoubleField").Type(ToTypeV3(EValueType::VT_DOUBLE, false)))
            .AddColumn(TColumnSchema().Name("Int32Field").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("BoolField").Type(ToTypeV3(EValueType::VT_BOOLEAN, false))));
    }

    Y_UNIT_TEST(TWithRequired)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TWithRequired>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("RequiredField").Type(ToTypeV3(EValueType::VT_STRING, true)))
            .AddColumn(TColumnSchema().Name("NotRequiredField").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(TAggregated)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TAggregated>();

        UNIT_ASSERT_VALUES_EQUAL(6, schema.Columns().size());
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("StringField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("BytesField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("NestedField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("NestedRepeatedField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("NestedOneOfField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("NestedRecursiveField").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(TAliased)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TAliased>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("key").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("subkey").Type(ToTypeV3(EValueType::VT_DOUBLE, false)))
            .AddColumn(TColumnSchema().Name("Data").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(SortColumns)
    {
        const TSortColumns keys = {"key", "subkey"};

        const auto schema = CreateTableSchema<NUnitTesting::TAliased>(keys);

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("key")
                .Type(ToTypeV3(EValueType::VT_INT32, false))
                .SortOrder(ESortOrder::SO_ASCENDING))
            .AddColumn(TColumnSchema()
                .Name("subkey")
                .Type(ToTypeV3(EValueType::VT_DOUBLE, false))
                .SortOrder(ESortOrder::SO_ASCENDING))
            .AddColumn(TColumnSchema().Name("Data").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(SortColumnsReordered)
    {
        const TSortColumns keys = {"subkey"};

        const auto schema = CreateTableSchema<NUnitTesting::TAliased>(keys);

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("subkey")
                .Type(ToTypeV3(EValueType::VT_DOUBLE, false))
                .SortOrder(ESortOrder::SO_ASCENDING))
            .AddColumn(TColumnSchema().Name("key").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("Data").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(SortColumnsInvalid)
    {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NUnitTesting::TAliased>({"subkey", "subkey"}), yexception);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NUnitTesting::TAliased>({"key", "junk"}), yexception);
    }

    Y_UNIT_TEST(KeepFieldsWithoutExtensionTrue)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TAliased>({}, true);
        UNIT_ASSERT(IsFieldPresent(schema, "key"));
        UNIT_ASSERT(IsFieldPresent(schema, "subkey"));
        UNIT_ASSERT(IsFieldPresent(schema, "Data"));
        UNIT_ASSERT(schema.Strict());
    }

    Y_UNIT_TEST(KeepFieldsWithoutExtensionFalse)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TAliased>({}, false);
        UNIT_ASSERT(IsFieldPresent(schema, "key"));
        UNIT_ASSERT(IsFieldPresent(schema, "subkey"));
        UNIT_ASSERT(!IsFieldPresent(schema, "Data"));
        UNIT_ASSERT(schema.Strict());
    }

    Y_UNIT_TEST(ProtobufTypeOption)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TWithTypeOptions>({});

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .Strict(false)
            .AddColumn(TColumnSchema().Name("ColorIntField").Type(ToTypeV3(EValueType::VT_INT64, false)))
            .AddColumn(TColumnSchema().Name("ColorStringField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("AnyField").Type(ToTypeV3(EValueType::VT_ANY, false)))
            .AddColumn(TColumnSchema().Name("EmbeddedField").Type(
                NTi::Optional(NTi::Struct({
                    {"ColorIntField", ToTypeV3(EValueType::VT_INT64, false)},
                    {"ColorStringField", ToTypeV3(EValueType::VT_STRING, false)},
                    {"AnyField", ToTypeV3(EValueType::VT_ANY, false)}}))))
            .AddColumn(TColumnSchema().Name("RepeatedEnumIntField").Type(NTi::List(NTi::Int64()))));
    }

    Y_UNIT_TEST(ProtobufTypeOption_TypeMismatch)
    {
        UNIT_ASSERT_EXCEPTION(
            CreateTableSchema<NUnitTesting::TWithTypeOptions_TypeMismatch_EnumInt>({}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            CreateTableSchema<NUnitTesting::TWithTypeOptions_TypeMismatch_EnumString>({}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            CreateTableSchema<NUnitTesting::TWithTypeOptions_TypeMismatch_Any>({}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            CreateTableSchema<NUnitTesting::TWithTypeOptions_TypeMismatch_OtherColumns>({}),
            yexception);
    }
}

Y_UNIT_TEST_SUITE(ProtoSchemaTest_Complex)
{
    Y_UNIT_TEST(TRepeated)
    {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NUnitTesting::TRepeated>(), yexception);

        const auto schema = CreateTableSchema<NUnitTesting::TRepeatedYtMode>();
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("Int32Field").Type(NTi::List(ToTypeV3(EValueType::VT_INT32, true)))));
    }

    Y_UNIT_TEST(TRepeatedOptionalList)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TOptionalList>();
        auto type = NTi::Optional(NTi::List(NTi::Int64()));
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("OptionalListInt64").TypeV3(type)));
    }

    NTi::TTypePtr GetUrlRowType(bool required)
    {
        static const NTi::TTypePtr structType = NTi::Struct({
            {"Host", ToTypeV3(EValueType::VT_STRING, false)},
            {"Path", ToTypeV3(EValueType::VT_STRING, false)},
            {"HttpCode", ToTypeV3(EValueType::VT_INT32, false)}});
        return required ? structType : NTi::TTypePtr(NTi::Optional(structType));
    }

    Y_UNIT_TEST(TRowFieldSerializationOption)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TRowFieldSerializationOption>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(GetUrlRowType(false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(TRowMessageSerializationOption)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TRowMessageSerializationOption>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(GetUrlRowType(false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(GetUrlRowType(false))));
    }

    Y_UNIT_TEST(TRowMixedSerializationOptions)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TRowMixedSerializationOptions>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(GetUrlRowType(false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    NTi::TTypePtr GetUrlRowType_ColumnNames(bool required)
    {
        static const NTi::TTypePtr type = NTi::Struct({
            {"Host_ColumnName", ToTypeV3(EValueType::VT_STRING, false)},
            {"Path_KeyColumnName", ToTypeV3(EValueType::VT_STRING, false)},
            {"HttpCode", ToTypeV3(EValueType::VT_INT32, false)},
        });
        return required ? type : NTi::TTypePtr(NTi::Optional(type));
    }

    Y_UNIT_TEST(TRowMixedSerializationOptions_ColumnNames)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TRowMixedSerializationOptions_ColumnNames>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(GetUrlRowType_ColumnNames(false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(NoOptionInheritance)
    {
        auto deepestEmbedded = NTi::Optional(NTi::Struct({{"x", ToTypeV3(EValueType::VT_INT64, false)}}));

        const auto schema = CreateTableSchema<NUnitTesting::TNoOptionInheritance>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("EmbeddedYt_YtOption")
                .Type(NTi::Optional(NTi::Struct({{"embedded", deepestEmbedded}}))))
            .AddColumn(TColumnSchema().Name("EmbeddedYt_ProtobufOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("EmbeddedYt_NoOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema()
                .Name("EmbeddedProtobuf_YtOption")
                .Type(NTi::Optional(NTi::Struct({{"embedded",  ToTypeV3(EValueType::VT_STRING, false)}}))))
            .AddColumn(TColumnSchema().Name("EmbeddedProtobuf_ProtobufOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("EmbeddedProtobuf_NoOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema()
                .Name("Embedded_YtOption")
                .Type(NTi::Optional(NTi::Struct({{"embedded",  ToTypeV3(EValueType::VT_STRING, false)}}))))
            .AddColumn(TColumnSchema().Name("Embedded_ProtobufOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("Embedded_NoOption").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(Cyclic)
    {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NUnitTesting::TCyclic>(), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NUnitTesting::TCyclic::TA>(), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NUnitTesting::TCyclic::TB>(), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NUnitTesting::TCyclic::TC>(), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NUnitTesting::TCyclic::TD>(), TApiUsageError);

        ASSERT_SERIALIZABLES_EQUAL(
            TTableSchema().AddColumn(
                TColumnSchema().Name("d").TypeV3(NTi::Optional(NTi::String()))),
            CreateTableSchema<NUnitTesting::TCyclic::TE>());
    }

    Y_UNIT_TEST(FieldSortOrder)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TFieldSortOrder>();

        auto byFieldNumber = NTi::Optional(NTi::Struct({
            {"z", NTi::Optional(NTi::Bool())},
            {"x", NTi::Optional(NTi::Int64())},
            {"y", NTi::Optional(NTi::String())},
        }));

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("EmbeddedDefault").Type(byFieldNumber))
            .AddColumn(TColumnSchema()
                .Name("EmbeddedAsInProtoFile")
                .Type(NTi::Optional(NTi::Struct({
                    {"x", NTi::Optional(NTi::Int64())},
                    {"y", NTi::Optional(NTi::String())},
                    {"z", NTi::Optional(NTi::Bool())},
                }))))
            .AddColumn(TColumnSchema().Name("EmbeddedByFieldNumber").Type(byFieldNumber)));
    }

    Y_UNIT_TEST(Map)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TWithMap>();

        auto createKeyValueStruct = [] (NTi::TTypePtr key, NTi::TTypePtr value) {
            return NTi::List(NTi::Struct({
                {"key", NTi::Optional(key)},
                {"value", NTi::Optional(value)},
            }));
        };

        auto embedded = NTi::Struct({
            {"x", NTi::Optional(NTi::Int64())},
            {"y", NTi::Optional(NTi::String())},
        });

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("MapDefault")
                .Type(createKeyValueStruct(NTi::Int64(), NTi::String())))
            .AddColumn(TColumnSchema()
                .Name("MapListOfStructsLegacy")
                .Type(createKeyValueStruct(NTi::Int64(), NTi::String())))
            .AddColumn(TColumnSchema()
                .Name("MapListOfStructs")
                .Type(createKeyValueStruct(NTi::Int64(), embedded)))
            .AddColumn(TColumnSchema()
                .Name("MapOptionalDict")
                .Type(NTi::Optional(NTi::Dict(NTi::Int64(), embedded))))
            .AddColumn(TColumnSchema()
                .Name("MapDict")
                .Type(NTi::Dict(NTi::Int64(), embedded))));
    }

    Y_UNIT_TEST(Oneof)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TWithOneof>();

        auto embedded = NTi::Struct({
            {"Oneof", NTi::Optional(NTi::Variant(NTi::Struct({
                {"x", NTi::Int64()},
                {"y", NTi::String()},
            })))},
        });

        auto createType = [&] (TString oneof2Name) {
            return NTi::Optional(NTi::Struct({
                {"field", NTi::Optional(NTi::String())},
                {oneof2Name, NTi::Optional(NTi::Variant(NTi::Struct({
                    {"x2", NTi::Int64()},
                    {"y2", NTi::String()},
                    {"z2", embedded},
                })))},
                {"y1", NTi::Optional(NTi::String())},
                {"z1", NTi::Optional(embedded)},
                {"x1", NTi::Optional(NTi::Int64())},
            }));
        };

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("DefaultSeparateFields")
                .Type(createType("variant_field_name")))
            .AddColumn(TColumnSchema()
                .Name("NoDefault")
                .Type(createType("Oneof2")))
            .AddColumn(TColumnSchema()
                .Name("SerializationProtobuf")
                .Type(NTi::Optional(NTi::Struct({
                    {"y1", NTi::Optional(NTi::String())},
                    {"x1", NTi::Optional(NTi::Int64())},
                    {"z1", NTi::Optional(NTi::String())},
                }))))
            .AddColumn(TColumnSchema()
                .Name("TopLevelOneof")
                .Type(
                    NTi::Optional(
                        NTi::Variant(NTi::Struct({
                            {"MemberOfTopLevelOneof", NTi::Int64()}
                        }))
                    )
                ))
        );
    }

    Y_UNIT_TEST(Embedded)
    {
        const auto schema = CreateTableSchema<NUnitTesting::TEmbeddingMessage>();
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .Strict(false)
            .AddColumn(TColumnSchema().Name("embedded2_num").Type(NTi::Optional(NTi::Uint64())))
            .AddColumn(TColumnSchema().Name("embedded2_struct").Type(NTi::Optional(NTi::Struct({
                {"float1", NTi::Optional(NTi::Double())},
                {"string1", NTi::Optional(NTi::String())},
            }))))
            .AddColumn(TColumnSchema().Name("embedded2_repeated").Type(NTi::List(NTi::String())))
            .AddColumn(TColumnSchema().Name("embedded_num").Type(NTi::Optional(NTi::Uint64())))
            .AddColumn(TColumnSchema().Name("embedded_extra_field").Type(NTi::Optional(NTi::String())))
            .AddColumn(TColumnSchema().Name("variant").Type(NTi::Optional(NTi::Variant(NTi::Struct({
                {"str_variant", NTi::String()},
                {"uint_variant", NTi::Uint64()},
            })))))
            .AddColumn(TColumnSchema().Name("num").Type(NTi::Optional(NTi::Uint64())))
            .AddColumn(TColumnSchema().Name("extra_field").Type(NTi::Optional(NTi::String())))
        );
    }
}

Y_UNIT_TEST_SUITE(ProtoSchemaTest_Proto3)
{
    Y_UNIT_TEST(TWithOptional)
    {
        const auto schema = CreateTableSchema<NTestingProto3::TWithOptional>();
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("x").Type(NTi::Optional(NTi::Int64()))
            )
        );
    }

    Y_UNIT_TEST(TWithOptionalMessage)
    {
        const auto schema = CreateTableSchema<NTestingProto3::TWithOptionalMessage>();
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("x").Type(
                    NTi::Optional(
                        NTi::Struct({{"x", NTi::Optional(NTi::Int64())}})
                    )
                )
            )
        );
    }
}
