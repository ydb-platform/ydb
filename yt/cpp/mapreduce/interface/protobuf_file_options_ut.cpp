#include "errors.h"
#include "format.h"
#include "common_ut.h"

#include <yt/cpp/mapreduce/interface/protobuf_file_options_ut.pb.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(ProtobufFileOptions)
{
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
        const auto schema = CreateTableSchema<NTestingFileOptions::TRowFieldSerializationOption>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(GetUrlRowType(false))));
    }

    Y_UNIT_TEST(TRowMixedSerializationOptions)
    {
        const auto schema = CreateTableSchema<NTestingFileOptions::TRowMixedSerializationOptions>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(GetUrlRowType(false))));
    }

    Y_UNIT_TEST(FieldSortOrder)
    {
        const auto schema = CreateTableSchema<NTestingFileOptions::TFieldSortOrder>();

        auto asInProtoFile = NTi::Optional(NTi::Struct({
            {"x", NTi::Optional(NTi::Int64())},
            {"y", NTi::Optional(NTi::String())},
            {"z", NTi::Optional(NTi::Bool())},
        }));
        auto byFieldNumber = NTi::Optional(NTi::Struct({
            {"z", NTi::Optional(NTi::Bool())},
            {"x", NTi::Optional(NTi::Int64())},
            {"y", NTi::Optional(NTi::String())},
        }));

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("EmbeddedDefault").Type(asInProtoFile))
            .AddColumn(TColumnSchema().Name("EmbeddedAsInProtoFile").Type(asInProtoFile))
            .AddColumn(TColumnSchema().Name("EmbeddedByFieldNumber").Type(byFieldNumber)));
    }

    Y_UNIT_TEST(Map)
    {
        const auto schema = CreateTableSchema<NTestingFileOptions::TWithMap>();

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
                .Type(createKeyValueStruct(NTi::Int64(), embedded)))
            .AddColumn(TColumnSchema()
                .Name("MapDict")
                .Type(NTi::Dict(NTi::Int64(), embedded))));
    }

    Y_UNIT_TEST(Oneof)
    {
        const auto schema = CreateTableSchema<NTestingFileOptions::TWithOneof>();

        auto embedded = NTi::Struct({
            {"x", NTi::Optional(NTi::Int64())},
            {"y", NTi::Optional(NTi::String())},
        });

        auto defaultVariantType = NTi::Optional(NTi::Struct({
            {"field", NTi::Optional(NTi::String())},
            {"Oneof2", NTi::Optional(NTi::Variant(NTi::Struct({
                {"y2", NTi::String()},
                {"z2", embedded},
                {"x2", NTi::Int64()},
            })))},
            {"x1", NTi::Optional(NTi::Int64())},
            {"y1", NTi::Optional(NTi::String())},
            {"z1", NTi::Optional(embedded)},
        }));

        auto noDefaultType = NTi::Optional(NTi::Struct({
            {"field", NTi::Optional(NTi::String())},
            {"y2", NTi::Optional(NTi::String())},
            {"z2", NTi::Optional(embedded)},
            {"x2", NTi::Optional(NTi::Int64())},
            {"x1", NTi::Optional(NTi::Int64())},
            {"y1", NTi::Optional(NTi::String())},
            {"z1", NTi::Optional(embedded)},
        }));

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("DefaultVariant")
                .Type(defaultVariantType)
            )
            .AddColumn(TColumnSchema()
                .Name("NoDefault")
                .Type(noDefaultType)
            )
            .AddColumn(TColumnSchema()
                .Name("SerializationProtobuf")
                .Type(NTi::Optional(NTi::Struct({
                    {"x1", NTi::Optional(NTi::Int64())},
                    {"y1", NTi::Optional(NTi::String())},
                    {"z1", NTi::Optional(NTi::String())},
                })))
            )
            .AddColumn(TColumnSchema()
                .Name("MemberOfTopLevelOneof")
                .Type(NTi::Optional(NTi::Int64()))
            )
        );
    }
}

static TNode GetColumns(const TFormat& format, int tableIndex = 0)
{
    return format.Config.GetAttributes()["tables"][tableIndex]["columns"];
}

Y_UNIT_TEST_SUITE(ProtobufFormatFileOptions)
{
    Y_UNIT_TEST(TRowFieldSerializationOption)
    {
        const auto format = TFormat::Protobuf<NTestingFileOptions::TRowFieldSerializationOption>();
        auto columns = GetColumns(format);

        UNIT_ASSERT_VALUES_EQUAL(columns[0]["name"], "UrlRow_1");
        UNIT_ASSERT_VALUES_EQUAL(columns[0]["proto_type"], "message");
        UNIT_ASSERT_VALUES_EQUAL(columns[0]["field_number"], 1);

        UNIT_ASSERT_VALUES_EQUAL(columns[1]["name"], "UrlRow_2");
        UNIT_ASSERT_VALUES_EQUAL(columns[1]["proto_type"], "structured_message");
        UNIT_ASSERT_VALUES_EQUAL(columns[1]["field_number"], 2);
        const auto& fields = columns[1]["fields"];
        UNIT_ASSERT_VALUES_EQUAL(fields[0]["name"], "Host");
        UNIT_ASSERT_VALUES_EQUAL(fields[0]["proto_type"], "string");
        UNIT_ASSERT_VALUES_EQUAL(fields[0]["field_number"], 1);

        UNIT_ASSERT_VALUES_EQUAL(fields[1]["name"], "Path");
        UNIT_ASSERT_VALUES_EQUAL(fields[1]["proto_type"], "string");
        UNIT_ASSERT_VALUES_EQUAL(fields[1]["field_number"], 2);

        UNIT_ASSERT_VALUES_EQUAL(fields[2]["name"], "HttpCode");
        UNIT_ASSERT_VALUES_EQUAL(fields[2]["proto_type"], "sint32");
        UNIT_ASSERT_VALUES_EQUAL(fields[2]["field_number"], 3);
    }

    Y_UNIT_TEST(Map)
    {
        const auto format = TFormat::Protobuf<NTestingFileOptions::TWithMap>();
        auto columns = GetColumns(format);

        UNIT_ASSERT_VALUES_EQUAL(columns.Size(), 2);
        {
            const auto& column = columns[0];
            UNIT_ASSERT_VALUES_EQUAL(column["name"], "MapDefault");
            UNIT_ASSERT_VALUES_EQUAL(column["proto_type"], "structured_message");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"].Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][0]["proto_type"], "int64");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][1]["proto_type"], "structured_message");
        }
        {
            const auto& column = columns[1];
            UNIT_ASSERT_VALUES_EQUAL(column["name"], "MapDict");
            UNIT_ASSERT_VALUES_EQUAL(column["proto_type"], "structured_message");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"].Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][0]["proto_type"], "int64");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][1]["proto_type"], "structured_message");
        }
    }

    Y_UNIT_TEST(Oneof)
    {
        const auto format = TFormat::Protobuf<NTestingFileOptions::TWithOneof>();
        auto columns = GetColumns(format);

        UNIT_ASSERT_VALUES_EQUAL(columns.Size(), 4);

        {
            const auto& column = columns[0];
            UNIT_ASSERT_VALUES_EQUAL(column["name"], "DefaultVariant");
            UNIT_ASSERT_VALUES_EQUAL(column["proto_type"], "structured_message");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"].Size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][0]["name"], "field");

            const auto& oneof2 = column["fields"][1];
            UNIT_ASSERT_VALUES_EQUAL(oneof2["name"], "Oneof2");
            UNIT_ASSERT_VALUES_EQUAL(oneof2["proto_type"], "oneof");
            UNIT_ASSERT_VALUES_EQUAL(oneof2["fields"][0]["name"], "y2");
            UNIT_ASSERT_VALUES_EQUAL(oneof2["fields"][1]["name"], "z2");
            UNIT_ASSERT_VALUES_EQUAL(oneof2["fields"][1]["proto_type"], "structured_message");
            const auto& embeddedFields = oneof2["fields"][1]["fields"];
            UNIT_ASSERT_VALUES_EQUAL(embeddedFields[0]["name"], "x");
            UNIT_ASSERT_VALUES_EQUAL(embeddedFields[1]["name"], "y");

            UNIT_ASSERT_VALUES_EQUAL(oneof2["fields"][2]["name"], "x2");

            UNIT_ASSERT_VALUES_EQUAL(column["fields"][2]["name"], "x1");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][3]["name"], "y1");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][4]["name"], "z1");
        };

        {
            const auto& column = columns[1];
            UNIT_ASSERT_VALUES_EQUAL(column["name"], "NoDefault");
            UNIT_ASSERT_VALUES_EQUAL(column["proto_type"], "structured_message");
            const auto& fields = column["fields"];
            UNIT_ASSERT_VALUES_EQUAL(fields.Size(), 7);

            UNIT_ASSERT_VALUES_EQUAL(fields[0]["name"], "field");

            UNIT_ASSERT_VALUES_EQUAL(fields[1]["name"], "y2");

            UNIT_ASSERT_VALUES_EQUAL(fields[2]["name"], "z2");
            UNIT_ASSERT_VALUES_EQUAL(fields[2]["proto_type"], "structured_message");
            const auto& embeddedFields = fields[2]["fields"];
            UNIT_ASSERT_VALUES_EQUAL(embeddedFields[0]["name"], "x");
            UNIT_ASSERT_VALUES_EQUAL(embeddedFields[1]["name"], "y");

            UNIT_ASSERT_VALUES_EQUAL(fields[3]["name"], "x2");

            UNIT_ASSERT_VALUES_EQUAL(fields[4]["name"], "x1");
            UNIT_ASSERT_VALUES_EQUAL(fields[5]["name"], "y1");
            UNIT_ASSERT_VALUES_EQUAL(fields[6]["name"], "z1");
        };

        {
            const auto& column = columns[2];
            UNIT_ASSERT_VALUES_EQUAL(column["name"], "SerializationProtobuf");
            UNIT_ASSERT_VALUES_EQUAL(column["proto_type"], "structured_message");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"].Size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][0]["name"], "x1");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][1]["name"], "y1");
            UNIT_ASSERT_VALUES_EQUAL(column["fields"][2]["name"], "z1");
        }
        {
            const auto& column = columns[3];
            UNIT_ASSERT_VALUES_EQUAL(column["name"], "MemberOfTopLevelOneof");
            UNIT_ASSERT_VALUES_EQUAL(column["proto_type"], "int64");
        }
    }
}
