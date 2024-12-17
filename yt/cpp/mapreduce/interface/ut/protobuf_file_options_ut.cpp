#include "common_ut.h"

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/format.h>

#include <yt/cpp/mapreduce/interface/ut/protobuf_file_options_ut.pb.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

namespace {

NTi::TTypePtr GetUrlRowType(bool required)
{
    static const NTi::TTypePtr structType = NTi::Struct({
        {"Host", ToTypeV3(EValueType::VT_STRING, false)},
        {"Path", ToTypeV3(EValueType::VT_STRING, false)},
        {"HttpCode", ToTypeV3(EValueType::VT_INT32, false)}});
    return required ? structType : NTi::TTypePtr(NTi::Optional(structType));
}

} // namespace

TEST(TProtobufFileOptionsTest, TRowFieldSerializationOption)
{
    const auto schema = CreateTableSchema<NTestingFileOptions::TRowFieldSerializationOption>();

    ASSERT_SERIALIZABLES_EQ(schema, TTableSchema()
        .AddColumn(TColumnSchema().Name("UrlRow_1").Type(ToTypeV3(EValueType::VT_STRING, false)))
        .AddColumn(TColumnSchema().Name("UrlRow_2").Type(GetUrlRowType(false))));
}

TEST(TProtobufFileOptionsTest, TRowMixedSerializationOptions)
{
    const auto schema = CreateTableSchema<NTestingFileOptions::TRowMixedSerializationOptions>();

    ASSERT_SERIALIZABLES_EQ(schema, TTableSchema()
        .AddColumn(TColumnSchema().Name("UrlRow_1").Type(ToTypeV3(EValueType::VT_STRING, false)))
        .AddColumn(TColumnSchema().Name("UrlRow_2").Type(GetUrlRowType(false))));
}

TEST(TProtobufFileOptionsTest, FieldSortOrder)
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

    ASSERT_SERIALIZABLES_EQ(schema, TTableSchema()
        .AddColumn(TColumnSchema().Name("EmbeddedDefault").Type(asInProtoFile))
        .AddColumn(TColumnSchema().Name("EmbeddedAsInProtoFile").Type(asInProtoFile))
        .AddColumn(TColumnSchema().Name("EmbeddedByFieldNumber").Type(byFieldNumber)));
}

TEST(TProtobufFileOptionsTest, Map)
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

    ASSERT_SERIALIZABLES_EQ(schema, TTableSchema()
        .AddColumn(TColumnSchema()
            .Name("MapDefault")
            .Type(createKeyValueStruct(NTi::Int64(), embedded)))
        .AddColumn(TColumnSchema()
            .Name("MapDict")
            .Type(NTi::Dict(NTi::Int64(), embedded))));
}

TEST(TProtobufFileOptionsTest, Oneof)
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

    ASSERT_SERIALIZABLES_EQ(schema, TTableSchema()
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

static TNode GetColumns(const TFormat& format, int tableIndex = 0)
{
    return format.Config.GetAttributes()["tables"][tableIndex]["columns"];
}

TEST(TProtobufFormatFileOptionsTest, TRowFieldSerializationOption)
{
    const auto format = TFormat::Protobuf<NTestingFileOptions::TRowFieldSerializationOption>();
    auto columns = GetColumns(format);

    EXPECT_EQ(columns[0]["name"], "UrlRow_1");
    EXPECT_EQ(columns[0]["proto_type"], "message");
    EXPECT_EQ(columns[0]["field_number"], 1);

    EXPECT_EQ(columns[1]["name"], "UrlRow_2");
    EXPECT_EQ(columns[1]["proto_type"], "structured_message");
    EXPECT_EQ(columns[1]["field_number"], 2);
    const auto& fields = columns[1]["fields"];
    EXPECT_EQ(fields[0]["name"], "Host");
    EXPECT_EQ(fields[0]["proto_type"], "string");
    EXPECT_EQ(fields[0]["field_number"], 1);

    EXPECT_EQ(fields[1]["name"], "Path");
    EXPECT_EQ(fields[1]["proto_type"], "string");
    EXPECT_EQ(fields[1]["field_number"], 2);

    EXPECT_EQ(fields[2]["name"], "HttpCode");
    EXPECT_EQ(fields[2]["proto_type"], "sint32");
    EXPECT_EQ(fields[2]["field_number"], 3);
}

TEST(TProtobufFormatFileOptionsTest, Map)
{
    const auto format = TFormat::Protobuf<NTestingFileOptions::TWithMap>();
    auto columns = GetColumns(format);

    EXPECT_EQ(columns.Size(), 2u);
    {
        const auto& column = columns[0];
        EXPECT_EQ(column["name"], "MapDefault");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 2u);
        EXPECT_EQ(column["fields"][0]["proto_type"], "int64");
        EXPECT_EQ(column["fields"][1]["proto_type"], "structured_message");
    }
    {
        const auto& column = columns[1];
        EXPECT_EQ(column["name"], "MapDict");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 2u);
        EXPECT_EQ(column["fields"][0]["proto_type"], "int64");
        EXPECT_EQ(column["fields"][1]["proto_type"], "structured_message");
    }
}

TEST(TProtobufFormatFileOptionsTest, Oneof)
{
    const auto format = TFormat::Protobuf<NTestingFileOptions::TWithOneof>();
    auto columns = GetColumns(format);

    EXPECT_EQ(columns.Size(), 4u);

    {
        const auto& column = columns[0];
        EXPECT_EQ(column["name"], "DefaultVariant");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 5u);
        EXPECT_EQ(column["fields"][0]["name"], "field");

        const auto& oneof2 = column["fields"][1];
        EXPECT_EQ(oneof2["name"], "Oneof2");
        EXPECT_EQ(oneof2["proto_type"], "oneof");
        EXPECT_EQ(oneof2["fields"][0]["name"], "y2");
        EXPECT_EQ(oneof2["fields"][1]["name"], "z2");
        EXPECT_EQ(oneof2["fields"][1]["proto_type"], "structured_message");
        const auto& embeddedFields = oneof2["fields"][1]["fields"];
        EXPECT_EQ(embeddedFields[0]["name"], "x");
        EXPECT_EQ(embeddedFields[1]["name"], "y");

        EXPECT_EQ(oneof2["fields"][2]["name"], "x2");

        EXPECT_EQ(column["fields"][2]["name"], "x1");
        EXPECT_EQ(column["fields"][3]["name"], "y1");
        EXPECT_EQ(column["fields"][4]["name"], "z1");
    };

    {
        const auto& column = columns[1];
        EXPECT_EQ(column["name"], "NoDefault");
        EXPECT_EQ(column["proto_type"], "structured_message");
        const auto& fields = column["fields"];
        EXPECT_EQ(fields.Size(), 7u);

        EXPECT_EQ(fields[0]["name"], "field");

        EXPECT_EQ(fields[1]["name"], "y2");

        EXPECT_EQ(fields[2]["name"], "z2");
        EXPECT_EQ(fields[2]["proto_type"], "structured_message");
        const auto& embeddedFields = fields[2]["fields"];
        EXPECT_EQ(embeddedFields[0]["name"], "x");
        EXPECT_EQ(embeddedFields[1]["name"], "y");

        EXPECT_EQ(fields[3]["name"], "x2");

        EXPECT_EQ(fields[4]["name"], "x1");
        EXPECT_EQ(fields[5]["name"], "y1");
        EXPECT_EQ(fields[6]["name"], "z1");
    };

    {
        const auto& column = columns[2];
        EXPECT_EQ(column["name"], "SerializationProtobuf");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 3u);
        EXPECT_EQ(column["fields"][0]["name"], "x1");
        EXPECT_EQ(column["fields"][1]["name"], "y1");
        EXPECT_EQ(column["fields"][2]["name"], "z1");
    }
    {
        const auto& column = columns[3];
        EXPECT_EQ(column["name"], "MemberOfTopLevelOneof");
        EXPECT_EQ(column["proto_type"], "int64");
    }
}
