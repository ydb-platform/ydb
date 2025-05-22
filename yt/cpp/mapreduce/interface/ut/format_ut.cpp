#include "common_ut.h"

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/format.h>

#include <yt/cpp/mapreduce/interface/ut/proto3_ut.pb.h>
#include <yt/cpp/mapreduce/interface/ut/protobuf_table_schema_ut.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

static TNode GetColumns(const TFormat& format, int tableIndex = 0)
{
    return format.Config.GetAttributes()["tables"][tableIndex]["columns"];
}

TEST(TProtobufFormatTest, TIntegral)
{
    const auto format = TFormat::Protobuf<NUnitTesting::TIntegral>();
    auto columns = GetColumns(format);

    struct TColumn
    {
        TString Name;
        TString ProtoType;
        int FieldNumber;
    };

    auto expected = TVector<TColumn>{
        {"DoubleField", "double", 1},
        {"FloatField", "float", 2},
        {"Int32Field", "int32", 3},
        {"Int64Field", "int64", 4},
        {"Uint32Field", "uint32", 5},
        {"Uint64Field", "uint64", 6},
        {"Sint32Field", "sint32", 7},
        {"Sint64Field", "sint64", 8},
        {"Fixed32Field", "fixed32", 9},
        {"Fixed64Field", "fixed64", 10},
        {"Sfixed32Field", "sfixed32", 11},
        {"Sfixed64Field", "sfixed64", 12},
        {"BoolField", "bool", 13},
        {"EnumField", "enum_string", 14},
    };

    EXPECT_EQ(columns.Size(), expected.size());
    for (int i = 0; i < static_cast<int>(columns.Size()); ++i) {
        EXPECT_EQ(columns[i]["name"], expected[i].Name);
        EXPECT_EQ(columns[i]["proto_type"], expected[i].ProtoType);
        EXPECT_EQ(columns[i]["field_number"], expected[i].FieldNumber);
    }
}

TEST(TProtobufFormatTest, TRowFieldSerializationOption)
{
    const auto format = TFormat::Protobuf<NUnitTesting::TRowFieldSerializationOption>();
    auto columns = GetColumns(format);

    EXPECT_EQ(columns[0]["name"], "UrlRow_1");
    EXPECT_EQ(columns[0]["proto_type"], "structured_message");
    EXPECT_EQ(columns[0]["field_number"], 1);
    const auto& fields = columns[0]["fields"];
    EXPECT_EQ(fields[0]["name"], "Host");
    EXPECT_EQ(fields[0]["proto_type"], "string");
    EXPECT_EQ(fields[0]["field_number"], 1);

    EXPECT_EQ(fields[1]["name"], "Path");
    EXPECT_EQ(fields[1]["proto_type"], "string");
    EXPECT_EQ(fields[1]["field_number"], 2);

    EXPECT_EQ(fields[2]["name"], "HttpCode");
    EXPECT_EQ(fields[2]["proto_type"], "sint32");
    EXPECT_EQ(fields[2]["field_number"], 3);

    EXPECT_EQ(columns[1]["name"], "UrlRow_2");
    EXPECT_EQ(columns[1]["proto_type"], "message");
    EXPECT_EQ(columns[1]["field_number"], 2);
}


TEST(TProtobufFormatTest, TPacked)
{
    const auto format = TFormat::Protobuf<NUnitTesting::TPacked>();
    auto column = GetColumns(format)[0];

    EXPECT_EQ(column["name"], "PackedListInt64");
    EXPECT_EQ(column["proto_type"], "int64");
    EXPECT_EQ(column["field_number"], 1);
    EXPECT_EQ(column["packed"], true);
    EXPECT_EQ(column["repeated"], true);
}

TEST(TProtobufFormatTest, TCyclic)
{
    EXPECT_THROW(TFormat::Protobuf<NUnitTesting::TCyclic>(), TApiUsageError);
    EXPECT_THROW(TFormat::Protobuf<NUnitTesting::TCyclic::TA>(), TApiUsageError);
    EXPECT_THROW(TFormat::Protobuf<NUnitTesting::TCyclic::TB>(), TApiUsageError);
    EXPECT_THROW(TFormat::Protobuf<NUnitTesting::TCyclic::TC>(), TApiUsageError);
    EXPECT_THROW(TFormat::Protobuf<NUnitTesting::TCyclic::TD>(), TApiUsageError);

    const auto format = TFormat::Protobuf<NUnitTesting::TCyclic::TE>();
    auto column = GetColumns(format)[0];
    EXPECT_EQ(column["name"], "d");
    EXPECT_EQ(column["proto_type"], "message");
    EXPECT_EQ(column["field_number"], 1);
}

TEST(TProtobufFormatTest, Map)
{
    const auto format = TFormat::Protobuf<NUnitTesting::TWithMap>();
    auto columns = GetColumns(format);

    EXPECT_EQ(columns.Size(), 5u);
    {
        const auto& column = columns[0];
        EXPECT_EQ(column["name"], "MapDefault");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 2u);
        EXPECT_EQ(column["fields"][0]["proto_type"], "int64");
        EXPECT_EQ(column["fields"][1]["proto_type"], "message");
    }
    {
        const auto& column = columns[1];
        EXPECT_EQ(column["name"], "MapListOfStructsLegacy");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 2u);
        EXPECT_EQ(column["fields"][0]["proto_type"], "int64");
        EXPECT_EQ(column["fields"][1]["proto_type"], "message");
    }
    {
        const auto& column = columns[2];
        EXPECT_EQ(column["name"], "MapListOfStructs");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 2u);
        EXPECT_EQ(column["fields"][0]["proto_type"], "int64");
        EXPECT_EQ(column["fields"][1]["proto_type"], "structured_message");
    }
    {
        const auto& column = columns[3];
        EXPECT_EQ(column["name"], "MapOptionalDict");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 2u);
        EXPECT_EQ(column["fields"][0]["proto_type"], "int64");
        EXPECT_EQ(column["fields"][1]["proto_type"], "structured_message");
    }
    {
        const auto& column = columns[4];
        EXPECT_EQ(column["name"], "MapDict");
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 2u);
        EXPECT_EQ(column["fields"][0]["proto_type"], "int64");
        EXPECT_EQ(column["fields"][1]["proto_type"], "structured_message");
    }
}


TEST(TProtobufFormatTest, Oneof)
{
    const auto format = TFormat::Protobuf<NUnitTesting::TWithOneof>();
    auto columns = GetColumns(format);

    EXPECT_EQ(columns.Size(), 4u);
    auto check = [] (const TNode& column, TStringBuf name, TStringBuf oneof2Name) {
        EXPECT_EQ(column["name"], name);
        EXPECT_EQ(column["proto_type"], "structured_message");
        EXPECT_EQ(column["fields"].Size(), 5u);
        EXPECT_EQ(column["fields"][0]["name"], "field");

        const auto& oneof2 = column["fields"][1];
        EXPECT_EQ(oneof2["name"], oneof2Name);
        EXPECT_EQ(oneof2["proto_type"], "oneof");
        EXPECT_EQ(oneof2["fields"][0]["name"], "y2");
        EXPECT_EQ(oneof2["fields"][1]["name"], "z2");
        EXPECT_EQ(oneof2["fields"][1]["proto_type"], "structured_message");
        const auto& embeddedOneof = oneof2["fields"][1]["fields"][0];
        EXPECT_EQ(embeddedOneof["name"], "Oneof");
        EXPECT_EQ(embeddedOneof["fields"][0]["name"], "x");
        EXPECT_EQ(embeddedOneof["fields"][1]["name"], "y");
        EXPECT_EQ(oneof2["fields"][2]["name"], "x2");

        EXPECT_EQ(column["fields"][2]["name"], "x1");
        EXPECT_EQ(column["fields"][3]["name"], "y1");
        EXPECT_EQ(column["fields"][4]["name"], "z1");
    };

    check(columns[0], "DefaultSeparateFields", "variant_field_name");
    check(columns[1], "NoDefault", "Oneof2");

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
        EXPECT_EQ(column["name"], "TopLevelOneof");
        EXPECT_EQ(column["proto_type"], "oneof");
        EXPECT_EQ(column["fields"].Size(), 1u);
        EXPECT_EQ(column["fields"][0]["name"], "MemberOfTopLevelOneof");
    }
}

TEST(TProto3Test, TWithOptional)
{
    const auto format = TFormat::Protobuf<NTestingProto3::TWithOptional>();
    auto columns = GetColumns(format);

    EXPECT_EQ(columns[0]["name"], "x");
    EXPECT_EQ(columns[0]["proto_type"], "int64");
    EXPECT_EQ(columns[0]["field_number"], 1);
}

TEST(TProto3Test, TWithOptionalMessage)
{
    const auto format = TFormat::Protobuf<NTestingProto3::TWithOptionalMessage>();
    auto columns = GetColumns(format);

    EXPECT_EQ(columns[0]["name"], "x");
    EXPECT_EQ(columns[0]["proto_type"], "structured_message");
    EXPECT_EQ(columns[0]["field_number"], 1);

    EXPECT_EQ(columns[0]["fields"].Size(), 1u);
    EXPECT_EQ(columns[0]["fields"][0]["name"], "x");
    EXPECT_EQ(columns[0]["fields"][0]["proto_type"], "int64");
    EXPECT_EQ(columns[0]["fields"][0]["field_number"], 1);
}
