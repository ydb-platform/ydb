#include <yt/yt/client/table_client/constrained_schema.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemaSerializationTest, ParseUsingNodeAndSerialize)
{
    TYsonStringBuf schemaString("<"
            "strict=%true;"
            "unique_keys=%false;"
            "deleted_columns=[{stable_name=b}]"
        ">"
        "[{name=a;required=%false;type=int64;}]");

    TTableSchema schema;
    Deserialize(schema, NYTree::ConvertToNode(schemaString));

    EXPECT_EQ(1, std::ssize(schema.Columns()));
    EXPECT_EQ("a", schema.Columns()[0].Name());
    EXPECT_EQ(1, std::ssize(schema.DeletedColumns()));
    EXPECT_EQ("b", schema.DeletedColumns()[0].StableName().Underlying());

    NYT::NFormats::TFormat format(NFormats::EFormatType::Json);

    TBlobOutput buffer;
    auto consumer = CreateConsumerForFormat(format, NFormats::EDataType::Structured, &buffer);

    Serialize(schema, consumer.get());
    consumer->Flush();
    auto ref = buffer.Flush();

    TStringBuf expected = "{"
            R"("$attributes":{)"
                R"("strict":true,)"
                R"("unique_keys":false,)"
                R"("deleted_columns":[{"stable_name":"b"}])"
            "},"
            R"("$value":[)"
                "{"
                    R"("name":"a",)"
                    R"("required":false,)"
                    R"("type":"int64",)"
                    R"("type_v3":{"type_name":"optional","item":"int64"})"
                "}"
            "]"
        "}";

    EXPECT_EQ(ref.ToStringBuf(), expected);
}

TEST(TSchemaSerializationTest, ParseEntityUsingNodeAndSerialize)
{
    NYT::NFormats::TFormat format(NFormats::EFormatType::Json);

    TBlobOutput buffer;
    auto consumer = CreateConsumerForFormat(format, NFormats::EDataType::Structured, &buffer);

    Serialize(TTableSchemaPtr{}, consumer.get());
    consumer->Flush();
    auto ref = buffer.Flush();
    EXPECT_EQ(ref.ToStringBuf(), "null");
}

TEST(TSchemaSerializationTest, Cursor)
{
    const char* schemaString = "<"
            "strict=%true;"
            "unique_keys=%false;"
            "deleted_columns=[{stable_name=b}]"
        ">"
        "[{name=a;required=%false;type=int64;}]";

    TMemoryInput input(schemaString);
    NYson::TYsonPullParser parser(&input, NYson::EYsonType::Node);
    NYson::TYsonPullParserCursor cursor(&parser);

    TTableSchema schema;
    Deserialize(schema, &cursor);

    EXPECT_EQ(1, std::ssize(schema.Columns()));
    EXPECT_EQ("a", schema.Columns()[0].Name());
    EXPECT_EQ(1, std::ssize(schema.DeletedColumns()));
    EXPECT_EQ("b", schema.DeletedColumns()[0].StableName().Underlying());
}

TEST(TSchemaSerializationTest, Deleted)
{
    TYsonStringBuf schemaString("<"
            "strict=%true;"
            "unique_keys=%false;"
            "deleted_columns=[{}]"
        ">"
        "[{name=a;required=%false;type=int64;}]");

    TTableSchema schema;
    EXPECT_THROW_WITH_SUBSTRING(
        Deserialize(schema, NYTree::ConvertToNode(schemaString)),
        "Stable name should be set for deleted column");
}

TEST(TConstrainedSchemaSerialization, YsonDeserializeAndSerialize)
{
    std::string schemaString = "<"
            "strict=%true;"
            "unique_keys=%false;"
            "deleted_columns=[{stable_name=b}]"
        ">"
        "[{name=a;stable_name=_a;required=%false;type=int64;constraint=\"BETWEEN 2 AND 3\"}]";

    auto checkSchemaAndSerialize = [] (const TConstrainedTableSchema& schema) {
        EXPECT_EQ(1, std::ssize(schema.TableSchema().Columns()));
        const auto& column = schema.TableSchema().Columns()[0];
        EXPECT_EQ("a", column.Name());
        EXPECT_EQ("_a", column.StableName().Underlying());
        EXPECT_EQ(1, std::ssize(schema.TableSchema().DeletedColumns()));
        EXPECT_EQ("b", schema.TableSchema().DeletedColumns()[0].StableName().Underlying());
        EXPECT_EQ(1, std::ssize(schema.ColumnToConstraint()));
        EXPECT_EQ("BETWEEN 2 AND 3", GetOrCrash(schema.ColumnToConstraint(), "a"));

        NFormats::TFormat format(NFormats::EFormatType::Json);

        TBlobOutput buffer;
        auto consumer = CreateConsumerForFormat(format, NFormats::EDataType::Structured, &buffer);

        Serialize(schema, consumer.get());
        consumer->Flush();
        auto ref = buffer.Flush();

        TStringBuf expected = "{"
            R"("$attributes":{)"
                R"("strict":true,)"
                R"("unique_keys":false,)"
                R"("deleted_columns":[{"stable_name":"b"}])"
            "},"
            R"("$value":[)"
                "{"
                    R"("constraint":"BETWEEN 2 AND 3",)"
                    R"("name":"a",)"
                    R"("required":false,)"
                    R"("stable_name":"_a",)"
                    R"("type":"int64",)"
                    R"("type_v3":{"type_name":"optional","item":"int64"})"
                "}"
            "]"
        "}";
        EXPECT_EQ(ref.ToStringBuf(), expected);
    };

    // Deserialization via INode.
    {
        TConstrainedTableSchema schema;
        Deserialize(schema, ConvertToNode(TYsonString(schemaString)));
        checkSchemaAndSerialize(schema);
    }

    // Deserialization via TYsonPullParserCursor.
    {
        TMemoryInput input(schemaString);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);

        TConstrainedTableSchema schema;
        Deserialize(schema, &cursor);
        checkSchemaAndSerialize(schema);
    }
}

TEST(TConstraintMap, NameToStableNameMapConversions)
{
    // StableNameToConstraintMap -> NameToConstraintMap
    {
        auto column = TColumnSchema("stable_a", ESimpleLogicalValueType::Int64);
        column.SetName("a");
        auto schema = TTableSchema({
            std::move(column),
        });
        TColumnStableNameToConstraintMap columnStableNameToConstraintMap;
        EmplaceOrCrash(columnStableNameToConstraintMap, TColumnStableName("stable_a"), "BETWEEN 1 AND 2");

        auto columnNameToConstraintMap = MakeColumnNameToConstraintMap(schema, std::move(columnStableNameToConstraintMap), 1);
        EXPECT_EQ(std::ssize(columnNameToConstraintMap), 1);
        EXPECT_EQ(GetOrCrash(columnNameToConstraintMap, "a"), "BETWEEN 1 AND 2");
    }

    // NameToConstraintMap -> StableNameToConstraintMap
    {
        auto column = TColumnSchema("stable_a", ESimpleLogicalValueType::Int64);
        column.SetName("a");
        auto schema = TTableSchema({
            std::move(column),
        });
        TColumnNameToConstraintMap columnNameToConstraintMap;
        EmplaceOrCrash(columnNameToConstraintMap, "a", "BETWEEN 1 AND 2");

        auto columnStableNameToConstraintMap = MakeColumnStableNameToConstraintMap(schema, std::move(columnNameToConstraintMap), 1);
        EXPECT_EQ(std::ssize(columnStableNameToConstraintMap), 1);
        EXPECT_EQ(GetOrCrash(columnStableNameToConstraintMap, TColumnStableName("stable_a")), "BETWEEN 1 AND 2");
    }
}

TEST(TInstantSerializationTest, YsonCompatibility)
{
    auto convert = [] (auto value) {
        TUnversionedValue unversioned;
        ToUnversionedValue(&unversioned, value, /*rowBuffer*/ nullptr);
        auto node = NYTree::ConvertToNode(unversioned);
        return NYTree::ConvertTo<TInstant>(node);
    };

    TInstant now = TInstant::Now();
    TInstant lower = TInstant::TInstant::ParseIso8601("1970-03-01");
    TInstant upper = TInstant::ParseIso8601("2100-01-01");

    EXPECT_EQ(now, convert(now));
    EXPECT_EQ(TInstant::MilliSeconds(now.MilliSeconds()), convert(now.MilliSeconds()));
    EXPECT_EQ(lower, convert(lower));
    EXPECT_EQ(TInstant::MilliSeconds(lower.MilliSeconds()), convert(lower.MilliSeconds()));
    EXPECT_EQ(upper, convert(upper));
    EXPECT_EQ(TInstant::MilliSeconds(upper.MilliSeconds()), convert(upper.MilliSeconds()));
}

TEST(TLegacyOwningKeySerializationTest, CompositeKeys)
{
    auto nameTable = New<TNameTable>();
    nameTable->RegisterName("key0");

    auto checkSerializeDeserialize = [] (auto&& value) {
        TStringStream str;
        NYson::TYsonWriter ysonWriter(&str);

        Serialize(value, &ysonWriter);

        auto node = NYTree::ConvertToNode(NYson::TYsonString(str.Str()));

        using TValueType = std::decay_t<decltype(value)>;
        TValueType result;
        Deserialize(result, node);
        EXPECT_EQ(value, result);
    };

#define CHECK_SERIALIZE_DESERIALIZE(x) do { SCOPED_TRACE(""); checkSerializeDeserialize(x);} while (0)

    CHECK_SERIALIZE_DESERIALIZE(
        NNamedValue::MakeRow(nameTable, {
            {"key0", EValueType::Any, "[1; 2]"},
        }));

    CHECK_SERIALIZE_DESERIALIZE(
        NNamedValue::MakeRow(nameTable, {
            {"key0", EValueType::Composite, "[1; 2]"},
        }));

#undef CHECK_SERIALIZE_DESERIALIZE
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
