#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/formats/web_json_writer.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/named_value/named_value.h>

#include <limits>

namespace NYT::NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

using NNamedValue::MakeRow;

INodePtr ParseJsonToNode(TStringBuf string)
{
    TBuildingYsonConsumerViaTreeBuilder<INodePtr> builder(EYsonType::Node);
    TMemoryInput stream(string);

    // For plain (raw) JSON parsing we need to switch off
    // "smart" attribute analysis and UTF-8 decoding.
    auto config = New<NJson::TJsonFormatConfig>();
    config->EncodeUtf8 = false;
    config->Plain = true;

    NJson::ParseJson(&stream, &builder, std::move(config));
    return builder.Finish();
}

class TWriterForWebJson
    : public ::testing::Test
{
protected:
    TNameTablePtr NameTable_ = New<TNameTable>();
    TWebJsonFormatConfigPtr Config_ = New<TWebJsonFormatConfig>();
    TStringStream OutputStream_;
    ISchemalessFormatWriterPtr Writer_;

    const TString ValueColumnName_ = "value";

    void CreateStandardWriter(const std::vector<TTableSchemaPtr>& schemas = {New<TTableSchema>()})
    {
        Writer_ = CreateWriterForWebJson(
            Config_,
            NameTable_,
            schemas,
            CreateAsyncAdapter(static_cast<IOutputStream*>(&OutputStream_)));
    }
};

TEST_F(TWriterForWebJson, Simple)
{
    Config_->MaxAllColumnNamesCount = 2;

    CreateStandardWriter();

    bool written = Writer_->Write({
        MakeRow(NameTable_, {
            {"column_a", 100500u},
            {"column_b", true},
            {"column_c", "row1_c"},
            {TString(RowIndexColumnName), 0},
        }).Get(),
        MakeRow(NameTable_, {
            {"column_c", "row2_c"},
            {"column_b", "row2_b"},
            {TString(RowIndexColumnName), 1},
        }).Get(),
    });
    EXPECT_TRUE(written);
    WaitFor(Writer_->Close())
        .ThrowOnError();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_a\":{"
                        "\"$type\":\"uint64\","
                        "\"$value\":\"100500\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"boolean\","
                        "\"$value\":\"true\""
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_c\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_c\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"true\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SliceColumnsByMaxCount)
{
    Config_->MaxSelectedColumnCount = 2;

    CreateStandardWriter();
    bool written = Writer_->Write({
        MakeRow(NameTable_, {
            {"column_a", "row1_a"},
            {"column_b", "row1_b"},
            {"column_c", "row1_c"},
        }).Get(),
        MakeRow(NameTable_, {
            {"column_c", "row2_c"},
            {"column_b", "row2_b"},
        }).Get(),
        MakeRow(NameTable_, {
            {"column_c", "row3_c"},
        }).Get(),
    });
    EXPECT_TRUE(written);
    YT_UNUSED_FUTURE(Writer_->Close());

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_b\""
                    "}"
                "},"
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "},"
                "{"
                "}"
            "],"
            "\"incomplete_columns\":\"true\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SliceStrings)
{
    Config_->FieldWeightLimit = 6;

    CreateStandardWriter();

    bool written = Writer_->Write({
        MakeRow(NameTable_, {
            {"column_b", "row1_b"},
            {"column_c", "rooooow1_c"},
            {"column_a", "row1_a"},
        }).Get(),
        MakeRow(NameTable_, {
            {"column_c", "row2_c"},
            {"column_b", "rooow2_b"},
        }).Get(),
        MakeRow(NameTable_, {
            {"column_c", "row3_c"},
        }).Get(),
    });
    EXPECT_TRUE(written);
    YT_UNUSED_FUTURE(Writer_->Close());

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_b\""
                    "},"
                    "\"column_c\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"string\","
                        "\"$value\":\"rooooo\""
                    "},"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_c\""
                    "},"
                    "\"column_b\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"string\","
                        "\"$value\":\"rooow2\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row3_c\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, ReplaceAnyWithNull)
{
    Config_->FieldWeightLimit = 8;

    CreateStandardWriter();

    bool written = Writer_->Write({
        MakeRow(NameTable_, {
            {"column_b", EValueType::Any, "{key=a}"},
            {"column_c", "row1_c"},
            {"column_a", "row1_a"},
        }).Get(),
        MakeRow(NameTable_, {
            {"column_c", EValueType::Any, "{key=aaaaaa}"},
            {"column_b", "row2_b"},
        }).Get(),
        MakeRow(NameTable_, {
            {"column_c", "row3_c"},
        }).Get(),
    });
    EXPECT_TRUE(written);
    WaitFor(Writer_->Close())
        .ThrowOnError();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"key\":{"
                            "\"$type\":\"string\","
                            "\"$value\":\"a\""
                        "}"
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_c\""
                    "},"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"any\","
                        "\"$value\":\"\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row3_c\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, NotSkipSystemColumns)
{
    Config_->SkipSystemColumns = false;

    CreateStandardWriter();

    bool written = Writer_->Write({
        MakeRow(NameTable_, {
            {TString(TableIndexColumnName), 0},
            {TString(RowIndexColumnName), 1},
            {TString(TabletIndexColumnName), 2},
            {ValueColumnName_, 3}
        }).Get(),
    });
    EXPECT_TRUE(written);
    WaitFor(Writer_->Close())
        .ThrowOnError();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"$$table_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"0\""
                    "},"
                    "\"$$row_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"1\""
                    "},"
                    "\"$$tablet_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"2\""
                    "},"
                    "\"value\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"3\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"$row_index\","
                "\"$table_index\","
                "\"$tablet_index\","
                "\"value\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SkipSystemColumns)
{
    Config_->SkipSystemColumns = true;

    CreateStandardWriter();

    bool written = Writer_->Write({
        MakeRow(NameTable_, {
            {TString(TableIndexColumnName), 0},
            {TString(RowIndexColumnName), 1},
            {TString(TabletIndexColumnName), 2},
            {ValueColumnName_, 3}
        }).Get(),
    });
    EXPECT_TRUE(written);
    WaitFor(Writer_->Close())
        .ThrowOnError();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"value\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"3\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"value\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, NotSkipRequestedSystemColumns)
{
    Config_->SkipSystemColumns = true;
    Config_->ColumnNames = std::vector<std::string>{TabletIndexColumnName, ValueColumnName_};

    CreateStandardWriter();

    bool written = Writer_->Write({
        MakeRow(NameTable_, {
            {TString(TableIndexColumnName), 0},
            {TString(RowIndexColumnName), 1},
            {TString(TabletIndexColumnName), 2},
            {ValueColumnName_, 3}
        }).Get(),
    });
    EXPECT_TRUE(written);
    WaitFor(Writer_->Close())
        .ThrowOnError();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"$$tablet_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"2\""
                    "},"
                    "\"value\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"3\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"$tablet_index\","
                "\"value\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SkipUnregisteredColumns)
{
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    int keyDId = -1;
    row.AddValue(MakeUnversionedBooleanValue(true, keyDId));
    std::vector<TUnversionedRow> rows = {row.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));

    keyDId = NameTable_->RegisterName("column_d");

    rows.clear();
    row.Reset();
    row.AddValue(MakeUnversionedBooleanValue(true, keyDId));
    rows.push_back(row.GetRow());

    EXPECT_EQ(true, Writer_->Write(rows));
    YT_UNUSED_FUTURE(Writer_->Close());

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                "},"
                "{"
                    "\"column_d\":{"
                        "\"$type\":\"boolean\","
                        "\"$value\":\"true\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_d\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SliceColumnsByName)
{
    Config_->ColumnNames = {
        "column_b",
        "column_c",
        "$tablet_index"};
    Config_->MaxSelectedColumnCount = 2;
    Config_->SkipSystemColumns = false;

    CreateStandardWriter();

    bool written = Writer_->Write({
        MakeRow(NameTable_, {
            {"column_a", 100500u},
            {"column_b", 0.42},
            {"column_c", "abracadabra"},
            {TString(TabletIndexColumnName), 10},
        }).Get(),
    });
    EXPECT_TRUE(written);
    WaitFor(Writer_->Close())
        .ThrowOnError();
    auto result = ParseJsonToNode(OutputStream_.Str());

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"double\","
                        "\"$value\":\"0.42\""
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"abracadabra\""
                    "},"
                    "\"$$tablet_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"10\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"true\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"$tablet_index\","
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(std::ssize(expectedOutput), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

template <typename TValue>
void CheckYqlValue(
    const INodePtr& valueNode,
    const TValue& expectedValue)
{
    using TDecayedValue = std::decay_t<TValue>;
    if constexpr (std::is_convertible_v<TDecayedValue, TString>) {
        ASSERT_EQ(valueNode->GetType(), ENodeType::String);
        EXPECT_EQ(valueNode->GetValue<TString>(), expectedValue);
    } else if constexpr (std::is_same_v<TDecayedValue, double>) {
        ASSERT_EQ(valueNode->GetType(), ENodeType::String);
        EXPECT_FLOAT_EQ(FromString<double>(valueNode->GetValue<TString>()), expectedValue);
    } else if constexpr (std::is_same_v<TDecayedValue, bool>) {
        ASSERT_EQ(valueNode->GetType(), ENodeType::Boolean);
        EXPECT_EQ(valueNode->GetValue<bool>(), expectedValue);
    } else if constexpr (std::is_same_v<TDecayedValue, INodePtr>) {
        EXPECT_TRUE(AreNodesEqual(valueNode, expectedValue))
            << "actualValueNode is " << ConvertToYsonString(valueNode, EYsonFormat::Pretty).AsStringBuf()
            << "\nexpectedValue is " << ConvertToYsonString(expectedValue, EYsonFormat::Pretty).AsStringBuf();
    } else {
        static_assert(TDependentFalse<TDecayedValue>, "Type not allowed");
    }
}

template <typename TType>
void CheckYqlType(
    const INodePtr& typeNode,
    const TType& expectedType,
    const std::vector<INodePtr>& yqlTypes)
{
    ASSERT_EQ(typeNode->GetType(), ENodeType::String);
    auto typeIndexString = typeNode->GetValue<TString>();
    auto typeIndex = FromString<int>(typeIndexString);
    ASSERT_LT(typeIndex, static_cast<int>(yqlTypes.size()));
    ASSERT_GE(typeIndex, 0);
    const auto& yqlType = yqlTypes[typeIndex];
    EXPECT_EQ(yqlType->GetType(), ENodeType::List);

    auto expectedTypeNode = [&] () -> INodePtr {
        using TDecayedType = std::decay_t<TType>;
        if constexpr (std::is_convertible_v<TDecayedType, TString>) {
            return ConvertToNode(TYsonString(TString(expectedType)));
        } else if constexpr (std::is_same_v<TDecayedType, INodePtr>) {
            return expectedType;
        } else {
            static_assert(TDependentFalse<TDecayedType>, "Type not allowed");
        }
    }();
    EXPECT_TRUE(AreNodesEqual(yqlType, expectedTypeNode))
        << "yqlType is " << ConvertToYsonString(yqlType, EYsonFormat::Pretty).AsStringBuf()
        << "\nexpectedTypeNode is " << ConvertToYsonString(expectedTypeNode, EYsonFormat::Pretty).AsStringBuf();
}

template <typename TValue, typename TType>
void CheckYqlTypeAndValue(
    const INodePtr& row,
    TStringBuf name,
    const TType& expectedType,
    const TValue& expectedValue,
    const std::vector<INodePtr>& yqlTypes)
{
    ASSERT_EQ(row->GetType(), ENodeType::Map);
    auto entry = row->AsMap()->FindChild(TString(name));
    ASSERT_TRUE(entry);
    ASSERT_EQ(entry->GetType(), ENodeType::List);
    ASSERT_EQ(entry->AsList()->GetChildCount(), 2);
    auto valueNode = entry->AsList()->GetChildOrThrow(0);
    CheckYqlValue(valueNode, expectedValue);
    auto typeNode = entry->AsList()->GetChildOrThrow(1);
    CheckYqlType(typeNode, expectedType, yqlTypes);
}

#define CHECK_YQL_TYPE_AND_VALUE(row, name, expectedType, expectedValue, yqlTypes) \
    do { \
        SCOPED_TRACE(name); \
        CheckYqlTypeAndValue(row, name, expectedType, expectedValue, yqlTypes); \
    } while (0)

TEST_F(TWriterForWebJson, YqlValueFormat_SimpleTypes)
{
    Config_->MaxAllColumnNamesCount = 2;
    Config_->ValueFormat = EWebJsonValueFormat::Yql;

    // We will emulate writing rows from two tables.
    CreateStandardWriter(std::vector{New<TTableSchema>(), New<TTableSchema>()});

    {
        bool written = Writer_->Write({
            MakeRow(NameTable_, {
                {"column_a", 100500u},
                {"column_b", true},
                {"column_c", "row1_c"},
                {TString(RowIndexColumnName), 0},
                {TString(TableIndexColumnName), 0},
            }).Get(),
            MakeRow(NameTable_, {
                {"column_c", "row2_c"},
                {"column_b", "row2_b"},
                {TString(RowIndexColumnName), 1},
                {TString(TableIndexColumnName), 0},
            }).Get(),
            MakeRow(NameTable_, {
                {"column_a", -100500},
                {"column_b", EValueType::Any, "{x=2;y=3}"},
                {"column_c", 2.71828},
                {TString(RowIndexColumnName), 1},
            }).Get(),
        });
        EXPECT_TRUE(written);
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto incompleteColumns = result->AsMap()->FindChild("incomplete_columns");
    ASSERT_TRUE(incompleteColumns);
    auto incompleteAllColumnNames = result->AsMap()->FindChild("incomplete_all_column_names");
    ASSERT_TRUE(incompleteAllColumnNames);
    auto allColumnNames = result->AsMap()->FindChild("all_column_names");
    ASSERT_TRUE(allColumnNames);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(incompleteColumns->GetType(), ENodeType::String);
    EXPECT_EQ(incompleteColumns->GetValue<TString>(), "false");

    ASSERT_EQ(incompleteAllColumnNames->GetType(), ENodeType::String);
    EXPECT_EQ(incompleteAllColumnNames->GetValue<TString>(), "true");

    ASSERT_EQ(allColumnNames->GetType(), ENodeType::List);
    std::vector<TString> allColumnNamesVector;
    ASSERT_NO_THROW(allColumnNamesVector = ConvertTo<decltype(allColumnNamesVector)>(allColumnNames));
    EXPECT_EQ(allColumnNamesVector, (std::vector<TString>{"column_a", "column_b"}));

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 3);

    auto row1 = rows->AsList()->GetChildOrThrow(0);
    auto row2 = rows->AsList()->GetChildOrThrow(1);
    auto row3 = rows->AsList()->GetChildOrThrow(2);

    ASSERT_EQ(row1->GetType(), ENodeType::Map);
    EXPECT_EQ(row1->AsMap()->GetChildCount(), 3);
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_a", R"(["DataType"; "Uint64"])", "100500", yqlTypes);
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_b", R"(["DataType"; "Boolean"])", true, yqlTypes);
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_c", R"(["DataType"; "String"])", "row1_c", yqlTypes);

    ASSERT_EQ(row2->GetType(), ENodeType::Map);
    EXPECT_EQ(row2->AsMap()->GetChildCount(), 2);
    CHECK_YQL_TYPE_AND_VALUE(row2, "column_b", R"(["DataType"; "String"])", "row2_b", yqlTypes);
    CHECK_YQL_TYPE_AND_VALUE(row2, "column_c", R"(["DataType"; "String"])", "row2_c", yqlTypes);

    ASSERT_EQ(row3->GetType(), ENodeType::Map);
    EXPECT_EQ(row3->AsMap()->GetChildCount(), 3);
    CHECK_YQL_TYPE_AND_VALUE(row3, "column_a", R"(["DataType"; "Int64"])", "-100500", yqlTypes);
    auto row3BValue = ConvertToNode(TYsonString(TStringBuf(R"({
        val = {
            x = {
                "$type" = "int64";
                "$value" = "2";
            };
            y = {
                "$type" = "int64";
                "$value" = "3";
            }
        }
    })")));
    CHECK_YQL_TYPE_AND_VALUE(row3, "column_b", R"(["DataType"; "Yson"])", row3BValue, yqlTypes);
    CHECK_YQL_TYPE_AND_VALUE(row3, "column_c", R"(["DataType"; "Double"])", 2.71828, yqlTypes);
}

TEST_F(TWriterForWebJson, ColumnNameEncoding)
{
    Config_->MaxAllColumnNamesCount = 2;
    Config_->ValueFormat = EWebJsonValueFormat::Yql;

    CreateStandardWriter();

    {
        bool written = Writer_->Write({
            MakeRow(NameTable_, {
                {"column_a", 100500u},
                {"column_non_ascii_\xd0\x81", -100500},
            }).Get()
        });
        EXPECT_TRUE(written);
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto incompleteColumns = result->AsMap()->FindChild("incomplete_columns");
    ASSERT_TRUE(incompleteColumns);
    auto incompleteAllColumnNames = result->AsMap()->FindChild("incomplete_all_column_names");
    ASSERT_TRUE(incompleteAllColumnNames);
    auto allColumnNames = result->AsMap()->FindChild("all_column_names");
    ASSERT_TRUE(allColumnNames);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(allColumnNames->GetType(), ENodeType::List);
    std::vector<TString> allColumnNamesVector;
    ASSERT_NO_THROW(allColumnNamesVector = ConvertTo<decltype(allColumnNamesVector)>(allColumnNames));
    EXPECT_EQ(allColumnNamesVector, (std::vector<TString>{"column_a", "column_non_ascii_\xc3\x90\xc2\x81"}));

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 1);

    auto row1 = rows->AsList()->GetChildOrThrow(0);

    ASSERT_EQ(row1->GetType(), ENodeType::Map);
    EXPECT_EQ(row1->AsMap()->GetChildCount(), 2);
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_a", R"(["DataType"; "Uint64"])", "100500", yqlTypes);
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_non_ascii_\xc3\x90\xc2\x81", R"(["DataType"; "Int64"])", "-100500", yqlTypes);
}

TEST_F(TWriterForWebJson, YqlValueFormat_ComplexTypes)
{
    Config_->ValueFormat = EWebJsonValueFormat::Yql;

    auto firstSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        {"column_a", OptionalLogicalType(
            ListLogicalType(MakeLogicalType(ESimpleLogicalValueType::Int64, true)))},
        {"column_b", StructLogicalType({
            {"key", MakeLogicalType(ESimpleLogicalValueType::String, true)},
            {"value", MakeLogicalType(ESimpleLogicalValueType::String, true)},
            {"variant_tuple", VariantTupleLogicalType({
                MakeLogicalType(ESimpleLogicalValueType::Int8, true),
                MakeLogicalType(ESimpleLogicalValueType::Boolean, false),
            })},
            {"variant_struct", VariantStructLogicalType({
                {"a", MakeLogicalType(ESimpleLogicalValueType::Int8, true)},
                {"b", MakeLogicalType(ESimpleLogicalValueType::Boolean, false)},
            })},
            {"dict", DictLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64),
                SimpleLogicalType(ESimpleLogicalValueType::String)),
            },
            {"tagged", TaggedLogicalType(
                "MyTag",
                SimpleLogicalType(ESimpleLogicalValueType::Int64)),
            },
            {"timestamp", SimpleLogicalType(ESimpleLogicalValueType::Timestamp)},
            {"date", SimpleLogicalType(ESimpleLogicalValueType::Date)},
            {"datetime", SimpleLogicalType(ESimpleLogicalValueType::Datetime)},
            {"interval", SimpleLogicalType(ESimpleLogicalValueType::Interval)},
            {"date32", SimpleLogicalType(ESimpleLogicalValueType::Date32)},
            {"datetime64", SimpleLogicalType(ESimpleLogicalValueType::Datetime64)},
            {"timestamp64", SimpleLogicalType(ESimpleLogicalValueType::Timestamp64)},
            {"interval64", SimpleLogicalType(ESimpleLogicalValueType::Interval64)},
            {"json", SimpleLogicalType(ESimpleLogicalValueType::Json)},
            {"float", SimpleLogicalType(ESimpleLogicalValueType::Float)},
        })},
        {"column_c", ListLogicalType(StructLogicalType({
            {"very_optional_key", OptionalLogicalType(MakeLogicalType(ESimpleLogicalValueType::String, false))},
            {"optional_value", MakeLogicalType(ESimpleLogicalValueType::String, false)},
        }))},
    });

    auto secondSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        {"column_a", VariantTupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Null),
            SimpleLogicalType(ESimpleLogicalValueType::Any),
        })},
        {"column_b", SimpleLogicalType(ESimpleLogicalValueType::Null)},
        {"column_c", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))},
        {"column_d", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    auto firstColumnAType = ConvertToNode(TYsonString(TStringBuf(R"([
        "OptionalType";
        [
            "ListType";
            ["DataType"; "Int64"]
        ]
    ])")));
    auto firstColumnBType = ConvertToNode(TYsonString(TStringBuf(R"([
        "StructType";
        [
            [
                "key";
                ["DataType"; "String"]
            ];
            [
                "value";
                ["DataType"; "String"]
            ];
            [
                "variant_tuple";
                [
                    "VariantType";
                    [
                        "TupleType";
                        [
                            ["DataType"; "Int8"];
                            [
                                "OptionalType";
                                ["DataType"; "Boolean"]
                            ]
                        ]
                    ]
                ]
            ];
            [
                "variant_struct";
                [
                    "VariantType";
                    [
                        "StructType";
                        [
                            [
                                "a";
                                ["DataType"; "Int8"]
                            ];
                            [
                                "b";
                                [
                                    "OptionalType";
                                    ["DataType"; "Boolean"]
                                ]
                            ]
                        ]
                    ]
                ]
            ];
            [
                "dict";
                [
                    "DictType";
                    ["DataType"; "Int64"];
                    ["DataType"; "String"]
                ]
            ];
            [
                "tagged";
                [
                    "TaggedType";
                    "MyTag";
                    ["DataType"; "Int64"]
                ]
            ];
            [
                "timestamp";
                ["DataType"; "Timestamp"]
            ];
            [
                "date";
                ["DataType"; "Date"]
            ];
            [
                "datetime";
                ["DataType"; "Datetime"]
            ];
            [
                "interval";
                ["DataType"; "Interval"]
            ];
            [
                "date32";
                ["DataType"; "Date32"]
            ];
            [
                "datetime64";
                ["DataType"; "Datetime64"]
            ];
            [
                "timestamp64";
                ["DataType"; "Timestamp64"]
            ];
            [
                "interval64";
                ["DataType"; "Interval64"]
            ];
            [
                "json";
                ["DataType"; "Json"]
            ];
            [
                "float";
                ["DataType"; "Float"]
            ];
        ]
    ])")));
    auto firstColumnCType = ConvertToNode(TYsonString(TStringBuf(R"([
        "ListType";
        [
            "StructType";
            [
                [
                    "very_optional_key";
                    [
                        "OptionalType";
                        [
                            "OptionalType";
                            ["DataType"; "String"]
                        ]
                    ]
                ];
                [
                    "optional_value";
                    [
                        "OptionalType";
                        ["DataType"; "String"]
                    ]
                ]
            ]
        ]
    ])")));
    auto secondColumnAType = ConvertToNode(TYsonString(TStringBuf(R"([
        "VariantType";
        [
            "TupleType";
            [
                ["NullType"];
                ["DataType"; "Yson"];
            ]
        ]
    ])")));
    auto secondColumnBType = ConvertToNode(TYsonString(TStringBuf(R"(["NullType"])")));
    auto secondColumnCType = ConvertToNode(TYsonString(TStringBuf(R"([
        "OptionalType";
        [
            "NullType";
        ]
    ])")));
    auto secondColumnDType = ConvertToNode(TYsonString(TStringBuf(R"([
        "OptionalType";
        ["DataType"; "Int64"]
    ])")));

    CreateStandardWriter(std::vector{firstSchema, secondSchema});
    {
        bool written = Writer_->Write({
            MakeRow(NameTable_, {
                {"column_a", EValueType::Composite, R"([-1; -2; -5])"},
                {
                    "column_b",
                    EValueType::Composite,
                    R"([
                        "key";
                        "value";
                        [0; 7];
                        [1; #];
                        [[1; "a"]; [2; "b"]];
                        99;
                        100u;
                        101u;
                        102u;
                        103;
                        -42;
                        42;
                        -42;
                        -1;
                        "[\"a\", {\"b\": 42}]";
                        -3.25;
                    ])",
                },
                {"column_c", EValueType::Composite, R"([[[#]; "value"]; [["key"]; #]])"},
                {"column_d", -49},
                {TString(TableIndexColumnName), 0},
                {TString(RowIndexColumnName), 0},
            }).Get(),
            MakeRow(NameTable_, {
                {"column_a", EValueType::Composite, R"([0; -2; -5; 177])"},
                {
                    "column_b",
                    EValueType::Composite,
                    R"([
                        "key1";
                        "value1";
                        [1; %false];
                        [1; #];
                        [];
                        199;
                        0u;
                        1101u;
                        1102u;
                        1103;
                        123;
                        -123;
                        123;
                        123;
                        "null";
                        0.0;
                    ])",
                },
                {"column_c", EValueType::Composite, R"([[#; #]; [["key1"]; #]])"},
                {"column_d", 49u},
                {TString(RowIndexColumnName), 1},
            }).Get(),
            MakeRow(NameTable_, {
                {"column_a", EValueType::Composite, "[]"},
                {
                    "column_b",
                    EValueType::Composite,
                    R"([
                        "key2";
                        "value2";
                        [0; 127];
                        [1; %true];
                        [[0; ""]];
                        399;
                        30u;
                        3101u;
                        3202u;
                        3103;
                        -53375809;
                        -4611669897600;
                        -4611669897600000000;
                        -9223339708799999999;
                        "{\"x\": false}";
                        1e10;
                    ])"
                },
                {"column_c", EValueType::Composite, "[[[key]; #]]"},
                {"column_d", "49"},
                {TString(RowIndexColumnName), 2},
            }).Get(),

            MakeRow(NameTable_, {
                {"column_a", nullptr},
                {
                    "column_b",
                    EValueType::Composite,
                    // First string is valid UTF-8, the second one should be Base64 encoded.
                    "["
                    "\"\xC3\xBF\";"
                    "\"\xFA\xFB\xFC\xFD\";"
                    R"(
                        [0; 127];
                        [1; %true];
                        [[-1; "-1"]; [0; ""]];
                        499;
                        40u;
                        4101u;
                        4202u;
                        4103;
                        53375807;
                        4611669811199;
                        4611669811199999999;
                        9223339708799999999;
                        "{}";
                        -2.125;
                    ])",
                },
                {"column_c", EValueType::Composite, "[]"},
                {"column_d", EValueType::Any, "{x=49}"},
                {TString(RowIndexColumnName), 3},
            }).Get(),

            // Here come rows from the second table.
            MakeRow(NameTable_, {
                {"column_a", EValueType::Composite, "[0; #]"},
                {"column_b", nullptr},
                {"column_c", nullptr},
                {"column_d", -49},
                {TString(TableIndexColumnName), 1},
                {TString(RowIndexColumnName), 0},
            }).Get(),

            MakeRow(NameTable_, {
                {"column_a", EValueType::Composite, "[1; {z=z}]"},
                {"column_b", nullptr},
                {"column_c", EValueType::Composite, "[#]"},
                {"column_d", nullptr},
                {TString(TableIndexColumnName), 1},
                {TString(RowIndexColumnName), 1},
            }).Get(),
        });
        EXPECT_TRUE(written);
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto incompleteColumns = result->AsMap()->FindChild("incomplete_columns");
    ASSERT_TRUE(incompleteColumns);
    auto incompleteAllColumnNames = result->AsMap()->FindChild("incomplete_all_column_names");
    ASSERT_TRUE(incompleteAllColumnNames);
    auto allColumnNames = result->AsMap()->FindChild("all_column_names");
    ASSERT_TRUE(allColumnNames);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(incompleteColumns->GetType(), ENodeType::String);
    EXPECT_EQ(incompleteColumns->GetValue<TString>(), "false");

    ASSERT_EQ(incompleteAllColumnNames->GetType(), ENodeType::String);
    EXPECT_EQ(incompleteAllColumnNames->GetValue<TString>(), "false");

    ASSERT_EQ(allColumnNames->GetType(), ENodeType::List);
    std::vector<TString> allColumnNamesVector;
    ASSERT_NO_THROW(allColumnNamesVector = ConvertTo<decltype(allColumnNamesVector)>(allColumnNames));
    EXPECT_EQ(allColumnNamesVector, (std::vector<TString>{"column_a", "column_b", "column_c", "column_d"}));

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 6);

    auto row1 = rows->AsList()->GetChildOrThrow(0);
    auto row2 = rows->AsList()->GetChildOrThrow(1);
    auto row3 = rows->AsList()->GetChildOrThrow(2);
    auto row4 = rows->AsList()->GetChildOrThrow(3);
    auto row5 = rows->AsList()->GetChildOrThrow(4);
    auto row6 = rows->AsList()->GetChildOrThrow(5);

    ASSERT_EQ(row1->GetType(), ENodeType::Map);
    EXPECT_EQ(row1->AsMap()->GetChildCount(), 4);
    auto row1AValue = ConvertToNode(TYsonString(TStringBuf(R"([{"val"=["-1"; "-2"; "-5"]}])")));
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_a", firstColumnAType, row1AValue, yqlTypes);
    auto row1BValue = ConvertToNode(TYsonString(TStringBuf(
        R"([
            "key";
            "value";
            ["0"; "7"];
            ["1"; #];
            {"val"=[["1"; "a"]; ["2"; "b"]]};
            "99";
            "100";
            "101";
            "102";
            "103";
            "-42";
            "42";
            "-42";
            "-1";
            "[\"a\", {\"b\": 42}]";
            "-3.25";
        ])")));
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_b", firstColumnBType, row1BValue, yqlTypes);
    auto row1CValue = ConvertToNode(TYsonString(TStringBuf(R"({
        "val"=[
            [[#]; ["value"]];
            [[["key"]]; #]
        ]
    })")));
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_c", firstColumnCType, row1CValue, yqlTypes);
    CHECK_YQL_TYPE_AND_VALUE(row1, "column_d", R"(["DataType"; "Int64"])", "-49", yqlTypes);

    ASSERT_EQ(row2->GetType(), ENodeType::Map);
    EXPECT_EQ(row2->AsMap()->GetChildCount(), 4);
    auto row2AValue = ConvertToNode(TYsonString(TStringBuf(R"([{"val"=["0"; "-2"; "-5"; "177"]}])")));
    CHECK_YQL_TYPE_AND_VALUE(row2, "column_a", firstColumnAType, row2AValue, yqlTypes);
    auto row2BValue = ConvertToNode(TYsonString(TStringBuf(
        R"([
            "key1";
            "value1";
            ["1"; [%false]];
            ["1"; #];
            {"val"=[]};
            "199";
            "0";
            "1101";
            "1102";
            "1103";
            "123";
            "-123";
            "123";
            "123";
            "null";
            "0";
        ])")));
    CHECK_YQL_TYPE_AND_VALUE(row2, "column_b", firstColumnBType, row2BValue, yqlTypes);
    auto row2CValue = ConvertToNode(TYsonString(TStringBuf(R"({
        "val"=[
            [#; #];
            [[["key1"]]; #]
        ]
    })")));
    CHECK_YQL_TYPE_AND_VALUE(row2, "column_c", firstColumnCType, row2CValue, yqlTypes);
    CHECK_YQL_TYPE_AND_VALUE(row2, "column_d", R"(["DataType"; "Uint64"])", "49", yqlTypes);

    ASSERT_EQ(row3->GetType(), ENodeType::Map);
    EXPECT_EQ(row3->AsMap()->GetChildCount(), 4);
    auto row3AValue = ConvertToNode(TYsonString(TStringBuf(R"([{"val"=[]}])")));
    CHECK_YQL_TYPE_AND_VALUE(row3, "column_a", firstColumnAType, row3AValue, yqlTypes);
    auto row3BValue = ConvertToNode(TYsonString(TStringBuf(
        R"([
            "key2";
            "value2";
            ["0"; "127"];
            ["1"; [%true]];
            {"val"=[["0"; ""]]};
            "399";
            "30";
            "3101";
            "3202";
            "3103";
            "-53375809";
            "-4611669897600";
            "-4611669897600000000";
            "-9223339708799999999";
            "{\"x\": false}";
            "10000000000";
        ])")));
    CHECK_YQL_TYPE_AND_VALUE(row3, "column_b", firstColumnBType, row3BValue, yqlTypes);
    auto row3CValue = ConvertToNode(TYsonString(TStringBuf(R"({
        "val"=[
            [[["key"]]; #]
        ]
    })")));
    CHECK_YQL_TYPE_AND_VALUE(row3, "column_c", firstColumnCType, row3CValue, yqlTypes);
    CHECK_YQL_TYPE_AND_VALUE(row3, "column_d", R"(["DataType"; "String"])", "49", yqlTypes);

    ASSERT_EQ(row4->GetType(), ENodeType::Map);
    EXPECT_EQ(row4->AsMap()->GetChildCount(), 4);
    auto row4AValue = ConvertToNode(TYsonString(TStringBuf(R"(#)")));
    CHECK_YQL_TYPE_AND_VALUE(row4, "column_a", firstColumnAType, row4AValue, yqlTypes);

    auto row4BValue = ConvertToNode(TYsonString(TStringBuf(
        "["
            "\"\xC3\xBF\";"
        R"(
            {"b64" = %true; "val" = "+vv8/Q=="};
            ["0"; "127"];
            ["1"; [%true]];
            {"val"=[["-1"; "-1"]; ["0"; ""]]};
            "499";
            "40";
            "4101";
            "4202";
            "4103";
            "53375807";
            "4611669811199";
            "4611669811199999999";
            "9223339708799999999";
            "{}";
            "-2.125";
        ])")));
    CHECK_YQL_TYPE_AND_VALUE(row4, "column_b", firstColumnBType, row4BValue, yqlTypes);

    auto row4CValue = ConvertToNode(TYsonString(TStringBuf(R"({"val"=[]})")));
    CHECK_YQL_TYPE_AND_VALUE(row4, "column_c", firstColumnCType, row4CValue, yqlTypes);
    auto row4DValue = ConvertToNode(TYsonString(TStringBuf(R"({
        val = {
            x = {
                "$type" = "int64";
                "$value" = "49";
            }
        }
    })")));
    CHECK_YQL_TYPE_AND_VALUE(row4, "column_d", R"(["DataType"; "Yson"])", row4DValue, yqlTypes);

    // Here must come rows from the second table.

    ASSERT_EQ(row5->GetType(), ENodeType::Map);
    EXPECT_EQ(row5->AsMap()->GetChildCount(), 4);
    auto row5AValue = ConvertToNode(TYsonString(TStringBuf(R"(["0"; #])")));
    CHECK_YQL_TYPE_AND_VALUE(row5, "column_a", secondColumnAType, row5AValue, yqlTypes);
    auto row5BValue = ConvertToNode(TYsonString(TStringBuf(R"(#)")));
    CHECK_YQL_TYPE_AND_VALUE(row5, "column_b", secondColumnBType, row5BValue, yqlTypes);
    auto row5CValue = ConvertToNode(TYsonString(TStringBuf(R"(#)")));
    CHECK_YQL_TYPE_AND_VALUE(row5, "column_c", secondColumnCType, row5CValue, yqlTypes);
    auto row5DValue = ConvertToNode(TYsonString(TStringBuf(R"(["-49"])")));
    CHECK_YQL_TYPE_AND_VALUE(row5, "column_d", secondColumnDType, row5DValue, yqlTypes);

    ASSERT_EQ(row6->GetType(), ENodeType::Map);
    EXPECT_EQ(row6->AsMap()->GetChildCount(), 4);
    auto row6AValue = ConvertToNode(TYsonString(TStringBuf(R"([
        "1";
        {
            val = {
                z = {
                    "$type" = "string";
                    "$value" = "z";
                }
            }
        };
    ])")));
    CHECK_YQL_TYPE_AND_VALUE(row6, "column_a", secondColumnAType, row6AValue, yqlTypes);
    auto row6BValue = ConvertToNode(TYsonString(TStringBuf(R"(#)")));
    CHECK_YQL_TYPE_AND_VALUE(row6, "column_b", secondColumnBType, row6BValue, yqlTypes);
    auto row6CValue = ConvertToNode(TYsonString(TStringBuf(R"([#])")));
    CHECK_YQL_TYPE_AND_VALUE(row6, "column_c", secondColumnCType, row6CValue, yqlTypes);
    auto row6DValue = ConvertToNode(TYsonString(TStringBuf(R"(#)")));
    CHECK_YQL_TYPE_AND_VALUE(row6, "column_d", secondColumnDType, row6DValue, yqlTypes);
}

TEST_F(TWriterForWebJson, YqlValueFormat_Incomplete)
{
    Config_->ValueFormat = EWebJsonValueFormat::Yql;
    Config_->FieldWeightLimit = 215;
    Config_->StringWeightLimit = 10;

    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        {"column_a", StructLogicalType({
            {"field1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"list", ListLogicalType(
                VariantStructLogicalType({
                    {"a", DictLogicalType(
                        SimpleLogicalType(ESimpleLogicalValueType::Int64),
                        SimpleLogicalType(ESimpleLogicalValueType::String)),
                    },
                    {"b", SimpleLogicalType(ESimpleLogicalValueType::Any)},
                })),
            },
            {"field2", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"field3", MakeLogicalType(ESimpleLogicalValueType::Int64, false)},
        })},
        {"column_b", SimpleLogicalType(ESimpleLogicalValueType::Any)},
        {"column_c", MakeLogicalType(ESimpleLogicalValueType::String, false)},
    });

    auto yqlTypeA = ConvertToNode(TYsonString(TStringBuf(R"([
        "StructType";
        [
            [
                "field1";
                ["DataType"; "Int64"]
            ];
            [
                "list";
                [
                    "ListType";
                    [
                        "VariantType";
                        [
                            "StructType";
                            [
                                [
                                    "a";
                                    [
                                        "DictType";
                                        ["DataType"; "Int64"];
                                        ["DataType"; "String"]
                                    ]
                                ];
                                [
                                    "b";
                                    ["DataType"; "Yson"]
                                ];
                            ]
                        ]
                    ]
                ]
            ];
            [
                "field2";
                ["DataType"; "String"]
            ];
            [
                "field3";
                [
                    "OptionalType";
                    ["DataType"; "Int64"]
                ]
            ];
        ]
    ])")));

    auto yqlTypeB = ConvertToNode(TYsonString(TStringBuf(R"(["DataType"; "Yson"])")));
    auto yqlTypeC = ConvertToNode(TYsonString(TStringBuf(R"(["OptionalType"; ["DataType"; "String"]])")));
    {
        CreateStandardWriter({schema});
        bool written = Writer_->Write({
            MakeRow(NameTable_, {
                {
                    "column_a",
                    EValueType::Composite,
                    R"([
                        -1;
                        [
                            [
                                0;
                                [
                    [-2; "UTF:)" + TString("\xF0\x90\x8D\x88") + "\xF0\x90\x8D\x88" + R"("];
                    [2; "!UTF:)" + TString("\xFA\xFB\xFC\xFD\xFA\xFB\xFC\xFD") + R"("];
                                    [0; ""];
                                ]
                            ];
                            [
                                1;
                                "{kinda_long_key = kinda_even_longer_value}"
                            ];
                            [
                                0;
                                [
                                    [0; "One more quite long string"];
                                    [1; "One more quite long string"];
                                    [2; "One more quite long string"];
                                    [3; "One more quite long string"];
                                    [4; "One more quite long string"];
                                    [5; "One more quite long string"];
                                ]
                            ];
                            [
                                1;
                                "{kinda_long_key = kinda_even_longer_value}"
                            ];
                        ];
                        "I'm short";
                        424242238133245
                    ])"
                },
                {"column_b", EValueType::Any, "{kinda_long_key = kinda_even_longer_value}"},
                {"column_c", "One more quite long string"},
            }).Get(),
        });
        EXPECT_TRUE(written);
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 1);

    auto row = rows->AsList()->GetChildOrThrow(0);
    ASSERT_EQ(row->GetType(), ENodeType::Map);
    EXPECT_EQ(row->AsMap()->GetChildCount(), 3);

    auto rowAValue = ConvertToNode(TYsonString(R"([
        "-1";
        {
            "inc" = %true;
            "val" = [
                [
                    "0";
                    {
                        "val" = [
                            ["-2"; {"inc"=%true; "val"="UTF:)" + TString("\xF0\x90\x8D\x88") + R"("}];
                            ["2"; {"inc"=%true; "b64"=%true; "val"="IVVURjr6"}];
                            ["0"; ""];
                        ]
                    }
                ];
                [
                    "1";
                    {"val"=""; "inc"=%true}
                ];
                [
                    "0";
                    {
                        "inc" = %true;
                        "val" = [
                            ["0"; {"val"="One more q"; "inc"=%true}];
                            ["1"; {"val"="One more "; "inc"=%true}];
                        ];
                    }
                ];
            ];
        };
        {
            "val" = "";
            "inc" = %true;
        };
        ["424242238133245"];
    ])"));
    CHECK_YQL_TYPE_AND_VALUE(row, "column_a", yqlTypeA, rowAValue, yqlTypes);

    // Simple values are not truncated to |StringWeightLimit|
    auto rowBValue = ConvertToNode(TYsonString(TStringBuf(R"({
        val = {
            kinda_long_key = {
                "$type" = "string";
                "$value" = kinda_even_longer_value;
            }
        }
    })")));
    CHECK_YQL_TYPE_AND_VALUE(row, "column_b", yqlTypeB, rowBValue, yqlTypes);
    auto rowCValue = ConvertToNode(TYsonString(TStringBuf(R"(["One more quite long string"])")));
    CHECK_YQL_TYPE_AND_VALUE(row, "column_c", yqlTypeC, rowCValue, yqlTypes);
}


TEST_F(TWriterForWebJson, YqlValueFormat_Any)
{
    Config_->ValueFormat = EWebJsonValueFormat::Yql;

    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        {"column_a", MakeLogicalType(ESimpleLogicalValueType::Any, false)},
    });

    auto yqlTypeA = ConvertToNode(TYsonString(TStringBuf(R"([
        "OptionalType";
        ["DataType"; "Yson"]
    ])")));

    CreateStandardWriter({schema});
    {
        bool written = Writer_->Write({
            MakeRow(NameTable_, {{"column_a", EValueType::Any, "{x=y;z=2}"}}).Get(),
            MakeRow(NameTable_, {{"column_a", true}}).Get(),
            MakeRow(NameTable_, {{"column_a", -42}}).Get(),
            MakeRow(NameTable_, {{"column_a", 42u}}).Get(),
        });
        EXPECT_TRUE(written);
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 4);

    {
        auto row = rows->AsList()->GetChildOrThrow(0);
        ASSERT_EQ(row->GetType(), ENodeType::Map);
        auto rowAValue = ConvertToNode(TYsonString(TStringBuf(R"([
            {
                val = {
                        x = {
                            "$type" = "string";
                            "$value" = "y";
                        };
                        z = {
                            "$type" = "int64";
                            "$value" = "2";
                        }
                }
            }
        ])")));
        CHECK_YQL_TYPE_AND_VALUE(row, "column_a", yqlTypeA, rowAValue, yqlTypes);
    }
    {
        auto row = rows->AsList()->GetChildOrThrow(1);
        ASSERT_EQ(row->GetType(), ENodeType::Map);
        auto rowAValue = ConvertToNode(TYsonString(TStringBuf(R"([
            {
                val = {
                        "$type" = "boolean";
                        "$value" = "true";
                }
            }
        ])")));
        CHECK_YQL_TYPE_AND_VALUE(row, "column_a", yqlTypeA, rowAValue, yqlTypes);
    }
    {
        auto row = rows->AsList()->GetChildOrThrow(2);
        ASSERT_EQ(row->GetType(), ENodeType::Map);
        auto rowAValue = ConvertToNode(TYsonString(TStringBuf(R"([
            {
                val = {
                    "$type" = "int64";
                    "$value" = "-42";
                }
            }
        ])")));
        CHECK_YQL_TYPE_AND_VALUE(row, "column_a", yqlTypeA, rowAValue, yqlTypes);
    }
    {
        auto row = rows->AsList()->GetChildOrThrow(3);
        ASSERT_EQ(row->GetType(), ENodeType::Map);
        auto rowAValue = ConvertToNode(TYsonString(TStringBuf(R"([
            {
                val = {
                    "$type" = "uint64";
                    "$value" = "42";
                }
            }
        ])")));
        CHECK_YQL_TYPE_AND_VALUE(row, "column_a", yqlTypeA, rowAValue, yqlTypes);
    }
}

TEST_F(TWriterForWebJson, YqlValueFormat_CompositeNoSchema)
{
    Config_->ValueFormat = EWebJsonValueFormat::Yql;

    auto schema = New<TTableSchema>();

    auto yqlTypeA = ConvertToNode(TYsonString(TStringBuf(R"(["DataType"; "Yson"])")));

    CreateStandardWriter({schema});
    {
        bool written = Writer_->Write({
            MakeRow(NameTable_, {{"column_a", EValueType::Composite, "[1;2]"}}).Get(),
        });
        EXPECT_TRUE(written);
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 1);

    {
        auto row = rows->AsList()->GetChildOrThrow(0);
        ASSERT_EQ(row->GetType(), ENodeType::Map);
        auto rowAValue = ConvertToNode(TYsonString(TStringBuf(R"({
            "val" = [
                {
                    "$type" = "int64";
                    "$value" = "1";
                };
                {
                    "$type" = "int64";
                    "$value" = "2";
                }
            ]
        })")));
        CHECK_YQL_TYPE_AND_VALUE(row, "column_a", yqlTypeA, rowAValue, yqlTypes);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
