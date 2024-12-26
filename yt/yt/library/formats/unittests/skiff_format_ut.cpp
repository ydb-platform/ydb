#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>
#include "value_examples.h"
#include "row_helpers.h"
#include "yson_helpers.h"

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>
#include <yt/yt/library/formats/skiff_parser.h>
#include <yt/yt/library/formats/skiff_writer.h>
#include <yt/yt/library/formats/format.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <yt/yt/library/named_value/named_value.h>
#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/tree_visitor.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <util/stream/null.h>
#include <util/string/hex.h>

namespace NYT {

namespace {

using namespace NFormats;
using namespace NNamedValue;
using namespace NSkiff;
using namespace NSkiffExt;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TString ConvertToSkiffSchemaShortDebugString(INodePtr node)
{
    auto skiffFormatConfig = ConvertTo<TSkiffFormatConfigPtr>(std::move(node));
    auto skiffSchemas = ParseSkiffSchemas(skiffFormatConfig->SkiffSchemaRegistry, skiffFormatConfig->TableSkiffSchemas);
    TStringStream result;
    result << '{';
    for (const auto& schema : skiffSchemas) {
        result <<  GetShortDebugString(schema);
        result << ',';
    }
    result << '}';
    return result.Str();
}

////////////////////////////////////////////////////////////////////////////////

TString ConvertToYsonTextStringStable(const INodePtr& node)
{
    TStringStream out;
    TYsonWriter writer(&out, EYsonFormat::Text);
    VisitTree(node, &writer, true, TAttributeFilter());
    writer.Flush();
    return out.Str();
}

TTableSchemaPtr CreateSingleValueTableSchema(const TLogicalTypePtr& logicalType)
{
    std::vector<TColumnSchema> columns;
    if (logicalType) {
        columns.emplace_back("value", logicalType);

    }
    auto strict = static_cast<bool>(logicalType);
    return New<TTableSchema>(columns, strict);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffSchemaParse, TestAllowedTypes)
{
    EXPECT_EQ(
        "{uint64,}",

        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("uint64")
                        .EndMap()
                    .EndList()
                .EndMap()));

    EXPECT_EQ(
        "{string32,}",

        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("string32")
                        .EndMap()
                    .EndList()
                .EndMap()));

    EXPECT_EQ(
        "{variant8<string32;int64;>,}",

        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                    .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("variant8")
                            .Item("children")
                            .BeginList()
                                .Item()
                                .BeginMap()
                                    .Item("wire_type")
                                    .Value("string32")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("wire_type")
                                    .Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()));

    EXPECT_EQ(
        "{variant8<int64;string32;>,}",

        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("skiff_schema_registry")
                    .BeginMap()
                        .Item("item1")
                        .BeginMap()
                            .Item("wire_type")
                            .Value("int64")
                        .EndMap()
                        .Item("item2")
                        .BeginMap()
                            .Item("wire_type")
                            .Value("string32")
                        .EndMap()
                    .EndMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                    .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("variant8")
                            .Item("children")
                            .BeginList()
                                .Item().Value("$item1")
                                .Item().Value("$item2")
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()));
}

TEST(TSkiffSchemaParse, TestRecursiveTypesAreDisallowed)
{
    try {
        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("skiff_schema_registry")
                    .BeginMap()
                        .Item("item1")
                        .BeginMap()
                            .Item("wire_type")
                            .Value("variant8")
                            .Item("children")
                            .BeginList()
                                .Item().Value("$item1")
                            .EndList()
                        .EndMap()
                    .EndMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                        .Item().Value("$item1")
                    .EndList()
                .EndMap());
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("recursive types are forbidden"));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffSchemaDescription, TestDescriptionDerivation)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        })->SetName("Bar"),
    });

    auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
    EXPECT_EQ(std::ssize(tableDescriptionList), 1);
    EXPECT_EQ(tableDescriptionList[0].HasOtherColumns, false);
    EXPECT_EQ(tableDescriptionList[0].SparseFieldDescriptionList.empty(), true);

    auto denseFieldDescriptionList = tableDescriptionList[0].DenseFieldDescriptionList;
    EXPECT_EQ(std::ssize(denseFieldDescriptionList), 2);

    EXPECT_EQ(denseFieldDescriptionList[0].Name(), "Foo");
    EXPECT_EQ(denseFieldDescriptionList[0].ValidatedSimplify(), EWireType::Uint64);
}

TEST(TSkiffSchemaDescription, TestKeySwitchColumn)
{
    {
        auto schema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"),
        });

        auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        EXPECT_EQ(std::ssize(tableDescriptionList), 1);
        EXPECT_EQ(tableDescriptionList[0].KeySwitchFieldIndex, std::optional<size_t>(1));
    }
    {
        auto schema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("$key_switch"),
        });

        try {
            auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
            ADD_FAILURE();
        } catch (const std::exception& e) {
            EXPECT_THAT(e.what(), testing::HasSubstr("Column \"$key_switch\" has unexpected Skiff type"));
        }
    }
}

TEST(TSkiffSchemaDescription, TestDisallowEmptyNames)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Int64)->SetName(""),
    });

    try {
        CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("must have a name"));
    }
}

TEST(TSkiffSchemaDescription, TestWrongRowType)
{
    auto schema = CreateRepeatedVariant16Schema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
    });

    try {
        CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Invalid wire type for table row"));
    }
}

TEST(TSkiffSchemaDescription, TestOtherColumnsOk)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("$other_columns"),
    });

    auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
    ASSERT_EQ(std::ssize(tableDescriptionList), 1);
    ASSERT_EQ(tableDescriptionList[0].HasOtherColumns, true);
}

TEST(TSkiffSchemaDescription, TestOtherColumnsWrongType)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("$other_columns"),
    });

    try {
        CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Invalid wire type for column \"$other_columns\""));
    }
}

TEST(TSkiffSchemaDescription, TestOtherColumnsWrongPlace)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("$other_columns"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
    });

    try {
        CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Invalid placement of special column \"$other_columns\""));
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSkiffWriter(
    std::shared_ptr<TSkiffSchema> skiffSchema,
    TNameTablePtr nameTable,
    IOutputStream* outputStream,
    const std::vector<TTableSchemaPtr>& tableSchemaList,
    int keyColumnCount = 0,
    bool enableEndOfStream = false)
{
    auto controlAttributesConfig = New<TControlAttributesConfig>();
    controlAttributesConfig->EnableKeySwitch = (keyColumnCount > 0);
    controlAttributesConfig->EnableEndOfStream = enableEndOfStream;
    return CreateWriterForSkiff(
        {std::move(skiffSchema)},
        std::move(nameTable),
        tableSchemaList,
        NConcurrency::CreateAsyncAdapter(outputStream),
        false,
        controlAttributesConfig,
        keyColumnCount);
}

TString TableToSkiff(
    const TLogicalTypePtr& logicalType,
    const std::shared_ptr<TSkiffSchema>& typeSchema,
    const TNamedValue::TValue& value)
{
    auto schema = CreateSingleValueTableSchema(logicalType);
    auto skiffSchema = CreateTupleSchema({
        typeSchema->SetName("value")
    });

    auto nameTable = New<TNameTable>();

    TStringStream resultStream;
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {schema});

    Y_UNUSED(writer->Write({
        MakeRow(nameTable, {
            {"value", value}
        }).Get(),
    }));
    writer->Close()
        .Get()
        .ThrowOnError();

    auto result = resultStream.Str();
    if (!TStringBuf(result).StartsWith(TString(2, '\0'))) {
        THROW_ERROR_EXCEPTION("Expected skiff value to start with \\x00\\x00, but prefix is %Qv",
                EscapeC(result.substr(0, 2)));
    }

    return result.substr(2);
}

TNamedValue::TValue SkiffToTable(
    const TLogicalTypePtr& logicalType,
    const std::shared_ptr<TSkiffSchema>& typeSchema,
    const TString& skiffValue)
{
    auto schema = CreateSingleValueTableSchema(logicalType);
    auto skiffSchema = CreateTupleSchema({
        typeSchema->SetName("value")
    });
    auto nameTable = New<TNameTable>();

    TCollectingValueConsumer rowCollector(schema);
    auto parser = CreateParserForSkiff(skiffSchema, &rowCollector);
    parser->Read(TString(2, 0));
    parser->Read(skiffValue);
    parser->Finish();

    if (rowCollector.Size() != 1) {
        THROW_ERROR_EXCEPTION("Expected 1 row collected, actual %v",
            rowCollector.Size());
    }
    auto value = rowCollector.GetRowValue(0, "value");
    return TNamedValue::ExtractValue(value);
}

#define CHECK_BIDIRECTIONAL_CONVERSION(logicalTypeArg, skiffSchemaArg, tableValueArg, hexSkiffArg) \
    do {                                                                                           \
        try {                                                                                      \
            TLogicalTypePtr logicalType = (logicalTypeArg);                                        \
            std::shared_ptr<TSkiffSchema> skiffSchema = (skiffSchemaArg);                          \
            TNamedValue::TValue tableValue = (tableValueArg);                                      \
            TString hexSkiff = (hexSkiffArg);                                                      \
            auto nameTable = New<TNameTable>();                                                    \
            auto actualSkiff = TableToSkiff(logicalType, skiffSchema, tableValue);                 \
            EXPECT_EQ(HexEncode(actualSkiff), hexSkiff);                                           \
            auto actualValue = SkiffToTable(logicalType, skiffSchema, HexDecode(hexSkiff));        \
            EXPECT_EQ(actualValue, tableValue);                                                    \
        } catch (const std::exception& ex) {                                                       \
            ADD_FAILURE() << "unexpected exception: " << ex.what();                                \
        } \
    } while (0)

////////////////////////////////////////////////////////////////////////////////

void TestAllWireTypes(bool useSchema)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double_1"),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double_2"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("boolean"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        CreateSimpleTypeSchema(EWireType::Nothing)->SetName("null"),

        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("opt_int64"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        })->SetName("opt_uint64"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Double),
        })->SetName("opt_double_1"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Double),
        })->SetName("opt_double_2"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Boolean),
        })->SetName("opt_boolean"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::String32),
        })->SetName("opt_string32"),
    });
    std::vector<TTableSchemaPtr> tableSchemas;
    if (useSchema) {
        tableSchemas.push_back(New<TTableSchema>(std::vector{
            TColumnSchema("int64", EValueType::Int64),
            TColumnSchema("uint64", EValueType::Uint64),
            TColumnSchema("double_1", EValueType::Double),
            TColumnSchema("double_2", ESimpleLogicalValueType::Float),
            TColumnSchema("boolean", EValueType::Boolean),
            TColumnSchema("string32", EValueType::String),
            TColumnSchema("null", EValueType::Null),
            TColumnSchema("opt_int64", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
            TColumnSchema("opt_uint64", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64))),
            TColumnSchema("opt_double_1", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Double))),
            TColumnSchema("opt_double_2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Float))),
            TColumnSchema("opt_boolean", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean))),
            TColumnSchema("opt_string32", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
        }));
    } else {
        tableSchemas.push_back(New<TTableSchema>());
    }
    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, tableSchemas);

        auto isWriterReady = writer->Write({
            MakeRow(nameTable, {
                {"int64", -1},
                {"uint64", 2u},
                {"double_1", 3.0},
                {"double_2", 3.0},
                {"boolean", true},
                {"string32", "four"},
                {"null", nullptr},

                {"opt_int64", -5},
                {"opt_uint64", 6u},
                {"opt_double_1", 7.0},
                {"opt_double_2", 7.0},
                {"opt_boolean", false},
                {"opt_string32", "eight"},
                {TString(TableIndexColumnName), 0},
            }).Get(),
        });
        if (!isWriterReady) {
            writer->GetReadyEvent().Get().ThrowOnError();
        }

        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {"int64", -9},
                {"uint64", 10u},
                {"double_1", 11.0},
                {"double_2", 11.0},
                {"boolean", false},
                {"string32", "twelve"},
                {"null", nullptr},

                {"opt_int64", nullptr},
                {"opt_uint64", nullptr},
                {"opt_double_1", nullptr},
                {"opt_double_2", nullptr},
                {"opt_boolean", nullptr},
                {"opt_string32", nullptr},
                {TString(TableIndexColumnName), 0},
            }).Get()
        }));

        writer->Close()
            .Get()
            .ThrowOnError();
    }

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -1);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 2u);
    // double_1
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 3.0);
    // double_2
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 3.0);
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), true);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "four");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -5);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 6u);

    // double_1
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 7.0);

    // double_2
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 7.0);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "eight");

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -9);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 10u);
    // double_1
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 11.0);
    // double_2
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 11.0);
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "twelve");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    // double_1
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    // double_2
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestAllWireTypesNoSchema)
{
    TestAllWireTypes(false);
}

TEST(TSkiffWriter, TestAllWireTypesWithSchema)
{
    TestAllWireTypes(true);
}

class TSkiffYsonWireTypeP
    : public ::testing::TestWithParam<std::tuple<
        TLogicalTypePtr,
        TNamedValue::TValue,
        TString
    >>
{
public:
    static std::vector<ParamType> GetCases()
    {
        using namespace NLogicalTypeShortcuts;
        std::vector<ParamType> result;

        for (const auto& example : GetPrimitiveValueExamples()) {
            result.emplace_back(example.LogicalType, example.Value, example.PrettyYson);
            result.emplace_back(nullptr, example.Value, example.PrettyYson);
        }

        for (const auto type : TEnumTraits<ESimpleLogicalValueType>::GetDomainValues()) {
            auto logicalType = OptionalLogicalType(SimpleLogicalType(type));
            if (IsV3Composite(logicalType)) {
                // Optional<Null> is not v1 type
                continue;
            }
            result.emplace_back(logicalType, nullptr, "#");
        }
        return result;
    }

    static const std::vector<ParamType> Cases;
};

const std::vector<TSkiffYsonWireTypeP::ParamType> TSkiffYsonWireTypeP::Cases = TSkiffYsonWireTypeP::GetCases();

INSTANTIATE_TEST_SUITE_P(
    Cases,
    TSkiffYsonWireTypeP,
    ::testing::ValuesIn(TSkiffYsonWireTypeP::Cases));

TEST_P(TSkiffYsonWireTypeP, Test)
{
    const auto& [logicalType, value, expectedYson] = GetParam();
    TTableSchemaPtr tableSchema;
    if (logicalType) {
        tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("column", logicalType),
        });
    } else {
        tableSchema = New<TTableSchema>();
    }
    auto skiffTableSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("column"),
    });
    auto nameTable = New<TNameTable>();
    TStringStream actualSkiffDataStream;
    auto writer = CreateSkiffWriter(skiffTableSchema, nameTable, &actualSkiffDataStream, {tableSchema});
    Y_UNUSED(writer->Write({
        MakeRow(nameTable, {{"column", value}})
    }));
    writer->Close()
        .Get()
        .ThrowOnError();

    auto actualSkiffData = actualSkiffDataStream.Str();
    {
        TMemoryInput in(actualSkiffData);
        TCheckedSkiffParser parser(CreateVariant16Schema({skiffTableSchema}), &in);
        EXPECT_EQ(parser.ParseVariant16Tag(), 0);
        auto actualYson = parser.ParseYson32();
        parser.ValidateFinished();

        EXPECT_EQ(CanonizeYson(actualYson), CanonizeYson(expectedYson));
    }

    TCollectingValueConsumer rowCollector(nameTable);
    auto parser = CreateParserForSkiff(skiffTableSchema, tableSchema, &rowCollector);
    parser->Read(actualSkiffDataStream.Str());
    parser->Finish();
    auto actualValue = rowCollector.GetRowValue(0, "column");
    EXPECT_EQ(actualValue, TNamedValue("column", value).ToUnversionedValue(nameTable));
}

TEST(TSkiffWriter, TestYsonWireType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson32"),

        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Yson32),
        })->SetName("opt_yson32"),
    });
    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

        auto write = [&] (TUnversionedRow row) {
            if (!writer->Write({row})) {
                writer->GetReadyEvent().Get().ThrowOnError();
            }
        };

        // Row 0 (Null)
        write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},

                {"yson32", nullptr},
                {"opt_yson32", nullptr},
            }).Get(),
        });

        // Row 1 (Int64)
        write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},

                {"yson32", -5},
                {"opt_yson32", -6},
            }).Get(),
        });

        // Row 2 (Uint64)
        write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},

                {"yson32", 42u},
                {"opt_yson32", 43u},
            }).Get(),
        });

        // Row 3 ((Double)
        write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},

                {"yson32", 2.7182818},
                {"opt_yson32", 3.1415926},
            }).Get(),
        });

        // Row 4 ((Boolean)
        write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},

                {"yson32", true},
                {"opt_yson32", false},
            }).Get(),
        });

        // Row 5 ((String)
        write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},

                {"yson32", "Yin"},
                {"opt_yson32", "Yang"},
            }).Get(),
        });

        // Row 6 ((Any)
        write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},

                {"yson32", EValueType::Any, "{foo=bar;}"},
                {"opt_yson32", EValueType::Any, "{bar=baz;}"},
            }).Get(),
        });

        // Row 7 ((missing optional values)
        write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},
            }).Get(),
        });

        writer->Close()
            .Get()
            .ThrowOnError();
    }

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    auto parseYson = [] (TCheckedSkiffParser* parser) {
        auto yson = TString{parser->ParseYson32()};
        return ConvertToNode(TYsonString(yson));
    };

    // Row 0 (Null)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->GetType(), ENodeType::Entity);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // Row 1 (Int64)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsInt64()->GetValue(), -5);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsInt64()->GetValue(), -6);

    // Row 2 (Uint64)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsUint64()->GetValue(), 42u);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsUint64()->GetValue(), 43u);

    // Row 3 (Double)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsDouble()->GetValue(), 2.7182818);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsDouble()->GetValue(), 3.1415926);

    // Row 4 (Boolean)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsBoolean()->GetValue(), true);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsBoolean()->GetValue(), false);

    // Row 5 (String)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsString()->GetValue(), "Yin");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsString()->GetValue(), "Yang");

    // Row 6 (Any)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsMap()->GetChildOrThrow("foo")->AsString()->GetValue(), "bar");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsMap()->GetChildOrThrow("bar")->AsString()->GetValue(), "baz");

    // Row 7 (Null)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->GetType(), ENodeType::Entity);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

class TSkiffFormatSmallIntP
: public ::testing::TestWithParam<std::tuple<
    std::shared_ptr<TSkiffSchema>,
    TLogicalTypePtr,
    TNamedValue::TValue,
    TString
>>
{
public:
    static std::vector<ParamType> GetCases()
    {
        using namespace NLogicalTypeShortcuts;

        std::vector<ParamType> result;

        auto addSimpleCase = [&result] (
            EWireType wireType,
            const TLogicalTypePtr& logicalType,
            auto value,
            TStringBuf skiffValue)
        {
            auto simpleSkiffSchema = CreateSimpleTypeSchema(wireType);
            auto simpleSkiffData = TString(2, 0) + skiffValue;
            result.emplace_back(simpleSkiffSchema, logicalType, value, simpleSkiffData);
        };

        auto addListCase = [&result] (
            EWireType wireType,
            const TLogicalTypePtr& logicalType,
            auto value,
            TStringBuf skiffValue)
        {
            auto listSkiffSchema = CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(wireType)});
            auto listSkiffData = TString(3, 0) + skiffValue + TString(1, '\xff');
            auto listValue = TNamedValue::TValue{
                TNamedValue::TComposite{
                    BuildYsonStringFluently()
                        .BeginList()
                            .Item().Value(value)
                        .EndList().ToString()
                }
            };
            result.emplace_back(listSkiffSchema, List(logicalType), listValue, listSkiffData);
        };

        auto addSimpleAndListCases = [&] (
            EWireType wireType,
            const TLogicalTypePtr& logicalType,
            auto value,
            TStringBuf skiffValue)
        {
            addSimpleCase(wireType, logicalType, value, skiffValue);
            addListCase(wireType, logicalType, value, skiffValue);
        };

        auto addMultiCase = [&] (EWireType wireType, auto value, TStringBuf skiffValue) {
            auto add = [&] (const TLogicalTypePtr& logicalType) {
                addSimpleAndListCases(wireType, logicalType, value, skiffValue);
            };
            addSimpleCase(wireType, Yson(), value, skiffValue);

            using T = std::decay_t<decltype(value)>;
            static_assert(std::is_integral_v<T>);
            if constexpr (std::is_signed_v<T>) {
                if (std::numeric_limits<i8>::min() <= value && value <= std::numeric_limits<i8>::max()) {
                    add(Int8());
                }
                if (std::numeric_limits<i16>::min() <= value && value <= std::numeric_limits<i16>::max()) {
                    add(Int16());
                }
                if (std::numeric_limits<i32>::min() <= value && value <= std::numeric_limits<i32>::max()) {
                    add(Int32());
                }
                add(Int64());
            } else {
                if (value <= std::numeric_limits<ui8>::max()) {
                    add(Uint8());
                }
                if (value <= std::numeric_limits<ui16>::max()) {
                    add(Uint16());
                }
                if (value <= std::numeric_limits<ui32>::max()) {
                    add(Uint32());
                }
                add(Uint64());
            }
        };
        addMultiCase(EWireType::Int8, 0, TStringBuf("\x00"sv));
        addMultiCase(EWireType::Int8, 42, TStringBuf("*"));
        addMultiCase(EWireType::Int8, -42, TStringBuf("\xd6"sv));
        addMultiCase(EWireType::Int8, 127, TStringBuf("\x7f"sv));
        addMultiCase(EWireType::Int8, -128, TStringBuf("\x80"sv));

        addMultiCase(EWireType::Int16, 0, TStringBuf("\x00\x00"sv));
        addMultiCase(EWireType::Int16, 42, TStringBuf("\x2a\x00"sv));
        addMultiCase(EWireType::Int16, -42, TStringBuf("\xd6\xff"sv));
        addMultiCase(EWireType::Int16, 0x7fff, TStringBuf("\xff\x7f"sv));
        addMultiCase(EWireType::Int16, -0x8000, TStringBuf("\x00\x80"sv));

        addMultiCase(EWireType::Int32, 0, TStringBuf("\x00\x00\x00\x00"sv));
        addMultiCase(EWireType::Int32, 42, TStringBuf("\x2a\x00\x00\x00"sv));
        addMultiCase(EWireType::Int32, -42, TStringBuf("\xd6\xff\xff\xff"sv));
        addMultiCase(EWireType::Int32, 0x7fffffff, TStringBuf("\xff\xff\xff\x7f"sv));
        addMultiCase(EWireType::Int32, -0x80000000l, TStringBuf("\x00\x00\x00\x80"sv));

        addMultiCase(EWireType::Uint8, 0ull, TStringBuf("\x00"sv));
        addMultiCase(EWireType::Uint8, 42ull, TStringBuf("*"));
        addMultiCase(EWireType::Uint8, 255ull, TStringBuf("\xff"sv));

        addMultiCase(EWireType::Uint16, 0ull, TStringBuf("\x00\x00"sv));
        addMultiCase(EWireType::Uint16, 42ull, TStringBuf("\x2a\x00"sv));
        addMultiCase(EWireType::Uint16, 0xFFFFull, TStringBuf("\xff\xff"sv));

        addMultiCase(EWireType::Uint32, 0ull, TStringBuf("\x00\x00\x00\x00"sv));
        addMultiCase(EWireType::Uint32, 42ull, TStringBuf("\x2a\x00\x00\x00"sv));
        addMultiCase(EWireType::Uint32, 0xFFFFFFFFull, TStringBuf("\xff\xff\xff\xff"sv));

        addSimpleAndListCases(EWireType::Uint16, Date(), 0ull, TStringBuf("\x00\x00"sv));
        addSimpleAndListCases(EWireType::Uint16, Date(), 42ull, TStringBuf("\x2a\x00"sv));
        addSimpleAndListCases(EWireType::Uint16, Date(), DateUpperBound - 1, TStringBuf("\x08\xc2"sv));

        addSimpleAndListCases(EWireType::Uint32, Datetime(), 0ull, TStringBuf("\x00\x00\x00\x00"sv));
        addSimpleAndListCases(EWireType::Uint32, Datetime(), 42ull, TStringBuf("\x2a\x00\x00\x00"sv));
        addSimpleAndListCases(EWireType::Uint32, Datetime(), DatetimeUpperBound - 1, TStringBuf("\x7f\xdd\xce\xff"sv));

        addSimpleAndListCases(EWireType::Int64, Date32(), 0ll, TStringBuf("\x00\x00\x00\x00\x00\x00\x00\x00"sv));
        addSimpleAndListCases(EWireType::Int64, Date32(), Date32UpperBound - 1, TStringBuf("\x3f\x73\x2e\x03\x00\x00\x00\x00"sv));
        addSimpleAndListCases(EWireType::Int64, Date32(), Date32LowerBound, TStringBuf("\xbf\x8c\xd1\xfc\xff\xff\xff\xff"sv));

        addSimpleAndListCases(EWireType::Int32, Date32(), 0ll, TStringBuf("\x00\x00\x00\x00"sv));
        addSimpleAndListCases(EWireType::Int32, Date32(), Date32UpperBound - 1, TStringBuf("\x3f\x73\x2e\x03"sv));
        addSimpleAndListCases(EWireType::Int32, Date32(), Date32LowerBound, TStringBuf("\xbf\x8c\xd1\xfc"sv));

        addSimpleAndListCases(EWireType::Int64, Datetime64(), 0ll, TStringBuf("\x00\x00\x00\x00\x00\x00\x00\x00"sv));
        addSimpleAndListCases(EWireType::Int64, Datetime64(), Datetime64UpperBound - 1, TStringBuf("\xff\xdf\xf0\xbc\x31\x04\x00\x00"sv));
        addSimpleAndListCases(EWireType::Int64, Datetime64(), Datetime64LowerBound, TStringBuf("\x80\xce\x0d\x43\xce\xfb\xff\xff"sv));

        addSimpleAndListCases(EWireType::Int64, Timestamp64(), 0ll, TStringBuf("\x00\x00\x00\x00\x00\x00\x00\x00"sv));
        addSimpleAndListCases(EWireType::Int64, Timestamp64(), Timestamp64UpperBound - 1, TStringBuf("\xff\xff\xf7\x75\x42\xf1\xff\x3f"sv));
        addSimpleAndListCases(EWireType::Int64, Timestamp64(), Timestamp64LowerBound, TStringBuf("\x00\xa0\x30\x6c\xa9\x0e\x00\xc0"sv));

        addSimpleAndListCases(EWireType::Int64, Interval64(), 0ll, TStringBuf("\x00\x00\x00\x00\x00\x00\x00\x00"sv));
        addSimpleAndListCases(EWireType::Int64, Interval64(), Interval64UpperBound - 1, TStringBuf("\x00\x60\xc7\x09\x99\xe2\xff\x7f"sv));
        addSimpleAndListCases(EWireType::Int64, Interval64(), -Interval64UpperBound + 1, TStringBuf("\x00\xa0\x38\xf6\x66\x1d\x00\x80"sv));

        return result;
    }

    static const std::vector<ParamType> Cases;
};

const std::vector<TSkiffFormatSmallIntP::ParamType> TSkiffFormatSmallIntP::Cases = TSkiffFormatSmallIntP::GetCases();

INSTANTIATE_TEST_SUITE_P(
    Cases,
    TSkiffFormatSmallIntP,
    ::testing::ValuesIn(TSkiffFormatSmallIntP::Cases));

TEST_P(TSkiffFormatSmallIntP, Test)
{
    const auto& [skiffValueSchema, logicalType, value, expectedSkiffData] = GetParam();

    const auto nameTable = New<TNameTable>();

    TStringStream actualSkiffData;
    auto skiffTableSchema = CreateTupleSchema({
        skiffValueSchema->SetName("column")
    });
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("column", logicalType),
    });
    auto writer = CreateSkiffWriter(skiffTableSchema, nameTable, &actualSkiffData, {tableSchema});
    Y_UNUSED(writer->Write({
        MakeRow(nameTable, {{"column", value}})
    }));
    writer->Close()
        .Get()
        .ThrowOnError();
    EXPECT_EQ(actualSkiffData.Str(), expectedSkiffData);

    TCollectingValueConsumer rowCollector(nameTable);
    auto parser = CreateParserForSkiff(skiffTableSchema, tableSchema, &rowCollector);
    parser->Read(expectedSkiffData);
    parser->Finish();
    auto actualValue = rowCollector.GetRowValue(0, "column");

    EXPECT_EQ(actualValue, TNamedValue("common", value).ToUnversionedValue(nameTable));
}

TEST(TSkiffWriter, TestBadSmallIntegers)
{
    using namespace NLogicalTypeShortcuts;
    auto writeSkiffValue = [] (
        std::shared_ptr<TSkiffSchema>&& typeSchema,
        TLogicalTypePtr logicalType,
        TNamedValue::TValue value)
    {
        TStringStream result;
        auto skiffSchema = CreateTupleSchema({
            typeSchema->SetName("column")
        });
        auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("column", std::move(logicalType)),
        });
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &result, {tableSchema});
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {{"column", std::move(value)}})
        }));
        writer->Close()
            .Get()
            .ThrowOnError();
        return result.Str();
    };

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int8), Int64(), 128),
        "is out of range for possible values");
    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int8), Int64(), -129),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int16), Int64(), 0x8000),
        "is out of range for possible values");
    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int16), Int64(), -0x8001),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int32), Int64(), 0x80000000ll),
        "is out of range for possible values");
    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int32), Int64(), -0x80000001ll),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Uint8), Uint64(), 256ull),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Uint16), Uint64(), 0x1FFFFull),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Uint32), Uint64(), 0x100000000ull),
        "is out of range for possible values");
}

class TSkiffFormatUuidTestP : public ::testing::TestWithParam<std::tuple<
    TNameTablePtr,
    TTableSchemaPtr,
    std::shared_ptr<TSkiffSchema>,
    std::vector<TUnversionedOwningRow>,
    TString
>>
{
public:
    static std::vector<ParamType> GetCases()
    {
        using namespace NLogicalTypeShortcuts;

        auto nameTable = New<TNameTable>();
        const auto stringUuidValue = TStringBuf("\xee\x1f\x37\x70" "\xb9\x93\x64\xb5" "\xe4\xdf\xe9\x03" "\x67\x5c\x30\x62");
        const auto uint128UuidValue = TStringBuf("\x62\x30\x5c\x67" "\x03\xe9\xdf\xe4" "\xb5\x64\x93\xb9" "\x70\x37\x1f\xee");

        const auto requiredTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{TColumnSchema("uuid", Uuid())});
        const auto optionalTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{TColumnSchema("uuid", Optional(Uuid()))});

        const auto optionalUint128SkiffSchema = CreateTupleSchema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Uint128),
            })->SetName("uuid"),
        });

        const auto requiredUint128SkiffSchema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Uint128)->SetName("uuid"),
        });

        const auto optionalStringSkiffSchema = CreateTupleSchema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::String32),
            })->SetName("uuid"),
        });

        const auto requiredStringSkiffSchema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("uuid"),
        });

        std::vector<ParamType> result;

        result.emplace_back(
            nameTable,
            requiredTableSchema,
            requiredUint128SkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", stringUuidValue}}),
            },
            TString(2, '\0') + uint128UuidValue);

        result.emplace_back(
            nameTable,
            optionalTableSchema,
            requiredUint128SkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", stringUuidValue}}),
            },
            TString(2, '\0') + uint128UuidValue);

        result.emplace_back(
            nameTable,
            requiredTableSchema,
            optionalUint128SkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", stringUuidValue}}),
            },
            TString(2, '\0') + "\1" + uint128UuidValue);

        result.emplace_back(
            nameTable,
            optionalTableSchema,
            optionalUint128SkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", stringUuidValue}}),
            },
            TString(2, '\0') + "\1" + uint128UuidValue);

        const TString uuidLen = TString(TStringBuf("\x10\x00\x00\x00"sv));

        result.emplace_back(
            nameTable,
            requiredTableSchema,
            requiredStringSkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", stringUuidValue}}),
            },
            TString(2, '\0') + uuidLen + stringUuidValue);

        result.emplace_back(
            nameTable,
            optionalTableSchema,
            requiredStringSkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", stringUuidValue}}),
            },
            TString(2, '\0') + uuidLen + stringUuidValue);

        result.emplace_back(
            nameTable,
            requiredTableSchema,
            optionalStringSkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", stringUuidValue}}),
            },
            TString(2, '\0') + "\1" + uuidLen + stringUuidValue);

        result.emplace_back(
            nameTable,
            optionalTableSchema,
            optionalStringSkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", stringUuidValue}}),
            },
            TString(2, '\0') + "\1" + uuidLen + stringUuidValue);

        return result;
    }

    static const std::vector<ParamType> Cases;
};

const std::vector<TSkiffFormatUuidTestP::ParamType> TSkiffFormatUuidTestP::Cases = TSkiffFormatUuidTestP::GetCases();

INSTANTIATE_TEST_SUITE_P(
    Cases,
    TSkiffFormatUuidTestP,
    ::testing::ValuesIn(TSkiffFormatUuidTestP::Cases));

TEST_P(TSkiffFormatUuidTestP, Test)
{
    const auto& [nameTable, tableSchema, skiffSchema, rows, skiffString] = GetParam();

    TStringStream result;
    std::vector<TUnversionedRow> nonOwningRows;
    for (const auto& row : rows) {
        nonOwningRows.emplace_back(row);
    }
    auto skiffWriter = CreateSkiffWriter(skiffSchema, nameTable, &result, {tableSchema});
    Y_UNUSED(skiffWriter->Write(TRange(nonOwningRows)));
    skiffWriter->Close().Get().ThrowOnError();
    ASSERT_EQ(result.Str(), skiffString);

    TCollectingValueConsumer rowCollector(nameTable);
    auto requiredParser = CreateParserForSkiff(skiffSchema, tableSchema, &rowCollector);
    requiredParser->Read(result.Str());
    requiredParser->Finish();
    ASSERT_EQ(rowCollector.GetRowList(), rows);
}

TEST(TSkiffFormatUuidTest, TestError)
{
    using namespace NLogicalTypeShortcuts;

    auto nameTable = New<TNameTable>();
    auto tableSchema = New<TTableSchema>(
        std::vector<TColumnSchema>{TColumnSchema("uuid", Optional(Uuid()))});

    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint128)->SetName("uuid"),
    });

    TStringStream result;
    auto skiffWriter = CreateSkiffWriter(skiffSchema, nameTable, &result, {tableSchema});
    Y_UNUSED(skiffWriter->Write({
        MakeRow(nameTable, {{"uuid", nullptr}}),
    }));
    EXPECT_THROW_WITH_SUBSTRING(skiffWriter->Close().Get().ThrowOnError(),
        "Unexpected type");

}

class TSkiffWriterSingular
    : public ::testing::Test
    , public ::testing::WithParamInterface<ESimpleLogicalValueType>
{};

INSTANTIATE_TEST_SUITE_P(
    Singular,
    TSkiffWriterSingular,
    ::testing::Values(ESimpleLogicalValueType::Null, ESimpleLogicalValueType::Void));

TEST_P(TSkiffWriterSingular, TestOptionalSingular)
{
    const auto singularType = GetParam();

    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Nothing),
        })->SetName("opt_null"),
    });

    auto nameTable = New<TNameTable>();
    const std::vector<TTableSchemaPtr> tableSchemas = {
        New<TTableSchema>(std::vector{
            TColumnSchema("opt_null", OptionalLogicalType(SimpleLogicalType(singularType))),
        }),
    };

    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, tableSchemas);
        // Row 0
        auto isReady = writer->Write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},
                {"opt_null", nullptr},
            }).Get(),
        });
        if (!isReady) {
            writer->GetReadyEvent().Get().ThrowOnError();
        }
        // Row 1
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},
                {"opt_null", EValueType::Composite, "[#]"},
            }).Get(),
        }));
        writer->Close()
            .Get()
            .ThrowOnError();
    }

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);

    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestRearrange)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("number"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::String32),
        })->SetName("eng"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::String32),
        })->SetName("rus"),
    });
    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

        auto write = [&] (TUnversionedRow row) {
            if (!writer->Write({row})) {
                writer->GetReadyEvent().Get().ThrowOnError();
            }
        };

        write(MakeRow(nameTable, {
            {TString(TableIndexColumnName), 0},
            {"number", 1},
            {"eng", "one"},
            {"rus", nullptr},
        }).Get());

        write(MakeRow(nameTable, {
            {TString(TableIndexColumnName), 0},
            {"eng", nullptr},
            {"number", 2},
            {"rus", "dva"},
        }).Get());

        write(MakeRow(nameTable, {
            {TString(TableIndexColumnName), 0},
            {"rus", "tri"},
            {"eng", "three"},
            {"number", 3},
        }).Get());

        writer->Close()
            .Get()
            .ThrowOnError();
    }

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 2);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "dva");

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 3);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "three");
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "tri");

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestMissingRequiredField)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("number"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("eng"),
    });
    auto nameTable = New<TNameTable>();
    TString result;
    try {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},
                {"number", 1},
            }).Get()
        }));
        writer->Close()
            .Get()
            .ThrowOnError();
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Unexpected type of \"eng\" column"));
    }
}

TEST(TSkiffWriter, TestSparse)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        })->SetName("$sparse_columns"),
    });

    auto nameTable = New<TNameTable>();
    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

    auto write = [&] (TUnversionedRow row) {
        if (!writer->Write({row})) {
            writer->GetReadyEvent().Get().ThrowOnError();
        }
    };

    write(MakeRow(nameTable, {
        {TString(TableIndexColumnName), 0},
        {"int64", -1},
        {"string32", "minus one"},
    }).Get());

    write(MakeRow(nameTable, {
        {TString(TableIndexColumnName), 0},
        {"string32", "minus five"},
        {"int64", -5},
    }).Get());

    write(MakeRow(nameTable, {
        {TString(TableIndexColumnName), 0},
        {"uint64", 42u},
    }).Get());

    write(MakeRow(nameTable, {
        {TString(TableIndexColumnName), 0},
        {"int64", -8},
        {"uint64", nullptr},
        {"string32", nullptr},
    }).Get());

    write(MakeRow(nameTable, {
        {TString(TableIndexColumnName), 0},
    }).Get());

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -1);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 2);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "minus one");
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 2);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "minus five");
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -5);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 42u);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // row 3
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -8);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // row 4
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestMissingFields)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
    });

    try {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},
                {"unknown_column", "four"},
            }).Get(),
        }));
        writer->Close()
            .Get()
            .ThrowOnError();
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Column \"unknown_column\" is not described by Skiff schema"));
    }

    try {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto unknownColumnId = nameTable->RegisterName("unknown_column");
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{New<TTableSchema>()});

        ASSERT_TRUE(unknownColumnId < nameTable->GetId("value"));

        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},
                {"unknown_column", "four"},
            }).Get(),
        }));
        writer->Close()
            .Get()
            .ThrowOnError();
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Column \"unknown_column\" is not described by Skiff schema"));
    }
}

TEST(TSkiffWriter, TestOtherColumns)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64)
        })->SetName("int64_column"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("$other_columns"),
    });

    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    nameTable->RegisterName("string_column");
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

    auto write = [&] (TUnversionedRow row) {
        if (!writer->Write({row})) {
            writer->GetReadyEvent().Get().ThrowOnError();
        }
    };

    // Row 0.
    write(MakeRow(nameTable, {
        {TString(TableIndexColumnName), 0},
        {"string_column", "foo"},
    }).Get());

    // Row 1.
    write(MakeRow(nameTable, {
        {TString(TableIndexColumnName), 0},
        {"int64_column", 42},
    }).Get());

    // Row 2.
    write(MakeRow(nameTable, {
        {TString(TableIndexColumnName), 0},
        {"other_string_column", "bar"},
    }).Get());
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    auto parseYson = [] (TCheckedSkiffParser* parser) {
        auto yson = TString{parser->ParseYson32()};
        return ConvertToYsonTextStringStable(ConvertToNode(TYsonString(yson)));
    };

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser), "{\"string_column\"=\"foo\";}");

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 42);
    ASSERT_EQ(parseYson(&checkedSkiffParser), "{}");

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser), "{\"other_string_column\"=\"bar\";}");

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestKeySwitch)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"),
    });

    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()}, 1);

    auto write = [&] (TUnversionedRow row) {
        if (!writer->Write({row})) {
            writer->GetReadyEvent().Get().ThrowOnError();
        }
    };

    // Row 0.
    write(MakeRow(nameTable, {
        {"value", "one"},
        {TString(TableIndexColumnName), 0},
    }).Get());
    // Row 1.
    write(MakeRow(nameTable, {
        {"value", "one"},
        {TString(TableIndexColumnName), 0},
    }).Get());
    // Row 2.
    write(MakeRow(nameTable, {
        {"value", "two"},
        {TString(TableIndexColumnName), 0},
    }).Get());
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    TString buf;

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "two");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), true);

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestEndOfStream)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
    });

    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()}, 1, true);

    auto write = [&] (TUnversionedRow row) {
        if (!writer->Write({row})) {
            writer->GetReadyEvent().Get().ThrowOnError();
        }
    };

    // Row 0.
    write(MakeRow(nameTable, {
        {"value", "zero"},
        {TString(TableIndexColumnName), 0},
    }).Get());
    // Row 1.
    write(MakeRow(nameTable, {
        {"value", "one"},
        {TString(TableIndexColumnName), 0},
    }).Get());
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    TString buf;

    // Row 0.
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "zero");

    // Row 1.
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");

    // End of stream.
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0xffff);

    // The End.
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestRowRangeIndex)
{
    const auto rowAndRangeIndex = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("$range_index"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("$row_index"),
    });

    struct TRow {
        int TableIndex;
        std::optional<int> RangeIndex;
        std::optional<int> RowIndex;
    };
    auto generateUnversionedRow = [] (const TRow& row, const TNameTablePtr& nameTable) {
        std::vector<TNamedValue> values = {
            {TString(TableIndexColumnName), row.TableIndex},
        };
        if (row.RangeIndex) {
            values.emplace_back(TString(RangeIndexColumnName), *row.RangeIndex);
        }
        if (row.RowIndex) {
            values.emplace_back(TString(RowIndexColumnName), *row.RowIndex);
        }
        return MakeRow(nameTable, values);
    };

    auto skiffWrite = [generateUnversionedRow] (const std::vector<TRow>& rows, const std::shared_ptr<TSkiffSchema>& skiffSchema) {
        std::vector<TTableSchemaPtr> tableSchemas;
        {
            THashSet<int> tableIndices;
            for (const auto& row : rows) {
                tableIndices.insert(row.TableIndex);
            }
            tableSchemas.assign(tableIndices.size(), New<TTableSchema>());
        }


        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(
            skiffSchema,
            nameTable,
            &resultStream,
            tableSchemas);

        for (const auto& row : rows) {
            if (!writer->Write({generateUnversionedRow(row, nameTable)})) {
                writer->GetReadyEvent().Get().ThrowOnError();
            }
        }
        writer->Close()
            .Get()
            .ThrowOnError();

        return HexEncode(resultStream.Str());
    };

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 0, 2},
        }, rowAndRangeIndex).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "00" "00");

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 0, 3},
        }, rowAndRangeIndex).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "00" "01""03000000""00000000");

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 1, 2},
            {0, 1, 3},
        }, rowAndRangeIndex).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "01""01000000""00000000" "01""02000000""00000000"
        "0000" "00" "00");

    EXPECT_THROW_WITH_SUBSTRING(skiffWrite({{0, 0, {}}}, rowAndRangeIndex), "index requested but reader did not return it");
    EXPECT_THROW_WITH_SUBSTRING(skiffWrite({{0, {}, 0}}, rowAndRangeIndex), "index requested but reader did not return it");

    const auto rowAndRangeIndexAllowMissing = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Nothing),
        })->SetName("$range_index"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Nothing),
        })->SetName("$row_index"),
    });

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 0, 2},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "00" "00");

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 0, 3},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "00" "01""03000000""00000000");

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 1, 2},
            {0, 1, 3},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "01""01000000""00000000" "01""02000000""00000000"
        "0000" "00" "00");

    EXPECT_STREQ(
        skiffWrite({
            {0, {}, {}},
            {0, {}, {}},
            {0, {}, {}},
            {0, {}, {}},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "02" "02"
        "0000" "02" "02"
        "0000" "02" "02"
        "0000" "02" "02");

    EXPECT_STREQ(
        skiffWrite({
            {0, {}, 0},
            {0, {}, 1},
            {0, {}, 3},
            {0, {}, 4},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "02" "01""00000000""00000000"
        "0000" "02" "00"
        "0000" "02" "01""03000000""00000000"
        "0000" "02" "00");

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, {}},
            {0, 0, {}},
            {0, 1, {}},
            {0, 1, {}},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "01""00000000""00000000" "02"
        "0000" "00" "02"
        "0000" "01""01000000""00000000" "02"
        "0000" "00" "02");
}

TEST(TSkiffWriter, TestRowIndexOnlyOrRangeIndexOnly)
{
    std::string columnNameList[] = {
        RowIndexColumnName,
        RangeIndexColumnName,
    };

    for (const auto& columnName : columnNameList) {
        auto skiffSchema = CreateTupleSchema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Int64),
            })->SetName(TString(columnName)),
        });

        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()}, 1);

        // Row 0.
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {TString(columnName), 0},
            }).Get(),
        }));
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestComplexType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
            CreateRepeatedVariant8Schema({
                CreateTupleSchema({
                    CreateSimpleTypeSchema(EWireType::Int64)->SetName("x"),
                    CreateSimpleTypeSchema(EWireType::Int64)->SetName("y"),
                })
            })->SetName("points")
        })->SetName("value"),
    });

    {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto tableSchema = New<TTableSchema>(std::vector{
            TColumnSchema("value", StructLogicalType({
                {"name",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                {
                    "points",
                    ListLogicalType(
                        StructLogicalType({
                            {"x", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                            {"y", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                        }))
                }
            })),
        });
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{tableSchema});

        // Row 0.
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {"value", EValueType::Composite, "[foo;[[0; 1];[2;3]]]"},
                {TString(TableIndexColumnName), 0},
            }).Get(),
        }));
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "foo");
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 2);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 3);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), EndOfSequenceTag<ui8>());

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestEmptyComplexType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
                CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
            })
        })->SetName("value"),
    });

    {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto tableSchema = New<TTableSchema>(std::vector{
            TColumnSchema("value", OptionalLogicalType(
                StructLogicalType({
                    {"name",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                    {"value",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                }))),
        });
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{tableSchema});

        // Row 0.
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {"value", nullptr},
                {TString(TableIndexColumnName), 0},
            }).Get(),
        }));
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestSparseComplexType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
                CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
            })->SetName("value"),
        })->SetName("$sparse_columns"),
    });

    {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto tableSchema = New<TTableSchema>(std::vector{
            TColumnSchema("value", OptionalLogicalType(
                StructLogicalType({
                    {"name",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                    {"value",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                }))),
        });
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{tableSchema});

        // Row 0.
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {"value", EValueType::Composite, "[foo;bar;]"},
                {TString(TableIndexColumnName), 0},
            }).Get(),
        }));
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "foo");
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "bar");
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestSparseComplexTypeWithExtraOptional)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateTupleSchema({
                        CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
                        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
                })
            })->SetName("value"),
        })->SetName("$sparse_columns"),
    });

    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("value", OptionalLogicalType(
            StructLogicalType({
                {"name", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"value", SimpleLogicalType(ESimpleLogicalValueType::String)},
            }))),
    });

    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{tableSchema});

    // Row 0.
    Y_UNUSED(writer->Write({
        MakeRow(nameTable, {
            {"value", EValueType::Composite, "[foo;bar;]"},
            {TString(TableIndexColumnName), 0},
        }).Get(),
    }));
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "foo");
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "bar");
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestBadWireTypeForSimpleColumn)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Yson32),
            })
        })->SetName("opt_yson32"),
    });
    auto nameTable = New<TNameTable>();
    TStringStream resultStream;
    EXPECT_THROW_WITH_SUBSTRING(
        CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{New<TTableSchema>()}),
        "cannot be represented with Skiff schema");
}

TEST(TSkiffWriter, TestMissingComplexColumn)
{
    auto optionalSkiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Int64)}),
        })->SetName("opt_list"),
    });
    auto requiredSkiffSchema = CreateTupleSchema({
        CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Int64)})->SetName("opt_list"),
    });

    { // Non optional Skiff schema
        auto nameTable = New<TNameTable>();
        EXPECT_THROW_WITH_SUBSTRING(
            CreateSkiffWriter(requiredSkiffSchema, nameTable, &Cnull, std::vector{New<TTableSchema>()}),
            "cannot be represented with Skiff schema");
    }

    {
        auto nameTable = New<TNameTable>();
        TStringStream resultStream;
        auto writer = CreateSkiffWriter(optionalSkiffSchema, nameTable, &resultStream, std::vector{New<TTableSchema>()});
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, { }).Get(),
            MakeRow(nameTable, {
                {"opt_list", nullptr},
            }).Get(),
            MakeRow(nameTable, { }).Get(),
        }));
        writer->Close()
            .Get()
            .ThrowOnError();

        EXPECT_EQ(HexEncode(resultStream.Str()), "0000" "00" "0000" "00" "0000" "00");
    }
}

TEST(TSkiffWriter, TestSkippedFields)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("number"),
        CreateSimpleTypeSchema(EWireType::Nothing)->SetName("string"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName(TString(RangeIndexColumnName)),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName(TString(RowIndexColumnName)),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double"),
    });
    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("number", EValueType::Int64),
        TColumnSchema("string", EValueType::String),
        TColumnSchema("double", EValueType::Double),
    });

    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {tableSchema});

        if (!writer->Write({
                MakeRow(nameTable, {
                    {"number", 1},
                    {"string", "hello"},
                    {TString(RangeIndexColumnName), 0},
                    {TString(RowIndexColumnName), 0},
                    {"double", 1.5},
                }).Get()
            }))
        {
            writer->GetReadyEvent().Get().ThrowOnError();
        }
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {"number", 1},
                {TString(RangeIndexColumnName), 5},
                {TString(RowIndexColumnName), 1},
                {"double", 2.5},
            }).Get()
        }));
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(result);
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseDouble(), 1.5);
        // row 1
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 5);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseDouble(), 2.5);
        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }

}

TEST(TSkiffWriter, TestSkippedFieldsOutOfRange)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Nothing)->SetName("string"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName(TString(RangeIndexColumnName)),
    });
    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("string", EValueType::String),
    });

    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {tableSchema});

        if (!writer->Write({
                MakeRow(nameTable, {
                    {"string", "hello"},
                    {TString(RangeIndexColumnName), 0},
                }).Get()
            }))
        {
            writer->GetReadyEvent().Get().ThrowOnError();
        }
        Y_UNUSED(writer->Write({
            MakeRow(nameTable, {
                {TString(RangeIndexColumnName), 5},
            }).Get()
        }));
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(result);
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);
        // row 1
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 5);
        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }

}

TEST(TSkiffWriter, TestSkippedFieldsAndKeySwitch)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
        CreateSimpleTypeSchema(EWireType::Nothing)->SetName("skipped"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"),
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("value1"),
    });
    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()}, 1);

    auto write = [&] (TUnversionedRow row) {
        if (!writer->Write({row})) {
            writer->GetReadyEvent().Get().ThrowOnError();
        }
    };

    // Row 0.
    write(MakeRow(nameTable, {
        {"value", "one"},
        {"value1", 0},
        {TString(TableIndexColumnName), 0},
    }).Get());
    // Row 1.
    write(MakeRow(nameTable, {
        {"value", "one"},
        {"value1", 1},
        {TString(TableIndexColumnName), 0},
    }).Get());
    // Row 2.
    write(MakeRow(nameTable, {
        {"value", "two"},
        {"value1", 2},
        {TString(TableIndexColumnName), 0},
    }).Get());
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    TString buf;

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "two");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), true);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 2);

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();

}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffParser, Simple)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("boolean"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        CreateSimpleTypeSchema(EWireType::Nothing)->SetName("null"),

        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("opt_int64"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        })->SetName("opt_uint64"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Double),
        })->SetName("opt_double"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Boolean),
        })->SetName("opt_boolean"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::String32),
        })->SetName("opt_string32"),
    });

    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteInt64(-1);
    checkedSkiffWriter.WriteUint64(2);
    checkedSkiffWriter.WriteDouble(3.0);
    checkedSkiffWriter.WriteBoolean(true);
    checkedSkiffWriter.WriteString32("foo");

    checkedSkiffWriter.WriteVariant8Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 1);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "int64")), -1);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "uint64")), 2u);
    ASSERT_EQ(GetDouble(collectedRows.GetRowValue(0, "double")), 3.0);
    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(0, "boolean")), true);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(0, "string32")), "foo");
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "null")), true);

    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_int64")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_uint64")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_double")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_boolean")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_string32")), true);
}

TEST(TSkiffParser, TestOptionalNull)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Nothing),
        })->SetName("opt_null"),
    });
    auto nameTable = New<TNameTable>();

    {
        TCollectingValueConsumer collectedRows;
        EXPECT_THROW_WITH_SUBSTRING(
            CreateParserForSkiff(skiffSchema, &collectedRows),
            "cannot be represented with Skiff schema");
    }

    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("opt_null", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))),
    });

    TCollectingValueConsumer collectedRows(tableSchema);
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);

    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(1);

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 2);

    ASSERT_EQ(collectedRows.GetRowValue(0, "opt_null").Type, EValueType::Null);
}

TEST(TSkiffParser, TestSparse)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        })->SetName("$sparse_columns"),
    });

    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // row 1
    checkedSkiffWriter.WriteVariant16Tag(0);
    // sparse fields begin
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteInt64(-42);
    checkedSkiffWriter.WriteVariant16Tag(1);
    checkedSkiffWriter.WriteUint64(54);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    // row 2
    checkedSkiffWriter.WriteVariant16Tag(0);
    // sparse fields begin
    checkedSkiffWriter.WriteVariant16Tag(2);
    checkedSkiffWriter.WriteString32("foo");
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 2);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "int64")), -42);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "uint64")), 54u);
    ASSERT_FALSE(collectedRows.FindRowValue(0, "string32"));

    ASSERT_FALSE(collectedRows.FindRowValue(1, "int64"));
    ASSERT_FALSE(collectedRows.FindRowValue(1, "uint64"));
    ASSERT_EQ(GetString(collectedRows.GetRowValue(1, "string32")), "foo");
}

TEST(TSkiffParser, TestYsonWireType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
    });

    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("-42");

    // Row 1.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("42u");

    // Row 2.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("\"foobar\"");

    // Row 3.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("%true");

    // Row 4.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("{foo=bar}");

    // Row 5.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("#");

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 6);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "yson")), -42);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(1, "yson")), 42u);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(2, "yson")), "foobar");
    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(3, "yson")), true);
    ASSERT_EQ(GetAny(collectedRows.GetRowValue(4, "yson"))->AsMap()->GetChildOrThrow("foo")->AsString()->GetValue(), "bar");
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(5, "yson")), true);
}

TEST(TSkiffParser, TestBadYsonWireType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
    });

    auto parseYsonUsingSkiff = [&] (TStringBuf ysonValue) {
        TCollectingValueConsumer collectedRows;
        auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        TStringStream dataStream;
        ASSERT_NO_THROW({
            TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

            checkedSkiffWriter.WriteVariant16Tag(0);
            checkedSkiffWriter.WriteYson32(ysonValue);

            checkedSkiffWriter.Finish();
        });

        parser->Read(dataStream.Str());
        parser->Finish();
    };

    try {
        parseYsonUsingSkiff("[42");
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Premature end of stream"));
    }

    try {
        parseYsonUsingSkiff("<foo=bar>42");
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Table values cannot have top-level attributes"));
    }
}

TEST(TSkiffParser, TestSpecialColumns)
{
    std::shared_ptr<TSkiffSchema> skiffSchemaList[] = {
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"),
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$row_switch"),
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$range_switch"),
        }),
    };

    for (const auto& skiffSchema : skiffSchemaList) {
        try {
            TCollectingValueConsumer collectedRows;
            auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        } catch (std::exception& e) {
            EXPECT_THAT(e.what(), testing::HasSubstr("Skiff parser does not support \"$key_switch\""));
        }
    }
}

TEST(TSkiffParser, TestOtherColumns)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("$other_columns"),
    });

    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteString32("row_0");
    checkedSkiffWriter.WriteYson32("{foo=-42;}");

    // Row 1.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteString32("row_1");
    checkedSkiffWriter.WriteYson32("{bar=qux;baz={boolean=%false;};}");

    // Row 2.
    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 2);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(0, "name")), "row_0");
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "foo")), -42);

    ASSERT_EQ(GetString(collectedRows.GetRowValue(1, "name")), "row_1");
    ASSERT_EQ(GetString(collectedRows.GetRowValue(1, "bar")), "qux");
    ASSERT_EQ(ConvertToYsonTextStringStable(GetAny(collectedRows.GetRowValue(1, "baz"))), "{\"boolean\"=%false;}");
}

TEST(TSkiffParser, TestComplexColumn)
{
    auto skiffSchema = CreateTupleSchema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("value"),
        })->SetName("column")
    });

    TCollectingValueConsumer collectedRows(
        New<TTableSchema>(std::vector{
            TColumnSchema("column", NTableClient::StructLogicalType({
                {"key", NTableClient::SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"value", NTableClient::SimpleLogicalType(ESimpleLogicalValueType::Int64)}
            }))
        }));
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteString32("row_0");
    checkedSkiffWriter.WriteInt64(42);

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 1);
    ASSERT_EQ(ConvertToYsonTextStringStable(GetComposite(collectedRows.GetRowValue(0, "column"))), "[\"row_0\";42;]");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffParser, TestEmptyInput)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("column"),
    });

    TCollectingValueConsumer collectedRows;

    {
        auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        parser->Finish();
        ASSERT_EQ(static_cast<int>(collectedRows.Size()), 0);
    }
    {
        auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        parser->Read("");
        parser->Finish();
        ASSERT_EQ(static_cast<int>(collectedRows.Size()), 0);
    }
    {
        auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        parser->Read("");
        parser->Read("");
        parser->Finish();
        ASSERT_EQ(static_cast<int>(collectedRows.Size()), 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffParser, ColumnIds)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("field_a"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("field_b")
    });

    TCollectingValueConsumer collectedRows;
    collectedRows.GetNameTable()->GetIdOrRegisterName("field_b");
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteInt64(-1);
    checkedSkiffWriter.WriteUint64(2);

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 1);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "field_a")), -1);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "field_b")), 2u);
}

TEST(TSkiffParser, TestSparseComplexType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
                CreateSimpleTypeSchema(EWireType::Int64)->SetName("value"),
            })->SetName("value"),
        })->SetName("$sparse_columns"),
    });

    TCollectingValueConsumer collectedRows(
        New<TTableSchema>(std::vector{
            TColumnSchema("value", OptionalLogicalType(
                StructLogicalType({
                    {"name", SimpleLogicalType(ESimpleLogicalValueType::String)},
                    {"value", SimpleLogicalType(ESimpleLogicalValueType::Int64)}
                })))
        }));
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteString32("row_0");
    checkedSkiffWriter.WriteInt64(10);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    // Row 1.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 2);
    EXPECT_EQ(ConvertToYsonTextStringStable(GetComposite(collectedRows.GetRowValue(0, "value"))), "[\"row_0\";10;]");
    EXPECT_FALSE(collectedRows.FindRowValue(1, "value"));
}

TEST(TSkiffParser, TestSparseComplexTypeWithExtraOptional)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateTupleSchema({
                    CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
                    CreateSimpleTypeSchema(EWireType::Int64)->SetName("value"),
                })
            })->SetName("column"),
        })->SetName("$sparse_columns"),
    });

    TCollectingValueConsumer collectedRows(
        New<TTableSchema>(std::vector{
            TColumnSchema("column", OptionalLogicalType(
                StructLogicalType({
                    {"key", NTableClient::SimpleLogicalType(ESimpleLogicalValueType::String)},
                    {"value", NTableClient::SimpleLogicalType(ESimpleLogicalValueType::Int64)}
                })))
        }));
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(1);
    checkedSkiffWriter.WriteString32("row_0");
    checkedSkiffWriter.WriteInt64(42);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    // Row 1.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 2);
    ASSERT_EQ(ConvertToYsonTextStringStable(GetComposite(collectedRows.GetRowValue(0, "column"))), "[\"row_0\";42;]");
    ASSERT_FALSE(collectedRows.FindRowValue(1, "column"));
}


TEST(TSkiffParser, TestBadWireTypeForSimpleColumn)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Yson32),
            })
        })->SetName("opt_yson32"),
    });

    TCollectingValueConsumer collectedRows;
    EXPECT_THROW_WITH_SUBSTRING(
        CreateParserForSkiff(skiffSchema, &collectedRows),
        "cannot be represented with Skiff schema");
}

TEST(TSkiffParser, TestEmptyColumns)
{
    auto skiffSchema = CreateTupleSchema({});
    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    parser->Read(TStringBuf("\x00\x00\x00\x00"sv));
    parser->Finish();

    ASSERT_EQ(static_cast<int>(collectedRows.Size()), 2);
}

TEST(TSkiffFormat, TestTimestamp)
{
    using namespace NLogicalTypeShortcuts;
    CHECK_BIDIRECTIONAL_CONVERSION(Timestamp(), CreateSimpleTypeSchema(EWireType::Uint64), 42ull, "2A000000" "00000000");
    CHECK_BIDIRECTIONAL_CONVERSION(Interval(), CreateSimpleTypeSchema(EWireType::Int64), 42, "2A000000" "00000000");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
