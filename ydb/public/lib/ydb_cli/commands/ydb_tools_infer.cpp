#include "ydb_tools_infer.h"

#include <ydb/library/arrow_inference/arrow_inference.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <arrow/csv/options.h>
#include <arrow/io/file.h>
#include <arrow/io/stdio.h>
#include <arrow/table.h>
#include <util/string/builder.h>
#include <library/cpp/string_utils/csv/csv.h>

namespace NYdb::NConsoleClient {

TCommandToolsInfer::TCommandToolsInfer()
    : TClientCommandTree("infer", {}, "Infer table schema")
{
    AddCommand(std::make_unique<TCommandToolsInferCsv>());
}

TCommandToolsInferCsv::TCommandToolsInferCsv()
    : TYdbCommand("csv", {}, "Generate CREATE TABLE SQL query from CSV file"
        "\n\nBy default, the command attempts to use the first row of the CSV as column names if possible."
        " Use the \"--columns\", \"--gen-names\" or \"--header\" options to set the column names source explicitly.")
{}

void TCommandToolsInferCsv::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->GetOpts().SetTrailingArgTitle("<input files...>",
            "One or more file paths to infer from. Or CSV data can be passed to stdin instead");
    config.Opts->AddLongOption('p', "path", "Database path to table that should be created")
        .RequiredArgument("STRING").DefaultValue("table").StoreResult(&Path);
    config.Opts->AddLongOption("columns",
        "Explicitly specifies table column names, as a comma-separated list.")
        .RequiredArgument("NAMES").StoreResult(&ColumnNames);
    config.Opts->AddLongOption("gen-columns",
        "Explicitly indicates that table column names should be generated automatically.")
        .NoArgument().StoreTrue(&GenerateColumnNames);
    config.Opts->AddLongOption("header", "Explicitly indicates that the first row in the CSV contains column names.")
        .NoArgument().StoreTrue(&HeaderHasColumnNames);
    config.Opts->AddLongOption("rows-to-analyze", "Number of rows to analyze. "
        "0 means unlimited. Reading will be stopped soon after this number of rows is read.")
        .DefaultValue(500000).StoreResult(&RowsToAnalyze);
    config.Opts->AddLongOption("execute", "Execute CREATE TABLE request right after generation.")
        .NoArgument().StoreTrue(&Execute);
}

void TCommandToolsInferCsv::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    for (const auto& filePath : config.ParseResult->GetFreeArgs()) {
        FilePaths.push_back(filePath);
    }
    for (const auto& filePath : FilePaths) {
        if (filePath.empty()) {
            throw TMisuseException() << "File path is not allowed to be empty";
        }
    }

    if (FilePaths.empty()) {
        if (IsStdinInteractive()) {
            throw TMisuseException() << "At least one file path should be provided";
        } else {
            ReadingFromStdin = true;
        }
    }

    if (HeaderHasColumnNames && !ColumnNames.empty()) {
        throw TMisuseException() << "Options --header and --columns are mutually exclusive."
            " Use --header if first row in the file  containscolumn names. Use --columns to list column names manually.";
    }
}

namespace {
    std::string GetRelativePath(const std::string& fullPath, TClientCommand::TConfig& config) {
        std::string databasePath = config.Database;
        if (databasePath.back() != '/' && databasePath.back() != '\\') {
            databasePath += '/';
        }
        if (fullPath.find(databasePath) == 0) {
            return fullPath.substr(databasePath.length());
        }
        return fullPath;
    }

    void PrintStringQuotedIfNeeded(TStringBuilder& builder, const std::string& str) {
        if (str.find_first_of(" \t\n\r\v\f/") != TString::npos) {
            builder << '`' << str << '`';
        } else {
            builder << str;
        }
    }
}

int TCommandToolsInferCsv::Run(TConfig& config) {
    Y_UNUSED(config);
    std::vector<std::shared_ptr<arrow::io::InputStream>> inputs;
    if (ReadingFromStdin) {
        inputs.push_back(std::make_shared<arrow::io::StdinStream>());
    } else {
        for (const auto& filePath : FilePaths) {
            auto maybeFile = arrow::io::ReadableFile::Open(filePath.c_str());
            if (!maybeFile.ok()) {
                throw TMisuseException() << "Failed to open file: " << filePath;
            }
            inputs.push_back(maybeFile.ValueOrDie());
        }
    }

    auto formatConfig = std::make_shared<NArrowInference::TCsvConfig>();
    formatConfig->RowsToAnalyze = RowsToAnalyze;
    if (!ColumnNames.empty()) {
        NCsvFormat::CsvSplitter splitter(ColumnNames);
        auto tmp = static_cast<TVector<TString>>(splitter);
        std::vector<std::string> columnNames;
        for (const auto& columnName : tmp) {
            columnNames.push_back(columnName.data());
        }
        formatConfig->ReadOpts.column_names = columnNames;
    } else if (!HeaderHasColumnNames) {
        formatConfig->ReadOpts.autogenerate_column_names = true;
    }

    formatConfig->Format = NArrowInference::EFileFormat::CsvWithNames;

    auto result = NYdb::NArrowInference::InferTypes(inputs, formatConfig);
    
    if (std::holds_alternative<TString>(result)) {
        throw TMisuseException() << "Failed to infer schema: " << std::get<TString>(result);
    }

    auto& arrowFields = std::get<NYdb::NArrowInference::ArrowFields>(result);
    TStringBuilder query;
    query << "CREATE TABLE ";
    PrintStringQuotedIfNeeded(query, GetRelativePath(Path, config));
    query << " (" << Endl;
    for (const auto& field : arrowFields) {
        if (field->name().empty()) {
            continue;
        }
        Ydb::Type inferredType;
        bool inferResult = NYdb::NArrowInference::ArrowToYdbType(inferredType, *field->type(), formatConfig);
        TString resultType = "Text";
        if (inferResult) {
            TTypeParser parser(inferredType);
            if (parser.GetKind() == TTypeParser::ETypeKind::Optional) {
                parser.OpenOptional();
            }
            if (parser.GetKind() == TTypeParser::ETypeKind::Primitive) {
                resultType = (parser.GetPrimitive() == EPrimitiveType::Utf8)
                    ? "Text"
                    : TTypeBuilder()
                        .Primitive(parser.GetPrimitive())
                        .Build()
                        .ToString();
            } else {
                throw TMisuseException() << "Only primitive types are supported for table columns."
                    " Inferred type kind: " << parser.GetKind();
            }
        } else if (config.IsVerbose()) {
            Cerr << "Failed to infer type for column " << field->name() << Endl;
        }
        query << "    ";
        PrintStringQuotedIfNeeded(query, field->name());
        query << " " << resultType << ',' << Endl;
        if (!field->nullable()) {
            query << " NOT NULL";
        }
    }
    query << "    PRIMARY KEY (" << arrowFields[0]->name() << ") -- First column is chosen. Probably need to change this." << Endl;
    query <<
R"()
WITH (
    STORE = ROW -- or COLUMN
    -- Other useful table options to consider:
    --, AUTO_PARTITIONING_BY_SIZE = ENABLED
    --, AUTO_PARTITIONING_BY_LOAD = ENABLED
    --, UNIFORM_PARTITIONS = 100 -- Initial number of partitions
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000
);)";

    if (Execute) {
        Cerr << "Executing request: " << Endl << Endl << query << Endl << Endl;
        TDriver driver = CreateDriver(config);
        NQuery::TQueryClient client(driver);
        auto result = client.RetryQuery(query, NQuery::TTxControl::NoTx(), TDuration::Zero(), true)
            .GetValueSync();
        if (result.IsSuccess()) {
            Cerr << "Query executed successfully." << Endl;
            if (!result.GetIssues().Empty()) {
                Cerr << "Issues: " << result.GetIssues().ToString() << Endl;
            }
        } else {
            Cerr << "Failed to create a table." << Endl;
            result.Out(Cerr);
            Cerr << Endl;
            return EXIT_FAILURE;
        }
        driver.Stop(true);
    } else {
        Cout << query << Endl;
    }

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
