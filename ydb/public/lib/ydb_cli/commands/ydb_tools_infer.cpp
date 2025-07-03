#include "ydb_tools_infer.h"

#include <ydb/library/arrow_inference/arrow_inference.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/lib/ydb_cli/common/csv_parser.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <arrow/csv/options.h>
#include <arrow/io/file.h>
#include <arrow/io/stdio.h>
#include <arrow/table.h>
#include <util/string/builder.h>
#include <library/cpp/string_utils/csv/csv.h>
#include <util/stream/file.h>
#include <regex>

namespace NYdb::NConsoleClient {

TCommandToolsInfer::TCommandToolsInfer()
    : TClientCommandTree("infer", {}, "Infer table schema")
{
    AddCommand(std::make_unique<TCommandToolsInferCsv>());
}

TCommandToolsInferCsv::TCommandToolsInferCsv()
    : TYdbCommand("csv", {}, "Generate CREATE TABLE SQL query from CSV file"
        "\n\nBy default, if no options are specified, "
        "the command uses the first row of the file as column names if possible"
        " (i.e., if the values meet the requirements for column names and do not match data types in the other rows)."
        " Otherwise, column names will be generated automatically.")
{
    Args[0] = "<input files...>";
}

void TCommandToolsInferCsv::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->GetOpts().SetTrailingArgTitle("<input files...>",
            "One or more file paths to infer from. Or CSV data can be passed to stdin instead");
    config.Opts->AddLongOption('p', "path", "Database path to table that should be created")
        .RequiredArgument("STRING").DefaultValue("table").StoreResult(&Path);
    auto& columnsOption = config.Opts->AddLongOption("columns",
        "Explicitly specifies table column names, as a comma-separated list.")
        .RequiredArgument("NAMES").StoreResult(&ColumnNames);
    auto& genColumnsOption = config.Opts->AddLongOption("gen-columns",
        "Explicitly indicates that table column names should be generated automatically.")
        .NoArgument().StoreTrue(&GenerateColumnNames);
    auto& headerOption = config.Opts->AddLongOption("header", "Explicitly indicates that the first row in the CSV contains column names.")
        .NoArgument().StoreTrue(&HeaderHasColumnNames);
    config.Opts->AddLongOption("rows-to-analyze", "Number of rows to analyze. "
        "0 means unlimited. Reading will be stopped soon after this number of rows is read.")
        .DefaultValue(500000).StoreResult(&RowsToAnalyze);
    config.Opts->AddLongOption("execute", "Execute CREATE TABLE request right after generation.")
        .NoArgument().StoreTrue(&Execute);

    config.Opts->MutuallyExclusiveOpt(columnsOption, genColumnsOption);
    config.Opts->MutuallyExclusiveOpt(columnsOption, headerOption);
    config.Opts->MutuallyExclusiveOpt(genColumnsOption, headerOption);
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

    bool IsValidColumnName(const std::string& name) {
        if (name.empty()) {
            return false;
        }

        // Column name must start with a letter or underscore and contain only letters, numbers and underscores
        static const std::regex namePattern("^[a-zA-Z_][a-zA-Z0-9_]*$");
        return std::regex_match(name, namePattern);
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
    formatConfig->Format = NArrowInference::EFileFormat::CsvWithNames;
    formatConfig->ShouldMakeOptional = true;

    // Configure CSV parsing options
    formatConfig->ParseOpts.delimiter = ','; // Use comma as default delimiter
    formatConfig->ParseOpts.quote_char = '"'; // Use double quotes as default quote character
    formatConfig->ParseOpts.escape_char = '\\'; // Use backslash as default escape character

    // Read the first line of the file if needed
    std::vector<std::string> firstRowValues;
    bool generateColumnNames = GenerateColumnNames;
    if (!ColumnNames.empty()) {
        // If --columns option is specified, use explicitly provided names
        auto tmp = static_cast<TVector<TString>>(NCsvFormat::CsvSplitter(ColumnNames));
        for (const auto& columnName : tmp) {
            firstRowValues.push_back(columnName.data());
        }
        formatConfig->ReadOpts.column_names = firstRowValues;
        formatConfig->ReadOpts.autogenerate_column_names = false;
    } else if (HeaderHasColumnNames) {
        // If --header option is specified, use first row as column names
        formatConfig->ReadOpts.column_names = {};
        formatConfig->ReadOpts.autogenerate_column_names = false;
    } else if (GenerateColumnNames) {
        // If --gen-columns option is specified, generate names automatically
        formatConfig->ReadOpts.column_names = {};
        formatConfig->ReadOpts.autogenerate_column_names = true;
    } else {
        // If no option is specified:
        // 1. Read the first line of the file
        TFile file;
        if (ReadingFromStdin) {
            if (config.IsVerbose()) {
                Cerr << "Reading first linefrom stdin" << Endl;
            }
            file = TFile(GetStdinFileno());
        } else {
            if (config.IsVerbose()) {
                Cerr << "Reading first line from file " << FilePaths[0] << Endl;
            }
            file = TFile(FilePaths[0], RdOnly);
        }
        auto input = MakeHolder<TFileInput>(file);
        NCsvFormat::TLinesSplitter csvSplitter(*input);
        TString firstLine = csvSplitter.ConsumeLine();

        // Check if the line contains newlines inside quotes
        if (firstLine.find('\n') != TString::npos || firstLine.find('\r') != TString::npos) {
            // If there are newlines, it's definitely not column names
            formatConfig->ReadOpts.column_names = {};
            formatConfig->ReadOpts.autogenerate_column_names = true;
        } else {
            // Split the line by delimiter
            auto tmp = static_cast<TVector<TString>>(NCsvFormat::CsvSplitter(firstLine));
            for (const auto& value : tmp) {
                firstRowValues.push_back(value.data());
            }

            // 2. Tell the library to generate names automatically
            // We will decide later if we would use generated names or first row as column names
            formatConfig->ReadOpts.column_names = {};
            formatConfig->ReadOpts.autogenerate_column_names = true;
            formatConfig->ReadOpts.skip_rows = 1;
        }
    }

    // Start file analysis
    auto result = NYdb::NArrowInference::InferTypes(inputs, formatConfig);
    if (std::holds_alternative<TString>(result)) {
        throw TMisuseException() << "Failed to infer schema: " << std::get<TString>(result);
    }

    auto& arrowFields = std::get<NYdb::NArrowInference::ArrowFields>(result);
    bool useFirstRowAsColumnNames = false;

    // If no option is specified, check if the first row can be used as data
    if (firstRowValues.size() > 0 && ColumnNames.empty() && !HeaderHasColumnNames && !GenerateColumnNames) {
        bool canUseFirstRowAsColumnNames = true;
        bool canUseFirstRowAsData = false;  // By default, assume we can't use it as data
        if (firstRowValues.size() != arrowFields.size()) {
            canUseFirstRowAsColumnNames = false;
            if (config.IsVerbose()) {
                Cerr << "First row size (" << firstRowValues.size() << ") doesn't match inferred fields count (" 
                     << arrowFields.size() << "), can't use first row as header or data" << Endl;
            }
        } else {
            // First check if all values in the first row can be column names
            for (const auto& value : firstRowValues) {
                if (!IsValidColumnName(value)) {
                    canUseFirstRowAsColumnNames = false;
                    generateColumnNames = true;
                    if (config.IsVerbose()) {
                        Cerr << "Value '" << value << "' is not a valid column name, can't use first row as header."
                            " Column names will be generated automatically" << Endl;
                    }
                    break;
                }
            }

            // Only if all values can be column names, check if they can be converted to types
            if (canUseFirstRowAsColumnNames) {
                if (config.IsVerbose()) {
                    Cerr << "All values in first row are valid column names, checking if they can be used as data..." << Endl;
                }
                
                canUseFirstRowAsData = true;  // Assume we can use as data until proven otherwise
                for (size_t i = 0; i < arrowFields.size(); ++i) {
                    auto field = arrowFields[i];
                    auto value = firstRowValues[i];

                    // Try to convert value to column type
                    Ydb::Type inferredType;
                    bool inferResult = NYdb::NArrowInference::ArrowToYdbType(inferredType, *field->type(), formatConfig);
                    if (!inferResult) {
                        canUseFirstRowAsData = false;
                        if (config.IsVerbose()) {
                            Cerr << "Failed to infer type for column " << i << ", assuming string type" << Endl;
                        }
                        break;
                    }
                    if (!NYdb::NConsoleClient::IsConvertibleToYdbValue(TString(value), inferredType)) {
                        canUseFirstRowAsData = false;
                        if (config.IsVerbose()) {
                            Cerr << "Value '" << value << "' in column " << i << " cannot be converted to inferred type "
                                 << TType(inferredType) << Endl;
                        }
                        break;
                    }
                }

                if (canUseFirstRowAsData) {
                    generateColumnNames = true;
                    if (config.IsVerbose()) {
                        Cerr << "All values in the first row can be used as data, "
                            "so considering it as data and generating column names" << Endl;
                    }
                } else {
                    useFirstRowAsColumnNames = true;
                    if (config.IsVerbose()) {
                        Cerr << "First row will be used as column names since values cannot be used as data" << Endl;
                    }
                }
            }
        }
    }

    // Generate SQL query for table creation
    TStringBuilder query;
    query << "CREATE TABLE ";
    PrintStringQuotedIfNeeded(query, GetRelativePath(Path, config));
    query << " (" << Endl;
    int columnIndex = -1;
    std::string firstColumnName;
    for (const auto& field : arrowFields) {
        ++columnIndex;
        std::string columnName;
        if (useFirstRowAsColumnNames)  {
            columnName = firstRowValues[columnIndex];
        } else if (generateColumnNames) {
            columnName = "column" + ToString(columnIndex);
        } else {
            columnName = field->name();
            if (columnName.empty()) {
                continue;
            }
        }
        if (columnIndex == 0) {
            firstColumnName = columnName;
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
            Cerr << "Failed to infer type for column " << columnName << " with index " << columnIndex << Endl;
        }
        query << "    ";
        PrintStringQuotedIfNeeded(query, columnName);
        query << " " << resultType;
        // Only setting NOT NULL for the first column because we consider it a PRIMARY KEY
        if (!columnIndex) {
            query << " NOT NULL";
        }
        query << ',' << Endl;
    }
    query << "    PRIMARY KEY (`" << firstColumnName << "`) -- First column is chosen. Probably need to change this." << Endl;
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
