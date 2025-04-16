#include "ydb_table_infer.h"

#include <ydb/library/arrow_inference/arrow_inference.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>

#include <arrow/csv/options.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <util/string/builder.h>

namespace NYdb::NConsoleClient {

TCommandTableInfer::TCommandTableInfer()
    : TClientCommandTree("infer", {}, "Infer table schema")
{
    AddCommand(std::make_unique<TCommandTableInferFile>());
}

TCommandTableInferFile::TCommandTableInferFile()
    : TClientCommandTree("file", {}, "Infer table schema from file")
{
    AddCommand(std::make_unique<TCommandTableInferCsv>());
}

TCommandTableInferCsv::TCommandTableInferCsv()
    : TYdbCommand("csv", {}, "Infer table schema from CSV file")
{}

void TCommandTableInferCsv::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("file", "Path to CSV file(s)")
        .RequiredArgument("PATH").AppendTo(&FilePaths);
    config.Opts->AddLongOption("header", "Use first row as header")
        .NoArgument().SetFlag(&Header);
    config.Opts->AddLongOption("columns", "Comma-separated list of column names")
        .RequiredArgument("NAMES").StoreResult(&HeaderRow);
    config.Opts->AddLongOption("null-value", "String(s) to interpret as NULL. Can be used multiple times")
        .RequiredArgument("NAME").AppendTo(&NullValues);

    config.SetFreeArgsNum(0);
}

void TCommandTableInferCsv::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if (FilePaths.empty()) {
        throw TMisuseException() << "At least one file path should be provided";
    }

    if (Header && !HeaderRow.empty()) {
        throw TMisuseException() << "Options --header and --columns are mutually exclusive";
    }
}

int TCommandTableInferCsv::Run(TConfig& config) {
    Y_UNUSED(config);
    
    for (const auto& filePath : FilePaths) {
        auto maybeFile = arrow::io::ReadableFile::Open(filePath.c_str());
        if (!maybeFile.ok()) {
            throw TMisuseException() << "Failed to open file: " << filePath;
        }

        THashMap<TString, TString> params;
        params["format"] = "csv_with_names";
        if (Header) {
            params["header"] = "true";
        }
        if (!HeaderRow.empty()) {
            params["columns"] = HeaderRow;
        }

        auto formatConfig = std::make_shared<NArrowInference::CsvConfig>();
        if (!NullValues.empty()) {
            formatConfig->ConvOpts.null_values = NullValues;
            formatConfig->ConvOpts.quoted_strings_can_be_null = true;
        }

        formatConfig->Format = NArrowInference::EFileFormat::CsvWithNames;
        formatConfig->ShouldMakeOptional = true;

        auto result = NYdb::NArrowInference::InferTypes(maybeFile.ValueOrDie(), formatConfig);
        
        if (std::holds_alternative<TString>(result)) {
            throw TMisuseException() << "Failed to infer schema: " << std::get<TString>(result);
        }

        auto& arrowFields = std::get<NYdb::NArrowInference::ArrowFields>(result);
        TStringBuilder query;
        query << "CREATE TABLE `table` (\n";
        bool first = true;
        for (const auto& field : arrowFields) {
            if (field->name().empty()) {
                continue;
            }
            Ydb::Type inferredType;
            bool inferResult = NYdb::NArrowInference::ArrowToYdbType(inferredType, *field->type(), formatConfig);
            if (!first) {
                query << ",\n";
            }
            TString resultType = "Text";
            bool isOptional = false;
            if (inferResult) {
                TTypeParser parser(inferredType);
                if (parser.GetKind() == TTypeParser::ETypeKind::Optional) {
                    isOptional = true;
                    parser.OpenOptional();
                }
                if (parser.GetKind() == TTypeParser::ETypeKind::Primitive) {
                    resultType = TTypeBuilder()
                        .Primitive(parser.GetPrimitive())
                        .Build()
                        .ToString();
                } else {
                    throw TMisuseException() << "Only primitive types are supported for table columns."
                        " Inferred type kind: " << parser.GetKind();
                }
            }
            query << "    `" << field->name() << "` " << resultType;
            if (!isOptional) {
                query << " NOT NULL";
            }
            first = false;
        }
        query << "\n) PRIMARY KEY ()";

        Cout << query << Endl;
    }

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
