#include "ydb_table_infer.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb::NConsoleClient {

TCommandTableInfer::TCommandTableInfer()
    : TClientCommandTree("infer", {}, "Infer CREATE TABLE text for a table from input data")
{
    AddCommand(std::make_unique<TCommandTableInferFile>());
}

TCommandTableInferFile::TCommandTableInferFile()
    : TClientCommandTree("file", {}, "Infer CREATE TABLE text for a table from file(s)")
{
    AddCommand(std::make_unique<TCommandTableInferCsv>());
}

TCommandTableInferCsv::TCommandTableInferCsv()
    : TYdbCommand("csv", {}, "Infer CREATE TABLE text for a table from CSV file(s)")
{}

void TCommandTableInferCsv::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->SetTrailingArgTitle("<input files...>",
            "One or more file paths to infer from");

    config.Opts->AddLongOption("header",
            "Set if file contains column names at the first not skipped row")
        .StoreTrue(&Header);
    config.Opts->AddLongOption("columns",
            "String with column names that replaces header")
        .RequiredArgument("STR").StoreResult(&HeaderRow);
    config.Opts->AddLongOption("null-value",
        "Value(s) that would be interpreted as NULL, no NULL value by default. Option can be used several times")
        .RequiredArgument("STRING").StoreResult(&NullValue);
}

void TCommandTableInferCsv::Parse(TConfig& config) {
    TYdbCommand::Parse(config);

    for (const auto& filePath : config.ParseResult->GetFreeArgs()) {
        FilePaths.push_back(filePath);
    }
    for (const auto& filePath : FilePaths) {
        if (filePath.empty()) {
            throw TMisuseException() << "File path is not allowed to be empty";
        }
    }
    // If no filenames or stdin isn't connected to tty, read from stdin.
    if (FilePaths.empty() || !IsStdinInteractive()) {
        FilePaths.push_back("");
    }
}

int TCommandTableInferCsv::Run(TConfig& config) {
    Y_UNUSED(config);
    return EXIT_SUCCESS;
}

}
