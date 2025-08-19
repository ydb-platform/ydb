#include "ydb_tools.h"
#include "ydb_tools_infer.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/pg_dump_parser.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/library/backup/util.h>

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/serialized_enum.h>
#include <util/stream/format.h>
#include <util/string/split.h>
#include <util/system/info.h>

namespace NYdb::NConsoleClient {

TCommandTools::TCommandTools()
    : TClientCommandTree("tools", {}, "YDB tools service")
{
    AddCommand(std::make_unique<TCommandDump>());
    AddCommand(std::make_unique<TCommandRestore>());
    AddCommand(std::make_unique<TCommandCopy>());
    AddCommand(std::make_unique<TCommandRename>());
    AddCommand(std::make_unique<TCommandPgConvert>());
    AddCommand(std::make_unique<TCommandToolsInfer>());
}

TToolsCommand::TToolsCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TYdbCommand(name, aliases, description)
{}

void TToolsCommand::Config(TConfig& config) {
    TYdbCommand::Config(config);
}

////////////////////////////////////////////////////////////////////////////////
//  Dump
////////////////////////////////////////////////////////////////////////////////
TCommandDump::TCommandDump()
    : TToolsCommand("dump", {}, "Dump specified database directory or table into local directory")
{}

void TCommandDump::Config(TConfig& config) {
    TToolsCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('p', "path", "Database path to a directory or a table to be dumped.")
        .DefaultValue(".").StoreResult(&Path);
    config.Opts->AddLongOption("exclude", "Pattern(s) (PCRE) for paths excluded from dump."
            " Option can be used several times - one for each pattern.")
        .RequiredArgument("STRING").Handler([this](const TString& arg) {
            ExclusionPatterns.emplace_back(TRegExMatch(arg));
        });
    config.Opts->AddLongOption('o', "output", "[Required] Path in a local filesystem to a directory to place dump into."
            " Directory should either not exist or be empty.")
        .StoreResult(&FilePath);

    NDump::TDumpSettings defaults;

    config.Opts->AddLongOption("scheme-only", "Dump only scheme including ACL and owner")
        .DefaultValue(defaults.SchemaOnly_).StoreTrue(&IsSchemeOnly);
    config.Opts->AddLongOption("avoid-copy", "Avoid copying."
            " By default, YDB makes a copy of a table before dumping it to reduce impact on workload and ensure consistency.\n"
            "In some cases (e.g. for tables with external blobs) copying should be disabled.")
        .DefaultValue(defaults.AvoidCopy_).StoreTrue(&AvoidCopy);
    config.Opts->AddLongOption("save-partial-result", "Do not remove partial dump result."
            " If this option is not enabled, all files that have already been created will be removed in case of error.")
        .DefaultValue(defaults.SavePartialResult_).StoreTrue(&SavePartialResult);
    config.Opts->AddLongOption("preserve-pool-kinds", "Preserve storage pool kind settings."
            " If this option is enabled, storage pool kind will be saved to dump."
            " In this case, if there will be no such storage pool kind in database on restore, error will occur."
            " By default this option is disabled and any existing storage pool kind will be used on restore.")
        .DefaultValue(defaults.PreservePoolKinds_).StoreTrue(&PreservePoolKinds);
    config.Opts->AddLongOption("consistency-level", "Consistency level."
            " Options: database, table\n"
            "database - take one consistent snapshot of all tables specified for dump."
            " Takes more time and is more likely to impact workload;\n"
            "table - take consistent snapshot per each table independently.")
        .DefaultValue(defaults.ConsistencyLevel_).StoreResult(&ConsistencyLevel);
    config.Opts->AddLongOption("ordered", "Preserve order by primary key in backup files.")
        .DefaultValue(defaults.Ordered_).StoreTrue(&Ordered);
}

void TCommandDump::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    AdjustPath(config);
}

int TCommandDump::Run(TConfig& config) {
    NDump::TDumpSettings::EConsistencyLevel consistencyLevel;
    if (!TryFromString<NDump::TDumpSettings::EConsistencyLevel>(ConsistencyLevel, consistencyLevel)) {
        throw yexception() << "Incorrect consistency level."
            " Available options: " << GetEnumAllNames<NDump::TDumpSettings::EConsistencyLevel>();
    }

    auto settings = NDump::TDumpSettings()
        .Database(config.Database)
        .ExclusionPatterns(std::move(ExclusionPatterns))
        .SchemaOnly(IsSchemeOnly)
        .ConsistencyLevel(consistencyLevel)
        .AvoidCopy(AvoidCopy)
        .SavePartialResult(SavePartialResult)
        .PreservePoolKinds(PreservePoolKinds)
        .Ordered(Ordered);

    auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel)));
    log->SetFormatter(GetPrefixLogFormatter(""));

    NDump::TClient client(CreateDriver(config), std::move(log));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.Dump(Path, FilePath, settings));

    return EXIT_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////
//  Restore
////////////////////////////////////////////////////////////////////////////////
TCommandRestore::TCommandRestore()
    : TToolsCommand("restore", {}, "Restore database from local dump into specified directory")
{}

void TCommandRestore::Config(TConfig& config) {
    TToolsCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('p', "path",
            "[Required] Database path to a destination directory where restored directory or table will be placed.")
        .StoreResult(&Path);
    config.Opts->AddLongOption('i', "input",
            "[Required] Path in a local filesystem to a directory with dump.")
        .StoreResult(&FilePath);

    config.Opts->AddLongOption("dry-run", TStringBuilder()
            << "Do not restore tables, only check that:" << Endl
            << "  - all dumped tables exist in database;" << Endl
            << "  - all dumped table schemes are the same as in database.")
        .StoreTrue(&IsDryRun);

    NDump::TRestoreSettings defaults;

    config.Opts->AddLongOption("restore-data", "Whether to restore data or not.")
        .DefaultValue(defaults.RestoreData_).StoreResult(&RestoreData);

    config.Opts->AddLongOption("restore-indexes", "Whether to restore indexes or not.")
        .DefaultValue(defaults.RestoreIndexes_).StoreResult(&RestoreIndexes);

    config.Opts->AddLongOption("restore-acl", "Whether to restore ACL and owner or not.")
        .DefaultValue(defaults.RestoreACL_).StoreResult(&RestoreACL);

    config.Opts->AddLongOption("skip-document-tables", "Skip Document API tables.")
        .DefaultValue(defaults.SkipDocumentTables_).StoreResult(&SkipDocumentTables)
        .Hidden(); // Deprecated

    config.Opts->AddLongOption("save-partial-result", "Do not remove partial restore result."
            " If this option is not enabled, all changes in database that have already been applied during restore"
            " will be reverted in case of error.")
        .StoreTrue(&SavePartialResult);

    config.Opts->AddLongOption("bandwidth", "Limit data upload bandwidth, bytes per second (example: 2MiB).")
        .DefaultValue("no limit")
        .Handler([this](const TString& arg) {
            UploadBandwidth = (arg == "no limit") ? "0" : arg;
        })
        .Hidden();

    config.Opts->AddLongOption("rps", "Limit requests per second (example: 100).")
        .DefaultValue("no limit")
        .Handler([this](const TString& arg) {
            UploadRps = (arg == "no limit") ? "0" : arg;
        });

    config.Opts->AddLongOption("upload-batch-rows", "Limit upload batch size in rows (example: 1K)."
            " Not applicable in ImportData mode.")
        .DefaultValue("no limit")
        .Handler([this](const TString& arg) {
            RowsPerRequest = (arg == "no limit") ? "0" : arg;
        });

    config.Opts->AddLongOption("upload-batch-rus", "Limit upload batch size in request units (example: 100)."
            " Not applicable in ImportData mode.")
        .DefaultValue("no limit")
        .Handler([this](const TString& arg) {
            RequestUnitsPerRequest = (arg == "no limit") ? "0" : arg;
        });

    config.Opts->AddLongOption("upload-batch-bytes", "Limit upload batch size in bytes (example: 1MiB).")
        .DefaultValue("auto")
        .Handler([this](const TString& arg) {
            BytesPerRequest = (arg == "auto") ? "0" : arg;
        });

    config.Opts->AddLongOption("in-flight", "Limit in-flight request count.")
        .DefaultValue("auto")
        .Handler([this](const TString& arg) {
            InFlight = (arg == "auto") ? 0 : FromString<ui32>(arg);
        });

    config.Opts->AddLongOption("bulk-upsert", "Use BulkUpsert - a more efficient way to upload data with lower consistency level."
            " Global secondary indexes are not supported in this mode.")
        .StoreTrue(&UseBulkUpsert)
        .Hidden(); // Deprecated. Using ImportData should be more effective.

    config.Opts->AddLongOption("import-data", "Use ImportData - a more efficient way to upload data."
            " ImportData will throw an error if you try to upload data into an existing table that has"
            " secondary indexes or is in the process of building them. If you need to restore a table"
            " with secondary indexes, make sure it's not already present in the scheme.")
        .StoreTrue(&UseImportData);

    config.Opts->AddLongOption("replace", "Remove existing objects from the database that match those in the backup before restoration."
        " Objects present in the backup but missing in the database are restored as usual; removal is skipped."
        " If both --replace and --verify-existence are specified, restoration stops with an error when the first such object is found.")
        .StoreTrue(&Replace);

    config.Opts->AddLongOption("verify-existence", "Use with --replace to report an error if an object in the backup is missing from the database"
        " instead of silently skipping its removal.")
        .StoreTrue(&VerifyExistence);

    config.Opts->MutuallyExclusive("bandwidth", "rps");
    config.Opts->MutuallyExclusive("import-data", "bulk-upsert");
    config.Opts->MutuallyExclusive("import-data", "upload-batch-rows");
    config.Opts->MutuallyExclusive("import-data", "upload-batch-rus");
}

void TCommandRestore::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    AdjustPath(config);
}

int TCommandRestore::Run(TConfig& config) {
    auto settings = NDump::TRestoreSettings()
        .DryRun(IsDryRun)
        .RestoreData(RestoreData)
        .RestoreIndexes(RestoreIndexes)
        .RestoreACL(RestoreACL)
        .SkipDocumentTables(SkipDocumentTables)
        .SavePartialResult(SavePartialResult)
        .RowsPerRequest(NYdb::SizeFromString(RowsPerRequest))
        .Replace(Replace)
        .VerifyExistence(VerifyExistence);

    if (InFlight) {
        settings.MaxInFlight(InFlight);
    } else if (!UseImportData) {
        settings.MaxInFlight(NSystemInfo::CachedNumberOfCpus());
    }

    if (auto bytesPerRequest = NYdb::SizeFromString(BytesPerRequest)) {
        if (UseImportData && bytesPerRequest > NDump::TRestoreSettings::MaxImportDataBytesPerRequest) {
            throw TMisuseException()
                << "--upload-batch-bytes cannot be larger than "
                << HumanReadableSize(NDump::TRestoreSettings::MaxImportDataBytesPerRequest, SF_BYTES);
        }

        settings.BytesPerRequest(bytesPerRequest);
    } else if (UseImportData) {
        settings.BytesPerRequest(NDump::TRestoreSettings::MaxImportDataBytesPerRequest);
    }

    if (RequestUnitsPerRequest) {
        settings.RequestUnitsPerRequest(NYdb::SizeFromString(RequestUnitsPerRequest));
    }

    if (auto bandwidth = NYdb::SizeFromString(UploadBandwidth)) {
        settings.RateLimiterSettings_.WithBandwidth(bandwidth, settings.BytesPerRequest_);
    } else if (auto rps = NYdb::SizeFromString(UploadRps)) {
        settings.RateLimiterSettings_.WithRps(rps);
    }

    if (UseBulkUpsert) {
        settings.Mode(NDump::TRestoreSettings::EMode::BulkUpsert);
    } else if (UseImportData) {
        settings.Mode(NDump::TRestoreSettings::EMode::ImportData);
    }

    if (VerifyExistence && !Replace) {
        throw TMisuseException()
            << "The --verify-existence option must be used together with the --replace option.";
    }

    auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel)));
    log->SetFormatter(GetPrefixLogFormatter(""));

    NDump::TClient client(CreateDriver(config), std::move(log));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.Restore(FilePath, Path, settings));

    return EXIT_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////
//  Copy
////////////////////////////////////////////////////////////////////////////////

TCommandCopy::TCommandCopy()
    : TTableCommand("copy", {}, "Copy table(s)")
{
    TItem::DefineFields({
        {"Source", {{"source", "src", "s"}, "Source table path", true}},
        {"Destination", {{"destination", "dst", "d"}, "Destination table path", true}}
    });
}

void TCommandCopy::Config(TConfig& config) {
    TTableCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("item", TItem::FormatHelp("[At least one] Item specification", config.HelpCommandVerbosiltyLevel, 2))
        .RequiredArgument("PROPERTY=VALUE,...");
}

void TCommandCopy::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    Items = TItem::Parse(config, "item");
    if (Items.empty()) {
        throw TMisuseException() << "At least one item should be provided";
    }
}

void TCommandCopy::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);

    for (auto& item : Items) {
        NConsoleClient::AdjustPath(item.Source, config);
        NConsoleClient::AdjustPath(item.Destination, config);
    }
}

int TCommandCopy::Run(TConfig& config) {
    TVector<NYdb::NTable::TCopyItem> copyItems;
    copyItems.reserve(Items.size());
    for (auto& item : Items) {
        copyItems.emplace_back(item.Source, item.Destination);
    }
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        GetSession(config).CopyTables(
            copyItems,
            FillSettings(NTable::TCopyTablesSettings())
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}


////////////////////////////////////////////////////////////////////////////////
//  Rename
////////////////////////////////////////////////////////////////////////////////

TCommandRename::TCommandRename()
    : TTableCommand("rename", {}, "Rename or replace table(s)")
{
    TItem::DefineFields({
        {"Source", {{"source", "src"}, "Source table path", true}},
        {"Destination", {{"destination", "dst"}, "Destination table path", true}},
        {"Replace", {{"replace", "force"}, "Replace destination table with source table, no replacement by default", false}}
    });
}

void TCommandRename::Config(TConfig& config) {
    AddExamplesOption(config);
    TTableCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("item", TItem::FormatHelp("[At least one] Item specification", config.HelpCommandVerbosiltyLevel, 2))
        .RequiredArgument("PROPERTY=VALUE,...");

    AddCommandExamples(
        TExampleSetBuilder()
            .BeginExample()
                .Title("Rename one table")
                .Text("ydb tools rename --item src=table_a,dst=table_b")
            .EndExample()

            .BeginExample()
                .Title("Rename using full and relative paths")
                .Text("ydb tools rename --item src=/root/db/dir/table,dst=dir/other_table")
            .EndExample()

            .BeginExample()
                .Title("Rename several tables together")
                .Text("ydb tools rename --item src=table_a,dst=table_b --item src=table_c,dst=table_d")
            .EndExample()

            .BeginExample()
                .Title("Rename tables as a chain in order to replace a table without loosing it")
                .Text("ydb tools rename --item src=prod_table,dst=backup --item src=test_table,dst=prod_table")
            .EndExample()

            .BeginExample()
                .Title("Rename tables as a replacement in order to replace a table with new one and delete older one")
                .Text("ydb tools rename --item src=test_table,dst=prod_table,replace=true")
            .EndExample()

            .Build()
       );

    CheckExamples(config);
}

void TCommandRename::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    Items = TItem::Parse(config, "item");
    if (Items.empty()) {
        throw TMisuseException() << "At least one item should be provided";
    }
}

void TCommandRename::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);

    for (auto& item : Items) {
        NConsoleClient::AdjustPath(item.Source, config);
        NConsoleClient::AdjustPath(item.Destination, config);
    }
}

int TCommandRename::Run(TConfig& config) {
    TVector<NYdb::NTable::TRenameItem> renameItems;
    renameItems.reserve(Items.size());
    for (auto& item : Items) {
        renameItems.emplace_back(item.Source, item.Destination);
        if (item.Replace) {
            renameItems.back().SetReplaceDestination();
        }
    }
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        GetSession(config).RenameTables(
            renameItems,
            FillSettings(NTable::TRenameTablesSettings())
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPgConvert::TCommandPgConvert()
    : TToolsCommand("pg-convert", {}, "Convert pg_dump result SQL file to format readable by YDB postgres layer")
{}

void TCommandPgConvert::Config(TConfig& config) {
    TToolsCommand::Config(config);
    config.NeedToConnect = false;
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('i', "input", "Path to input SQL file. Read from stdin if not specified.").StoreResult(&Path);
    config.Opts->AddLongOption("ignore-unsupported", "Comment unsupported statements in result dump file if specified.").StoreTrue(&IgnoreUnsupported);
}

int TCommandPgConvert::Run(TConfig& config) {
    Y_UNUSED(config);
    TPgDumpParser parser(Cout, IgnoreUnsupported);
    if (Path) {
        std::unique_ptr<TFileInput> fileInput = std::make_unique<TFileInput>(Path);
        parser.Prepare(*fileInput);
        fileInput = std::make_unique<TFileInput>(Path);
        parser.WritePgDump(*fileInput);
    } else {
        TFixedStringStream stream(Cin.ReadAll());
        parser.Prepare(stream);
        stream.MovePointer();
        parser.WritePgDump(stream);
    }
    return EXIT_SUCCESS;
}

} // NYdb::NConsoleClient
