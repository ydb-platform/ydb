#include "ydb_tools.h"

#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/pg_dump_parser.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/library/backup/backup.h>
#include <ydb/library/backup/util.h>

#include <util/stream/format.h>
#include <util/string/split.h>

#include <algorithm>
#include <queue>

namespace NYdb::NConsoleClient {

TCommandTools::TCommandTools()
    : TClientCommandTree("tools", {}, "YDB tools service")
{
    AddCommand(std::make_unique<TCommandDump>());
    AddCommand(std::make_unique<TCommandRestore>());
    AddCommand(std::make_unique<TCommandCopy>());
    AddCommand(std::make_unique<TCommandRename>());
    AddCommand(std::make_unique<TCommandPgConvert>());
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
        .RequiredArgument("STRING").Handler1T<TString>([this](const TString& arg) {
            ExclusionPatterns.emplace_back(TRegExMatch(arg));
        });
    config.Opts->AddLongOption('o', "output", "[Required] Path in a local filesystem to a directory to place dump into."
            " Directory should either not exist or be empty.")
        .StoreResult(&FilePath);
    config.Opts->AddLongOption("scheme-only", "Dump only scheme")
        .StoreTrue(&IsSchemeOnly);
    config.Opts->AddLongOption("avoid-copy", "Avoid copying."
            " By default, YDB makes a copy of a table before dumping it to reduce impact on workload and ensure consistency.\n"
            "In some cases (e.g. for tables with external blobs) copying should be disabled.")
        .StoreTrue(&AvoidCopy);
    config.Opts->AddLongOption("save-partial-result", "Do not remove partial dump result."
            " If this option is not enabled, all files that have already been created will be removed in case of error.")
        .StoreTrue(&SavePartialResult);
    config.Opts->AddLongOption("preserve-pool-kinds", "Preserve storage pool kind settings."
            " If this option is enabled, storage pool kind will be saved to dump."
            " In this case, if there will be no such storage pool kind in database on restore, error will occur."
            " By default this option is disabled and any existing storage pool kind will be used on restore.")
        .StoreTrue(&PreservePoolKinds);
    config.Opts->AddLongOption("consistency-level", "Consistency level."
            " Options: database, table\n"
            "database - take one consistent snapshot of all tables specified for dump."
            " Takes more time and is more likely to impact workload;\n"
            "table - take consistent snapshot per each table independently.")
        .DefaultValue("database").StoreResult(&ConsistencyLevel);
    config.Opts->AddLongOption("ordered", "Preserve order by primary key in backup files.")
            .StoreTrue(&Ordered);
    config.Opts->AddLongOption("preserve-acl", "Preserve ACL including owner."
            " If this option is enabled, ACL and owner will be saved to dump."
            " In this case, the dump may not be restored if it is restored with an older version of the CLI."
            " By default this option is disabled and the user who will perform the restore will be used"
            " as the owner of the restored objects")
        .StoreTrue(&PreserveACL);
}

void TCommandDump::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    AdjustPath(config);
}

int TCommandDump::Run(TConfig& config) {

    bool useConsistentCopyTable;
    if (ConsistencyLevel == "database") {
        useConsistentCopyTable = true;
    } else if (ConsistencyLevel == "table") {
        useConsistentCopyTable = false;
    } else {
        throw yexception() << "Incorrect consistency level. Available options: \"database\", \"table\"" << Endl;
    }

    NYdb::SetVerbosity(config.IsVerbose());

    try {
        TString relPath = NYdb::RelPathFromAbsolute(config.Database, Path);
        NYdb::NBackup::BackupFolder(CreateDriver(config), config.Database, relPath, FilePath, ExclusionPatterns,
            IsSchemeOnly, useConsistentCopyTable, AvoidCopy, SavePartialResult, PreservePoolKinds, Ordered, PreserveACL);
    } catch (const NYdb::NBackup::TYdbErrorException& e) {
        e.LogToStderr();
        return EXIT_FAILURE;
    } catch (const yexception& e) {
        Cerr << "General error, what# " << e.what() << Endl;
        return EXIT_FAILURE;
    }
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

    config.Opts->AddLongOption("restore-data", "Whether to restore data or not")
        .DefaultValue(defaults.RestoreData_).StoreResult(&RestoreData);

    config.Opts->AddLongOption("restore-indexes", "Whether to restore indexes or not")
        .DefaultValue(defaults.RestoreIndexes_).StoreResult(&RestoreIndexes);

    config.Opts->AddLongOption("skip-document-tables", TStringBuilder()
            << "Document API tables cannot be restored for now. "
            << "Specify this option to skip such tables")
        .DefaultValue(defaults.SkipDocumentTables_).StoreResult(&SkipDocumentTables)
        .Hidden(); // Deprecated

    config.Opts->AddLongOption("save-partial-result", "Do not remove partial restore result."
            " If this option is not enabled, all changes in database that have already been applied during restore"
            " will be reverted in case of error.")
        .StoreTrue(&SavePartialResult);

    config.Opts->AddLongOption("bandwidth", "Limit data upload bandwidth, bytes per second (example: 2MiB)")
        .DefaultValue("0").StoreResult(&UploadBandwidth);

    config.Opts->AddLongOption("rps", "Limit requests per second (example: 100)")
        .DefaultValue(defaults.RateLimiterSettings_.GetRps()).StoreResult(&UploadRps);

    config.Opts->AddLongOption("upload-batch-rows", "Limit upload batch size in rows (example: 1K)")
        .DefaultValue(defaults.RowsPerRequest_).StoreResult(&RowsPerRequest);

    config.Opts->AddLongOption("upload-batch-bytes", "Limit upload batch size in bytes (example: 1MiB)")
        .DefaultValue(HumanReadableSize(defaults.BytesPerRequest_, SF_BYTES)).StoreResult(&BytesPerRequest);

    config.Opts->AddLongOption("upload-batch-rus", "Limit upload batch size in request units (example: 100)")
        .DefaultValue(defaults.RequestUnitsPerRequest_).StoreResult(&RequestUnitsPerRequest);

    config.Opts->AddLongOption("in-flight", "Limit in-flight request count")
        .DefaultValue(defaults.InFly_).StoreResult(&InFly);

    config.Opts->AddLongOption("bulk-upsert", "Use BulkUpsert - a more efficient way to upload data with lower consistency level."
        " Global secondary indexes are not supported in this mode.")
        .StoreTrue(&UseBulkUpsert)
        .Hidden(); // Deprecated. Using ImportData should be more effective.

    config.Opts->AddLongOption("import-data", "Use ImportData - a more efficient way to upload data with lower consistency level."
        " Global secondary indexes are not supported in this mode.")
        .StoreTrue(&UseImportData);

    config.Opts->MutuallyExclusive("bandwidth", "rps");
    config.Opts->MutuallyExclusive("import-data", "bulk-upsert");
}

void TCommandRestore::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    AdjustPath(config);
}

int TCommandRestore::Run(TConfig& config) {
    NYdb::SetVerbosity(config.IsVerbose());

    auto settings = NDump::TRestoreSettings()
        .DryRun(IsDryRun)
        .RestoreData(RestoreData)
        .RestoreIndexes(RestoreIndexes)
        .SkipDocumentTables(SkipDocumentTables)
        .SavePartialResult(SavePartialResult)
        .RowsPerRequest(NYdb::SizeFromString(RowsPerRequest))
        .InFly(InFly);

    if (auto bytesPerRequest = NYdb::SizeFromString(BytesPerRequest)) {
        if (bytesPerRequest > NDump::TRestoreSettings::MaxBytesPerRequest) {
            throw TMisuseException()
                << "--upload-batch-bytes cannot be larger than "
                << HumanReadableSize(NDump::TRestoreSettings::MaxBytesPerRequest, SF_BYTES);
        }

        settings.BytesPerRequest(bytesPerRequest);
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

    NDump::TClient client(CreateDriver(config));
    ThrowOnError(client.Restore(FilePath, Path, settings));

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

    TStringBuilder itemHelp;
    itemHelp << "[At least one] Item specification" << Endl
        << "  Possible property names:" << Endl
        << TItem::FormatHelp(2);
    config.Opts->AddLongOption("item", itemHelp)
        .RequiredArgument("PROPERTY=VALUE,...");
}

void TCommandCopy::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    Items = TItem::Parse(config, "item");
    if (Items.empty()) {
        throw TMisuseException() << "At least one item should be provided";
    }

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
    ThrowOnError(
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

    TStringBuilder itemHelp;
    itemHelp << "[At least one] Item specification" << Endl
        << "  Possible property names:" << Endl
        << TItem::FormatHelp(2);
    config.Opts->AddLongOption("item", itemHelp)
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
    ThrowOnError(
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

void TCommandPgConvert::Parse(TConfig& config) {
    TToolsCommand::Parse(config);
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
