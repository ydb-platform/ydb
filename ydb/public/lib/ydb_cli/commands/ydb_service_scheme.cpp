#include "ydb_service_scheme.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/describe.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/scheme_printers.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <google/protobuf/port_def.inc>

#include <util/stream/format.h>
#include <util/string/join.h>

namespace NYdb {
namespace NConsoleClient {

TCommandScheme::TCommandScheme()
    : TClientCommandTree("scheme", {}, "Scheme service operations")
{
    AddCommand(std::make_unique<TCommandMakeDirectory>());
    AddCommand(std::make_unique<TCommandRemoveDirectory>());
    AddCommand(std::make_unique<TCommandDescribe>());
    AddCommand(std::make_unique<TCommandList>());
    AddCommand(std::make_unique<TCommandPermissions>());
}

TCommandMakeDirectory::TCommandMakeDirectory()
    : TYdbOperationCommand("mkdir", std::initializer_list<TString>(), "Make directory")
{}

void TCommandMakeDirectory::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to create");
}

void TCommandMakeDirectory::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandMakeDirectory::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        client.MakeDirectory(
            Path,
            FillSettings(NScheme::TMakeDirectorySettings())
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandRemoveDirectory::TCommandRemoveDirectory()
    : TYdbOperationCommand("rmdir", std::initializer_list<TString>(), "Remove directory")
{}

void TCommandRemoveDirectory::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption('r', "recursive", "Remove directory and its content recursively. Prompt once by default")
        .StoreTrue(&Recursive);
    config.Opts->AddLongOption('f', "force", "Never prompt")
        .NoArgument().StoreValue(&Prompt, ERecursiveRemovePrompt::Never);
    config.Opts->AddCharOption('i', "Prompt before every removal")
        .NoArgument().StoreValue(&Prompt, ERecursiveRemovePrompt::Always);
    config.Opts->AddCharOption('I', "Prompt once")
        .NoArgument().StoreValue(&Prompt, ERecursiveRemovePrompt::Once);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to remove");
}

void TCommandRemoveDirectory::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandRemoveDirectory::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NScheme::TSchemeClient schemeClient(driver);
    const auto settings = FillSettings(NScheme::TRemoveDirectorySettings());

    if (Recursive) {
        const auto settings = TRemoveDirectoryRecursiveSettings()
            .Prompt(Prompt.GetOrElse(ERecursiveRemovePrompt::Once))
            .CreateProgressBar(true);
        NStatusHelpers::ThrowOnErrorOrPrintIssues(RemoveDirectoryRecursive(driver, Path, settings));
    } else {
        if (Prompt) {
            if (!NConsoleClient::Prompt(*Prompt, Path, NScheme::ESchemeEntryType::Directory)) {
                return EXIT_SUCCESS;
            }
        }
        NStatusHelpers::ThrowOnErrorOrPrintIssues(schemeClient.RemoveDirectory(Path, settings).GetValueSync());
    }

    return EXIT_SUCCESS;
}

TCommandDescribe::TCommandDescribe()
    : TYdbOperationCommand("describe", std::initializer_list<TString>(), "Show information about object at given object")
{
    OutputStream = &Cout;
}

void TCommandDescribe::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    // Common options
    config.Opts->AddLongOption("permissions", "Show owner and permissions").StoreTrue(&ShowPermissions);

    // Table options
    config.Opts->AddLongOption("partition-boundaries", "[Table] Show partition key boundaries").StoreTrue(&ShowKeyShardBoundaries)
        .AddLongName("shard-boundaries");
    config.Opts->AddLongOption("stats", "[Table|Topic|Replication] Show table/topic/replication statistics").StoreTrue(&ShowStats);
    config.Opts->AddLongOption("partition-stats", "[Table|Topic|Consumer] Show partition statistics").StoreTrue(&ShowPartitionStats);

    AddDeprecatedJsonOption(config, "(Deprecated, will be removed soon. Use --format option instead) [Table] Output in json format");
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to an object to describe. If object is topic consumer, it must be specified as <topic_path>/<consumer_name>");
}

void TCommandDescribe::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    Database = config.Database;
    ParseOutputFormats();
}

void TCommandDescribe::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandDescribe::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    TDescribeOptions options;
    options.ShowPermissions = ShowPermissions;
    options.ShowKeyShardBoundaries = ShowKeyShardBoundaries;
    options.ShowStats = ShowStats;
    options.ShowPartitionStats = ShowPartitionStats;
    options.Database = Database;

    TDescribeLogic describeLogic(driver, *OutputStream);
    return describeLogic.Describe(Path, options, OutputFormat);
}

TCommandList::TCommandList()
    : TYdbOperationCommand("ls", std::initializer_list<TString>(), "Show information about objects inside given directory")
{}

void TCommandList::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.Opts->AddCharOption('l', "List objects with detailed information")
        .StoreTrue(&AdvancedMode);
    config.Opts->AddCharOption('R', "List subdirectories recursively")
        .StoreTrue(&Recursive);
    config.Opts->AddCharOption('1', "List one object per line")
        .StoreTrue(&FromNewLine);
    config.Opts->AddCharOption('m', "Multithread recursive request")
        .StoreTrue(&Multithread);
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::Json });
    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<path>", "Path to list");
}

void TCommandList::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    if (AdvancedMode && FromNewLine) {
        // TODO: add "consider using --format shell"
        throw TMisuseException() << "Options -1 and -l are incompatible";
    }
}

void TCommandList::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0, true);
}

int TCommandList::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    ISchemePrinter::TSettings settings = {
        Path,
        Recursive,
        Multithread,
        FromNewLine,
        FillSettings(NScheme::TListDirectorySettings()),
        FillSettings(NTable::TDescribeTableSettings().WithTableStatistics(true))
    };
    std::unique_ptr<ISchemePrinter> printer;

    switch (OutputFormat) {
    case EDataFormat::Default:
    case EDataFormat::Pretty:
        if (AdvancedMode) {
            printer = std::make_unique<TTableSchemePrinter>(driver, std::move(settings));
        } else {
            printer = std::make_unique<TDefaultSchemePrinter>(driver, std::move(settings));
        }
        break;
    case EDataFormat::Json:
    {
        printer = std::make_unique<TJsonSchemePrinter>(driver, std::move(settings), AdvancedMode);
        break;
    }
    default:
        throw TMisuseException() << "This command doesn't support " << OutputFormat << " output format";
    }
    printer->Print();
    return EXIT_SUCCESS;
}

TCommandPermissions::TCommandPermissions()
    : TClientCommandTree("permissions", {}, "Modify permissions")
{
    AddCommand(std::make_unique<TCommandPermissionGrant>());
    AddCommand(std::make_unique<TCommandPermissionRevoke>());
    AddCommand(std::make_unique<TCommandPermissionSet>());
    AddCommand(std::make_unique<TCommandChangeOwner>());
    AddCommand(std::make_unique<TCommandPermissionClear>());
    AddCommand(std::make_unique<TCommandPermissionSetInheritance>());
    AddCommand(std::make_unique<TCommandPermissionClearInheritance>());
    AddCommand(std::make_unique<TCommandPermissionList>());
}

TCommandPermissionGrant::TCommandPermissionGrant()
    : TYdbOperationCommand("grant", { "add" }, "Grant permission")
{}

void TCommandPermissionGrant::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(2);
    SetFreeArgTitle(0, "<path>", "Path to grant permissions to");
    SetFreeArgTitle(1, "<subject>", "Subject to grant permissions");

    config.Opts->AddLongOption('p', "permission", "[At least one] Permission(s) to grant")
        .DocLink("ydb.tech/docs/en/yql/reference/syntax/grant")
        .RequiredArgument("NAME").AppendTo(&PermissionsToGrant);
}

void TCommandPermissionGrant::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    Subject = config.ParseResult->GetFreeArgs()[1];
    if (Subject.empty()) {
        throw TMisuseException() << "Missing required argument <subject>";
    }
    if (!PermissionsToGrant.size()) {
        throw TMisuseException() << "At least one permission to grant should be provided";
    }
}

void TCommandPermissionGrant::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandPermissionGrant::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddGrantPermissions({ Subject, PermissionsToGrant })
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionRevoke::TCommandPermissionRevoke()
    : TYdbOperationCommand("revoke", { "remove" }, "Revoke permission")
{}

void TCommandPermissionRevoke::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(2);
    SetFreeArgTitle(0, "<path>", "Path to revoke permissions to");
    SetFreeArgTitle(1, "<subject>", "Subject to revoke permissions");

    config.Opts->AddLongOption('p', "permission", "[At least one] Permission(s) to revoke")
        .DocLink("ydb.tech/docs/en/yql/reference/syntax/revoke")
        .RequiredArgument("NAME").AppendTo(&PermissionsToRevoke);
}

void TCommandPermissionRevoke::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    Subject = config.ParseResult->GetFreeArgs()[1];
    if (Subject.empty()) {
        throw TMisuseException() << "Missing required argument <subject>";
    }
    if (!PermissionsToRevoke.size()) {
        throw TMisuseException() << "At least one permission to revoke should be provided";
    }
}

void TCommandPermissionRevoke::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandPermissionRevoke::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddRevokePermissions({ Subject, PermissionsToRevoke })
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionSet::TCommandPermissionSet()
    : TYdbOperationCommand("set", std::initializer_list<TString>(), "Set permissions")
{}

void TCommandPermissionSet::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(2);
    SetFreeArgTitle(0, "<path>", "Path to set permissions to");
    SetFreeArgTitle(1, "<subject>", "Subject to set permissions");

    config.Opts->AddLongOption('p', "permission", "[At least one] Permission(s) to set")
        .DocLink("ydb.tech/docs/en/yql/reference/syntax/grant")
        .RequiredArgument("NAME").AppendTo(&PermissionsToSet);
}

void TCommandPermissionSet::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    Subject = config.ParseResult->GetFreeArgs()[1];
    if (Subject.empty()) {
        throw TMisuseException() << "Missing required argument <subject>";
    }
    if (!PermissionsToSet.size()) {
        throw TMisuseException() << "At least one permission to set should be provided";
    }
}

void TCommandPermissionSet::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandPermissionSet::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddSetPermissions({ Subject, PermissionsToSet })
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandChangeOwner::TCommandChangeOwner()
    : TYdbOperationCommand("chown", std::initializer_list<TString>(), "Change owner")
{}

void TCommandChangeOwner::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(2);
    SetFreeArgTitle(0, "<path>", "Path to change owner for");
    SetFreeArgTitle(1, "<owner>", "Owner to set");
}

void TCommandChangeOwner::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    Owner = config.ParseResult->GetFreeArgs()[1];
    if (!Owner){
        throw TMisuseException() << "Missing required argument <owner>";
    }
}

void TCommandChangeOwner::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandChangeOwner::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddChangeOwner(Owner)
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionClear::TCommandPermissionClear()
    : TYdbOperationCommand("clear", std::initializer_list<TString>(), "Clear permissions")
{}

void TCommandPermissionClear::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to clear permissions to");
}

void TCommandPermissionClear::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandPermissionClear::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddClearAcl()
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionSetInheritance::TCommandPermissionSetInheritance()
    : TYdbOperationCommand("set-inheritance", std::initializer_list<TString>(), "Set to inherit permissions from the parent")
{}

void TCommandPermissionSetInheritance::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to set interrupt-inheritance flag for");
}

void TCommandPermissionSetInheritance::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandPermissionSetInheritance::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddInterruptInheritance(false)
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionClearInheritance::TCommandPermissionClearInheritance()
    : TYdbOperationCommand("clear-inheritance", std::initializer_list<TString>(), "Set to do not inherit permissions from the parent")
{}

void TCommandPermissionClearInheritance::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to set interrupt-inheritance flag for");
}

void TCommandPermissionClearInheritance::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandPermissionClearInheritance::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddInterruptInheritance(true)
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionList::TCommandPermissionList()
    : TYdbOperationCommand("list", std::initializer_list<TString>(), "List permissions")
{}

void TCommandPermissionList::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to list permissions for");
}

void TCommandPermissionList::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    ParsePath(config, 0);
}

int TCommandPermissionList::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NScheme::TSchemeClient client(driver);
    NScheme::TDescribePathResult result = client.DescribePath(
        Path,
        FillSettings(NScheme::TDescribePathSettings())
    ).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    NScheme::TSchemeEntry entry = result.GetEntry();
    Cout << Endl;
    PrintAllPermissions(entry.Owner, entry.Permissions, entry.EffectivePermissions);
    return EXIT_SUCCESS;
}

}
}
