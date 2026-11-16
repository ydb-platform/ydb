#include "ydb_bridge.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NYdb::NConsoleClient {

namespace {
    NYdb::NBridge::EPileState ParsePileState(TString stateStr) {
        TString stateStrUpper = stateStr;
        stateStrUpper.to_upper();

        if (stateStrUpper == "DISCONNECTED") {
            return NYdb::NBridge::EPileState::DISCONNECTED;
        }
        if (stateStrUpper == "NOT_SYNCHRONIZED") {
            return NYdb::NBridge::EPileState::NOT_SYNCHRONIZED;
        }
        if (stateStrUpper == "SYNCHRONIZED") {
            return NYdb::NBridge::EPileState::SYNCHRONIZED;
        }
        if (stateStrUpper == "PROMOTED") {
            return NYdb::NBridge::EPileState::PROMOTED;
        }
        if (stateStrUpper == "PRIMARY") {
            return NYdb::NBridge::EPileState::PRIMARY;
        }
        if (stateStrUpper == "SUSPENDED") {
            return NYdb::NBridge::EPileState::SUSPENDED;
        }
        ythrow yexception() << "Invalid pile state: \"" << stateStr
            << "\". Please use one of: DISCONNECTED, NOT_SYNCHRONIZED, SYNCHRONIZED, PROMOTED, PRIMARY, SUSPENDED.";
    }
}

TCommandBridge::TCommandBridge(bool allowEmptyDatabase)
    : TClientCommandTree("bridge", {}, "Manage cluster in bridge mode")
{
    AddHiddenCommand(std::make_unique<TCommandBridgeUpdate>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeList>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeSwitchover>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeFailover>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeTakedown>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeJoinPile>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeConvert>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeProvisionPile>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeDestroyPile>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeRollback>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeSetDatabaseMode>());
}

TCommandBridgeSetDatabaseMode::TCommandBridgeSetDatabaseMode()
    : TYdbCommand("set-database-mode", {}, "Change bridge mode of the selected database. "
        "Requires bridge_config.enable_non_uniform_storage=true. For serverless databases, select their shared database; "
        "the change affects all its serverless databases. Existing data is preserved.")
{
    CompletionDescription = "Change bridge mode of the database";
}

void TCommandBridgeSetDatabaseMode::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.AllowEmptyDatabase = true;
    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<mode>", "bridged: enable bridge replication across all provisioned piles; "
        "data synchronization continues in the background.\n"
        "ordinary: disable bridge replication; redundant copies are deleted in the background.");
    config.Opts->GetOpts().GetFreeArgSpec(0)
        .Completer(NLastGetopt::NComp::Choice({{"bridged", "Enable bridge replication"},
                                             {"ordinary", "Disable bridge replication"}}));
    config.Opts->AddLongOption("database-path", "Absolute path of the database to change. "
            "The global --database option selects the connection database.")
        .Required().RequiredArgument("PATH").StoreResult(&DatabasePath);
    config.Opts->AddLongOption("retain-pile", "With mode ordinary, keep data in this pile, which must already have synchronized copies. "
            "By default, select copies automatically; they may be in different piles. Does not relocate existing ordinary data.")
        .RequiredArgument("PILE_NAME").StoreResult(&RetainPile);
}

void TCommandBridgeSetDatabaseMode::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    const auto mode = config.ParseResult->GetFreeArgs()[0];
    if (mode != "bridged" && mode != "ordinary") {
        throw TMisuseException() << "Invalid mode \"" << mode << "\". Use bridged or ordinary.";
    }
    Bridged = mode == "bridged";
    if (config.ParseResult->Has("retain-pile") && Bridged) {
        throw TMisuseException() << "--retain-pile requires mode ordinary.";
    }
}

int TCommandBridgeSetDatabaseMode::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NBridge::TBridgeClient client(driver);
    auto result = client.SetDatabaseBridgeMode(DatabasePath, Bridged, NBridge::TSetDatabaseBridgeModeSettings()
        .RetainPileName(RetainPile)).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    Cout << "Database bridge mode updated successfully. Data synchronization and cleanup continue in the background." << Endl;
    return EXIT_SUCCESS;
}

TCommandBridgeUpdate::TCommandBridgeUpdate(bool allowEmptyDatabase)
    : TYdbCommand("update", {}, "Update cluster state in bridge mode")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
}

void TCommandBridgeUpdate::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("set", "Set new state for a pile. Format: <pile-name>:<state>. Can be used multiple times.")
        .RequiredArgument("NAME:STATE").Handler([this](const TString& value) {
            PileStateUpdates.push_back(value);
        });
    config.Opts->AddLongOption('f', "file", "Path to a JSON file with state updates.")
        .RequiredArgument("PATH").StoreResult(&FilePath);
    config.Opts->MutuallyExclusive("set", "file");
    config.Opts->AddLongOption("expected-generation",
            "Apply the update only if the cluster state still has this generation. "
            "Otherwise the update is rejected with ABORTED and not applied.")
        .RequiredArgument("N").Handler([this](const TString& value) {
            ExpectedGeneration = FromString<ui64>(value);
        });

    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeUpdate::Parse(TConfig& config) {
    TYdbCommand::Parse(config);

    if (PileStateUpdates.empty() && FilePath.empty()) {
        ythrow yexception() << "Either --set or --file must be specified.";
    }

    if (!PileStateUpdates.empty()) {
        for (const auto& updateStr : PileStateUpdates) {
            TStringBuf pileNameStr, stateStr;
            if (!TStringBuf(updateStr).TrySplit(':', pileNameStr, stateStr) || pileNameStr.empty() || stateStr.empty()) {
                ythrow yexception() << "Invalid format for --set option. Expected '<pile-name>:<state>'.";
            }
            NYdb::NBridge::TPileStateUpdate update;
            update.PileName = pileNameStr;
            update.State = ParsePileState(TString(stateStr));
            Updates.push_back(update);
        }
    }

    if (!FilePath.empty()) {
        TString jsonStr = TFileInput(FilePath).ReadAll();
        NJson::TJsonValue jsonValue;
        if (!NJson::ReadJsonTree(jsonStr, &jsonValue)) {
            ythrow yexception() << "Failed to parse JSON from file \"" << FilePath << "\"";
        }
        if (!jsonValue.IsArray()) {
            ythrow yexception() << "Root of the JSON document must be an array. File should start with '['. "
                << "Location: " << FilePath;
        }

        for (const auto& item : jsonValue.GetArray()) {
            if (!item.IsMap() || !item.Has("pile_name") || !item.Has("state")) {
                ythrow yexception() << "Invalid object in JSON array: each item must be an object with string \"state\" and string \"pile_name\" keys.";
            }
            NYdb::NBridge::TPileStateUpdate update;
            update.PileName = item["pile_name"].GetString();
            update.State = ParsePileState(item["state"].GetString());
            Updates.push_back(update);
        }
    }
}

int TCommandBridgeUpdate::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    NYdb::NBridge::TUpdateClusterStateSettings settings;
    if (ExpectedGeneration) {
        settings.ExpectedGeneration(*ExpectedGeneration);
    }
    auto result = client.UpdateClusterState(Updates, {}, settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeRollback::TCommandBridgeRollback(bool allowEmptyDatabase)
    : TYdbCommand("rollback", {}, "Roll back bridge conversion.\n"
        "Requires the original pile to be the only pile and all bridged storage groups to be inline in it. "
        "Storage groups retain their IDs and data. Monitor progress with \"ydb admin cluster bridge list piles\". "
        "Wait for storage rollback to finish before removing bridge_config and hosts' bridge_pile_name in one configuration change. "
        "Then restart all nodes without bridge configuration. Start dynamic nodes without --bridge-pile-name. "
        "The report covers static and connected dynamic nodes.")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
    CompletionDescription = "Roll a converted cluster back to ordinary mode";
}

void TCommandBridgeRollback::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption("skip-bsc-validation",
            "Don't ask the storage controller whether it can make its groups ordinary ones first. Use it only when the "
            "storage controller can't start; it reports what blocks it then.")
        .StoreTrue(&SkipBscValidation);
}

void TCommandBridgeRollback::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

bool TCommandBridgeRollback::Prompt(TConfig& config) {
    if (!config.AssumeYes) {
        NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
        Cout << colors.RedColor() << "The cluster will stop being a bridged one; piles can't be added to it until it is "
            "converted again." << colors.OldColor() << " Use \"--assume-yes\" (\"-y\") option to proceed without "
            "prompting." << Endl;
        return AskYesOrNo("Do you want to roll the bridged cluster back?", /* defaultAnswer */ false);
    }
    return true;
}

int TCommandBridgeRollback::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    auto result = client.Rollback(NYdb::NBridge::TRollbackSettings()
        .SkipBscValidation(SkipBscValidation)).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Rollback started. Monitor progress with \"ydb admin cluster bridge list piles\"." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeSwitchover::TCommandBridgeSwitchover(bool allowEmptyDatabase)
    : TYdbCommand("switchover", {}, "Perform a planned, graceful switchover to a new primary pile")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
}

void TCommandBridgeSwitchover::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("new-primary", "Name of the pile to become the new primary.")
        .Required().RequiredArgument("PILE_NAME").StoreResult(&NewPrimaryPile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeSwitchover::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeSwitchover::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    std::vector<NYdb::NBridge::TPileStateUpdate> updates;
    updates.push_back({NewPrimaryPile, NYdb::NBridge::EPileState::PROMOTED});

    auto result = client.UpdateClusterState(updates, {}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeFailover::TCommandBridgeFailover(bool allowEmptyDatabase)
    : TYdbCommand("failover", {}, "Perform an emergency failover when one or more piles become unresponsive")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
}

void TCommandBridgeFailover::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("pile", "Name of a pile that is down. Can be used multiple times.")
        .Required().RequiredArgument("PILE_NAME").AppendTo(&DownPiles);
    config.Opts->AddLongOption("new-primary", "Name of the pile to become the new primary (optional, only if the target pile was the primary).")
        .Optional().RequiredArgument("PILE_NAME").StoreResult(&NewPrimaryPile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeFailover::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeFailover::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    std::vector<NYdb::NBridge::TPileStateUpdate> updates;
    for (const auto& pile : DownPiles) {
        updates.push_back({pile, NYdb::NBridge::EPileState::DISCONNECTED});
    }
    if (!NewPrimaryPile.empty()) {
        updates.push_back({NewPrimaryPile, NYdb::NBridge::EPileState::PRIMARY});
    }

    auto result = client.UpdateClusterState(updates, {}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeTakedown::TCommandBridgeTakedown(bool allowEmptyDatabase)
    : TYdbCommand("takedown", {}, "Gracefully take a pile down for maintenance")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
}

void TCommandBridgeTakedown::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("pile", "Name of the pile to take down.")
        .Required().RequiredArgument("PILE_NAME").StoreResult(&DownPile);
    config.Opts->AddLongOption("new-primary", "Name of the pile to become the new primary (optional, only if the target pile was the primary).")
        .Optional().RequiredArgument("PILE_NAME").StoreResult(&NewPrimaryPile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeTakedown::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeTakedown::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    std::vector<NYdb::NBridge::TPileStateUpdate> updates;
    updates.push_back({DownPile, NYdb::NBridge::EPileState::SUSPENDED});
    if (!NewPrimaryPile.empty()) {
        updates.push_back({NewPrimaryPile, NYdb::NBridge::EPileState::PROMOTED});
    }

    auto result = client.UpdateClusterState(updates, {}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeJoinPile::TCommandBridgeJoinPile(bool allowEmptyDatabase)
    : TYdbCommand("join-pile", {"rejoin"}, "Join a provisioned pile or rejoin one after maintenance or recovery. "
        "Starts synchronization in the background; use bridge list to check when the pile becomes SYNCHRONIZED.")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
    CompletionDescription = "Join or rejoin a pile and start synchronization";
}

void TCommandBridgeJoinPile::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("pile", "Name of the pile to join or rejoin.")
        .Required().RequiredArgument("PILE_NAME").StoreResult(&Pile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeJoinPile::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeJoinPile::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    std::vector<NYdb::NBridge::TPileStateUpdate> updates;
    updates.push_back({Pile, NYdb::NBridge::EPileState::NOT_SYNCHRONIZED});

    auto result = client.UpdateClusterState(updates, {}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeConvert::TCommandBridgeConvert(bool allowEmptyDatabase)
    : TYdbCommand("convert", {}, "Convert a non-bridged cluster into a bridged one with a single pile. All nodes must "
        "already run with a single-pile bridge config. By default, existing storage pools and their groups are "
        "converted in the background.")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
    CompletionDescription = "Convert a non-bridged cluster into a bridged one with a single pile";
}

void TCommandBridgeConvert::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption("skip-storage-pool-conversion",
            "Leave existing storage pools and their groups non-bridged. They will not be replicated to added piles "
            "until converted separately (ydb admin cluster bridge set-database-mode bridged --database-path PATH). "
            "Static storage groups are still converted to bridge mode. Requires bridge_config.enable_non_uniform_storage=true.")
        .StoreTrue(&SkipStoragePoolConversion);
}

void TCommandBridgeConvert::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeConvert::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    auto result = client.ConvertToBridge(NYdb::NBridge::TConvertToBridgeSettings()
        .SkipStoragePoolConversion(SkipStoragePoolConversion)).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster converted to bridge mode successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeProvisionPile::TCommandBridgeProvisionPile(bool allowEmptyDatabase)
    : TYdbCommand("provision-pile", {}, "Prepare storage in an INACTIVE pile listed in the bridge config with its hosts, "
        "then set it to DISCONNECTED. Run join-pile to start synchronization.")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
}

void TCommandBridgeProvisionPile::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("pile", "Name of the pile to provision.")
        .Required().RequiredArgument("PILE_NAME").StoreResult(&Pile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeProvisionPile::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeProvisionPile::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    auto result = client.AddPile(Pile).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Pile provisioned successfully. Use join-pile to join it to the cluster." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeDestroyPile::TCommandBridgeDestroyPile(bool allowEmptyDatabase)
    : TYdbCommand("destroy-pile", {}, "Delete storage-group parts and data in a DISCONNECTED pile, then set it to INACTIVE. "
        "This cannot be undone. Waits for removal to complete; afterwards, the pile and its hosts can be removed from the bridge config.")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
    CompletionDescription = "Destroy a DISCONNECTED pile along with ALL ITS DATA";
}

void TCommandBridgeDestroyPile::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("pile", "Name of the pile to destroy.")
        .Required().RequiredArgument("PILE_NAME").StoreResult(&Pile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeDestroyPile::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

bool TCommandBridgeDestroyPile::Prompt(TConfig& config) {
    if (!config.AssumeYes) {
        NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
        Cout << colors.RedColor() << "All storage-group parts and data in pile " << Pile << " will be deleted. "
            "This cannot be undone." << colors.OldColor() << " Use \"--assume-yes\" (\"-y\") option to "
            "proceed without prompting." << Endl;
        return AskYesOrNo("Do you want to destroy pile " + Pile + "?", /* defaultAnswer */ false);
    }
    return true;
}

int TCommandBridgeDestroyPile::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    auto result = client.RemovePile(Pile).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Pile destroyed successfully, it is INACTIVE now." << Endl;

    return EXIT_SUCCESS;
}

}
