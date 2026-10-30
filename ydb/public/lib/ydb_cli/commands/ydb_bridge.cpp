#include "ydb_bridge.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/public/lib/ydb_cli/common/pretty_table.h>

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

    TString PileStateToString(NYdb::NBridge::EPileState state) {
        switch (state) {
            case NYdb::NBridge::EPileState::UNSPECIFIED: return "UNSPECIFIED";
            case NYdb::NBridge::EPileState::DISCONNECTED: return "DISCONNECTED";
            case NYdb::NBridge::EPileState::NOT_SYNCHRONIZED: return "NOT_SYNCHRONIZED";
            case NYdb::NBridge::EPileState::SYNCHRONIZED: return "SYNCHRONIZED";
            case NYdb::NBridge::EPileState::PROMOTED: return "PROMOTED";
            case NYdb::NBridge::EPileState::PRIMARY: return "PRIMARY";
            case NYdb::NBridge::EPileState::SUSPENDED: return "SUSPENDED";
            case NYdb::NBridge::EPileState::INACTIVE: return "INACTIVE";
        }
        return "UNKNOWN";
    }

    TString PendingPileOperationKindToString(NYdb::NBridge::TPendingPileOperation::EKind kind) {
        switch (kind) {
            case NYdb::NBridge::TPendingPileOperation::EKind::UNSPECIFIED: return "UNSPECIFIED";
            case NYdb::NBridge::TPendingPileOperation::EKind::ADD: return "ADD";
            case NYdb::NBridge::TPendingPileOperation::EKind::REMOVE: return "REMOVE";
        }
        return "UNKNOWN";
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
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption("target-database", "Absolute path of the database whose bridge mode to change. "
            "Independent of the global --database connection and authentication context.")
        .Required().RequiredArgument("PATH").StoreResult(&TargetDatabase);
    config.Opts->AddLongOption("bridged", "Replicate database data across active piles; synchronization continues in the background.")
        .StoreTrue(&Bridged);
    config.Opts->AddLongOption("ordinary", "Disable replication between piles; redundant data copies are deleted in the background.")
        .StoreTrue(&Ordinary);
    config.Opts->MutuallyExclusive("bridged", "ordinary");
    config.Opts->AddLongOption("retain-pile", "With --ordinary, retain data copies in this pile. "
            "By default, copies to retain are selected automatically.")
        .RequiredArgument("NAME").StoreResult(&RetainPile);
}

void TCommandBridgeSetDatabaseMode::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    if (Bridged == Ordinary) {
        ythrow yexception() << "Specify exactly one of --bridged or --ordinary.";
    }
    if (config.ParseResult->Has("retain-pile") && !Ordinary) {
        ythrow yexception() << "--retain-pile requires --ordinary.";
    }
}

int TCommandBridgeSetDatabaseMode::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NBridge::TBridgeClient client(driver);
    auto result = client.SetDatabaseBridgeMode(TargetDatabase, Bridged, NBridge::TSetDatabaseBridgeModeSettings()
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
    config.Opts->AddLongOption("quorum-pile", "Deprecated and ignored: the update is committed by the quorum of every pile"
            " taking part in the configuration quorum in the new state; set an unavailable pile DISCONNECTED instead.")
        .RequiredArgument("NAME").Handler([this](const TString& value) {
            QuorumPiles.push_back(value);
        });
    config.Opts->AddLongOption('f', "file", "Path to a JSON file with state updates.")
        .RequiredArgument("PATH").StoreResult(&FilePath);
    config.Opts->MutuallyExclusive("set", "file");

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

    if (!QuorumPiles.empty()) {
        Cerr << "Warning: --quorum-pile is deprecated and ignored." << Endl;
    }

    auto result = client.UpdateClusterState(Updates, {}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeList::TCommandBridgeList(bool allowEmptyDatabase)
    : TYdbReadOnlyCommand("list", {}, "List state of each pile in bridge mode")
    , AllowEmptyDatabase(allowEmptyDatabase)
{}

void TCommandBridgeList::Config(TConfig& config) {
    TYdbReadOnlyCommand::Config(config);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::Json,
        EDataFormat::Csv
    });
    config.Opts->AddLongOption("detailed", "Merge and show detailed state from all piles. Useful for diagnosing inconsistencies, e.g. after a split-brain scenario.")
        .StoreTrue(&Detailed);
}

void TCommandBridgeList::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandBridgeList::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    auto client = NYdb::NBridge::TBridgeClient(driver);

    // TODO(mregrock): support detailed
    auto result = client.GetClusterState().GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& state = result.GetState();

    switch (OutputFormat) {
        case EDataFormat::Json: {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("generation", result.GetGeneration());
            NJson::TJsonValue piles(NJson::JSON_ARRAY);
            for (const auto& s : state) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("pile_name", s.PileName);
                item.InsertValue("state", PileStateToString(s.State));
                piles.AppendValue(item);
            }
            json.InsertValue("piles", piles);
            json.InsertValue("conversion_in_progress", result.IsConversionInProgress());
            if (const auto& op = result.GetPendingPileOperation()) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("pile_name", op->PileName);
                item.InsertValue("kind", PendingPileOperationKindToString(op->Kind));
                item.InsertValue("error_reason", op->ErrorReason);
                json.InsertValue("pending_pile_operation", item);
            }
            NJson::WriteJson(&Cout, &json, true);
            Cout << Endl;
            break;
        }
        case EDataFormat::Csv: {
            Cout << "pile_name,state,generation" << Endl;
            for (const auto& s : state) {
                Cout << s.PileName << "," << PileStateToString(s.State) << "," << result.GetGeneration() << Endl;
            }
            break;
        }
        default: {
            TStringStream ss;
            for (const auto& s : state) {
                ss << "Pile " << s.PileName << ": " << PileStateToString(s.State) << Endl;
            }
            ss << "Generation: " << result.GetGeneration() << Endl;
            if (result.IsConversionInProgress()) {
                ss << "Conversion from non-bridged cluster is in progress, piles can't be added yet" << Endl;
            }
            if (const auto& op = result.GetPendingPileOperation()) {
                ss << "Pending operation on pile " << op->PileName << ": " << PendingPileOperationKindToString(op->Kind)
                    << Endl;
                if (!op->ErrorReason.empty()) {
                    ss << "Last attempt has failed (being retried): " << op->ErrorReason << Endl;
                }
            }
            Cout << ss.Str();
            break;
        }
    }

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
    : TYdbCommand("join-pile", {"rejoin"}, "Join a provisioned pile or rejoin a pile after maintenance or recovery")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
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
            "until converted separately (ydb admin cluster bridge set-database-mode --bridged). Static storage groups are still converted "
            "to bridge mode. Requires bridge_config.enable_non_uniform_storage=true.")
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
    : TYdbCommand("provision-pile", {}, "Provision a pile listed in the bridge config with its hosts (and shown as INACTIVE) in the "
        "cluster: create its parts of storage groups and make it DISCONNECTED.")
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
    : TYdbCommand("destroy-pile", {}, "Destroy a DISCONNECTED pile: delete its parts of every storage "
        "group along with ALL THEIR DATA, which can't be undone, and make it INACTIVE. Waits until every storage group "
        "loses its part in the pile; then the pile may be dropped from the bridge config along with its hosts.")
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
        Cout << colors.RedColor() << "Every storage group of pile " << Pile << " will be deleted along with all the data "
            "it holds. This can't be undone." << colors.OldColor() << " Use \"--assume-yes\" (\"-y\") option to "
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
