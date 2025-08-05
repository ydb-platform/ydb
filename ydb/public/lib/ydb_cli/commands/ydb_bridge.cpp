#include "ydb_bridge.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>
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
        if (stateStrUpper == "PROMOTE") {
            return NYdb::NBridge::EPileState::PROMOTE;
        }
        if (stateStrUpper == "PRIMARY") {
            return NYdb::NBridge::EPileState::PRIMARY;
        }
        if (stateStrUpper == "SUSPENDED") {
            return NYdb::NBridge::EPileState::SUSPENDED;
        }
        ythrow yexception() << "Invalid pile state: \"" << stateStr
            << "\". Please use one of: DISCONNECTED, NOT_SYNCHRONIZED, SYNCHRONIZED, PROMOTE, PRIMARY, SUSPENDED.";
    }

    TString PileStateToString(NYdb::NBridge::EPileState state) {
        switch (state) {
            case NYdb::NBridge::EPileState::UNSPECIFIED: return "UNSPECIFIED";
            case NYdb::NBridge::EPileState::DISCONNECTED: return "DISCONNECTED";
            case NYdb::NBridge::EPileState::NOT_SYNCHRONIZED: return "NOT_SYNCHRONIZED";
            case NYdb::NBridge::EPileState::SYNCHRONIZED: return "SYNCHRONIZED";
            case NYdb::NBridge::EPileState::PROMOTE: return "PROMOTE";
            case NYdb::NBridge::EPileState::PRIMARY: return "PRIMARY";
            case NYdb::NBridge::EPileState::SUSPENDED: return "SUSPENDED";
        }
        return "UNKNOWN";
    }
}

TCommandBridge::TCommandBridge(bool allowEmptyDatabase)
    : TClientCommandTree("bridge", {}, "Manage cluster in bridge mode")
{
    AddCommand(std::make_unique<TCommandBridgeUpdate>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeList>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeSwitchover>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeFailover>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeTakedown>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeRejoin>(allowEmptyDatabase));
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
    config.Opts->AddLongOption("quorum-pile", "Acquire quorum only for specific set of piles. Can be used multiple times.")
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
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NBridge::TBridgeClient(*driver);

    std::vector<std::string> quorumPiles;
    for (const auto& s : QuorumPiles) {
        quorumPiles.push_back(std::string(s));
    }

    auto result = client.UpdateClusterState(Updates, quorumPiles).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeList::TCommandBridgeList(bool allowEmptyDatabase)
    : TYdbReadOnlyCommand("list", {}, "List current bridge cluster state")
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
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NBridge::TBridgeClient(*driver);

    // TODO(mregrock): support detailed
    auto result = client.GetClusterState().GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& state = result.GetState();

    switch (OutputFormat) {
        case EDataFormat::Json: {
            NJson::TJsonValue json(NJson::JSON_ARRAY);
            for (const auto& s : state) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("pile_name", s.PileName);
                item.InsertValue("state", PileStateToString(s.State));
                json.AppendValue(item);
            }
            NJson::WriteJson(&Cout, &json, true);
            Cout << Endl;
            break;
        }
        case EDataFormat::Csv: {
            Cout << "pile_name,state" << Endl;
            for (const auto& s : state) {
                Cout << s.PileName << "," << PileStateToString(s.State) << Endl;
            }
            break;
        }
        default: {
            TStringStream ss;
            for (const auto& s : state) {
                ss << "Pile " << s.PileName << ": " << PileStateToString(s.State) << Endl;
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
    config.Opts->AddLongOption("primary", "Name of the pile to become the new primary.")
        .Required().RequiredArgument("PILE_NAME").StoreResult(&PrimaryPile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeSwitchover::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeSwitchover::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NBridge::TBridgeClient(*driver);

    std::vector<NYdb::NBridge::TPileStateUpdate> updates;
    updates.push_back({PrimaryPile, NYdb::NBridge::EPileState::PROMOTE});

    auto result = client.UpdateClusterState(updates, {}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeFailover::TCommandBridgeFailover(bool allowEmptyDatabase)
    : TYdbCommand("failover", {}, "Perform an emergency failover when a pile becomes unresponsive")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
}

void TCommandBridgeFailover::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("pile", "Name of the pile that is down.")
        .Required().RequiredArgument("PILE_NAME").StoreResult(&DownPile);
    config.Opts->AddLongOption("primary", "Name of the pile to become the new primary (optional, only if the target pile was the primary).")
        .Optional().RequiredArgument("PILE_NAME").StoreResult(&PrimaryPile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeFailover::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeFailover::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NBridge::TBridgeClient(*driver);

    std::vector<NYdb::NBridge::TPileStateUpdate> updates;
    updates.push_back({DownPile, NYdb::NBridge::EPileState::DISCONNECTED});
    if (!PrimaryPile.empty()) {
        updates.push_back({PrimaryPile, NYdb::NBridge::EPileState::PRIMARY});
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
    config.Opts->AddLongOption("primary", "Name of the pile to become the new primary (optional, only if the target pile was the primary).")
        .Optional().RequiredArgument("PILE_NAME").StoreResult(&PrimaryPile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeTakedown::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeTakedown::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NBridge::TBridgeClient(*driver);

    std::vector<NYdb::NBridge::TPileStateUpdate> updates;
    updates.push_back({DownPile, NYdb::NBridge::EPileState::SUSPENDED});
    if (!PrimaryPile.empty()) {
        updates.push_back({PrimaryPile, NYdb::NBridge::EPileState::PRIMARY});
    }

    auto result = client.UpdateClusterState(updates, {}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeRejoin::TCommandBridgeRejoin(bool allowEmptyDatabase)
    : TYdbCommand("rejoin", {}, "Rejoin a pile to the cluster after maintenance or recovery")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
}

void TCommandBridgeRejoin::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("pile", "Name of the pile to rejoin.")
        .Required().RequiredArgument("PILE_NAME").StoreResult(&Pile);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
}

void TCommandBridgeRejoin::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
}

int TCommandBridgeRejoin::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NBridge::TBridgeClient(*driver);

    std::vector<NYdb::NBridge::TPileStateUpdate> updates;
    updates.push_back({Pile, NYdb::NBridge::EPileState::NOT_SYNCHRONIZED});

    auto result = client.UpdateClusterState(updates, {}).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

}
