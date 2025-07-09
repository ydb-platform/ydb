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
    Ydb::Bridge::PileState ParsePileState(TString stateStr) {
        TString stateStrUpper = stateStr;
        stateStrUpper.to_upper();

        if (stateStrUpper == "DISCONNECTED") {
            return Ydb::Bridge::PileState::DISCONNECTED;
        }
        if (stateStrUpper == "NOT_SYNCHRONIZED") {
            return Ydb::Bridge::PileState::NOT_SYNCHRONIZED;
        }
        if (stateStrUpper == "SYNCHRONIZED") {
            return Ydb::Bridge::PileState::SYNCHRONIZED;
        }
        if (stateStrUpper == "PROMOTE") {
            return Ydb::Bridge::PileState::PROMOTE;
        }
        if (stateStrUpper == "PRIMARY") {
            return Ydb::Bridge::PileState::PRIMARY;
        }
        ythrow yexception() << "Invalid pile state: \"" << stateStr
            << "\". Please use one of: DISCONNECTED, NOT_SYNCHRONIZED, SYNCHRONIZED, PROMOTE, PRIMARY.";
    }

    TString PileStateToString(NYdb::NBridge::EPileState state) {
        switch (state) {
            case NYdb::NBridge::EPileState::DISCONNECTED: return "DISCONNECTED";
            case NYdb::NBridge::EPileState::NOT_SYNCHRONIZED: return "NOT_SYNCHRONIZED";
            case NYdb::NBridge::EPileState::SYNCHRONIZED: return "SYNCHRONIZED";
            case NYdb::NBridge::EPileState::PROMOTE: return "PROMOTE";
            case NYdb::NBridge::EPileState::PRIMARY: return "PRIMARY";
        }
        return "UNKNOWN";
    }
}

TCommandBridge::TCommandBridge(bool allowEmptyDatabase)
    : TClientCommandTree("bridge", {}, "Manage cluster in bridge mode")
{
    AddCommand(std::make_unique<TCommandBridgeUpdate>(allowEmptyDatabase));
    AddCommand(std::make_unique<TCommandBridgeGet>(allowEmptyDatabase));
}

TCommandBridgeUpdate::TCommandBridgeUpdate(bool allowEmptyDatabase)
    : TYdbCommand("update", {}, "Update cluster state in bridge mode")
    , AllowEmptyDatabase(allowEmptyDatabase)
{
}

void TCommandBridgeUpdate::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("set", "Set new state for a pile. Format: <pile-id>:<state>. Can be used multiple times.")
        .RequiredArgument("ID:STATE").Handler([this](const TString& value) {
            PileStateUpdates.push_back(value);
        });
    config.Opts->AddLongOption("specific-pile-id", "Acquire quorum only for specific set of piles. Can be used multiple times.")
        .RequiredArgument("ID").Handler([this](const TString& value) {
            SpecificPileIds.push_back(FromString(value));
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
            TStringBuf pileIdStr, stateStr;
            if (!TStringBuf(updateStr).TrySplit(':', pileIdStr, stateStr) || pileIdStr.empty() || stateStr.empty()) {
                ythrow yexception() << "Invalid format for --set option. Expected '<pile-id>:<state>'.";
            }
            NYdb::NBridge::TPileStateUpdate update;
            update.PileId = FromString<ui32>(pileIdStr);
            update.State = static_cast<NYdb::NBridge::EPileState>(ParsePileState(TString(stateStr)));
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
            if (!item.IsMap() || !item.Has("pile_id") || !item.Has("state")) {
                ythrow yexception() << "Invalid object in JSON array: each item must be an object with string \"state\" and integer \"pile_id\" keys.";
            }
            NYdb::NBridge::TPileStateUpdate update;
            update.PileId = item["pile_id"].GetInteger();
            update.State = static_cast<NYdb::NBridge::EPileState>(ParsePileState(item["state"].GetString()));
            Updates.push_back(update);
        }
    }
}

int TCommandBridgeUpdate::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NBridge::TBridgeClient(*driver);

    auto result = client.UpdateClusterState(Updates, SpecificPileIds).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Cluster state updated successfully." << Endl;

    return EXIT_SUCCESS;
}

TCommandBridgeGet::TCommandBridgeGet(bool allowEmptyDatabase)
    : TYdbReadOnlyCommand("get", {"state"}, "Get current bridge cluster state")
    , AllowEmptyDatabase(allowEmptyDatabase)
{}

void TCommandBridgeGet::Config(TConfig& config) {
    TYdbReadOnlyCommand::Config(config);
    config.AllowEmptyDatabase = AllowEmptyDatabase;
    config.SetFreeArgsNum(0);
    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::Json,
        EDataFormat::Csv
    });
}

void TCommandBridgeGet::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandBridgeGet::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NBridge::TBridgeClient(*driver);

    auto result = client.GetClusterState().GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& state = result.GetState();

    switch (OutputFormat) {
        case EDataFormat::Json: {
            NJson::TJsonValue json(NJson::JSON_ARRAY);
            for (const auto& s : state) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("pile_id", s.PileId);
                item.InsertValue("state", PileStateToString(s.State));
                json.AppendValue(item);
            }
            NJson::WriteJson(&Cout, &json, true);
            Cout << Endl;
            break;
        }
        case EDataFormat::Csv: {
            Cout << "pile_id,state" << Endl;
            for (const auto& s : state) {
                Cout << s.PileId << "," << PileStateToString(s.State) << Endl;
            }
            break;
        }
        default: {
            TStringStream ss;
            for (const auto& s : state) {
                ss << "Pile " << s.PileId << ": " << PileStateToString(s.State) << Endl;
            }
            Cout << ss.Str();
            break;
        }
    }

    return EXIT_SUCCESS;
}

}
