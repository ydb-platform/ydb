#include "ydb_state.h"

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>

namespace NYdb::NConsoleClient {

TCommandClusterState::TCommandClusterState()
    : TClientCommandTree("state", {}, "Manage cluster internal state")
{
    AddCommand(std::make_unique<TCommandClusterStateFetch>());
}

TCommandClusterStateFetch::TCommandClusterStateFetch()
    : TYdbReadOnlyCommand("fetch", {}, "Fetch cluster internal state")
{}

void TCommandClusterStateFetch::Config(TConfig& config) {
    TYdbReadOnlyCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption("duration", "Duration of collecting cluster state in seconds")
        .OptionalArgument("NUM").StoreResult(&Duration);
    config.Opts->AddLongOption("period", "Period of collectiong counters in seconds")
        .OptionalArgument("NUM").StoreResult(&Period);
    AddOutputFormats(config, {
        EDataFormat::Json
    }, EDataFormat::Json);
    config.AllowEmptyDatabase = true;
}

void TCommandClusterStateFetch::Parse(TConfig& config) {
    TYdbReadOnlyCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandClusterStateFetch::Run(TConfig& config) {
    NMonitoring::TMonitoringClient client(CreateDriver(config));
    NMonitoring::TClusterStateSettings settings;
    settings.Duration(Duration);
    settings.Period(Period);
    NMonitoring::TClusterStateResult result = client.ClusterState(settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    const auto& proto = NYdb::TProtoAccessor::GetProto(result);

    Cout << proto.Getresult();
    return EXIT_SUCCESS;
}

}
