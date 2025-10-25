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
    : TYdbReadOnlyCommand("fetch", {},
        "Fetch aggregated cluster node state and metrics over a time period.\n"
        "Sends a cluster-wide request to collect state as a set of metrics from all nodes.\n"
        "One of the nodes gathers metrics from all others over the specified duration, then returns an aggregated result.")
{}

void TCommandClusterStateFetch::Config(TConfig& config) {
    TYdbReadOnlyCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption("duration",
        "Total time in seconds during which metrics should be collected on the server.\n"
        "Defines how long the server-side node will keep polling other nodes. "
        "If --period is unset or zero, only one snapshot is collected and the server waits up to --duration for all responses.")
    .DefaultValue(0)
        .OptionalArgument("NUM").StoreResult(&DurationSeconds);
    config.Opts->AddLongOption("period",
        "Interval in seconds between metric collections during the --duration period.\n"
        "If set to zero or omitted, only a single collection is done.")
    .DefaultValue(0)
        .OptionalArgument("NUM").StoreResult(&PeriodSeconds);
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
    settings.DurationSeconds(DurationSeconds);
    settings.PeriodSeconds(PeriodSeconds);
    NMonitoring::TClusterStateResult result = client.ClusterState(settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    const auto& proto = NYdb::TProtoAccessor::GetProto(result);

    Cout << proto.Getresult();
    return EXIT_SUCCESS;
}

}
