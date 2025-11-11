#include "ydb_state.h"

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>

#include <util/generic/xrange.h>

#include <library/cpp/streams/bzip2/bzip2.h>

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
    .DefaultValue(60)
        .OptionalArgument("NUM").StoreResult(&DurationSeconds);
    config.Opts->AddLongOption("period",
        "Interval in seconds between metric collections during the --duration period.\n"
        "If set to zero or omitted, only a single collection is done.")
    .DefaultValue(0)
        .OptionalArgument("NUM").StoreResult(&PeriodSeconds);
    config.Opts->AddLongOption('f', "filename", "File name to save the results\n")
        .RequiredArgument("[out.tar.gz]").StoreResult(&FileName);
    config.AllowEmptyDatabase = true;
}

void TCommandClusterStateFetch::Parse(TConfig& config) {
    TYdbReadOnlyCommand::Parse(config);
    ParseOutputFormats();
}

struct TARFileHeader {
    char filename[100]; //NUL-terminated
    char mode[8];
    char uid[8];
    char gid[8];
    char fileSize[12];
    char lastModification[12];
    char checksum[8];
    char typeFlag; //Also called link indicator for none-UStar format
    char linkedFileName[100];
    //USTar-specific fields -- NUL-filled in non-USTAR version
    char ustarIndicator[6]; //"ustar" -- 6th character might be NUL but results show it doesn't have to
    char ustarVersion[2]; //00
    char ownerUserName[32];
    char ownerGroupName[32];
    char deviceMajorNumber[8];
    char deviceMinorNumber[8];
    char filenamePrefix[155];
    char padding[12];

    TARFileHeader(TString name, ui32 size) {
        memset(this, 0, sizeof(TARFileHeader));
        strcpy(filename, name.c_str());
        strcpy(fileSize, ToOct(size).c_str());
        CalcChecksum();
    }

    std::string ToOct(ui64 n){
        std::string result;
        std::stringstream ss;
        ss << std::oct << n;
        ss >> result;
        return result;
    }

    void CalcChecksum() {
        memset(checksum, ' ', 8);
        ui64 unsignedSum = 0;
        for (ui32 i = 0; i < sizeof(TARFileHeader); i++) {
            unsignedSum += ((unsigned char*) this)[i];
        }
        strcpy(checksum, ToOct(unsignedSum).c_str());
    }

    void ToStream(IOutputStream& out) {
        for (ui32 i = 0; i < sizeof(TARFileHeader); i++) {
            out.Write(((char*) this)[i]);
        }
    }

};

int TCommandClusterStateFetch::Run(TConfig& config) {
    NMonitoring::TMonitoringClient client(CreateDriver(config));
    NMonitoring::TClusterStateSettings settings;
    settings.DurationSeconds(DurationSeconds);
    settings.PeriodSeconds(PeriodSeconds);
    NMonitoring::TClusterStateResult result = client.ClusterState(settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    const auto& proto = NYdb::TProtoAccessor::GetProto(result);

    TFileOutput out(FileName);
    TBZipCompress compress(&out);
    TString r = proto.Getresult();
    if (!r.empty()) {
        TARFileHeader h("result.json", r.size());
        h.ToStream(compress);
        compress << r;
    }
    for (ui32 i : xrange(proto.blocksSize())) {
        auto& block = proto.Getblocks(i);
        auto& content = block.Getcontent();
        TARFileHeader h(block.Getname(), content.size());
        h.ToStream(compress);
        compress << content;
    }

    return EXIT_SUCCESS;
}

}
