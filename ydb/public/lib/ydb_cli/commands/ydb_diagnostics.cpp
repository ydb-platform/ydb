#include "ydb_diagnostics.h"

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>

#include <chrono>
#include <util/generic/xrange.h>
#include <util/stream/mem.h>
#include <library/cpp/streams/bzip2/bzip2.h>

namespace NYdb::NConsoleClient {

TCommandClusterDiagnostics::TCommandClusterDiagnostics()
    : TClientCommandTree("diagnostics", {}, "Manage cluster internal state")
{
    AddCommand(std::make_unique<TCommandClusterDiagnosticsCollect>());
}

TCommandClusterDiagnosticsCollect::TCommandClusterDiagnosticsCollect()
    : TYdbReadOnlyCommand("collect", {},
        "Fetch aggregated cluster node state and metrics over a time period.\n"
        "Sends a cluster-wide request to collect state as a set of metrics from all nodes.\n"
        "One of the nodes gathers metrics from all others over the specified duration, then returns an aggregated result.")
{}

void TCommandClusterDiagnosticsCollect::Config(TConfig& config) {
    TYdbReadOnlyCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption("duration",
        "Total time in seconds during which metrics should be collected on the server."
        "Defines how long the server-side node will keep polling other nodes. "
        "If --period is unset or zero, only one snapshot is collected and the server waits up to --duration for all responses.")
    .DefaultValue(60)
        .OptionalArgument("NUM").StoreResult(&DurationSeconds);
    config.Opts->AddLongOption("period",
        "Interval in seconds between metric collections during the --duration period."
        "If set to zero or omitted, only a single collection is done.")
    .DefaultValue(0)
        .OptionalArgument("NUM").StoreResult(&PeriodSeconds);
    config.Opts->AddLongOption('o', "output", "Path to the output .tar.bz2 file")
    .DefaultValue("out.tar.bz2")
        .OptionalArgument("PATH").StoreResult(&FileName);
    config.Opts->AddLongOption("no-sanitize", "Disable sanitization and preserve sensitive data in the output, including table names, "
        "authentication information, and other personally identifiable information")
        .StoreTrue(&NoSanitize);
    config.AllowEmptyDatabase = true;
}

void TCommandClusterDiagnosticsCollect::Parse(TConfig& config) {
    TYdbReadOnlyCommand::Parse(config);
    ParseOutputFormats();
}

struct TARFile {
    struct TARFileHeader {
        char filename[100];
        char padding[24];
        char fileSize[12];
        char lastModification[12];
        char checksum[8];
        char padding2[356];

        TARFileHeader(const TString& name, ui32 size, TInstant time) {
            memset(this, 0, sizeof(TARFileHeader));
            strcpy(filename, name.c_str());
            strcpy(fileSize, ToOct(size).c_str());
            strcpy(lastModification, ToOct(time.Seconds()).c_str());
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
            for (ui32 i : xrange(sizeof(TARFileHeader))) {
                unsignedSum += ((unsigned char*) this)[i];
            }
            strcpy(checksum, ToOct(unsignedSum).c_str());
        }

        void ToStream(IOutputStream& out) {
            for (ui32 i : xrange(sizeof(TARFileHeader))) {
                out.Write(((char*) this)[i]);
            }
        }
    };

    static void ToStream(IOutputStream& out, const TString& name, const TString& content, TInstant time) {
        TARFileHeader h(name, content.size(), time);
        h.ToStream(out);
        out << content;
        size_t paddingBytes = (512 - (content.size() % 512)) % 512;
        TString s;
        s.resize(paddingBytes);
        out << s;
    }
};

int TCommandClusterDiagnosticsCollect::Run(TConfig& config) {
    NMonitoring::TMonitoringClient client(CreateDriver(config));
    NMonitoring::TClusterStateSettings settings;
    settings.DurationSeconds(DurationSeconds);
    settings.PeriodSeconds(PeriodSeconds);
    settings.NoSanitize(NoSanitize);
    NMonitoring::TClusterStateResult result = client.ClusterState(settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    const auto& proto = NYdb::TProtoAccessor::GetProto(result);

    TFileOutput out(FileName);
    TBZipCompress compress(&out);
    for (ui32 i : xrange(proto.blocksSize())) {
        auto& block = proto.Getblocks(i);
        TString data = block.Getcontent();
        TMemoryInput input(data.data(), data.size());
        TBZipDecompress decompress(&input);
        TARFile::ToStream(compress, block.Getname(), decompress.ReadAll(), TInstant::Seconds(block.Gettimestamp().seconds()));
    }
    return EXIT_SUCCESS;
}

}
