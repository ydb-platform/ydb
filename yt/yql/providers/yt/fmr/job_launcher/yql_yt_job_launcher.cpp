#include "yql_yt_job_launcher.h"

#include <yt/yql/providers/yt/fmr/request_options/proto_helpers/yql_yt_request_proto_helpers.h>
#include <util/system/shellcommand.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

TFmrUserJobLauncher::TFmrUserJobLauncher(const TFmrUserJobLauncherOptions& options)
    : RunInSeparateProcess_(options.RunInSeparateProcess)
    , FmrJobBinaryPath_(options.FmrJobBinaryPath)
    , TableDataServiceDiscoveryFilePath_(options.TableDataServiceDiscoveryFilePath)
    , GatewayType_(options.GatewayType)
{
}

std::variant<TError, TStatistics> TFmrUserJobLauncher::LaunchJob(TFmrUserJob& job) {
    if (!RunInSeparateProcess_) {
        return job.DoFmrJob(TFmrUserJobOptions{.WriteStatsToFile = false});
    }

    YQL_ENSURE(!FmrJobBinaryPath_.empty(), "Job should be executed in separate process");
    YQL_ENSURE(!TableDataServiceDiscoveryFilePath_.empty());
    YQL_ENSURE(GatewayType_ == "native" || GatewayType_ == "file");
    // serialize to temporary file
    TTempDir tmpDir;
    TFile jobStateFile(tmpDir.Path().Child("fmrjob.bin"), CreateNew | RdWr);
    TFile mapResultStatsFile(tmpDir.Path().Child("stats.bin"), CreateNew | RdWr);

    TString tmpDirTableDataServiceDiscoveryPath = "table_data_service_discovery.txt";
    NFs::HardLinkOrCopy(TableDataServiceDiscoveryFilePath_, tmpDir.Path().Child(tmpDirTableDataServiceDiscoveryPath));
    job.SetTableDataService(tmpDirTableDataServiceDiscoveryPath);

    job.SetYtJobServiceType(GatewayType_);

    TFileOutput jobStateFileOutputStream(jobStateFile);
    job.Save(jobStateFileOutputStream);
    jobStateFileOutputStream.Flush();

    // execute map in separate process
    TShellCommandOptions opts;
    TStringStream fmrJobOutputStream, fmrJobErrorStream;
    opts.SetUseShell(false).SetDetachSession(false).SetOutputStream(&fmrJobOutputStream).SetErrorStream(&fmrJobErrorStream);

    TShellCommand command(FmrJobBinaryPath_, {}, opts, tmpDir.Path());
    command.Run();
    command.Wait();

    auto code = command.GetExitCode();
    if (code != 0) {
        TString errorStr = fmrJobErrorStream.Str();
        TStringBuf errorMessage = errorStr;
        TryParseTerminationMessage(errorMessage);

        if (errorMessage.size() < errorStr.size()) {
            ythrow yexception() << "Process terminated with error: " << errorMessage;
        }
        return TError{
            .ErrorMessage = TStringBuilder() << "Process terminated with exit code " << code << " and error message " << fmrJobErrorStream.Str()
        };
    }

    YQL_CLOG(DEBUG, FastMapReduce) << "Process cerr: " << fmrJobErrorStream.Str();

    TFileInput statsStream(mapResultStatsFile);
    auto serializedProtoStats = statsStream.ReadAll();
    NProto::TStatistics protoStats;
    protoStats.ParseFromStringOrThrow(serializedProtoStats);
    return StatisticsFromProto(protoStats);
}

bool TFmrUserJobLauncher::RunInSeperateProcess() const {
    return RunInSeparateProcess_;
}


} // namespace NYql::NFmr
