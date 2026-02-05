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

void TFmrUserJobLauncher::InitializeJobEnvironment(
    const TString& jobEnvironmentDir,
    const std::vector<TFileInfo>& jobFiles,
    const std::vector<TYtResourceInfo>& jobYtResources,
    const std::vector<TFmrResourceTaskInfo>& jobFmrResources
) {
    YQL_ENSURE(!jobEnvironmentDir.empty());
    std::vector<std::pair<TString, TString>> filePaths; // LocalPath, FileAlias

    for (auto& fileInfo: jobFiles) {
        TString filePath = fileInfo.Alias.empty() ? fileInfo.LocalPath : fileInfo.Alias;
        filePaths.emplace_back(fileInfo.LocalPath, filePath);
    }

    for (auto& remoteFileInfo: jobYtResources) {
        YQL_ENSURE(remoteFileInfo.RichPath.FileName_.Defined());
        filePaths.emplace_back(remoteFileInfo.LocalPath, *remoteFileInfo.RichPath.FileName_);
    }

    for (auto& fmrResourceInfo: jobFmrResources) {
        filePaths.emplace_back(fmrResourceInfo.LocalPath, fmrResourceInfo.Alias);
    }

    for (auto& [localPath, alias]: filePaths) {
        YQL_ENSURE(!localPath.empty());
        NFs::SetExecutable(localPath, true);
        NFs::SymLink(localPath, TFsPath(jobEnvironmentDir) / alias);
    }
}

std::variant<TFmrError, TStatistics> TFmrUserJobLauncher::LaunchJob(
    TFmrUserJob& job,
    const TMaybe<TString>& jobEnvironmentDir,
    const std::vector<TFileInfo>& jobFiles,
    const std::vector<TYtResourceInfo>& jobYtResources,
    const std::vector<TFmrResourceTaskInfo>& jobFmrResources)
{
    if (!RunInSeparateProcess_) {
        YQL_ENSURE(jobFiles.empty() && jobYtResources.empty(), "Fmr job with linked resource files should be launched only in separate process");
        return job.DoFmrJob(TFmrUserJobOptions{.WriteStatsToFile = false});
    }

    YQL_ENSURE(!FmrJobBinaryPath_.empty(), "Job should be executed in separate process");
    YQL_ENSURE(!TableDataServiceDiscoveryFilePath_.empty());
    YQL_ENSURE(GatewayType_ == "native" || GatewayType_ == "file");
    // serialize to temporary file

    YQL_ENSURE(jobEnvironmentDir.Defined());
    auto jobtmpDir = TFsPath(*jobEnvironmentDir);

    InitializeJobEnvironment(*jobEnvironmentDir, jobFiles, jobYtResources, jobFmrResources);

    TFile jobStateFile(jobtmpDir.Child("fmrjob.bin"), CreateNew | RdWr);
    TFile mapResultStatsFile(jobtmpDir.Child("stats.bin"), CreateNew | RdWr);

    TString tmpDirTableDataServiceDiscoveryPath = "tds_discovery.txt";
    NFs::HardLinkOrCopy(TableDataServiceDiscoveryFilePath_, jobtmpDir.Child(tmpDirTableDataServiceDiscoveryPath));
    job.SetTableDataService(tmpDirTableDataServiceDiscoveryPath);

    job.SetYtJobServiceType(GatewayType_);

    TFileOutput jobStateFileOutputStream(jobStateFile);
    job.Save(jobStateFileOutputStream);
    jobStateFileOutputStream.Flush();

    // execute map in separate process
    TShellCommandOptions opts;
    TStringStream fmrJobOutputStream, fmrJobErrorStream;
    opts.SetUseShell(false).SetDetachSession(false).SetOutputStream(&fmrJobOutputStream).SetErrorStream(&fmrJobErrorStream);

    TShellCommand command(FmrJobBinaryPath_, {}, opts, jobtmpDir);
    command.Run();
    command.Wait();

    auto code = command.GetExitCode();
    if (code != 0) {
        TString errorStr = fmrJobErrorStream.Str();
        EFmrErrorReason errorReason = ParseFmrReasonFromErrorMessage(errorStr);

        return TFmrError{
            .Reason = errorReason,
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
