#pragma once

#include <yt/yql/providers/yt/fmr/process/yql_yt_job_fmr.h>

namespace NYql::NFmr {

struct TFmrUserJobLauncherOptions {
    bool RunInSeparateProcess;
    TString FmrJobBinaryPath;
    TString GatewayType;
    // Optional: if set, the discovery file is hard-linked into the job environment.
    // If not set, the job must carry its own discovery info (e.g. TVanillaInfo
    // set via FillMapFmrJob) so the binary can resolve peers at runtime.
    TString TableDataServiceDiscoveryFilePath;
};

class TFmrUserJobLauncher: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TFmrUserJobLauncher>;

    virtual ~TFmrUserJobLauncher() = default;

    TFmrUserJobLauncher(const TFmrUserJobLauncherOptions& jobLauncherOptions);

    std::variant<TFmrError, TStatistics> LaunchJob(
        TFmrUserJob& job,
        const TMaybe<TString>& jobEnvironmentDir = Nothing(),
        const std::vector<TFileInfo>& jobFiles = {},
        const std::vector<TYtResourceInfo>& jobYtResources = {},
        const std::vector<TFmrResourceTaskInfo>& jobFmrResources = {}
    );

    bool RunInSeperateProcess() const;

private:
    void InitializeJobEnvironment(
        const TString& jobEnvironmentDir,
        const std::vector<TFileInfo>& jobFiles,
        const std::vector<TYtResourceInfo>& jobYtResources,
        const std::vector<TFmrResourceTaskInfo>& jobFmrResources
    );

private:
    const bool RunInSeparateProcess_;
    const TString FmrJobBinaryPath_;
    const TString TableDataServiceDiscoveryFilePath_;
    const TString GatewayType_;
};

} // namespace NYql::NFmr
