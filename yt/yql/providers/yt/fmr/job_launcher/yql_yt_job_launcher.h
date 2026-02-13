#pragma once

#include <yt/yql/providers/yt/fmr/process/yql_yt_job_fmr.h>

namespace NYql::NFmr {

struct TFmrUserJobLauncherOptions {
    bool RunInSeparateProcess;
    TString FmrJobBinaryPath;
    TString TableDataServiceDiscoveryFilePath;
    TString GatewayType;
};

class TFmrUserJobLauncher: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TFmrUserJobLauncher>;

    virtual ~TFmrUserJobLauncher() = default;

    TFmrUserJobLauncher(const TFmrUserJobLauncherOptions& jobLauncherOptions);

    std::variant<TError, TStatistics> LaunchJob(
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
