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

    std::variant<TError, TStatistics> LaunchJob(TFmrUserJob& job);

    bool RunInSeperateProcess() const;

private:
    const bool RunInSeparateProcess_;
    const TString FmrJobBinaryPath_;
    const TString TableDataServiceDiscoveryFilePath_;
    const TString GatewayType_;
};

} // namespace NYql::NFmr
