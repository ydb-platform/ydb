#pragma once

#include <yt/yql/providers/yt/fmr/process/yql_yt_job_fmr.h>

namespace NYql::NFmr {

class TFmrUserJobLauncher: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TFmrUserJobLauncher>;

    virtual ~TFmrUserJobLauncher() = default;

    TFmrUserJobLauncher(bool runInSeparateProcess, const TString& fmrJobBinaryPath = TString());

    std::variant<TError, TStatistics> LaunchJob(TFmrUserJob& job);

private:
    const bool RunInSeparateProcess_;
    const TString FmrJobBinaryPath_;
};

} // namespace NYql::NFmr
