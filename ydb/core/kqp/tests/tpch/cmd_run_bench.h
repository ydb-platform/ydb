#pragma once

#include "commands.h"
#include <util/generic/set.h>

namespace NYdb::NTpch {

class TCommandRunBenchmark : public TTpchCommandBase {
public:
    TCommandRunBenchmark();

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    struct TTestInfo {
        TDuration ColdTime;
        TDuration Min;
        TDuration Max;
        double Mean = 0;
        double Std = 0;
    };

    static TTestInfo AnalyzeTestRuns(const TVector<TDuration>& timings);

    TString ReportFileName;
    TString JsonOutputFileName;
    ui32 IterationsCount = 0;
    TSet<ui32> TestCases;
    bool MixedMode = false;
    bool MemoryProfile = false;
};

} // namespace NYdb::NTpch
