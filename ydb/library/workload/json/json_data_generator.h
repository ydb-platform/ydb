#pragma once

#include "json_workload_params.h"

#include <ydb/library/workload/benchmark_base/workload.h>

namespace NYdbWorkload {

class TJsonDataInitializer final: public TWorkloadDataInitializerBase {
public:
    TJsonDataInitializer(const TJsonWorkloadParams& params);

    void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    TBulkDataGeneratorList DoGetBulkInitialData() override;
    int PostImport() override;

private:
    const TJsonWorkloadParams& JsonParams;
    ui64 RowCount = 100000;
    ui64 Seed = 0xC0DE;
};

} // namespace NYdbWorkload
