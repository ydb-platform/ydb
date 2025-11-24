#pragma once

#include "vector_workload_params.h"

#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/benchmark_base/data_generator.h>

namespace NYdbWorkload {

class TWorkloadVectorFilesDataInitializer : public TWorkloadDataInitializerBase {
private:
    const TVectorWorkloadParams& Params;
    TString DataFiles;
    TString EmbeddingColumnName = "embedding";
    // TString InputBinaryStringEncodingFormat = "unicode";

public:
    TWorkloadVectorFilesDataInitializer(const TVectorWorkloadParams& params);

    virtual void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    virtual TBulkDataGeneratorList DoGetBulkInitialData() override;
};

} // namespace NYdbWorkload
