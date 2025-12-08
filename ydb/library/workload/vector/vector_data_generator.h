#pragma once

#include "configure_opts.h"
#include "vector_workload_params.h"

#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/benchmark_base/data_generator.h>

namespace NYdbWorkload {

class TWorkloadVectorFilesDataInitializer : public TWorkloadDataInitializerBase {
private:
    const TVectorWorkloadParams& Params;
    TString DataFiles;
    TString EmbeddingColumnName = "embedding";

public:
    TWorkloadVectorFilesDataInitializer(const TVectorWorkloadParams& params);

    virtual void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    virtual TBulkDataGeneratorList DoGetBulkInitialData() override;
};

class TWorkloadVectorGenerateDataInitializer : public TWorkloadDataInitializerBase {
private:
    const TVectorWorkloadParams& Params;
    NVector::TVectorOpts VectorOpts;
    size_t RowCount;
    uint32_t RandomSeed = 42;

public:
    TWorkloadVectorGenerateDataInitializer(const TVectorWorkloadParams& params);

    virtual void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    virtual TBulkDataGeneratorList DoGetBulkInitialData() override;
};

} // namespace NYdbWorkload
