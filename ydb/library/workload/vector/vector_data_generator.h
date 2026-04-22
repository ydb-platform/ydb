#pragma once

#include "configure_opts.h"
#include "vector_workload_params.h"

#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/benchmark_base/data_generator.h>

namespace NYdbWorkload {

class TWorkloadVectorDataInitializerBase : public TWorkloadDataInitializerBase {
protected:
    const TVectorWorkloadParams& VectorParams;

public:
    TWorkloadVectorDataInitializerBase(const TString& name, const TString& description, const TVectorWorkloadParams& params);
    virtual int PostImport() override;
};

class TWorkloadVectorFilesDataInitializer : public TWorkloadVectorDataInitializerBase {
private:
    TString DataFiles;
    TString EmbeddingColumnName = "embedding";
    TString Format;

public:
    TWorkloadVectorFilesDataInitializer(const TVectorWorkloadParams& params);

    virtual void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    virtual TBulkDataGeneratorList DoGetBulkInitialData() override;
};

class TWorkloadVectorGenerateDataInitializer : public TWorkloadVectorDataInitializerBase {
private:
    NVector::TVectorOpts VectorOpts;
    size_t RowCount = 10000;
    size_t PrefixCount = 100;
    uint32_t RandomSeed = 42;

public:
    TWorkloadVectorGenerateDataInitializer(const TVectorWorkloadParams& params);

    virtual void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    virtual TBulkDataGeneratorList DoGetBulkInitialData() override;
};

} // namespace NYdbWorkload
