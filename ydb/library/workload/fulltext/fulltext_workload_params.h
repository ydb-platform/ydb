#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <string>

namespace NYdbWorkload {

class TFulltextWorkloadParams final: public TWorkloadBaseParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
    TString GetWorkloadName() const override;
    void Validate(const ECommandType commandType, int workloadType) override;

    void Init() override;

    TString TableName = "fulltext_test";
    ui64 MinPartitions = 1;
    ui64 PartitionSizeMb = 0;
    bool AutoPartitioningByLoad = true;
};

} // namespace NYdbWorkload
