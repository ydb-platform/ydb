#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/abstract/workload_query_generator.h>

namespace NYdbWorkload {

    class TFulltextWorkloadParams final: public TWorkloadBaseParams {
    public:
        void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
        THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
        TWorkloadDataInitializer::TList CreateDataInitializers() const override;
        TString GetWorkloadName() const override;
        void Validate(const ECommandType commandType, int workloadType) override;

        void Init() override;

        TString TableName = "fulltext_workload";
        TString IndexName = "fulltext_index";
        TString IndexType = "fulltext_plain";
        THashSet<TString> IndexParams;

        ui64 MinPartitions = 1;
        ui64 PartitionSizeMb = 0;
        bool AutoPartitioningByLoad = true;

        TString QueryTable;
        ui64 Limit = 0;
    };

} // namespace NYdbWorkload
