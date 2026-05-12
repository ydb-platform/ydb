#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/abstract/workload_query_generator.h>

namespace NYdbWorkload {

    enum class EFulltextWorkloadType {
        Select,
        Upsert
    };

    class TFulltextWorkloadParams final: public TWorkloadBaseParams {
    public:
        void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
        THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
        TWorkloadDataInitializer::TList CreateDataInitializers() const override;
        TString GetWorkloadName() const override;
        void Validate(const ECommandType commandType, int workloadType) override;

        void Init() override;

        TString TableName = "fulltext_workload";
        TString IndexName = "index";
        TString IndexType = "fulltext_relevance";
        THashSet<TString> IndexParams;

        ui64 MinPartitions = 40;
        ui64 PartitionSizeMb = 2000;
        bool AutoPartitioningByLoad = true;

        TString QueryTable;
        ui64 TopSize = 1000;
        ui64 Limit = 0;

        int RunWorkloadType = -1;

        bool IndexIsRelevance = false;

        TString ModelPath;

        size_t SelectMinQueryLen = 1;
        size_t SelectMaxQueryLen = 5;

        ui64 UpsertBulkSize = 100;
        size_t UpsertMinSentenceLen = 100;
        size_t UpsertMaxSentenceLen = 1000;
    };

} // namespace NYdbWorkload
