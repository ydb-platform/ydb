#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/abstract/workload_query_generator.h>

namespace NYdbWorkload {

enum class EJsonWorkloadType {
    Select,
    Upsert,
};

class TJsonWorkloadParams final: public TWorkloadBaseParams {
public:
    TJsonWorkloadParams() {
        BulkSize = 1000;
    }

    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
    TString GetWorkloadName() const override;
    void Validate(const ECommandType commandType, int workloadType) override;

    TString TableName = "json_workload";
    TString IndexName = "json_idx";

    bool Binary = false;
    bool IsStrict = false;

    ui64 MinPartitions = 40;
    ui64 PartitionSizeMb = 2000;
    bool AutoPartitioningByLoad = true;

    ui64 RowCount = 100000;
    ui64 Seed = 0xC0DE;
    size_t MaxPredicates = 100;
    ui64 Limit = 0;

    ui64 UpsertBulkSize = 50;

    int RunWorkloadType = -1;
    bool ImportOptsRegistered = false;

    TString GetJsonTypeName() const;
};

} // namespace NYdbWorkload
