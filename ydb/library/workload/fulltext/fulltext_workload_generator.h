#pragma once

#include "fulltext_workload_params.h"

#include <ydb/library/workload/abstract/workload_query_generator.h>

namespace NYdbWorkload {

class TFulltextWorkloadGenerator final: public TWorkloadQueryGeneratorBase<TFulltextWorkloadParams> {
public:
    using TBase = TWorkloadQueryGeneratorBase<TFulltextWorkloadParams>;
    TFulltextWorkloadGenerator(const TFulltextWorkloadParams* params);

    void Init() override;
    std::string GetDDLQueries() const override;
    TQueryInfoList GetInitialData() override;
    TVector<std::string> GetCleanPaths() const override;
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

private:
    TQueryInfoList Upsert();
    TQueryInfoList Select();
};

} // namespace NYdbWorkload
