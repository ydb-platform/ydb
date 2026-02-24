#pragma once

#include "fulltext_workload_params.h"

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <util/generic/vector.h>

namespace NYdbWorkload {

    enum class EFulltextWorkloadType {
        Select
    };

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
        TQueryInfoList Select();
        void LoadQueries();

        TVector<TString> Queries;
        size_t CurrentIndex = 0;
    };

} // namespace NYdbWorkload
