#pragma once

#include "fulltext_workload_params.h"
#include "markov_model_evaluator.h"

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <random>

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
        TQueryInfoList Select();
        TQueryInfoList Upsert();
        void LoadQueries();

        TVector<TString> Queries;
        size_t CurrentIndex = 0;

        TMaybe<TMarkovModelEvaluator> Evaluator;
    };

} // namespace NYdbWorkload
