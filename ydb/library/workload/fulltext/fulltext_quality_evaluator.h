#pragma once

#include "fulltext_workload_params.h"

#include <util/generic/vector.h>

#include <unordered_map>
#include <vector>
#include <string>

namespace NYdbWorkload {

class TFulltextQualityEvaluator {
public:
    TFulltextQualityEvaluator(const TFulltextWorkloadParams& params);

    void MeasureQuality();

    double GetAverageNdcg() const;
    size_t GetProcessedQueries() const;

private:
    struct TRelevanceJudgment {
        ui64 DocumentId;
        float Relevance;
    };

    void LoadQueries();
    void LoadRelevances();
    double ComputeDcg(const TVector<float>& relevances, size_t k) const;
    double ComputeNdcg(const TVector<float>& retrievedRelevances, const TVector<float>& idealRelevances, size_t k) const;

    const TFulltextWorkloadParams& Params;

    // query_id -> query text
    std::unordered_map<ui64, TString> Queries;
    // query_id -> list of (document_id, relevance)
    std::unordered_map<ui64, std::vector<TRelevanceJudgment>> Relevances;

    double TotalNdcg = 0.0;
    size_t ProcessedQueries = 0;

    static constexpr size_t K = 10;
};

} // namespace NYdbWorkload
