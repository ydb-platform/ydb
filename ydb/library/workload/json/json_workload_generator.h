#pragma once

#include "json_workload_params.h"

#include <atomic>

#include <ydb/library/json_index/json_corpus.h>
#include <ydb/library/json_index/json_predicate.h>
#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NYdbWorkload {

using NKikimr::NJsonIndex::EJsonShape;
using NKikimr::NJsonIndex::kJsonCorpusNumShapes;
using NKikimr::NJsonIndex::TCorpusOptions;
using NKikimr::NJsonIndex::TJsonCorpus;
using NKikimr::NJsonIndex::TBuiltPredicate;

class TJsonWorkloadGenerator final: public TWorkloadQueryGeneratorBase<TJsonWorkloadParams> {
public:
    using TBase = TWorkloadQueryGeneratorBase<TJsonWorkloadParams>;
    TJsonWorkloadGenerator(const TJsonWorkloadParams* params);

    void Init() override;
    std::string GetDDLQueries() const override;
    TQueryInfoList GetInitialData() override;
    TVector<std::string> GetCleanPaths() const override;
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

private:
    TQueryInfoList Select();
    TQueryInfoList Upsert();

    std::optional<TJsonCorpus> Corpus;
    std::vector<TBuiltPredicate> Predicates;
    std::atomic<size_t> PredicateIndex = 0;
};

} // namespace NYdbWorkload
