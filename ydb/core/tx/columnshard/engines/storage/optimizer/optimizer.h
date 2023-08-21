#pragma once
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NOlap {
struct TCompactionLimits;
class TGranuleMeta;
}

namespace NKikimr::NOlap::NStorageOptimizer {

class IOptimizerPlanner {
private:
    const ui64 GranuleId;
protected:
    virtual void DoAddPortion(const std::shared_ptr<TPortionInfo>& info) = 0;
    virtual void DoRemovePortion(const std::shared_ptr<TPortionInfo>& info) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const = 0;
    virtual i64 DoGetUsefulMetric() const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<IOptimizerPlanner, TString>;
    IOptimizerPlanner(const ui64 granuleId)
        : GranuleId(granuleId)
    {

    }


    virtual ~IOptimizerPlanner() = default;
    virtual TString GetDescription() const {
        return "";
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& info) {
        Y_VERIFY(info);
        NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("granule_id", GranuleId));
        DoAddPortion(info);
    }
    void RemovePortion(const std::shared_ptr<TPortionInfo>& info) {
        Y_VERIFY(info);
        NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("granule_id", GranuleId));
        DoRemovePortion(info);
    }
    std::shared_ptr<TColumnEngineChanges> GetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const {
        NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("granule_id", GranuleId));
        return DoGetOptimizationTask(limits, granule, busyPortions);
    }
    i64 GetUsefulMetric() const {
        return DoGetUsefulMetric();
    }
};

} // namespace NKikimr::NOlap
