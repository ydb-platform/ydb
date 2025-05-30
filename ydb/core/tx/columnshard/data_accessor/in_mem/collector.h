#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/data_accessor/abstract/collector.h>

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {
class TCollector: public IGranuleDataAccessor {
private:
    using TBase = IGranuleDataAccessor;
    THashMap<ui64, TPortionDataAccessor> Accessors;
    virtual void DoAskData(
        THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback) override;
    virtual TDataCategorized DoAnalyzeData(const TPortionsByConsumer& portions) override;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor> &add, const std::vector<ui64> &remove) override;
    virtual void DoResize(ui64 size) override;


public:
    TCollector(const TTabletId tabletId, const TInternalPathId pathId)
        : TBase(tabletId, pathId) {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl::NInMem
