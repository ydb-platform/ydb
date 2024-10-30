#pragma once

#include <ydb/core/tx/columnshard/engines/changes/abstract/compaction_info.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>

namespace NKikimr::NOlap {

class TGranuleMeta;

class TCompactColumnEngineChanges: public TChangesWithAppend {
private:
    using TBase = TChangesWithAppend;
    bool NeedGranuleStatusProvide = false;
protected:
    std::shared_ptr<TGranuleMeta> GranuleMeta;

    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;

    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual TPortionMeta::EProduced GetResultProducedClass() const = 0;
    virtual void OnAbortEmergency() override {
        NeedGranuleStatusProvide = false;
    }
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const override {
        const THashSet<ui64> pathIds = { GranuleMeta->GetPathId() };
        return std::make_shared<NDataLocks::TListTablesLock>(TypeString() + "::" + GetTaskIdentifier(), pathIds);
    }

public:
    std::vector<TPortionDataAccessor> SwitchedPortions; // Portions that would be replaced by new ones

    TCompactColumnEngineChanges(std::shared_ptr<TGranuleMeta> granule, const std::vector<TPortionDataAccessor>& portions, const TSaverContext& saverContext);
    ~TCompactColumnEngineChanges();

    static TString StaticTypeName() {
        return "CS::GENERAL";
    }
};

}
