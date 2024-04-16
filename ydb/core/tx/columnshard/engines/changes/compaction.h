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

    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) override;
    virtual TPortionMeta::EProduced GetResultProducedClass() const = 0;
    virtual void OnAbortEmergency() override {
        NeedGranuleStatusProvide = false;
    }
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const override {
        return std::make_shared<NDataLocks::TListPortionsLock>(SwitchedPortions);
    }

public:
    std::vector<TPortionInfo> SwitchedPortions; // Portions that would be replaced by new ones

    TCompactColumnEngineChanges(const TSplitSettings& splitSettings, std::shared_ptr<TGranuleMeta> granule, const std::vector<std::shared_ptr<TPortionInfo>>& portions, const TSaverContext& saverContext);
    ~TCompactColumnEngineChanges();

    static TString StaticTypeName() {
        return "CS::GENERAL";
    }
};

}
