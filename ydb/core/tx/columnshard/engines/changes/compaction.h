#pragma once

#include <ydb/core/tx/columnshard/engines/changes/abstract/compaction_info.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap {

class TGranuleMeta;

class TCompactColumnEngineChanges: public TChangesWithAppend {
private:
    using TBase = TChangesWithAppend;
    bool NeedGranuleStatusProvide = false;
protected:
    std::vector<TPortionInfo::TConstPtr> SwitchedPortions;   // Portions that would be replaced by new ones
    std::shared_ptr<TGranuleMeta> GranuleMeta;

    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;

    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual NPortion::EProduced GetResultProducedClass() const = 0;
    virtual void OnAbortEmergency() override {
        NeedGranuleStatusProvide = false;
    }
    virtual NDataLocks::ELockCategory GetLockCategory() const override {
        return NDataLocks::ELockCategory::Compaction;
    }
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const override {
        const THashSet<TInternalPathId> pathIds = { GranuleMeta->GetPathId() };
        return std::make_shared<NDataLocks::TListTablesLock>(TypeString() + "::" + GetTaskIdentifier(), pathIds, GetLockCategory());
    }

    virtual void OnDataAccessorsInitialized(const TDataAccessorsInitializationContext& context) override {
        TBase::OnDataAccessorsInitialized(context);
        THashMap<TString, THashSet<TBlobRange>> blobRanges;
        for (const auto& p : SwitchedPortions) {
            GetPortionDataAccessor(p->GetPortionId()).FillBlobRangesByStorage(blobRanges, *context.GetVersionedIndex());
        }

        for (const auto& p : blobRanges) {
            auto action = BlobsAction.GetReading(p.first);
            for (auto&& b : p.second) {
                action->AddRange(b);
            }
        }
    }

public:
    TCompactColumnEngineChanges(std::shared_ptr<TGranuleMeta> granule, const std::vector<TPortionInfo::TConstPtr>& portions, const TSaverContext& saverContext);
    ~TCompactColumnEngineChanges();

    const std::vector<TPortionInfo::TConstPtr>& GetSwitchedPortions() const {
        return SwitchedPortions;
    }

    static TString StaticTypeName() {
        return "CS::GENERAL";
    }
};

}
