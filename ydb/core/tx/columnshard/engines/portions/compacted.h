#pragma once
#include "portion_info.h"

namespace NKikimr::NOlap {

class TCompactedPortionInfo: public TPortionInfo {
private:
    using TBase = TPortionInfo;
    friend class TPortionInfoConstructor;
    virtual void DoSaveMetaToDatabase(NIceDb::TNiceDb& db) const override;

    virtual bool DoIsVisible(const TSnapshot& snapshot, const bool /*checkCommitSnapshot*/) const override {
        return RecordSnapshotMin(std::nullopt) <= snapshot;
    }

    virtual EPortionType GetPortionType() const override {
        return EPortionType::Compacted;
    }

    virtual std::shared_ptr<TPortionInfo> MakeCopy() const override {
        return std::make_shared<TCompactedPortionInfo>(*this);
    }

    virtual bool IsCommitted() const override {
        return true;
    }

public:
    using TBase::TBase;

    virtual void FillDefaultColumn(
        NAssembling::TColumnAssemblingInfo& /*column*/, const std::optional<TSnapshot>& /*defaultSnapshot*/) const override {
//        AFL_VERIFY(false);
    }
    virtual NSplitter::TEntityGroups GetEntityGroupsByStorageId(
        const TString& specialTier, const IStoragesManager& storages, const TIndexInfo& indexInfo) const override;
    virtual const TString& GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const override;
    virtual const TString& GetEntityStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const override;
    virtual const TString& GetIndexStorageId(const ui32 indexId, const TIndexInfo& indexInfo) const override;
    virtual std::unique_ptr<TPortionInfoConstructor> BuildConstructor(const bool withMetadata, const bool withMetadataBlobs) const override;
    virtual const TSnapshot& RecordSnapshotMin(const std::optional<TSnapshot>& /*snapshotDefault*/) const override;
    virtual const TSnapshot& RecordSnapshotMax(const std::optional<TSnapshot>& /*snapshotDefault*/) const override;
};

/// Ensure that TPortionInfo can be effectively assigned by moving the value.
static_assert(std::is_nothrow_move_assignable<TCompactedPortionInfo>::value);

/// Ensure that TPortionInfo can be effectively constructed by moving the value.
static_assert(std::is_nothrow_move_constructible<TCompactedPortionInfo>::value);

}   // namespace NKikimr::NOlap
