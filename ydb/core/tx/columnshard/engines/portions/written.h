#pragma once
#include "portion_info.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

class TWrittenPortionInfoConstructor;
class IDbWrapper;

class TWrittenPortionInfo: public TPortionInfo {
private:
    using TBase = TPortionInfo;
    std::optional<TSnapshot> CommitSnapshot;
    std::optional<TInsertWriteId> InsertWriteId;
    friend class TWrittenPortionInfoConstructor;

    virtual void DoSaveMetaToDatabase(const std::vector<TUnifiedBlobId>& blobIds, NIceDb::TNiceDb& db) const override;

    virtual EPortionType GetPortionType() const override {
        return EPortionType::Written;
    }

    virtual bool DoIsVisible(const TSnapshot& snapshot, const bool checkCommitSnapshot) const override;

    virtual std::shared_ptr<TPortionInfo> MakeCopy() const override {
        return std::make_shared<TWrittenPortionInfo>(*this);
    }

    virtual TString DoDebugString(const bool /*withDetails*/) const override {
        TStringBuilder sb;
        if (CommitSnapshot) {
            sb << "cs:" << CommitSnapshot->DebugString() << ";";
        }
        if (InsertWriteId) {
            sb << "wi:" << (ui64)*InsertWriteId << ";";
        }
        return sb;
    }

public:
    virtual void FillDefaultColumn(NAssembling::TColumnAssemblingInfo& column, const std::optional<TSnapshot>& defaultSnapshot) const override;

    void CommitToDatabase(IDbWrapper& wrapper);

    virtual NSplitter::TEntityGroups GetEntityGroupsByStorageId(
        const TString& specialTier, const IStoragesManager& storages, const TIndexInfo& /*indexInfo*/) const override {
        const TString& storageId = [&]() {
            if (specialTier && specialTier != IStoragesManager::DefaultStorageId) {
                return specialTier;
            }
            return IStoragesManager::DefaultStorageId;
        }();
        NSplitter::TEntityGroups groups(storages.GetOperatorVerified(storageId)->GetBlobSplitSettings(), storageId);
        return groups;
    }

    virtual const TString& GetColumnStorageId(const ui32 /*columnId*/, const TIndexInfo& /*indexInfo*/) const override {
        if (GetMeta().GetTierName()) {
            return GetMeta().GetTierName();
        }
        return NBlobOperations::TGlobal::DefaultStorageId;
    }

    virtual const TString& GetEntityStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const override {
        if (indexInfo.GetIndexOptional(columnId)) {
            return GetIndexStorageId(columnId, indexInfo);
        }
        return GetColumnStorageId(columnId, indexInfo);
    }

    virtual const TString& GetIndexStorageId(const ui32 indexId, const TIndexInfo& indexInfo) const override {
        if (indexInfo.GetIndexVerified(indexId)->GetInheritPortionStorage()) {
            return GetColumnStorageId(0, indexInfo);
        } else {
            return IStoragesManager::DefaultStorageId;
        }
    }

    virtual std::unique_ptr<TPortionInfoConstructor> BuildConstructor(const bool withMetadata) const override;

    TWrittenPortionInfo(TPortionMeta&& meta)
        : TBase(std::move(meta)) {
//        AFL_VERIFY(!GetMeta().GetTierName());
    }

    bool HasCommitSnapshot() const {
        return !!CommitSnapshot;
    }

    virtual bool IsCommitted() const override {
        return !!CommitSnapshot;
    }

    const TSnapshot& GetCommitSnapshotVerified() const {
        AFL_VERIFY(!!CommitSnapshot);
        return *CommitSnapshot;
    }
    const std::optional<TSnapshot>& GetCommitSnapshotOptional() const {
        return CommitSnapshot;
    }
    TInsertWriteId GetInsertWriteId() const {
        AFL_VERIFY(!!InsertWriteId);
        return *InsertWriteId;
    }
    void SetCommitSnapshot(const TSnapshot& value) {
        AFL_VERIFY(!!InsertWriteId);
        AFL_VERIFY(!CommitSnapshot);
        AFL_VERIFY(value.Valid());
        CommitSnapshot = value;
    }

    virtual const TSnapshot& RecordSnapshotMin(const std::optional<TSnapshot>& snapshotDefault = std::nullopt) const override {
        if (CommitSnapshot) {
            return *CommitSnapshot;
        } else {
            AFL_VERIFY(snapshotDefault);
            return *snapshotDefault;
        }
    }

    virtual const TSnapshot& RecordSnapshotMax(const std::optional<TSnapshot>& snapshotDefault = std::nullopt) const override {
        if (CommitSnapshot) {
            return *CommitSnapshot;
        } else {
            AFL_VERIFY(snapshotDefault);
            return *snapshotDefault;
        }
    }
};

/// Ensure that TWrittenPortionInfo can be effectively assigned by moving the value.
static_assert(std::is_nothrow_move_assignable<TWrittenPortionInfo>::value);

/// Ensure that TWrittenPortionInfo can be effectively constructed by moving the value.
static_assert(std::is_nothrow_move_constructible<TWrittenPortionInfo>::value);

}   // namespace NKikimr::NOlap
