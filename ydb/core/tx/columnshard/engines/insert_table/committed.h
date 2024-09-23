#pragma once
#include "user_data.h"

#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap {

class TCommittedData: public TUserDataContainer {
private:
    using TBase = TUserDataContainer;
    YDB_READONLY(TSnapshot, Snapshot, NOlap::TSnapshot::Zero());
    YDB_READONLY(TInsertWriteId, InsertWriteId, (TInsertWriteId)0);
    YDB_READONLY_DEF(TString, DedupId);
    YDB_READONLY(bool, Remove, false);

public:
    TCommittedData(const std::shared_ptr<TUserData>& userData, const ui64 planStep, const ui64 txId, const TInsertWriteId insertWriteId)
        : TBase(userData)
        , Snapshot(planStep, txId)
        , InsertWriteId(insertWriteId)
        , DedupId(ToString(planStep) + ":" + ToString((ui64)insertWriteId)) {
    }

    TCommittedData(const std::shared_ptr<TUserData>& userData, const ui64 planStep, const ui64 txId, const TInsertWriteId insertWriteId,
        const TString& dedupId)
        : TBase(userData)
        , Snapshot(planStep, txId)
        , InsertWriteId(insertWriteId)
        , DedupId(dedupId) {
    }

    TCommittedData(const std::shared_ptr<TUserData>& userData, const TSnapshot& ss, const ui64 generation, const TInsertWriteId ephemeralWriteId)
        : TBase(userData)
        , Snapshot(ss)
        , InsertWriteId(ephemeralWriteId)
        , DedupId(ToString(generation) + ":" + ToString((ui64)ephemeralWriteId)) {
    }

    void SetRemove() {
        AFL_VERIFY(!Remove);
        Remove = true;
    }

    bool operator<(const TCommittedData& key) const {
        if (Snapshot == key.Snapshot) {
            if (UserData->GetPathId() == key.UserData->GetPathId()) {
                return DedupId < key.DedupId;
            } else {
                return UserData->GetPathId() < key.UserData->GetPathId();
            }
        } else {
            return Snapshot < key.Snapshot;
        }
    }
};

class TCommittedBlob {
private:
    TBlobRange BlobRange;
    std::optional<TSnapshot> CommittedSnapshot;
    const TInsertWriteId InsertWriteId;
    YDB_READONLY(ui64, SchemaVersion, 0);
    YDB_READONLY(ui64, RecordsCount, 0);
    YDB_READONLY(bool, IsDelete, false);
    NArrow::TReplaceKey First;
    NArrow::TReplaceKey Last;
    YDB_READONLY_DEF(NArrow::TSchemaSubset, SchemaSubset);

public:
    const std::optional<TSnapshot>& GetCommittedSnapshot() const {
        return CommittedSnapshot;
    }

    const TSnapshot& GetCommittedSnapshotDef(const TSnapshot& def) const {
        if (CommittedSnapshot) {
            return *CommittedSnapshot;
        } else {
            return def;
        }
    }

    const TSnapshot& GetCommittedSnapshotVerified() const {
        AFL_VERIFY(!!CommittedSnapshot);
        return *CommittedSnapshot;
    }

    bool IsCommitted() const {
        return !!CommittedSnapshot;
    }

    TInsertWriteId GetInsertWriteId() const {
        return InsertWriteId;
    }

    const NArrow::TReplaceKey& GetFirst() const {
        return First;
    }
    const NArrow::TReplaceKey& GetLast() const {
        return Last;
    }

    ui64 GetSize() const {
        return BlobRange.Size;
    }

    TCommittedBlob(const TBlobRange& blobRange, const TSnapshot& snapshot, const TInsertWriteId insertWriteId, const ui64 schemaVersion, const ui64 recordsCount,
        const NArrow::TReplaceKey& first, const NArrow::TReplaceKey& last, const bool isDelete,
        const NArrow::TSchemaSubset& subset)
        : BlobRange(blobRange)
        , CommittedSnapshot(snapshot)
        , InsertWriteId(insertWriteId)
        , SchemaVersion(schemaVersion)
        , RecordsCount(recordsCount)
        , IsDelete(isDelete)
        , First(first)
        , Last(last)
        , SchemaSubset(subset) {
    }

    TCommittedBlob(const TBlobRange& blobRange, const TInsertWriteId insertWriteId, const ui64 schemaVersion, const ui64 recordsCount,
        const NArrow::TReplaceKey& first, const NArrow::TReplaceKey& last, const bool isDelete,
        const NArrow::TSchemaSubset& subset)
        : BlobRange(blobRange)
        , InsertWriteId(insertWriteId)
        , SchemaVersion(schemaVersion)
        , RecordsCount(recordsCount)
        , IsDelete(isDelete)
        , First(first)
        , Last(last)
        , SchemaSubset(subset) {
    }

    /// It uses trick then we place key with planStep:txId in container and find them later by BlobId only.
    /// So hash() and equality should depend on BlobId only.
    bool operator==(const TCommittedBlob& key) const {
        return BlobRange == key.BlobRange;
    }
    ui64 Hash() const noexcept {
        return BlobRange.Hash();
    }
    TString DebugString() const {
        TStringBuilder sb;
        sb << BlobRange;
        if (CommittedSnapshot) {
            sb << ";snapshot=" << CommittedSnapshot->DebugString();
        }
        sb << ";write_id=" << GetInsertWriteId();
        return sb;
    }

    const TBlobRange& GetBlobRange() const {
        return BlobRange;
    }
};

}   // namespace NKikimr::NOlap

template <>
struct THash<NKikimr::NOlap::TCommittedBlob> {
    inline size_t operator()(const NKikimr::NOlap::TCommittedBlob& key) const {
        return key.Hash();
    }
};
