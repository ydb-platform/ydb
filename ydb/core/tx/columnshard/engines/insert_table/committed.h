#pragma once
#include "user_data.h"

#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap {

class TCommittedData: public TUserDataContainer {
private:
    using TBase = TUserDataContainer;
    YDB_READONLY(TSnapshot, Snapshot, NOlap::TSnapshot::Zero());
    YDB_READONLY_DEF(TString, DedupId);
    YDB_READONLY(bool, Remove, false);

public:
    TCommittedData(const std::shared_ptr<TUserData>& userData, const ui64 planStep, const ui64 txId, const TInsertWriteId insertWriteId)
        : TBase(userData)
        , Snapshot(planStep, txId)
        , DedupId(ToString(planStep) + ":" + ToString((ui64)insertWriteId)) {
    }

    TCommittedData(const std::shared_ptr<TUserData>& userData, const ui64 planStep, const ui64 txId, const TString& dedupId)
        : TBase(userData)
        , Snapshot(planStep, txId)
        , DedupId(dedupId) {
    }

    TCommittedData(const std::shared_ptr<TUserData>& userData, const TSnapshot& ss, const ui64 generation, const TInsertWriteId ephemeralWriteId)
        : TBase(userData)
        , Snapshot(ss)
        , DedupId(ToString(generation) + ":" + ToString(ephemeralWriteId)) {
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
    std::variant<TSnapshot, TInsertWriteId> WriteInfo;
    YDB_READONLY(ui64, SchemaVersion, 0);
    YDB_READONLY(ui64, RecordsCount, 0);
    YDB_READONLY(bool, IsDelete, false);
    NArrow::TReplaceKey First;
    NArrow::TReplaceKey Last;
    YDB_READONLY_DEF(NArrow::TSchemaSubset, SchemaSubset);

public:
    const NArrow::TReplaceKey& GetFirst() const {
        return First;
    }
    const NArrow::TReplaceKey& GetLast() const {
        return Last;
    }

    ui64 GetSize() const {
        return BlobRange.Size;
    }

    TCommittedBlob(const TBlobRange& blobRange, const TSnapshot& snapshot, const ui64 schemaVersion, const ui64 recordsCount,
        const NArrow::TReplaceKey& first, const NArrow::TReplaceKey& last, const bool isDelete,
        const NArrow::TSchemaSubset& subset)
        : BlobRange(blobRange)
        , WriteInfo(snapshot)
        , SchemaVersion(schemaVersion)
        , RecordsCount(recordsCount)
        , IsDelete(isDelete)
        , First(first)
        , Last(last)
        , SchemaSubset(subset) {
    }

    TCommittedBlob(const TBlobRange& blobRange, const TInsertWriteId writeId, const ui64 schemaVersion, const ui64 recordsCount,
        const NArrow::TReplaceKey& first, const NArrow::TReplaceKey& last, const bool isDelete,
        const NArrow::TSchemaSubset& subset)
        : BlobRange(blobRange)
        , WriteInfo(writeId)
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
        if (auto* ss = GetSnapshotOptional()) {
            return TStringBuilder() << BlobRange << ";snapshot=" << ss->DebugString();
        } else {
            return TStringBuilder() << BlobRange << ";write_id=" << (ui64)GetWriteIdVerified();
        }
    }

    bool HasSnapshot() const {
        return GetSnapshotOptional();
    }

    const TSnapshot& GetSnapshotDef(const TSnapshot& def) const {
        if (auto* snapshot = GetSnapshotOptional()) {
            return *snapshot;
        } else {
            return def;
        }
    }

    const TSnapshot* GetSnapshotOptional() const {
        return std::get_if<TSnapshot>(&WriteInfo);
    }

    const TSnapshot& GetSnapshotVerified() const {
        auto* result = GetSnapshotOptional();
        AFL_VERIFY(result);
        return *result;
    }

    const TInsertWriteId* GetWriteIdOptional() const {
        return std::get_if<TInsertWriteId>(&WriteInfo);
    }

    TInsertWriteId GetWriteIdVerified() const {
        auto* result = GetWriteIdOptional();
        AFL_VERIFY(result);
        return *result;
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
