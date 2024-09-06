#pragma once
#include "meta.h"

#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/defs.h>

namespace NKikimr::NOlap {

struct TInsertedData {
private:
    TInsertedDataMeta Meta;
    YDB_READONLY_DEF(TBlobRange, BlobRange);
    class TBlobStorageGuard {
    private:
        YDB_READONLY_DEF(TString, Data);

    public:
        TBlobStorageGuard(const TString& data)
            : Data(data) {
        }
        ~TBlobStorageGuard();
    };

    std::shared_ptr<TBlobStorageGuard> BlobDataGuard;
    YDB_READONLY(bool, Remove, false);

public:
    ui64 PlanStep = 0;
    ui64 WriteTxId = 0;
    ui64 PathId = 0;
    TString DedupId;

private:
    YDB_READONLY(ui64, SchemaVersion, 0);
    YDB_READONLY_FLAG(NotAbortable, false);

public:
    void SetRemove() {
        AFL_VERIFY(!Remove);
        Remove = true;
    }

    void MarkAsNotAbortable() {
        NotAbortableFlag = true;
    }

    std::optional<TString> GetBlobData() const {
        if (BlobDataGuard) {
            return BlobDataGuard->GetData();
        } else {
            return {};
        }
    }

    ui64 GetTxVolume() const {
        return Meta.GetTxVolume() + sizeof(TBlobRange);
    }

    const TInsertedDataMeta& GetMeta() const {
        return Meta;
    }

    TInsertedData() = delete;   // avoid invalid TInsertedData anywhere

    TInsertedData(ui64 planStep, ui64 writeTxId, ui64 pathId, TString dedupId, const TBlobRange& blobRange,
        const NKikimrTxColumnShard::TLogicalMetadata& proto, const ui64 schemaVersion, const std::optional<TString>& blobData);

    TInsertedData(ui64 writeTxId, ui64 pathId, TString dedupId, const TBlobRange& blobRange, const NKikimrTxColumnShard::TLogicalMetadata& proto,
        const ui64 schemaVersion, const std::optional<TString>& blobData)
        : TInsertedData(0, writeTxId, pathId, dedupId, blobRange, proto, schemaVersion, blobData) {
    }

    TInsertedData(ui64 writeTxId, ui64 pathId, TString dedupId, const TUnifiedBlobId& blobId,
        const NKikimrTxColumnShard::TLogicalMetadata& proto, const ui64 schemaVersion, const std::optional<TString>& blobData)
        : TInsertedData(0, writeTxId, pathId, dedupId, TBlobRange(blobId, 0, blobId.BlobSize()), proto, schemaVersion, blobData) {
    }

    ~TInsertedData();

    bool operator<(const TInsertedData& key) const {
        if (PlanStep < key.PlanStep) {
            return true;
        } else if (PlanStep > key.PlanStep) {
            return false;
        }

        // PlanStep == key.PlanStep
        if (WriteTxId < key.WriteTxId) {
            return true;
        } else if (WriteTxId > key.WriteTxId) {
            return false;
        }

        // PlanStep == key.PlanStep && WriteTxId == key.WriteTxId
        if (PathId < key.PathId) {
            return true;
        } else if (PathId > key.PathId) {
            return false;
        }

        return DedupId < key.DedupId;
    }

    bool operator==(const TInsertedData& key) const {
        return (PlanStep == key.PlanStep) && (WriteTxId == key.WriteTxId) && (PathId == key.PathId) && (DedupId == key.DedupId);
    }

    /// We commit many writeIds in one txId. There could be several blobs with same WriteId and different DedupId.
    /// One of them wins and becomes committed. Original DedupId would be lost then.
    /// After commit we use original Initiator:WriteId as DedupId of inserted blob inside {PlanStep, TxId}.
    /// pathId, initiator, {writeId}, {dedupId} -> pathId, planStep, txId, {dedupId}
    void Commit(const ui64 planStep, const ui64 txId) {
        DedupId = ToString(PlanStep) + ":" + ToString((ui64)WriteTxId);
        PlanStep = planStep;
        WriteTxId = txId;
    }

    /// Undo Commit() operation. Restore Initiator:WriteId from DedupId.
    void Undo() {
        TVector<TString> tokens;
        size_t numTokens = Split(DedupId, ":", tokens);
        Y_ABORT_UNLESS(numTokens == 2);

        PlanStep = FromString<ui64>(tokens[0]);
        WriteTxId = FromString<ui64>(tokens[1]);
        DedupId.clear();
    }

    TSnapshot GetSnapshot() const {
        return TSnapshot(PlanStep, WriteTxId);
    }

    ui32 BlobSize() const {
        return BlobRange.GetBlobSize();
    }
};

class TCommittedBlob {
private:
    TBlobRange BlobRange;
    std::variant<TSnapshot, TWriteId> WriteInfo;
    YDB_READONLY(ui64, SchemaVersion, 0);
    YDB_READONLY(ui64, RecordsCount, 0);
    YDB_READONLY(bool, IsDelete, false);
    YDB_READONLY_DEF(std::optional<NArrow::TReplaceKey>, First);
    YDB_READONLY_DEF(std::optional<NArrow::TReplaceKey>, Last);
    YDB_READONLY_DEF(NArrow::TSchemaSubset, SchemaSubset);

public:
    ui64 GetSize() const {
        return BlobRange.Size;
    }

    const NArrow::TReplaceKey& GetFirstVerified() const {
        Y_ABORT_UNLESS(First);
        return *First;
    }

    const NArrow::TReplaceKey& GetLastVerified() const {
        Y_ABORT_UNLESS(Last);
        return *Last;
    }

    TCommittedBlob(const TBlobRange& blobRange, const TSnapshot& snapshot, const ui64 schemaVersion, const ui64 recordsCount,
        const std::optional<NArrow::TReplaceKey>& first, const std::optional<NArrow::TReplaceKey>& last, const bool isDelete,
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

    TCommittedBlob(const TBlobRange& blobRange, const TWriteId writeId, const ui64 schemaVersion, const ui64 recordsCount,
        const std::optional<NArrow::TReplaceKey>& first, const std::optional<NArrow::TReplaceKey>& last, const bool isDelete,
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

    const TWriteId* GetWriteIdOptional() const {
        return std::get_if<TWriteId>(&WriteInfo);
    }

    TWriteId GetWriteIdVerified() const {
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
