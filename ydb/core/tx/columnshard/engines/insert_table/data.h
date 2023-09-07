#pragma once
#include "meta.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/defs.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

namespace NKikimr::NOlap {

struct TInsertedData {
private:
    TInsertedDataMeta Meta;
    YDB_READONLY_DEF(TBlobRange, BlobRange);

public:
    const TInsertedDataMeta& GetMeta() const {
        return Meta;
    }

    ui64 PlanStep = 0;
    ui64 WriteTxId = 0;
    ui64 PathId = 0;
    TString DedupId;

    TInsertedData() = delete; // avoid invalid TInsertedData anywhere

    TInsertedData(ui64 planStep, ui64 writeTxId, ui64 pathId, TString dedupId, const TBlobRange& blobRange,
        const NKikimrTxColumnShard::TLogicalMetadata& proto, const TSnapshot& schemaVersion)
        : Meta(proto)
        , BlobRange(blobRange)
        , PlanStep(planStep)
        , WriteTxId(writeTxId)
        , PathId(pathId)
        , DedupId(dedupId)
        , SchemaVersion(schemaVersion) {
        Y_VERIFY(SchemaVersion.Valid());
    }

    TInsertedData(ui64 writeTxId, ui64 pathId, TString dedupId, const TBlobRange& blobRange,
        const NKikimrTxColumnShard::TLogicalMetadata& proto, const TSnapshot& schemaVersion)
        : TInsertedData(0, writeTxId, pathId, dedupId, blobRange, proto, schemaVersion)
    {}


    TInsertedData(ui64 writeTxId, ui64 pathId, TString dedupId, const TUnifiedBlobId& blobId,
        const NKikimrTxColumnShard::TLogicalMetadata& proto, const TSnapshot& schemaVersion)
        : TInsertedData(0, writeTxId, pathId, dedupId, TBlobRange(blobId, 0, blobId.BlobSize()), proto, schemaVersion)
    {
    }

    bool operator < (const TInsertedData& key) const {
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

    bool operator == (const TInsertedData& key) const {
        return (PlanStep == key.PlanStep) &&
            (WriteTxId == key.WriteTxId) &&
            (PathId == key.PathId) &&
            (DedupId == key.DedupId);
    }

    /// We commit many writeIds in one txId. There could be several blobs with same WriteId and different DedupId.
    /// One of them wins and becomes committed. Original DedupId would be lost then.
    /// After commit we use original Initiator:WriteId as DedupId of inserted blob inside {PlanStep, TxId}.
    /// pathId, initiator, {writeId}, {dedupId} -> pathId, planStep, txId, {dedupId}
    void Commit(ui64 planStep, ui64 txId) {
        DedupId = ToString(PlanStep) + ":" + ToString((ui64)WriteTxId);
        PlanStep = planStep;
        WriteTxId = txId;
    }

    /// Undo Commit() operation. Restore Initiator:WriteId from DedupId.
    void Undo() {
        TVector<TString> tokens;
        size_t numTokens = Split(DedupId, ":", tokens);
        Y_VERIFY(numTokens == 2);

        PlanStep = FromString<ui64>(tokens[0]);
        WriteTxId = FromString<ui64>(tokens[1]);
        DedupId.clear();
    }

    TSnapshot GetSnapshot() const {
        return TSnapshot(PlanStep, WriteTxId);
    }

    const TSnapshot& GetSchemaSnapshot() const {
        return SchemaVersion;
    }

    ui32 BlobSize() const { return BlobRange.GetBlobSize(); }

private:
    TSnapshot SchemaVersion = TSnapshot::Zero();
};

class TCommittedBlob {
private:
    TBlobRange BlobRange;
    TSnapshot CommitSnapshot;
    TSnapshot SchemaSnapshot;
    YDB_READONLY_DEF(std::optional<NArrow::TReplaceKey>, First);
    YDB_READONLY_DEF(std::optional<NArrow::TReplaceKey>, Last);
public:
    const NArrow::TReplaceKey& GetFirstVerified() const {
        Y_VERIFY(First);
        return *First;
    }

    const NArrow::TReplaceKey& GetLastVerified() const {
        Y_VERIFY(Last);
        return *Last;
    }

    TCommittedBlob(const TBlobRange& blobRange, const TSnapshot& snapshot, const TSnapshot& schemaSnapshot, const std::optional<NArrow::TReplaceKey>& first, const std::optional<NArrow::TReplaceKey>& last)
        : BlobRange(blobRange)
        , CommitSnapshot(snapshot)
        , SchemaSnapshot(schemaSnapshot)
        , First(first)
        , Last(last)
    {}

    /// It uses trick then we place key wtih planStep:txId in container and find them later by BlobId only.
    /// So hash() and equality should depend on BlobId only.
    bool operator == (const TCommittedBlob& key) const { return BlobRange == key.BlobRange; }
    ui64 Hash() const noexcept { return BlobRange.Hash(); }
    TString DebugString() const {
        return TStringBuilder() << BlobRange << ";ps=" << CommitSnapshot.GetPlanStep() << ";ti=" << CommitSnapshot.GetTxId();
    }

    const TSnapshot& GetSnapshot() const {
        return CommitSnapshot;
    }

    const TSnapshot& GetSchemaSnapshot() const {
        return SchemaSnapshot;
    }

    const TBlobRange& GetBlobRange() const {
        return BlobRange;
    }
};

}

template <>
struct THash<NKikimr::NOlap::TCommittedBlob> {
    inline size_t operator() (const NKikimr::NOlap::TCommittedBlob& key) const {
        return key.Hash();
    }
};
