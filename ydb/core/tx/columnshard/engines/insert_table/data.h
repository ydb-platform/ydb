#pragma once
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/defs.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

namespace NKikimr::NOlap {

struct TInsertedData {
public:
    ui64 ShardOrPlan = 0;
    ui64 WriteTxId = 0;
    ui64 PathId = 0;
    TString DedupId;
    TUnifiedBlobId BlobId;
    TString Metadata;
    TInstant DirtyTime;

    TInsertedData() = delete; // avoid invalid TInsertedData anywhere

    TInsertedData(ui64 writeTxId, ui64 pathId, TString dedupId, const TUnifiedBlobId& blobId,
                  const NKikimrTxColumnShard::TLogicalMetadata& meta, const TInstant& writeTime, const TSnapshot& schemaVersion)
        : WriteTxId(writeTxId)
        , PathId(pathId)
        , DedupId(dedupId)
        , BlobId(blobId)
        , DirtyTime(writeTime)
        , SchemaVersion(schemaVersion)
    {
        Y_VERIFY(meta.SerializeToString(&Metadata));
    }

    TInsertedData(ui64 shardOrPlan, ui64 writeTxId, ui64 pathId, TString dedupId, const TUnifiedBlobId& blobId,
                  const TString& meta, const TInstant& writeTime, const std::optional<TSnapshot>& schemaVersion)
        : ShardOrPlan(shardOrPlan)
        , WriteTxId(writeTxId)
        , PathId(pathId)
        , DedupId(dedupId)
        , BlobId(blobId)
        , Metadata(meta)
        , DirtyTime(writeTime)    {
        if (schemaVersion) {
            SchemaVersion = *schemaVersion;
            Y_VERIFY(SchemaVersion.Valid());
        }
    }

    bool operator < (const TInsertedData& key) const {
        if (ShardOrPlan < key.ShardOrPlan) {
            return true;
        } else if (ShardOrPlan > key.ShardOrPlan) {
            return false;
        }

        // ShardOrPlan == key.ShardOrPlan
        if (WriteTxId < key.WriteTxId) {
            return true;
        } else if (WriteTxId > key.WriteTxId) {
            return false;
        }

        // ShardOrPlan == key.ShardOrPlan && WriteTxId == key.WriteTxId
        if (PathId < key.PathId) {
            return true;
        } else if (PathId > key.PathId) {
            return false;
        }

        return DedupId < key.DedupId;
    }

    bool operator == (const TInsertedData& key) const {
        return (ShardOrPlan == key.ShardOrPlan) &&
            (WriteTxId == key.WriteTxId) &&
            (PathId == key.PathId) &&
            (DedupId == key.DedupId);
    }

    /// We commit many writeIds in one txId. There could be several blobs with same WriteId and different DedupId.
    /// One of them wins and becomes committed. Original DedupId would be lost then.
    /// After commit we use original Initiator:WriteId as DedupId of inserted blob inside {PlanStep, TxId}.
    /// pathId, initiator, {writeId}, {dedupId} -> pathId, planStep, txId, {dedupId}
    void Commit(ui64 planStep, ui64 txId) {
        DedupId = ToString(ShardOrPlan) + ":" + ToString((ui64)WriteTxId);
        ShardOrPlan = planStep;
        WriteTxId = txId;
    }

    /// Undo Commit() operation. Restore Initiator:WriteId from DedupId.
    void Undo() {
        TVector<TString> tokens;
        size_t numTokens = Split(DedupId, ":", tokens);
        Y_VERIFY(numTokens == 2);

        ShardOrPlan = FromString<ui64>(tokens[0]);
        WriteTxId = FromString<ui64>(tokens[1]);
        DedupId.clear();
    }

    TSnapshot GetSnapshot() const {
        return TSnapshot(ShardOrPlan, WriteTxId);
    }

    const TSnapshot& GetSchemaSnapshot() const {
        return SchemaVersion;
    }

    ui32 BlobSize() const { return BlobId.BlobSize(); }

private:
    TSnapshot SchemaVersion = TSnapshot::Zero();
};

class TCommittedBlob {
private:
    TUnifiedBlobId BlobId;
    TSnapshot CommitSnapshot;
    TSnapshot SchemaSnapshot;
public:
    TCommittedBlob(const TUnifiedBlobId& blobId, const TSnapshot& snapshot, const TSnapshot& schemaSnapshot)
        : BlobId(blobId)
        , CommitSnapshot(snapshot)
        , SchemaSnapshot(schemaSnapshot)
    {}

    static TCommittedBlob BuildKeyBlob(const TUnifiedBlobId& blobId) {
        return TCommittedBlob(blobId, TSnapshot::Zero(), TSnapshot::Zero());
    }

    /// It uses trick then we place key wtih planStep:txId in container and find them later by BlobId only.
    /// So hash() and equality should depend on BlobId only.
    bool operator == (const TCommittedBlob& key) const { return BlobId == key.BlobId; }
    ui64 Hash() const noexcept { return BlobId.Hash(); }
    TString DebugString() const {
        return TStringBuilder() << BlobId << ";ps=" << CommitSnapshot.GetPlanStep() << ";ti=" << CommitSnapshot.GetTxId();
    }

    const TSnapshot& GetSnapshot() const {
        return CommitSnapshot;
    }

    const TSnapshot& GetSchemaSnapshot() const {
        return SchemaSnapshot;
    }

    const TUnifiedBlobId& GetBlobId() const {
        return BlobId;
    }
};

}

template <>
struct THash<NKikimr::NOlap::TCommittedBlob> {
    inline size_t operator() (const NKikimr::NOlap::TCommittedBlob& key) const {
        return key.Hash();
    }
};
