#pragma once
#include "column_record.h"
#include "common.h"
#include "index_chunk.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/replace_key.h>

#include <util/generic/hash_set.h>

namespace NKikimrColumnShardDataSharingProto {
class TPortionInfo;
}

namespace NKikimr::NOlap {

namespace NBlobOperations::NRead {
class TCompositeReadBlobs;
}
class TPortionInfoConstructor;

struct TIndexInfo;
class TVersionedIndex;
class IDbWrapper;

class TEntityChunk {
private:
    TChunkAddress Address;
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY(ui64, RawBytes, 0);
    YDB_READONLY_DEF(TBlobRangeLink16, BlobRange);

public:
    const TChunkAddress& GetAddress() const {
        return Address;
    }

    TEntityChunk(const TChunkAddress& address, const ui32 recordsCount, const ui64 rawBytesSize, const TBlobRangeLink16& blobRange)
        : Address(address)
        , RecordsCount(recordsCount)
        , RawBytes(rawBytesSize)
        , BlobRange(blobRange) {
    }
};

class TPortionInfoConstructor;
class TGranuleShardingInfo;
class TPortionDataAccessor;

class TPortionInfo {
public:
    using TConstPtr = std::shared_ptr<const TPortionInfo>;
    using TPtr = std::shared_ptr<TPortionInfo>;
    using TRuntimeFeatures = ui8;
    enum class ERuntimeFeature : TRuntimeFeatures {
        Optimized = 1 /* "optimized" */
    };

private:
    friend class TPortionDataAccessor;
    friend class TPortionInfoConstructor;

    ui64 PrecalculatedColumnRawBytes = 0;
    ui64 PrecalculatedColumnBlobBytes = 0;
    ui64 PrecalculatedRecordsCount = 0;
    ui64 PrecalculatedIndexBlobBytes = 0;
    ui64 PrecalculatedIndexRawBytes = 0;
    bool Precalculated = false;

    void Precalculate();

    TPortionInfo(TPortionMeta&& meta)
        : Meta(std::move(meta)) {
        if (HasInsertWriteId()) {
            AFL_VERIFY(!Meta.GetTierName());
        }
    }
    std::optional<TSnapshot> CommitSnapshot;
    std::optional<TInsertWriteId> InsertWriteId;

    ui64 PathId = 0;
    ui64 PortionId = 0;   // Id of independent (overlayed by PK) portion of data in pathId
    TSnapshot MinSnapshotDeprecated = TSnapshot::Zero();   // {PlanStep, TxId} is min snapshot for {Granule, Portion}
    TSnapshot RemoveSnapshot = TSnapshot::Zero();
    std::optional<ui64> SchemaVersion;
    std::optional<ui64> ShardingVersion;

    TPortionMeta Meta;
    TRuntimeFeatures RuntimeFeatures = 0;
    std::vector<TUnifiedBlobId> BlobIds;

    std::vector<TIndexChunk> Indexes;
    std::vector<TColumnRecord> Records;

    void FullValidation() const {
        AFL_VERIFY(PathId);
        AFL_VERIFY(PortionId);
        AFL_VERIFY(MinSnapshotDeprecated.Valid());
        AFL_VERIFY(BlobIds.size());
    }

public:
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto);

    const std::vector<TUnifiedBlobId>& GetBlobIds() const {
        return BlobIds;
    }

    ui32 GetCompactionLevel() const {
        return GetMeta().GetCompactionLevel();
    }

    bool NeedShardingFilter(const TGranuleShardingInfo& shardingInfo) const;

    ui64 GetChunksCount() const {
        return Records.size() + Indexes.size();
    }

    NSplitter::TEntityGroups GetEntityGroupsByStorageId(
        const TString& specialTier, const IStoragesManager& storages, const TIndexInfo& indexInfo) const;

    const std::optional<ui64>& GetShardingVersionOptional() const {
        return ShardingVersion;
    }

    bool HasCommitSnapshot() const {
        return !!CommitSnapshot;
    }
    bool HasInsertWriteId() const {
        return !!InsertWriteId;
    }
    const TSnapshot& GetCommitSnapshotVerified() const {
        AFL_VERIFY(!!CommitSnapshot);
        return *CommitSnapshot;
    }
    TInsertWriteId GetInsertWriteIdVerified() const {
        AFL_VERIFY(InsertWriteId);
        return *InsertWriteId;
    }
    const std::optional<TSnapshot>& GetCommitSnapshotOptional() const {
        return CommitSnapshot;
    }
    const std::optional<TInsertWriteId>& GetInsertWriteIdOptional() const {
        return InsertWriteId;
    }
    void SetCommitSnapshot(const TSnapshot& value) {
        AFL_VERIFY(!!InsertWriteId);
        AFL_VERIFY(!CommitSnapshot);
        AFL_VERIFY(value.Valid());
        CommitSnapshot = value;
    }

    bool CrossSSWith(const TPortionInfo& p) const {
        return std::min(RecordSnapshotMax(), p.RecordSnapshotMax()) <= std::max(RecordSnapshotMin(), p.RecordSnapshotMin());
    }

    ui64 GetShardingVersionDef(const ui64 verDefault) const {
        return ShardingVersion.value_or(verDefault);
    }

    void SetRemoveSnapshot(const TSnapshot& snap) {
        AFL_VERIFY(!RemoveSnapshot.Valid());
        RemoveSnapshot = snap;
    }

    void SetRemoveSnapshot(const ui64 planStep, const ui64 txId) {
        SetRemoveSnapshot(TSnapshot(planStep, txId));
    }

    std::vector<TString> GetIndexInplaceDataVerified(const ui32 indexId) const {
        std::vector<TString> result;
        for (auto&& i : Indexes) {
            if (i.GetEntityId() == indexId) {
                result.emplace_back(i.GetBlobDataVerified());
            }
        }
        return result;
    }

    void InitRuntimeFeature(const ERuntimeFeature feature, const bool activity) {
        if (activity) {
            AddRuntimeFeature(feature);
        } else {
            RemoveRuntimeFeature(feature);
        }
    }

    void AddRuntimeFeature(const ERuntimeFeature feature) {
        RuntimeFeatures |= (TRuntimeFeatures)feature;
    }

    void RemoveRuntimeFeature(const ERuntimeFeature feature) {
        RuntimeFeatures &= (Max<TRuntimeFeatures>() - (TRuntimeFeatures)feature);
    }

    TString GetTierNameDef(const TString& defaultTierName) const {
        if (GetMeta().GetTierName()) {
            return GetMeta().GetTierName();
        }
        return defaultTierName;
    }

    bool HasRuntimeFeature(const ERuntimeFeature feature) const {
        if (feature == ERuntimeFeature::Optimized) {
            if ((RuntimeFeatures & (TRuntimeFeatures)feature)) {
                return true;
            } else {
                return GetTierNameDef(NOlap::NBlobOperations::TGlobal::DefaultStorageId) != NOlap::NBlobOperations::TGlobal::DefaultStorageId;
            }
        }
        return (RuntimeFeatures & (TRuntimeFeatures)feature);
    }

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return linkRange.RestoreRange(GetBlobId(linkRange.GetBlobIdxVerified()));
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        AFL_VERIFY(linkId < BlobIds.size());
        return BlobIds[linkId];
    }

    ui32 GetBlobIdsCount() const {
        return BlobIds.size();
    }

    const TString& GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const;
    const TString& GetIndexStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const;
    const TString& GetEntityStorageId(const ui32 entityId, const TIndexInfo& indexInfo) const;

    ui64 GetTxVolume() const;   // fake-correct method for determ volume on rewrite this portion in transaction progress
    ui64 GetMetadataMemorySize() const;

    static TConclusion<TPortionInfo::TPtr> BuildFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& indexInfo);
    void SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const;

    ui64 GetPathId() const {
        return PathId;
    }

    bool OlderThen(const TPortionInfo& info) const {
        return RecordSnapshotMin() < info.RecordSnapshotMin();
    }

    bool CrossPKWith(const TPortionInfo& info) const {
        return CrossPKWith(info.IndexKeyStart(), info.IndexKeyEnd());
    }

    bool CrossPKWith(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const {
        if (from < IndexKeyStart()) {
            if (to < IndexKeyEnd()) {
                return IndexKeyStart() <= to;
            } else {
                return true;
            }
        } else {
            if (to < IndexKeyEnd()) {
                return true;
            } else {
                return from <= IndexKeyEnd();
            }
        }
    }

    ui64 GetPortionId() const {
        return PortionId;
    }

    NJson::TJsonValue SerializeToJsonVisual() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("id", PortionId);
        result.InsertValue("s_max", RecordSnapshotMax().GetPlanStep() / 1000);
        /*
        result.InsertValue("s_min", RecordSnapshotMin().GetPlanStep());
        result.InsertValue("tx_min", RecordSnapshotMin().GetTxId());
        result.InsertValue("s_max", RecordSnapshotMax().GetPlanStep());
        result.InsertValue("tx_max", RecordSnapshotMax().GetTxId());
        result.InsertValue("pk_min", IndexKeyStart().DebugString());
        result.InsertValue("pk_max", IndexKeyEnd().DebugString());
        */
        return result;
    }

    static constexpr const ui32 BLOB_BYTES_LIMIT = 8 * 1024 * 1024;

    const TPortionMeta& GetMeta() const {
        return Meta;
    }

    TPortionMeta& MutableMeta() {
        return Meta;
    }

    bool ValidSnapshotInfo() const {
        return MinSnapshotDeprecated.Valid() && PathId && PortionId;
    }

    TString DebugString(const bool withDetails = false) const;

    bool HasRemoveSnapshot() const {
        return RemoveSnapshot.Valid();
    }

    bool IsRemovedFor(const TSnapshot& snapshot) const {
        if (!HasRemoveSnapshot()) {
            return false;
        } else {
            return GetRemoveSnapshotVerified() <= snapshot;
        }
    }

    bool CheckForCleanup(const TSnapshot& snapshot) const {
        return IsRemovedFor(snapshot);
    }

    bool CheckForCleanup() const {
        return HasRemoveSnapshot();
    }

    TPortionAddress GetAddress() const {
        return TPortionAddress(PathId, PortionId);
    }

    void ResetShardingVersion() {
        ShardingVersion.reset();
    }

    void SetPathId(const ui64 pathId) {
        PathId = pathId;
    }

    void SetPortionId(const ui64 id) {
        PortionId = id;
    }

    const TSnapshot& GetMinSnapshotDeprecated() const {
        return MinSnapshotDeprecated;
    }

    const TSnapshot& GetRemoveSnapshotVerified() const {
        AFL_VERIFY(HasRemoveSnapshot());
        return RemoveSnapshot;
    }

    std::optional<TSnapshot> GetRemoveSnapshotOptional() const {
        if (RemoveSnapshot.Valid()) {
            return RemoveSnapshot;
        } else {
            return {};
        }
    }

    ui64 GetSchemaVersionVerified() const {
        AFL_VERIFY(SchemaVersion);
        return SchemaVersion.value();
    }

    std::optional<ui64> GetSchemaVersionOptional() const {
        return SchemaVersion;
    }

    bool IsVisible(const TSnapshot& snapshot, const bool checkCommitSnapshot = true) const {
        const bool visible = (Meta.RecordSnapshotMin <= snapshot) && (!RemoveSnapshot.Valid() || snapshot < RemoveSnapshot) &&
                             (!checkCommitSnapshot || !CommitSnapshot || *CommitSnapshot <= snapshot);

        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "IsVisible")("analyze_portion", DebugString())("visible", visible)(
            "snapshot", snapshot.DebugString());
        return visible;
    }

    const NArrow::TReplaceKey& IndexKeyStart() const {
        return Meta.IndexKeyStart;
    }

    const NArrow::TReplaceKey& IndexKeyEnd() const {
        return Meta.IndexKeyEnd;
    }

    const TSnapshot& RecordSnapshotMin(const std::optional<TSnapshot>& snapshotDefault = std::nullopt) const {
        if (InsertWriteId) {
            if (CommitSnapshot) {
                return *CommitSnapshot;
            } else {
                AFL_VERIFY(snapshotDefault);
                return *snapshotDefault;
            }
        } else {
            return Meta.RecordSnapshotMin;
        }
    }

    const TSnapshot& RecordSnapshotMax(const std::optional<TSnapshot>& snapshotDefault = std::nullopt) const {
        if (InsertWriteId) {
            if (CommitSnapshot) {
                return *CommitSnapshot;
            } else {
                AFL_VERIFY(snapshotDefault);
                return *snapshotDefault;
            }
        } else {
            return Meta.RecordSnapshotMax;
        }
    }

    class TSchemaCursor {
        const NOlap::TVersionedIndex& VersionedIndex;
        ISnapshotSchema::TPtr CurrentSchema;
        TSnapshot LastSnapshot = TSnapshot::Zero();

    public:
        TSchemaCursor(const NOlap::TVersionedIndex& versionedIndex)
            : VersionedIndex(versionedIndex) {
        }

        ISnapshotSchema::TPtr GetSchema(const TPortionInfoConstructor& portion);

        ISnapshotSchema::TPtr GetSchema(const TPortionInfo& portion) {
            if (!CurrentSchema || portion.MinSnapshotDeprecated != LastSnapshot) {
                CurrentSchema = portion.GetSchema(VersionedIndex);
                LastSnapshot = portion.MinSnapshotDeprecated;
            }
            AFL_VERIFY(!!CurrentSchema)("portion", portion.DebugString());
            return CurrentSchema;
        }
    };

    ISnapshotSchema::TPtr GetSchema(const TVersionedIndex& index) const;

    ui32 GetRecordsCount() const {
        AFL_VERIFY(Precalculated);
        return PrecalculatedRecordsCount;
    }

    ui64 GetIndexBlobBytes() const noexcept {
        AFL_VERIFY(Precalculated);
        return PrecalculatedIndexBlobBytes;
    }

    ui64 GetIndexRawBytes() const noexcept {
        AFL_VERIFY(Precalculated);
        return PrecalculatedIndexRawBytes;
    }

    ui64 GetColumnRawBytes() const;
    ui64 GetColumnBlobBytes() const;

    ui64 GetTotalBlobBytes() const noexcept {
        return GetIndexBlobBytes() + GetColumnBlobBytes();
    }

    ui64 GetTotalRawBytes() const {
        return GetColumnRawBytes() + GetIndexRawBytes();
    }

    friend IOutputStream& operator<<(IOutputStream& out, const TPortionInfo& info) {
        out << info.DebugString();
        return out;
    }
};

/// Ensure that TPortionInfo can be effectively assigned by moving the value.
static_assert(std::is_nothrow_move_assignable<TPortionInfo>::value);

/// Ensure that TPortionInfo can be effectively constructed by moving the value.
static_assert(std::is_nothrow_move_constructible<TPortionInfo>::value);

}   // namespace NKikimr::NOlap
