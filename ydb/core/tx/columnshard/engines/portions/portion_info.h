#pragma once
#include "column_record.h"
#include "common.h"
#include "index_chunk.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/replace_key.h>

#include <util/generic/hash_set.h>

namespace NKikimrColumnShardDataSharingProto {
class TPortionInfo;
}

namespace NKikimr {
namespace NIceDb {
class TNiceDb;
}
}   // namespace NKikimr

namespace NKikimr::NOlap {

namespace NAssembling {
class TColumnAssemblingInfo;
}

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
class TCompactedPortionInfo;
class TWrittenPortionInfo;

enum class EPortionType {
    Written,
    Compacted
};

class TPortionInfo {
public:
    using TConstPtr = std::shared_ptr<const TPortionInfo>;
    using TPtr = std::shared_ptr<TPortionInfo>;
    using TRuntimeFeatures = ui8;
    enum class ERuntimeFeature : TRuntimeFeatures {
        Optimized = 1 /* "optimized" */
    };

private:
    friend class TPortionInfoConstructor;
    friend class TCompactedPortionInfo;
    friend class TWrittenPortionInfo;

    TPortionInfo(const TPortionInfo&) = default;
    TPortionInfo& operator=(const TPortionInfo&) = default;

    TInternalPathId PathId;
    ui64 PortionId = 0;   // Id of independent (overlayed by PK) portion of data in pathId
    TSnapshot RemoveSnapshot = TSnapshot::Zero();
    ui64 SchemaVersion = 0;
    std::optional<ui64> ShardingVersion;

    TPortionMeta Meta;
    TRuntimeFeatures RuntimeFeatures = 0;

    virtual void DoSaveMetaToDatabase(const std::vector<TUnifiedBlobId>& blobIds, NIceDb::TNiceDb& db) const = 0;

    virtual bool DoIsVisible(const TSnapshot& snapshot, const bool checkCommitSnapshot) const = 0;
    virtual TString DoDebugString(const bool /*withDetails*/) const {
        return "";
    }

public:
    struct TPortionAddressComparator {
        bool operator()(const TPortionInfo::TConstPtr& left, const TPortionInfo::TConstPtr& right) const {
            return left->GetAddress() < right->GetAddress();
        }
        bool operator()(const TPortionInfo::TPtr& left, const TPortionInfo::TPtr& right) const {
            return left->GetAddress() < right->GetAddress();
        }
    };

    void FullValidation() const {
        AFL_VERIFY(PathId);
        AFL_VERIFY(PortionId);
        AFL_VERIFY(SchemaVersion);
        Meta.FullValidation();
    }

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto);

    virtual EPortionType GetPortionType() const = 0;
    virtual bool IsCommitted() const = 0;
    NPortion::TPortionInfoForCompaction GetCompactionInfo() const {
        return NPortion::TPortionInfoForCompaction(GetTotalBlobBytes(), GetMeta().IndexKeyStart(), GetMeta().IndexKeyEnd());
    }

    ui64 GetMemorySize() const {
        return sizeof(TPortionInfo) + Meta.GetMemorySize() - sizeof(TPortionMeta);
    }

    virtual std::shared_ptr<TPortionInfo> MakeCopy() const = 0;

    ui64 GetDataSize() const {
        return sizeof(TPortionInfo) + Meta.GetDataSize() - sizeof(TPortionMeta);
    }

    virtual ~TPortionInfo() = default;

    TPortionInfo(TPortionMeta&& meta)
        : Meta(std::move(meta)) {
    }
    TPortionInfo(TPortionInfo&&) = default;
    TPortionInfo& operator=(TPortionInfo&&) = default;

    virtual void FillDefaultColumn(NAssembling::TColumnAssemblingInfo& column, const std::optional<TSnapshot>& defaultSnapshot) const = 0;

    ui32 PredictAccessorsMemory(const ISnapshotSchema::TPtr& schema) const {
        return (GetRecordsCount() / 10000 + 1) * sizeof(TColumnRecord) * schema->GetColumnsCount() +
               schema->GetIndexesCount() * sizeof(TIndexChunk);
    }

    ui32 PredictMetadataMemorySize(const ui32 columnsCount) const {
        return (GetRecordsCount() / 10000 + 1) * sizeof(TColumnRecord) * columnsCount;
    }

    void SaveMetaToDatabase(const std::vector<TUnifiedBlobId>& blobIds, NIceDb::TNiceDb& db) const {
        FullValidation();
        DoSaveMetaToDatabase(blobIds, db);
    }

    virtual std::unique_ptr<TPortionInfoConstructor> BuildConstructor(const bool withMetadata) const = 0;

    ui32 GetCompactionLevel() const {
        return GetMeta().GetCompactionLevel();
    }

    bool NeedShardingFilter(const TGranuleShardingInfo& shardingInfo) const;

    virtual NSplitter::TEntityGroups GetEntityGroupsByStorageId(
        const TString& specialTier, const IStoragesManager& storages, const TIndexInfo& indexInfo) const = 0;
    virtual const TString& GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const = 0;
    virtual const TString& GetEntityStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const = 0;
    virtual const TString& GetIndexStorageId(const ui32 indexId, const TIndexInfo& indexInfo) const = 0;

    const std::optional<ui64>& GetShardingVersionOptional() const {
        return ShardingVersion;
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

    ui64 GetTxVolume() const {
        return 1024;
    }

    ui64 GetApproxChunksCount(const ui32 schemaColumnsCount) const;
    ui64 GetMetadataMemorySize() const;

    void SerializeToProto(const std::vector<TUnifiedBlobId>& blobIds, NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const;

    TInternalPathId GetPathId() const {
        return PathId;
    }

    bool OlderThen(const TPortionInfo& info) const {
        return RecordSnapshotMin() < info.RecordSnapshotMin();
    }

    bool CrossPKWith(const TPortionInfo& info) const {
        return CrossPKWith(info.IndexKeyStart(), info.IndexKeyEnd());
    }

    template <class TReplaceKeyImpl>
    bool CrossPKWith(const TReplaceKeyImpl& from, const TReplaceKeyImpl& to) const {
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

    NPortion::EProduced GetProduced() const {
        if (HasRemoveSnapshot()) {
            return NPortion::INACTIVE;
        }
        if (GetTierNameDef(NBlobOperations::TGlobal::DefaultStorageId) != NBlobOperations::TGlobal::DefaultStorageId) {
            return NPortion::EVICTED;
        }
        return GetPortionType() == EPortionType::Compacted ? NPortion::EProduced::SPLIT_COMPACTED : NPortion::EProduced::INSERTED;
    }

    bool ValidSnapshotInfo() const {
        return SchemaVersion && PathId && PortionId;
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

    void SetPathId(const TInternalPathId pathId) {
        PathId = pathId;
    }

    void SetPortionId(const ui64 id) {
        PortionId = id;
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
        return SchemaVersion;
    }

    bool IsVisible(const TSnapshot& snapshot, const bool checkCommitSnapshot = true) const {
        const bool visible = (!RemoveSnapshot.Valid() || snapshot < RemoveSnapshot) && DoIsVisible(snapshot, checkCommitSnapshot);

        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "IsVisible")("analyze_portion", DebugString())("visible", visible)(
            "snapshot", snapshot.DebugString());
        return visible;
    }

    NArrow::TSimpleRow IndexKeyStart() const {
        return Meta.IndexKeyStart();
    }

    NArrow::TSimpleRow IndexKeyEnd() const {
        return Meta.IndexKeyEnd();
    }

    virtual const TSnapshot& RecordSnapshotMin(const std::optional<TSnapshot>& snapshotDefault = std::nullopt) const = 0;
    virtual const TSnapshot& RecordSnapshotMax(const std::optional<TSnapshot>& snapshotDefault = std::nullopt) const = 0;

    ISnapshotSchema::TPtr GetSchema(const TVersionedIndex& index) const;

    ui32 GetRecordsCount() const {
        return GetMeta().GetRecordsCount();
    }

    ui64 GetIndexBlobBytes() const noexcept {
        return GetMeta().GetIndexBlobBytes();
    }

    ui64 GetIndexRawBytes() const noexcept {
        return GetMeta().GetIndexRawBytes();
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

}   // namespace NKikimr::NOlap
