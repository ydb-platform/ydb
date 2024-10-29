#include "column_record.h"
#include "constructor.h"
#include "data_accessor.h"
#include "portion_info.h"

#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

namespace NKikimr::NOlap {

ui64 TPortionInfo::GetColumnRawBytes() const {
    AFL_VERIFY(Precalculated);
    return PrecalculatedColumnRawBytes;
}

ui64 TPortionInfo::GetColumnBlobBytes() const {
    AFL_VERIFY(Precalculated);
    return PrecalculatedColumnBlobBytes;
}

TString TPortionInfo::DebugString(const bool withDetails) const {
    TStringBuilder sb;
    sb << "(portion_id:" << PortionId << ";"
       << "path_id:" << PathId << ";records_count:" << GetRecordsCount()
       << ";"
          "min_schema_snapshot:("
       << MinSnapshotDeprecated.DebugString()
       << ");"
          "schema_version:"
       << SchemaVersion.value_or(0)
       << ";"
          "level:"
       << GetMeta().GetCompactionLevel() << ";";
    if (withDetails) {
        sb << "records_snapshot_min:(" << RecordSnapshotMin().DebugString() << ");"
           << "records_snapshot_max:(" << RecordSnapshotMax().DebugString() << ");"
           << "from:" << IndexKeyStart().DebugString() << ";"
           << "to:" << IndexKeyEnd().DebugString() << ";";
    }
    sb << "column_size:" << GetColumnBlobBytes() << ";"
       << "index_size:" << GetIndexBlobBytes() << ";"
       << "meta:(" << Meta.DebugString() << ");";
    if (RemoveSnapshot.Valid()) {
        sb << "remove_snapshot:(" << RemoveSnapshot.DebugString() << ");";
    }
    return sb << ")";
}

ui64 TPortionInfo::GetMetadataMemorySize() const {
    return sizeof(TPortionInfo) + Records.size() * (sizeof(TColumnRecord) + 8) + Indexes.size() * sizeof(TIndexChunk) +
           BlobIds.size() * sizeof(TUnifiedBlobId) - sizeof(TPortionMeta) + Meta.GetMetadataMemorySize();
}

ui64 TPortionInfo::GetTxVolume() const {
    return 1024 + Records.size() * 256 + Indexes.size() * 256;
}

void TPortionInfo::SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const {
    proto.SetPathId(PathId);
    proto.SetPortionId(PortionId);
    proto.SetSchemaVersion(GetSchemaVersionVerified());
    *proto.MutableMinSnapshotDeprecated() = MinSnapshotDeprecated.SerializeToProto();
    if (!RemoveSnapshot.IsZero()) {
        *proto.MutableRemoveSnapshot() = RemoveSnapshot.SerializeToProto();
    }
    for (auto&& i : BlobIds) {
        *proto.AddBlobIds() = i.SerializeToProto();
    }

    *proto.MutableMeta() = Meta.SerializeToProto();
}

TConclusionStatus TPortionInfo::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto) {
    PathId = proto.GetPathId();
    PortionId = proto.GetPortionId();
    SchemaVersion = proto.GetSchemaVersion();
    for (auto&& i : proto.GetBlobIds()) {
        auto blobId = TUnifiedBlobId::BuildFromProto(i);
        if (!blobId) {
            return blobId;
        }
        BlobIds.emplace_back(blobId.DetachResult());
    }
    {
        auto parse = MinSnapshotDeprecated.DeserializeFromProto(proto.GetMinSnapshotDeprecated());
        if (!parse) {
            return parse;
        }
    }
    if (proto.HasRemoveSnapshot()) {
        auto parse = RemoveSnapshot.DeserializeFromProto(proto.GetRemoveSnapshot());
        if (!parse) {
            return parse;
        }
    }
    for (auto&& i : proto.GetRecords()) {
        auto parse = TColumnRecord::BuildFromProto(i);
        if (!parse) {
            return parse;
        }
        Records.emplace_back(std::move(parse.DetachResult()));
    }
    for (auto&& i : proto.GetIndexes()) {
        auto parse = TIndexChunk::BuildFromProto(i);
        if (!parse) {
            return parse;
        }
        Indexes.emplace_back(std::move(parse.DetachResult()));
    }
    Precalculate();
    return TConclusionStatus::Success();
}

TConclusion<TPortionInfo> TPortionInfo::BuildFromProto(
    const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& indexInfo) {
    TPortionMetaConstructor constructor;
    if (!constructor.LoadMetadata(proto.GetMeta(), indexInfo)) {
        return TConclusionStatus::Fail("cannot parse meta");
    }
    TPortionInfo result(constructor.Build());
    {
        auto parse = result.DeserializeFromProto(proto);
        if (!parse) {
            return parse;
        }
    }
    {
        auto parse = TPortionDataAccessor(result).DeserializeFromProto(proto);
        if (!parse) {
            return parse;
        }
    }
    return result;
}

const TString& TPortionInfo::GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const {
    if (HasInsertWriteId()) {
        return { NBlobOperations::TGlobal::DefaultStorageId };
    }
    return indexInfo.GetColumnStorageId(columnId, GetMeta().GetTierName());
}

const TString& TPortionInfo::GetEntityStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const {
    if (HasInsertWriteId()) {
        return { NBlobOperations::TGlobal::DefaultStorageId };
    }
    return indexInfo.GetEntityStorageId(columnId, GetMeta().GetTierName());
}

const TString& TPortionInfo::GetIndexStorageId(const ui32 indexId, const TIndexInfo& indexInfo) const {
    if (HasInsertWriteId()) {
        return { NBlobOperations::TGlobal::DefaultStorageId };
    }
    return indexInfo.GetIndexStorageId(indexId);
}

ISnapshotSchema::TPtr TPortionInfo::GetSchema(const TVersionedIndex& index) const {
    AFL_VERIFY(SchemaVersion);
    if (SchemaVersion) {
        auto schema = index.GetSchema(SchemaVersion.value());
        AFL_VERIFY(!!schema)("details", TStringBuilder() << "cannot find schema for version " << SchemaVersion.value());
        return schema;
    }
    return index.GetSchema(MinSnapshotDeprecated);
}

ISnapshotSchema::TPtr TPortionInfo::TSchemaCursor::GetSchema(const TPortionInfoConstructor& portion) {
    if (!CurrentSchema || portion.GetMinSnapshotDeprecatedVerified() != LastSnapshot) {
        CurrentSchema = portion.GetSchema(VersionedIndex);
        LastSnapshot = portion.GetMinSnapshotDeprecatedVerified();
    }
    AFL_VERIFY(!!CurrentSchema);
    return CurrentSchema;
}

bool TPortionInfo::NeedShardingFilter(const TGranuleShardingInfo& shardingInfo) const {
    if (ShardingVersion && shardingInfo.GetSnapshotVersion() <= *ShardingVersion) {
        return false;
    }
    return true;
}

NSplitter::TEntityGroups TPortionInfo::GetEntityGroupsByStorageId(
    const TString& specialTier, const IStoragesManager& storages, const TIndexInfo& indexInfo) const {
    if (HasInsertWriteId()) {
        NSplitter::TEntityGroups groups(storages.GetDefaultOperator()->GetBlobSplitSettings(), IStoragesManager::DefaultStorageId);
        return groups;
    } else {
        return indexInfo.GetEntityGroupsByStorageId(specialTier, storages);
    }
}

void TPortionInfo::Precalculate() {
    AFL_VERIFY(!Precalculated);
    Precalculated = true;
    {
        PrecalculatedColumnRawBytes = 0;
        PrecalculatedColumnBlobBytes = 0;
        PrecalculatedRecordsCount = 0;
        const auto aggr = [&](const TColumnRecord& r) {
            PrecalculatedColumnRawBytes += r.GetMeta().GetRawBytes();
            PrecalculatedColumnBlobBytes += r.BlobRange.GetSize();
            if (r.GetColumnId() == Records.front().GetColumnId()) {
                PrecalculatedRecordsCount += r.GetMeta().GetRecordsCount();
            }
        };
        TPortionDataAccessor::AggregateIndexChunksData(aggr, Records, nullptr, true);
    }
    {
        PrecalculatedIndexRawBytes = 0;
        PrecalculatedIndexBlobBytes = 0;
        const auto aggr = [&](const TIndexChunk& r) {
            PrecalculatedIndexRawBytes += r.GetRawBytes();
            PrecalculatedIndexBlobBytes += r.GetDataSize();
        };
        TPortionDataAccessor::AggregateIndexChunksData(aggr, Indexes, nullptr, true);
    }
}

}   // namespace NKikimr::NOlap
