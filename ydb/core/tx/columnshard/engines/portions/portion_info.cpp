#include "column_record.h"
#include "constructor_portion.h"
#include "data_accessor.h"
#include "portion_info.h"

#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

namespace NKikimr::NOlap {

ui64 TPortionInfo::GetColumnRawBytes() const {
    return GetMeta().GetColumnRawBytes();
}

ui64 TPortionInfo::GetColumnBlobBytes() const {
    return GetMeta().GetColumnBlobBytes();
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
    return sizeof(TPortionInfo) - sizeof(TPortionMeta) + Meta.GetMetadataMemorySize();
}

ui64 TPortionInfo::GetApproxChunksCount(const ui32 schemaColumnsCount) const {
    return schemaColumnsCount * (GetRecordsCount() / 10000 + 1);
}

void TPortionInfo::SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const {
    proto.SetPathId(PathId.GetInternalPathIdValue());
    proto.SetPortionId(PortionId);
    proto.SetSchemaVersion(GetSchemaVersionVerified());
    *proto.MutableMinSnapshotDeprecated() = MinSnapshotDeprecated.SerializeToProto();
    if (!RemoveSnapshot.IsZero()) {
        *proto.MutableRemoveSnapshot() = RemoveSnapshot.SerializeToProto();
    }

    *proto.MutableMeta() = Meta.SerializeToProto();
}

TConclusionStatus TPortionInfo::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto) {
    PathId = NColumnShard::TInternalPathId::FromInternalPathIdValue(proto.GetPathId());
    PortionId = proto.GetPortionId();
    SchemaVersion = proto.GetSchemaVersion();
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
    return TConclusionStatus::Success();
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
        auto schema = index.GetSchemaVerified(SchemaVersion.value());
        AFL_VERIFY(!!schema)("details", TStringBuilder() << "cannot find schema for version " << SchemaVersion.value());
        return schema;
    }
    return index.GetSchemaVerified(MinSnapshotDeprecated);
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

void TPortionInfo::SaveMetaToDatabase(IDbWrapper& db) const {
    FullValidation();
    db.WritePortion(*this);
}

}   // namespace NKikimr::NOlap
