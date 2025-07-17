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
          "schema_version:"
       << SchemaVersion
       << ";"
          "level:"
       << GetMeta().GetCompactionLevel() << ";";
    if (withDetails) {
        sb << "records_snapshot_min:(" << RecordSnapshotMin().DebugString() << ");"
           << "records_snapshot_max:(" << RecordSnapshotMax().DebugString() << ");"
           << "from:" << IndexKeyStart().DebugString() << ";"
           << "to:" << IndexKeyEnd().DebugString() << ";";
    }
    sb << DoDebugString(withDetails) << ";";
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

void TPortionInfo::SerializeToProto(const std::vector<TUnifiedBlobId>& blobIds, NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const {
    PathId.ToProto(proto);
    proto.SetPortionId(PortionId);
    proto.SetSchemaVersion(GetSchemaVersionVerified());
    if (!RemoveSnapshot.IsZero()) {
        *proto.MutableRemoveSnapshot() = RemoveSnapshot.SerializeToProto();
    }

    *proto.MutableMeta() = Meta.SerializeToProto(blobIds, GetProduced());
}

TConclusionStatus TPortionInfo::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto) {
    PathId = TInternalPathId::FromProto(proto);
    PortionId = proto.GetPortionId();
    SchemaVersion = proto.GetSchemaVersion();
    if (!SchemaVersion) {
        return TConclusionStatus::Fail("portion's schema version cannot been equals to zero");
    }
    if (proto.HasRemoveSnapshot()) {
        auto parse = RemoveSnapshot.DeserializeFromProto(proto.GetRemoveSnapshot());
        if (!parse) {
            return parse;
        }
    }
    return TConclusionStatus::Success();
}

ISnapshotSchema::TPtr TPortionInfo::GetSchema(const TVersionedIndex& index) const {
    AFL_VERIFY(SchemaVersion);
    auto schema = index.GetSchemaVerified(SchemaVersion);
    AFL_VERIFY(!!schema)("details", TStringBuilder() << "cannot find schema for version " << SchemaVersion);
    return schema;
}

bool TPortionInfo::NeedShardingFilter(const TGranuleShardingInfo& shardingInfo) const {
    if (ShardingVersion && shardingInfo.GetSnapshotVersion() <= *ShardingVersion) {
        return false;
    }
    return true;
}

}   // namespace NKikimr::NOlap
