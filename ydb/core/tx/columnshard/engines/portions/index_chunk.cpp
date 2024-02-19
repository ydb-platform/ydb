#include "index_chunk.h"
#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>

namespace NKikimr::NOlap {

NKikimr::TConclusionStatus TIndexChunk::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TIndexChunk& proto) {
    IndexId = proto.GetIndexId();
    ChunkIdx = proto.GetChunkIdx();
    {
        if (!proto.HasMeta()) {
            return TConclusionStatus::Fail("no meta information");
        }
        RecordsCount = proto.GetMeta().GetRecordsCount();
        RawBytes = proto.GetMeta().GetRawBytes();
    }
    {
        auto parsed = TBlobRange::BuildFromProto(proto.GetBlobRange());
        if (!parsed) {
            return parsed;
        }
        BlobRange = parsed.DetachResult();
    }
    return TConclusionStatus::Success();
}

NKikimrColumnShardDataSharingProto::TIndexChunk TIndexChunk::SerializeToProto() const {
    NKikimrColumnShardDataSharingProto::TIndexChunk result;
    result.SetIndexId(IndexId);
    result.SetChunkIdx(ChunkIdx);
    {
        auto* meta = result.MutableMeta();
        meta->SetRecordsCount(RecordsCount);
        meta->SetRawBytes(RawBytes);
    }
    *result.MutableBlobRange() = BlobRange.SerializeToProto();
    return result;
}

}
