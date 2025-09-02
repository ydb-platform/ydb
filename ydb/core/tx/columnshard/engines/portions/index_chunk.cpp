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
    if (proto.HasBlobRange()) {
        auto parsed = TBlobRangeLink16::BuildFromProto(proto.GetBlobRange());
        if (!parsed) {
            return parsed;
        }
        Data = parsed.DetachResult();
    } else if (proto.HasBlobData()) {
        Data = proto.GetBlobData();
    } else {
        return TConclusionStatus::Fail("incorrect blob info - neither BlobData nor BlobRange");
    }
    return TConclusionStatus::Success();
}

namespace {
class TBlobInfoSerializer {
private:
    NKikimrColumnShardDataSharingProto::TIndexChunk& Proto;

public:
    TBlobInfoSerializer(NKikimrColumnShardDataSharingProto::TIndexChunk& proto)
        : Proto(proto) {
    }

    void operator()(const TBlobRangeLink16& link) {
        *Proto.MutableBlobRange() = link.SerializeToProto();
    }
    void operator()(const TString& data) {
        *Proto.MutableBlobData() = data;
    }
};
}   // namespace

NKikimrColumnShardDataSharingProto::TIndexChunk TIndexChunk::SerializeToProto() const {
    NKikimrColumnShardDataSharingProto::TIndexChunk result;
    result.SetIndexId(IndexId);
    result.SetChunkIdx(ChunkIdx);
    {
        auto* meta = result.MutableMeta();
        meta->SetRecordsCount(RecordsCount);
        meta->SetRawBytes(RawBytes);
    }
    std::visit(TBlobInfoSerializer(result), Data);
    return result;
}

namespace {
class TDataSizeExtractor {
public:
    TDataSizeExtractor() = default;

    ui64 operator()(const TBlobRangeLink16& link) {
        return link.GetSize();
    }
    ui64 operator()(const TString& data) {
        return data.size();
    }
};
}   // namespace

ui64 TIndexChunk::GetDataSize() const {
    return std::visit(TDataSizeExtractor(), Data);
}

}   // namespace NKikimr::NOlap
