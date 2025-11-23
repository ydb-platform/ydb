#include "cursor.h"
#include <ydb/core/tx/columnshard/export/protos/cursor.pb.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusionStatus TCursor::DeserializeFromProto(const NKikimrColumnShardExportProto::TCursor& proto) {
    if (proto.HasLastKey()) {
        LastKey = TOwnedCellVec(TSerializedCellVec(proto.GetLastKey()).GetCells());
    }
    if (proto.HasFinished()) {
        Finished = proto.GetFinished();
    }
    ChunkIdx = proto.GetChunkIdx();
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<NKikimr::NOlap::NExport::TCursor> TCursor::BuildFromProto(const NKikimrColumnShardExportProto::TCursor& proto) {
    TCursor result;
    auto parsedResult = result.DeserializeFromProto(proto);
    if (!parsedResult) {
        return parsedResult;
    }
    return result;
}

NKikimrColumnShardExportProto::TCursor TCursor::SerializeToProto() const {
    NKikimrColumnShardExportProto::TCursor result;
    if (LastKey) {
        result.SetLastKey(TSerializedCellVec::Serialize(*LastKey));
    }
    result.SetFinished(Finished);
    result.SetChunkIdx(ChunkIdx);
    return result;
}

void TCursor::InitNext(const TOwnedCellVec &lastKey, const bool finished) {
    ++ChunkIdx;
    LastKey = lastKey;
    Finished = finished;
}

bool TCursor::IsFinished() const { 
    return Finished; 
}

bool TCursor::HasLastKey() const { 
    return !!LastKey; 
}

ui32 TCursor::GetChunkIdx() const { 
    return ChunkIdx; 
}

const std::optional<TOwnedCellVec> &TCursor::GetLastKey() const {
    return LastKey;
}

TCursor::TCursor(const TOwnedCellVec &lastKey, const bool finished)
    : LastKey(lastKey), Finished(finished) {       
}

} // namespace NKikimr::NOlap::NExport