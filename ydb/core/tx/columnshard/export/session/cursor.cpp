#include "cursor.h"

#include <ydb/core/tx/columnshard/export/protos/cursor.pb.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusionStatus TCursor::DeserializeFromProto(const NKikimrColumnShardExportProto::TCursor &proto) {
    if (proto.HasLastKey()) {
        LastKey = TOwnedCellVec(TSerializedCellVec(proto.GetLastKey()).GetCells());
    }
    if (proto.HasFinished()) {
        Finished = proto.GetFinished();
    }
    ChunkIdx = proto.GetChunkIdx();
    if (proto.HasMultiFile()) {
        MultiFile = proto.GetMultiFile();
    }
    FileIdx = proto.GetFileIdx();
    if (proto.HasFileStartKey()) {
        FileStartKey = TOwnedCellVec(TSerializedCellVec(proto.GetFileStartKey()).GetCells());
    }
    FileRows = proto.GetFileRows();
    DataFinished = proto.GetDataFinished();
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<NKikimr::NOlap::NExport::TCursor> TCursor::BuildFromProto(const NKikimrColumnShardExportProto::TCursor &proto) {
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
    if (MultiFile) {
        result.SetMultiFile(*MultiFile);
    }
    result.SetFileIdx(FileIdx);
    if (FileStartKey) {
        result.SetFileStartKey(TSerializedCellVec::Serialize(*FileStartKey));
    }
    result.SetFileRows(FileRows);
    result.SetDataFinished(DataFinished);
    return result;
}

void TCursor::InitNext(const TOwnedCellVec &lastKey, const bool finished) {
    ++ChunkIdx;
    LastKey = lastKey;
    Finished = finished;
}

void TCursor::RewindToFileStart() {
    AFL_VERIFY(!Finished);
    AFL_VERIFY(!DataFinished);
    LastKey = FileStartKey;
    FileRows = 0;
}

void TCursor::StartNextFile(const TOwnedCellVec &lastKey) {
    AFL_VERIFY(!Finished);
    AFL_VERIFY(!DataFinished);
    ++ChunkIdx;
    ++FileIdx;
    FileStartKey = lastKey;
    LastKey = lastKey;
    FileRows = 0;
}

void TCursor::FinishData() {
    AFL_VERIFY(!Finished);
    AFL_VERIFY(!DataFinished);
    ++ChunkIdx;
    ++FileIdx;
    FileRows = 0;
    DataFinished = true;
}

void TCursor::FinishDataWithoutCurrentFile() {
    AFL_VERIFY(!Finished);
    AFL_VERIFY(!DataFinished);
    AFL_VERIFY(FileIdx > 0);
    AFL_VERIFY(FileRows == 0);
    ++ChunkIdx;
    DataFinished = true;
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
    : LastKey(lastKey)
    , Finished(finished)
{
}

}   // namespace NKikimr::NOlap::NExport
