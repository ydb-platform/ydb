#pragma once
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimrColumnShardExportProto {
class TCursor;
}

namespace NKikimr::NOlap::NExport {

class TCursor {
private:
    ui32 ChunkIdx = 1;
    std::optional<TOwnedCellVec> LastKey;
    bool Finished = false;

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrColumnShardExportProto::TCursor& proto);
public:
    TCursor() = default;
    TCursor(const TOwnedCellVec& lastKey, const bool finished)
        : LastKey(lastKey)
        , Finished(finished)
    {

    }

    const std::optional<TOwnedCellVec>& GetLastKey() const {
        return LastKey;
    }

    ui32 GetChunkIdx() const {
        return ChunkIdx;
    }

    bool HasLastKey() const {
        return !!LastKey;
    }

    bool IsFinished() const {
        return Finished;
    }

    void InitNext(const TOwnedCellVec& lastKey, const bool finished) {
        ++ChunkIdx;
        LastKey = lastKey;
        Finished = finished;
    }

    static TConclusion<TCursor> BuildFromProto(const NKikimrColumnShardExportProto::TCursor& proto);

    NKikimrColumnShardExportProto::TCursor SerializeToProto() const;
};

}