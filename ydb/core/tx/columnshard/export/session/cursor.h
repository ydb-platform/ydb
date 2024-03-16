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

    bool IsFinished() const {
        return Finished;
    }

    static TConclusion<TCursor> BuildFromProto(const NKikimrColumnShardExportProto::TCursor& proto);

    NKikimrColumnShardExportProto::TCursor SerializeToProto() const;
};

}