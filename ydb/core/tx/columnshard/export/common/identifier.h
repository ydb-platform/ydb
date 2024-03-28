#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

namespace NKikimrColumnShardExportProto {
class TIdentifier;
}

namespace NKikimrTxColumnShard {
class TBackupTxBody;
}

namespace NKikimr::NOlap::NExport {

class TIdentifier {
private:
    YDB_READONLY(ui64, PathId, 0);

    TIdentifier() = default;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardExportProto::TIdentifier& proto);
public:
    TIdentifier(const ui64 pathId)
        : PathId(pathId)
    {

    }

    static TConclusion<TIdentifier> BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto);
    static TConclusion<TIdentifier> BuildFromProto(const NKikimrColumnShardExportProto::TIdentifier& proto);

    NKikimrColumnShardExportProto::TIdentifier SerializeToProto() const;

    TString ToString() const;

    operator size_t() const {
        return PathId;
    }

    bool operator==(const TIdentifier& id) const {
        return PathId == id.PathId;
    }

    TString DebugString() const;
};

}