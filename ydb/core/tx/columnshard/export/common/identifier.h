#pragma once
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimrColumnShardExportProto {
class TIdentifier;
}

namespace NKikimrTxColumnShard {
class TBackupTxBody;
}

namespace NKikimr::NOlap::NExport {

class TIdentifier {
private:
    YDB_READONLY_DEF(NColumnShard::TSchemeShardLocalPathId, SchemeShardLocalPathId);

    TIdentifier() = default;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardExportProto::TIdentifier& proto);

public:
    TIdentifier(const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId)
        : SchemeShardLocalPathId(schemeShardLocalPathId)
    {
    }

    static TConclusion<TIdentifier> BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto);
    static TConclusion<TIdentifier> BuildFromProto(const NKikimrColumnShardExportProto::TIdentifier& proto);

    NKikimrColumnShardExportProto::TIdentifier SerializeToProto() const;

    TString ToString() const;

    bool operator==(const TIdentifier& id) const {
        return SchemeShardLocalPathId == id.SchemeShardLocalPathId;
    }

    TString DebugString() const;
};

}   // namespace NKikimr::NOlap::NExport
