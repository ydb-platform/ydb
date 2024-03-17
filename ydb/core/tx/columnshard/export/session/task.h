#pragma once
#include "selector/abstract/selector.h"
#include "storage/abstract/storage.h"

#include <ydb/core/tx/columnshard/export/common/identifier.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimrColumnShardExportProto {
class TExportTask;
}

namespace NKikimr::NOlap::NExport {

class TExportTask {
private:
    TIdentifier Identifier = TIdentifier(0);
    YDB_READONLY_DEF(TSelectorContainer, Selector);
    YDB_READONLY_DEF(TStorageInitializerContainer, StorageInitializer);
    YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, Serializer);

    TExportTask() = default;

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardExportProto::TExportTask& proto);

public:
    NKikimrColumnShardExportProto::TExportTask SerializeToProto() const;

    static TConclusion<TExportTask> BuildFromProto(const NKikimrColumnShardExportProto::TExportTask& proto);

    const TIdentifier& GetIdentifier() const {
        return Identifier;
    }

    TExportTask(const TIdentifier& id, const TSelectorContainer& selector, const TStorageInitializerContainer& storageInitializer)
        : Identifier(id)
        , Selector(selector)
        , StorageInitializer(storageInitializer)
    {
    }

    TString DebugString() const {
        return TStringBuilder() << "{task_id=" << Identifier.DebugString() << ";selector=" << Selector.DebugString() << ";}";
    }
};
}
