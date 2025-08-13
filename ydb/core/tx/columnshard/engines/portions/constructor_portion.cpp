#include "compacted.h"
#include "constructor_portion.h"
#include "written.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap {

std::shared_ptr<TPortionInfo> TPortionInfoConstructor::Build() {
    AFL_VERIFY(!Constructed);
    Constructed = true;
    std::shared_ptr<TPortionInfo> result;
    {
        TMemoryProfileGuard mGuard0("portion_construct/meta::" + ::ToString(GetType()));
        auto meta = MetaConstructor.Build();
        TMemoryProfileGuard mGuard("portion_construct/main::" + ::ToString(GetType()));
        result = BuildPortionImpl(std::move(meta));
    }
    {
        TMemoryProfileGuard mGuard1("portion_construct/others::" + ::ToString(GetType()));
        AFL_VERIFY(PathId);
        result->PathId = PathId;
        result->PortionId = GetPortionIdVerified();

        if (RemoveSnapshot) {
            AFL_VERIFY(RemoveSnapshot->Valid());
            result->RemoveSnapshot = *RemoveSnapshot;
        }
        AFL_VERIFY(SchemaVersion && *SchemaVersion);
        result->SchemaVersion = *SchemaVersion;
        result->ShardingVersion = ShardingVersion;
    }
    static TAtomicCounter countValues = 0;
    static TAtomicCounter sumValues = 0;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("memory_size", result->GetMemorySize())("data_size", result->GetDataSize())(
        "sum", sumValues.Add(result->GetMemorySize()))("count", countValues.Inc())("size_of_portion", sizeof(TPortionInfo));
    return result;
}

ISnapshotSchema::TPtr TPortionInfoConstructor::GetSchema(const TVersionedIndex& index) const {
    AFL_VERIFY(SchemaVersion);
    auto schema = index.GetSchemaVerified(SchemaVersion.value());
    AFL_VERIFY(!!schema)("details", TStringBuilder() << "cannot find schema for version " << SchemaVersion.value());
    return schema;
}

std::shared_ptr<TPortionInfo> TWrittenPortionInfoConstructor::BuildPortionImpl(TPortionMeta&& meta) {
    auto result = std::make_shared<TWrittenPortionInfo>(std::move(meta));
    if (CommitSnapshot) {
        result->CommitSnapshot = *CommitSnapshot;
    }
    AFL_VERIFY(InsertWriteId);
    result->InsertWriteId = *InsertWriteId;
    return result;
}

std::shared_ptr<TPortionInfo> TCompactedPortionInfoConstructor::BuildPortionImpl(TPortionMeta&& meta) {
    auto result = std::make_shared<TCompactedPortionInfo>(std::move(meta));
    AFL_VERIFY(AppearanceSnapshot);
    result->AppearanceSnapshot = *AppearanceSnapshot;
    return result;
}

}   // namespace NKikimr::NOlap
