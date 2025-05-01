#include "constructor_portion.h"

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
        TMemoryProfileGuard mGuard0("portion_construct_meta");
        auto meta = MetaConstructor.Build();
        TMemoryProfileGuard mGuard("portion_construct_main");
        result = std::make_shared<TPortionInfo>(std::move(meta));
    }
    {
        TMemoryProfileGuard mGuard1("portion_construct_after");
        AFL_VERIFY(PathId);
        result->PathId = PathId;
        result->PortionId = GetPortionIdVerified();

        AFL_VERIFY(MinSnapshotDeprecated);
        AFL_VERIFY(MinSnapshotDeprecated->Valid());
        result->MinSnapshotDeprecated = *MinSnapshotDeprecated;
        if (RemoveSnapshot) {
            AFL_VERIFY(RemoveSnapshot->Valid());
            result->RemoveSnapshot = *RemoveSnapshot;
        }
        result->SchemaVersion = SchemaVersion;
        result->ShardingVersion = ShardingVersion;
        result->CommitSnapshot = CommitSnapshot;
        result->InsertWriteId = InsertWriteId;
        AFL_VERIFY(!CommitSnapshot || !!InsertWriteId);

        if (result->GetMeta().GetProduced() == NPortion::EProduced::INSERTED) {
            //        AFL_VERIFY(!!InsertWriteId);
        } else {
            AFL_VERIFY(!CommitSnapshot);
            AFL_VERIFY(!InsertWriteId);
        }
    }
    static TAtomicCounter countValues = 0;
    static TAtomicCounter sumValues = 0;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("memory_size", result->GetMemorySize())("data_size", result->GetDataSize())(
        "sum", sumValues.Add(result->GetMemorySize()))("count", countValues.Inc())("size_of_portion", sizeof(TPortionInfo));
    return result;
}

ISnapshotSchema::TPtr TPortionInfoConstructor::GetSchema(const TVersionedIndex& index) const {
    if (SchemaVersion) {
        auto schema = index.GetSchemaVerified(SchemaVersion.value());
        AFL_VERIFY(!!schema)("details", TStringBuilder() << "cannot find schema for version " << SchemaVersion.value());
        return schema;
    } else {
        AFL_VERIFY(MinSnapshotDeprecated);
        return index.GetSchemaVerified(*MinSnapshotDeprecated);
    }
}

}   // namespace NKikimr::NOlap
