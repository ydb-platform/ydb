#include "events.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

void TInsertedPortion::Finalize(TColumnShard* shard, NTabletFlatExecutor::TTransactionContext& txc) {
    AFL_VERIFY(PortionInfoConstructor);
    auto* lastPortionId = shard->MutableIndexAs<NOlap::TColumnEngineForLogs>().GetLastPortionPointer();
    PortionInfoConstructor->SetPortionId(++*lastPortionId);
    NOlap::TDbWrapper wrapper(txc.DB, nullptr);
    wrapper.WriteCounter(NOlap::TColumnEngineForLogs::LAST_PORTION, *lastPortionId);
    PortionInfo = PortionInfoConstructor->BuildPtr(true);
    PortionInfoConstructor = nullptr;
}

}   // namespace NKikimr::NColumnShard

namespace NKikimr::NColumnShard::NPrivateEvents::NWrite {}
