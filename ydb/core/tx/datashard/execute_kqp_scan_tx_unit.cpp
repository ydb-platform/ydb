#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

THolder<TExecutionUnit> CreateExecuteKqpScanTxUnit(TDataShard& , TPipeline& ) {
    return nullptr;
}

} // namespace NDataShard
} // namespace NKikimr
