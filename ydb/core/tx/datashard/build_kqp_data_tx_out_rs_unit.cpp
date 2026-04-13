#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

THolder<TExecutionUnit> CreateBuildKqpDataTxOutRSUnit(TDataShard& , TPipeline& ) {
    Y_ENSURE(false, "Not implemented");
}

} // namespace NDataShard
} // namespace NKikimr
