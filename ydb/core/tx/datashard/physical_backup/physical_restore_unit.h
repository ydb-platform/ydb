#pragma once

#include <ydb/core/tx/datashard/execution_unit.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;
class TPipeline;

THolder<TExecutionUnit> CreatePhysicalRestoreUnit(TDataShard& dataShard, TPipeline& pipeline);

} // namespace NDataShard
} // namespace NKikimr
