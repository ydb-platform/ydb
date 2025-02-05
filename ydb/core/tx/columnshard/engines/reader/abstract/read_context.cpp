#include "read_context.h"

#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader {

IDataReader::IDataReader(const std::shared_ptr<TReadContext>& context)
    : Context(context) {
}

TReadContext::TReadContext(const std::shared_ptr<IStoragesManager>& storagesManager,
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
    const NColumnShard::TConcreteScanCounters& counters, const TReadMetadataBase::TConstPtr& readMetadata, const TActorId& scanActorId,
    const TActorId& resourceSubscribeActorId, const TActorId& readCoordinatorActorId, const TComputeShardingPolicy& computeShardingPolicy,
    const ui64 scanId)
    : StoragesManager(storagesManager)
    , DataAccessorsManager(dataAccessorsManager)
    , Counters(counters)
    , ReadMetadata(readMetadata)
    , ResourcesTaskContext("CS::SCAN_READ", counters.ResourcesSubscriberCounters)
    , ScanId(scanId)
    , ScanActorId(scanActorId)
    , ResourceSubscribeActorId(resourceSubscribeActorId)
    , ReadCoordinatorActorId(readCoordinatorActorId)
    , ComputeShardingPolicy(computeShardingPolicy)
    , ConveyorProcessGuard(NConveyor::TScanServiceOperator::StartProcess(ScanId)) {
    Y_ABORT_UNLESS(ReadMetadata);
}

}   // namespace NKikimr::NOlap::NReader
