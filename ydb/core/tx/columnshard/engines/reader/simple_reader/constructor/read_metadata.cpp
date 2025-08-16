#include "read_metadata.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NSimple {

std::unique_ptr<TScanIteratorBase> TReadMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TColumnShardScanIterator>(readContext);
}

TConclusionStatus TReadMetadata::DoInitCustom(const NColumnShard::TColumnShard* /* owner */, const TReadDescription& /* readDescription */) {
    return TConclusionStatus::Success();
}

std::shared_ptr<IDataReader> TReadMetadata::BuildReader(const std::shared_ptr<TReadContext>& context) const {
    return std::make_shared<TPlainReadData>(context);
}

}   // namespace NKikimr::NOlap::NReader::NSimple
