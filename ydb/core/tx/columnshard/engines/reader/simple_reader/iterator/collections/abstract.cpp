#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>

namespace NKikimr::NOlap::NReader::NSimple {

ISourcesCollection::ISourcesCollection(
    const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
    : Context(context)
    , SourcesConstructor(std::move(sourcesConstructor)) {
    if (HasAppData() && AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
        MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
