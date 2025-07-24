#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>

#include <util/string/join.h>

namespace NKikimr::NOlap::NReader::NSimple {

ISourcesCollection::ISourcesCollection(
    const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
    : Context(context)
    , SourcesConstructor(std::move(sourcesConstructor)) {
    if (HasAppData() && AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
        MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
    }
}

TString ISourcesCollection::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "internal:{" << DoDebugString() << "};";
    sb << "constructor:{" << SourcesConstructor->DebugString() << "};";
    sb << "in_fly=" << SourcesInFlightCount.Val() << ";";
    sb << "type=" << GetClassName() << ";";
    sb << "}";
    return sb;
}

std::shared_ptr<IScanCursor> ISourcesCollection::BuildCursor(
    const std::shared_ptr<NCommon::IDataSource>& source, const ui32 readyRecords, const ui64 tabletId) const {
    AFL_VERIFY(source);
    AFL_VERIFY(readyRecords <= source->GetRecordsCount())("count", source->GetRecordsCount())("ready", readyRecords);
    auto result = DoBuildCursor(source, readyRecords);
    AFL_VERIFY(result);
    result->SetTabletId(tabletId);
    AFL_VERIFY(tabletId);
    return result;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
