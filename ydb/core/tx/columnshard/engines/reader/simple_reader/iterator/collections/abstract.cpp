#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/string/join.h>

namespace NKikimr::NOlap::NReader::NSimple {

ISourcesCollection::ISourcesCollection(
    const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
    : Context(context)
    , SourcesConstructor(std::move(sourcesConstructor)) {
    if (HasAppData() && AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
        MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
    }
    // Initialize MaxPagesInFlight from streaming config in AppData
    MaxPagesInFlight = TStreamingConfigHelper::GetMaxPagesInFlight();
}

TString ISourcesCollection::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "finished:{" << IsFinished() << "};";
    sb << "internal:{" << DoDebugString() << "};";
    sb << "constructor:{" << SourcesConstructor->DebugString() << "};";
    sb << "in_fly=" << SourcesInFlightCount.Val() << ";";
    sb << "pages_in_fly=" << PagesInFlightCount.Val() << ";";
    sb << "max_pages=" << MaxPagesInFlight << ";";
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

void ISourcesCollection::OnPageCreated() {
    PagesInFlightCount.Inc();
    NYDBTest::TControllers::GetColumnShardController()->OnPageCreated(PagesInFlightCount.Val());
}

void ISourcesCollection::OnPageSent() {
    // PagesInFlightCount must be > 0 here: OnPageSent is only called from
    // OnSentDataFromInterval when sourceAddress.IsStreamingPage() is true,
    // which is set only when OnPageCreated() was called for this page.
    // TPositiveControlInteger::Dec() will AFL_VERIFY on underflow.
    PagesInFlightCount.Dec();
    NYDBTest::TControllers::GetColumnShardController()->OnPageSent(PagesInFlightCount.Val());
}

}   // namespace NKikimr::NOlap::NReader::NSimple
