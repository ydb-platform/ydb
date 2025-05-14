#include "fetching.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/source_cache.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple {

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)

TDuplicateFilterConstructor::TDuplicateFilterConstructor(const TSpecialReadContext& context)
    : TActor(&TDuplicateFilterConstructor::StateMain)
    , SourceCache([this]() {
        TSourceCache* cache = new TSourceCache();
        RegisterWithSameMailbox((IActor*)cache);
        return cache;
    }())
    , Intervals([&context]() {
        const auto& portions = context.GetReadMetadata()->SelectInfo->Portions;
        std::remove_const_t<decltype(TDuplicateFilterConstructor::Intervals)> intervals(
            portions.front()->IndexKeyStart()   // TODO: change tree implementation, don't require default value/constructor
        );
        for (ui64 i = 0; i < portions.size(); ++i) {
            const auto& portion = portions[i];
            intervals.insert({ portion->IndexKeyStart(), portion->IndexKeyEnd() }, TSourceInfo(i, portion));
        }
        return intervals;
    }()) {
}

void TDuplicateFilterConstructor::Handle(const TEvRequestFilter::TPtr& ev) {
    auto source = std::dynamic_pointer_cast<TPortionDataSource>(ev->Get()->GetSource());
    AFL_VERIFY(source);

    std::vector<std::shared_ptr<IDataSource>> sourcesToFetch;
    {
        const auto collector = [&sourcesToFetch, context = ev->Get()->GetSource()->GetContextAsVerified<TSpecialReadContext>()](
                                   const TInterval<NArrow::TSimpleRow>& /*interval*/, const TSourceInfo& info) {
            sourcesToFetch.emplace_back(info.Construct(context));
            return true;
        };
        Intervals.find(
            TInterval<NArrow::TSimpleRow>(source->GetPortionInfo().IndexKeyStart(), source->GetPortionInfo().IndexKeyEnd()), collector);
    }

    AFL_VERIFY(sourcesToFetch.size());
    if (sourcesToFetch.size() == 1) {
        AFL_VERIFY(sourcesToFetch.front()->GetSourceId() == source->GetSourceId());
        auto filter  = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, sourcesToFetch.front()->GetRecordsCount());
        ev->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
        return;
    }

    SourceCache->GetSourcesData(std::move(sourcesToFetch), ev->Get()->GetSource()->GetGroupGuard(),
        std::make_unique<TSourceDataSubscriber>(SelfId(), source, ev->Get()->GetSubscriber()));
}

std::shared_ptr<TPortionDataSource> TDuplicateFilterConstructor::TSourceInfo::Construct(
    const std::shared_ptr<TSpecialReadContext>& context) const {
    const auto& portions = context->GetReadMetadata()->SelectInfo->Portions;
    AFL_VERIFY(SourceIdx < portions.size());
    return std::make_shared<TPortionDataSource>(SourceIdx, portions[SourceIdx], context);
}

void TDuplicateFilterConstructor::TSourceDataSubscriber::OnSourcesReady(TSourceCache::TSourcesData&& result) {
    // FIXME: don't merge on exclusive intervals
    // FIXME: strip sources
    NArrow::NMerger::TCursor maxVersion = [snapshot = Source->GetContext()->GetReadMetadata()->GetRequestSnapshot()]() {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, snapshot, std::numeric_limits<ui64>::max());
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    }();

    const std::shared_ptr<TBuildDuplicateFilters> task =
        std::make_shared<TBuildDuplicateFilters>(Source->GetContext()->GetReadMetadata()->GetReplaceKey(), IIndexInfo::GetSnapshotColumnNames(),
            Source->GetContext()->GetCommonContext()->GetCounters(), maxVersion,
            std::make_unique<TFilterResultSubscriber>(Owner, Source, std::move(Callback)));
    for (const auto& [sourceId, data] : result) {
        task->AddSource(std::move(data), std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter()), sourceId);
    }

    NConveyor::TScanServiceOperator::SendTaskToExecute(task, Source->GetContext()->GetCommonContext()->GetConveyorProcessId());
}

void TDuplicateFilterConstructor::TFilterResultSubscriber::OnResult(THashMap<ui64, NArrow::TColumnFilter>&& result) {
    auto findFilter = result.FindPtr(Source->GetSourceId());
    AFL_VERIFY(findFilter);
    Callback->OnFilterReady(*findFilter);
}

}   // namespace NKikimr::NOlap::NReader::NSimple
