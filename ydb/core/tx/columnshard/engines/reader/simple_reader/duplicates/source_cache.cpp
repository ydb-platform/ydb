#include "source_cache.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple {

TSourceCache::TResponseConstructor::TResponseConstructor(
    const std::vector<std::shared_ptr<IDataSource>>& sources, std::unique_ptr<ISubscriber>&& callback)
    : Callback(std::move(callback)) {
    AFL_VERIFY(Callback);
    AFL_VERIFY(sources.size());

    for (const auto& source : sources) {
        InFlight.insert(source->GetSourceId());
    }
}

void TSourceCache::Handle(const TEvDuplicateFilterDataFetched::TPtr& ev) {
    const ui64 sourceId = ev->Get()->GetSourceId();
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)("source", sourceId));

    auto findFetching = FetchingSources.find(sourceId);
    AFL_VERIFY(findFetching != FetchingSources.end());
    TFetchingInfo info = std::move(findFetching->second);
    FetchingSources.erase(findFetching);

    if (ev->Get()->GetResult().IsFail()) {
        info.Abort(ev->Get()->GetResult().GetErrorMessage());
    } else {
        auto cached = std::make_shared<TColumnsData>(ev->Get()->GetResult().GetResult());
        AFL_VERIFY(SourcesData.Insert(sourceId, cached));
        info.Complete(cached);
    }
}

void TSourceCache::GetSourcesData(const std::vector<std::shared_ptr<IDataSource>>& sources,
    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& memoryGroup, std::unique_ptr<ISubscriber>&& subscriber) {
    std::shared_ptr<TResponseConstructor> response = std::make_shared<TResponseConstructor>(sources, std::move(subscriber));
    for (const auto& source : sources) {
        const ui64 sourceId = source->GetSourceId();

        auto findFetched = SourcesData.Find(sourceId);
        if (findFetched != SourcesData.End()) {
            response->AddData(sourceId, *findFetched);
            continue;
        }

        TFetchingInfo* findFetching = FetchingSources.FindPtr(sourceId);
        if (!findFetching) {
            findFetching = &FetchingSources.emplace(sourceId, sourceId).first->second;
        }

        findFetching->AddCallback(response);
        if (findFetching->GetStatus()->SetStartAllocation(memoryGroup->GetGroupId())) {
            std::shared_ptr<TColumnFetchingContext> fetchingContext =
                std::make_shared<TColumnFetchingContext>(findFetching->GetStatus(), source, SelfId(), memoryGroup);
            TColumnFetchingContext::StartAllocation(fetchingContext);
        }
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
