#include "private_events.h"
#include "source_cache.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

namespace {

class TCallback: TMoveOnly {
private:
    TActorId Owner;
    TEvRequestFilter::TPtr OriginalRequest;

public:
    void OnSourcesReady(TSourceCache::TSourcesData&& result) {
        AFL_VERIFY(Owner);
        TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvDuplicateSourceCacheResult(OriginalRequest, std::move(result)));
        Owner = TActorId();
    }
    void OnFailure(const TString& error) {
        AFL_VERIFY(Owner);
        TActivationContext::AsActorContext().Send(Owner, new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(error)));
        Owner = TActorId();
    }

    TCallback(const TActorId& owner, const TEvRequestFilter::TPtr& originalRequest)
        : Owner(owner)
        , OriginalRequest(originalRequest) {
    }

    TCallback(TCallback&& other)
        : Owner(other.Owner) {
        other.Owner = TActorId();
    }

    bool IsDone() const {
        return !Owner;
    }
};

class TResponseConstructor {
private:
    TCallback Callback;
    TSourceCache::TSourcesData Result;
    THashSet<ui64> InFlight;

    bool FinishIfReady() {
        if (InFlight.size()) {
            return false;
        }

        Callback.OnSourcesReady(std::move(Result));
        return true;
    }

public:
    TResponseConstructor(const std::vector<std::shared_ptr<TPortionInfo>>& sources, TCallback&& callback)
        : Callback(std::move(callback)) {
        AFL_VERIFY(sources.size());

        for (const auto& source : sources) {
            InFlight.insert(source->GetPortionId());
        }
    }

    void AddData(const ui64 sourceId, const TSourceCache::TCacheItem& data) {
        NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)("source", sourceId));

        auto findInFlight = InFlight.find(sourceId);
        AFL_VERIFY(findInFlight != InFlight.end());
        InFlight.erase(findInFlight);

        AFL_VERIFY(Result.emplace(sourceId, std::move(data)).second);

        Y_UNUSED(FinishIfReady());
    }

    void Abort(const TString& error) {
        Callback.OnFailure(error);
    }

    ~TResponseConstructor() {
        AFL_VERIFY(Callback.IsDone());
    }
};

}   // namespace

class TSourceCache::TFetchingInfo {
private:
    YDB_READONLY(std::shared_ptr<TFetchingStatus>, Status, std::make_shared<TFetchingStatus>());
    const ui64 SourceId;
    std::vector<std::shared_ptr<TResponseConstructor>> Callbacks;

public:
    TFetchingInfo(const ui64 sourceId)
        : SourceId(sourceId) {
    }

    void AddCallback(const std::shared_ptr<TResponseConstructor>& callback) {
        Callbacks.emplace_back(callback);
    }

    void Complete(const TCacheItem& result) {
        for (const auto& callback : Callbacks) {
            callback->AddData(SourceId, result);
        }
    }

    void Abort(const TString& error) {
        for (const auto& callback : Callbacks) {
            callback->Abort(error);
        }
    }
};

void TSourceCache::OnFetchingResult(const ui64 sourceId, TConclusion<TColumnsData>&& result) {
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)("source", sourceId));

    auto findFetching = FetchingSources.find(sourceId);
    AFL_VERIFY(findFetching != FetchingSources.end());
    TFetchingInfo info = std::move(findFetching->second);
    FetchingSources.erase(findFetching);

    if (result.IsFail()) {
        info.Abort(result.GetErrorMessage());
    } else {
        auto cached = std::make_shared<TColumnsData>(result.GetResult());
        SourcesData.Insert(sourceId, cached);
        info.Complete(cached);
    }
}

void TSourceCache::GetSourcesData(const std::vector<std::shared_ptr<TPortionInfo>>& sources, const TEvRequestFilter::TPtr& originalRequest) {
    std::shared_ptr<TResponseConstructor> response =
        std::make_shared<TResponseConstructor>(sources, TCallback(FetchingContext->GetOwnerVerified(), originalRequest));
    ui64 cacheHits = 0;
    for (const auto& source : sources) {
        const ui64 sourceId = source->GetPortionId();

        auto findFetched = SourcesData.Find(sourceId);
        if (findFetched != SourcesData.End()) {
            ++cacheHits;
            response->AddData(sourceId, *findFetched);
            continue;
        }

        TFetchingInfo* findFetching = FetchingSources.FindPtr(sourceId);
        if (!findFetching) {
            findFetching = &FetchingSources.emplace(sourceId, sourceId).first->second;
        }

        findFetching->AddCallback(response);
        // TODO: change resource management
        std::shared_ptr<TColumnFetchingContext> fetchingContext =
            std::make_shared<TColumnFetchingContext>(FetchingContext, source, findFetching->GetStatus());
        TColumnFetchingContext::StartAllocation(fetchingContext);
    }

    Counters.OnSourceCacheRequest(cacheHits, sources.size() - cacheHits);
}

TSourceCache::~TSourceCache() {
    for (auto& [_, info] : FetchingSources) {
        info.Abort("aborted");
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
