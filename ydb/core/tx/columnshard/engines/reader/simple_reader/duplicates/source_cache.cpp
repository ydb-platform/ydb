#include "private_events.h"
#include "source_cache.h"

#include <ydb/core/tx/columnshard/data_reader/fetcher.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

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

class TFetchingExecutor: public NOlap::NDataFetcher::IFetchCallback {
private:
    const NActors::TActorId ParentActorId;
    std::shared_ptr<ISnapshotSchema> ResultSchema;
    TEvRequestFilter::TPtr OriginalRequest;
    ui64 PortionId;

    virtual bool IsAborted() const override {
        return OriginalRequest->Get()->GetAbortionFlag()->Val();
    }

    virtual TString GetClassName() const override {
        return "DUPLICATE_FILTERING";
    }

    virtual void DoOnFinished(NOlap::NDataFetcher::TCurrentContext&& context) override {
        AFL_VERIFY(context.GetPortionAccessors().size() == 1)("size", context.GetPortionAccessors().size());
        AFL_VERIFY(context.GetResourceGuards().size() == 1)("size", context.GetResourceGuards().size());
        const auto portion = context.ExtractPortionAccessors().front();
        AFL_VERIFY(portion.GetPortionInfo().GetPortionId() == PortionId);
        auto blobs = portion.DecodeBlobAddresses(context.ExtractBlobs(), ResultSchema->GetIndexInfo());
        TActorContext::AsActorContext().Send(
            ParentActorId, new NPrivate::TEvDuplicateFilterDataFetched(portion.GetPortionInfo().GetPortionId(),
                               TColumnsData(portion
                                                .PrepareForAssemble(*ResultSchema, *ResultSchema, blobs,
                                                    portion.GetPortionInfo().GetDataSnapshot(OriginalRequest->Get()->GetMaxVersion()))
                                                .AssembleToGeneralContainer({})
                                                .DetachResult(),
                                   std::move(context.GetResourceGuards().front()))));
    }

    virtual void DoOnError(const TString& errorMessage) override {
        TActorContext::AsActorContext().Send(
            ParentActorId, new NPrivate::TEvDuplicateFilterDataFetched(PortionId, TConclusionStatus::Fail(errorMessage)));
    }

public:
    TFetchingExecutor(
        const TActorId& owner, const std::shared_ptr<ISnapshotSchema>& resultSchema, const TEvRequestFilter::TPtr& request, const ui64 portionId)
        : ParentActorId(owner)
        , ResultSchema(resultSchema)
        , OriginalRequest(request)
        , PortionId(portionId) {
    }
};

}   // namespace

class TSourceCache::TResponseConstructor {
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

void TSourceCache::TFetchingInfo::AddCallback(const std::shared_ptr<TResponseConstructor>& callback) {
    Callbacks.emplace_back(callback);
}

void TSourceCache::TFetchingInfo::Complete(const TCacheItem& result) {
    for (const auto& callback : Callbacks) {
        callback->AddData(SourceId, result);
    }
}

void TSourceCache::TFetchingInfo::Abort(const TString& error) {
    for (const auto& callback : Callbacks) {
        callback->Abort(error);
    }
}

void TSourceCache::OnFetchingResult(const NPrivate::TEvDuplicateFilterDataFetched::TPtr& ev) {
    const ui64 sourceId = ev->Get()->GetSourceId();
    const TConclusion<TColumnsData>& result = ev->Get()->GetResult();

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
    std::shared_ptr<TResponseConstructor> response = std::make_shared<TResponseConstructor>(sources, TCallback(Owner, originalRequest));
    const ui64 requestId = MakeNextRequestId();
    auto memroyProcessGuard = NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildProcessGuard(requestId, {});
    auto memoryScopeGuard = NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildScopeGuard(requestId, 1);
    auto conveyorProcessGuard = std::make_shared<NConveyorComposite::TProcessGuard>(
        NConveyorComposite::TDeduplicationServiceOperator::StartProcess(requestId, NConveyorComposite::TCPULimitsConfig()));

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
        // std::shared_ptr<TColumnFetchingContext> fetchingContext =
        //     std::make_shared<TColumnFetchingContext>(FetchingContext, source, findFetching->GetStatus(), processGuard);
        // TColumnFetchingContext::StartAllocation(fetchingContext);

        NOlap::NDataFetcher::TRequestInput rInput({ source }, SchemaIndex, NOlap::NBlobOperations::EConsumer::DUPLICATE_FILTERING, "" /*TODO*/);
        NOlap::NDataFetcher::TPortionsDataFetcher::StartAccessorPortionsFetching(std::move(rInput),
            std::make_shared<TFetchingExecutor>(Owner, ResultSchema, originalRequest, sourceId), FetchingEnvironment,
            NConveyorComposite::ESpecialTaskCategory::Scan);
    }

    Counters.OnSourceCacheRequest(cacheHits, sources.size() - cacheHits);
}

TSourceCache::~TSourceCache() {
    for (auto& [_, info] : FetchingSources) {
        info.Abort("aborted");
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
