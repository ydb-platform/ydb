#pragma once

#include "events.h"
#include "fetching.h"

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <library/cpp/cache/cache.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TEvDuplicateSourceCacheResult
    : public NActors::TEventLocal<TEvDuplicateSourceCacheResult, NColumnShard::TEvPrivate::EvDuplicateSourceCacheResult> {
private:
    using TDataBySource = THashMap<ui64, std::shared_ptr<TColumnsData>>;
    YDB_READONLY_DEF(TDataBySource, ColumnData);
    YDB_READONLY_DEF(TEvRequestFilter::TPtr, OriginalRequest);

public:
    TEvDuplicateSourceCacheResult(const TEvRequestFilter::TPtr& originalRequest, TDataBySource&& data)
        : ColumnData(std::move(data))
        , OriginalRequest(originalRequest) {
    }
};

class TSourceCache: public NActors::TActor<TSourceCache> {
public:
    using TCacheItem = std::shared_ptr<TColumnsData>;
    using TSourcesData = THashMap<ui64, TCacheItem>;

    class TCallback: TMoveOnly {
    private:
        TActorId Owner;
        TEvRequestFilter::TPtr OriginalRequest;

    public:
        void OnSourcesReady(TSourcesData&& result) {
            AFL_VERIFY(Owner);
            TActorContext::AsActorContext().Send(Owner, new TEvDuplicateSourceCacheResult(OriginalRequest, std::move(result)));
            Owner = TActorId();
        }
        void OnFailure(const TString& error) {
            AFL_VERIFY(Owner);
            TActivationContext::AsActorContext().Send(Owner, new TEvFilterConstructionResult(TConclusionStatus::Fail(error)));
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

private:
    class TSizeProvider {
    public:
        size_t operator()(const TCacheItem& item) const {
            return item->GetRawSize();
        }
    };

    class TResponseConstructor {
    private:
        TCallback Callback;
        TSourcesData Result;
        THashSet<ui64> InFlight;

        bool FinishIfReady() {
            if (InFlight.size()) {
                return false;
            }

            Callback.OnSourcesReady(std::move(Result));
            return true;
        }

    public:
        TResponseConstructor(const std::vector<std::shared_ptr<TPortionInfo>>& sources, TCallback&& callback);

        void AddData(const ui64 sourceId, const TCacheItem& data) {
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

    class TFetchingInfo {
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

private:
    const NColumnShard::TDuplicateFilteringCounters& Counters;
    std::shared_ptr<TCommonFetchingContext> FetchingContext;
    THashMap<ui64, TFetchingInfo> FetchingSources;
    TLRUCache<ui64, TCacheItem, TNoopDelete, TSizeProvider> SourcesData;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDuplicateFilterDataFetched, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvDuplicateFilterDataFetched::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    virtual TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& /*parentId*/) override {
        FetchingContext->SetOwner(self);
        return TAutoPtr<IEventHandle>();
    }

public:
    void GetSourcesData(const std::vector<std::shared_ptr<TPortionInfo>>& sources,
        const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& memoryGroup, TCallback&& subscriber);

    TSourceCache(const std::shared_ptr<TReadContext>& readContext)
        : TActor(&TSourceCache::StateMain)
        , Counters(readContext->GetDuplicateFilteringCounters())
        , FetchingContext(std::make_shared<TCommonFetchingContext>(readContext,
              [&readContext]() {
                  std::set<ui32> columnIds = readContext->GetReadMetadata()->GetIndexVersions().GetLastSchema()->GetPkColumnsIds();
                  for (const auto& id : IIndexInfo::GetSnapshotColumnIds()) {
                      columnIds.insert(id);
                  }
                  return columnIds;
              }()))
        , SourcesData(NYDBTest::TControllers::GetColumnShardController()->GetDuplicateManagerCacheSize()) {
    }

    ~TSourceCache() {
        for (auto& [_, info] : FetchingSources) {
            info.Abort("aborted");
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
