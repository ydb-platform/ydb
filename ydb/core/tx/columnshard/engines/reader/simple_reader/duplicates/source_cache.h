#pragma once

#include "events.h"
#include "fetching.h"

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <library/cpp/cache/cache.h>

namespace NKikimr::NOlap::NReader::NSimple {

class IDataSource;

class TSourceCache: public NActors::TActor<TSourceCache> {
    // TODO: add cache hit/miss metrics
public:
    using TColumnsData = NArrow::TGeneralContainer;
    using TCacheItem = std::shared_ptr<TColumnsData>;
    using TSourcesData = THashMap<ui64, TCacheItem>;

    class ISubscriber {
    public:
        virtual void OnSourcesReady(TSourcesData&&) = 0;
        virtual void OnFailure(const TString& error) = 0;
        virtual ~ISubscriber() = default;
    };

private:
    class TSizeProvider {
    public:
        size_t operator()(const TCacheItem& item) const {
            return item->GetRawSizeVerified();
        }
    };

    class TResponseConstructor {
    private:
        std::unique_ptr<ISubscriber> Callback;
        TSourcesData Result;
        THashSet<ui64> InFlight;

        bool FinishIfReady() {
            if (InFlight.size()) {
                return false;
            }

            Callback->OnSourcesReady(std::move(Result));
            Callback.reset();
            return true;
        }

    public:
        TResponseConstructor(const std::vector<std::shared_ptr<IDataSource>>& sources, std::unique_ptr<ISubscriber>&& callback);

        void AddData(const ui64 sourceId, const TCacheItem& data) {
            NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)("source", sourceId));

            auto findInFlight = InFlight.find(sourceId);
            AFL_VERIFY(findInFlight != InFlight.end());
            InFlight.erase(findInFlight);

            AFL_VERIFY(Result.emplace(sourceId, std::move(data)).second);

            Y_UNUSED(FinishIfReady());
        }

        void Abort(const TString& error) {
            Callback->OnFailure(error);
            Callback.reset();
        }

        ~TResponseConstructor() {
            AFL_VERIFY(!Callback);
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

public:
    void GetSourcesData(const std::vector<std::shared_ptr<IDataSource>>& sources,
        const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& memoryGroup, std::unique_ptr<ISubscriber>&& subscriber);

    TSourceCache()
        : TActor(&TSourceCache::StateMain)
        , SourcesData(NYDBTest::TControllers::GetColumnShardController()->GetDuplicateManagerCacheSize()) {
    }

    ~TSourceCache() {
        for (auto& [_, info] : FetchingSources) {
            info.Abort("aborted");
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
