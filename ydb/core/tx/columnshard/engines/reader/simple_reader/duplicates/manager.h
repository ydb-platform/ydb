#pragma once

#include "events.h"
#include "source_cache.h"
#include "interval_tree.h"
#include "merge.h"
#include "source_cache.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TSpecialReadContext;
class IDataSource;
class TPortionDataSource;
class TColumnFetchingContext;

class TDuplicateFilterConstructor: public NActors::TActor<TDuplicateFilterConstructor> {
private:
    class TSourceInfo {
    private:
        ui64 SourceIdx;
        std::shared_ptr<TPortionInfo> PortionInfo;

    public:
        TSourceInfo(const ui64 sourceIdx, const std::shared_ptr<TPortionInfo>& portionInfo)
            : SourceIdx(sourceIdx)
            , PortionInfo(portionInfo) {
        }

        std::shared_ptr<TPortionDataSource> Construct(const std::shared_ptr<TSpecialReadContext>& context) const;
    };

    class TSourceDataSubscriber: public TSourceCache::ISubscriber {
    private:
        TActorId Owner;
        std::shared_ptr<IDataSource> Source;
        std::shared_ptr<IFilterSubscriber> Callback;

        virtual void OnSourcesReady(TSourceCache::TSourcesData&& result) override;
        virtual void OnFailure(const TString& error) override {
            Y_UNUSED(error);   // FIXME
            TActivationContext::AsActorContext().Send(Owner, new NActors::TEvents::TEvPoison);
        }

    public:
        TSourceDataSubscriber(
            const TActorId& owner, const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFilterSubscriber>& callback)
            : Owner(owner)
            , Source(source)
            , Callback(callback) {
        }
    };

    class TFilterResultSubscriber: public TBuildDuplicateFilters::ISubscriber {
    private:
        TActorId Owner;
        std::shared_ptr<IDataSource> Source;
        std::shared_ptr<IFilterSubscriber> Callback;

        virtual void OnResult(THashMap<ui64, NArrow::TColumnFilter>&& result) override;
        virtual void OnFailure(const TString& error) override {
            Y_UNUSED(error);   // FIXME
            TActivationContext::AsActorContext().Send(Owner, new NActors::TEvents::TEvPoison);
        }

    public:
        TFilterResultSubscriber(
            const TActorId& owner, const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFilterSubscriber>& callback)
            : Owner(owner)
            , Source(source)
            , Callback(callback) {
        }
    };

private:
    TSourceCache* SourceCache;
    const TIntervalTree<TInterval<NArrow::TSimpleRow>, TSourceInfo> Intervals;
    // const std::shared_ptr<TSourceIntervals> Intervals;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

public:
    TDuplicateFilterConstructor(const TSpecialReadContext& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
