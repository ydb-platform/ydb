#pragma once

#include "defs.h"
#include "query_statalgo.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexStatActor
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec, class TRequest = TEvBlobStorage::TEvVDbStat, class TResponse = TEvBlobStorage::TEvVDbStatResult>
    class TLevelIndexStatActor : public TActorBootstrapped<TLevelIndexStatActor<TKey, TMemRec, TRequest, TResponse>> {

        using TThis = ::NKikimr::TLevelIndexStatActor<TKey, TMemRec, TRequest, TResponse>;
        using TBase = TActorBootstrapped<TThis>;
        using TLevelIndex = ::NKikimr::TLevelIndex<TKey, TMemRec>;
        using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
        using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
        using TMemIterator = typename TLevelSegment::TMemIterator;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
        using TYieldedState = TDbStatYeildedState<TKey, TMemRec>;
        using TTraversal = std::function<std::optional<TYieldedState>(
            const TLevelIndexSnapshot&, std::optional<TYieldedState>)>;

        friend class TActorBootstrapped<TThis>;

        void Bootstrap(const TActorContext &ctx) {
            if constexpr (std::is_same_v<TRequest, TEvBlobStorage::TEvVDbStat>) {
                const bool prettyPrint = Ev->Get()->Record.GetPrettyPrint();
                PrepareStat(Output, prettyPrint);
            } else {
                PrepareStat(Result);
            }
            TThis::Become(&TThis::StateFunc);
            ContinueTraversal(ctx);
        }

        void PrepareStat(IOutputStream &str, bool pretty);

        void PrepareStat(std::unique_ptr<TResponse> &result);

        template <class TAggr>
        void SetAggregator(std::shared_ptr<TAggr> aggr) {
            Traversal = [this, aggr = std::move(aggr)](
                    const TLevelIndexSnapshot& snapshot,
                    std::optional<TYieldedState> yieldedState) mutable {
                return TraverseDbWithoutMerge(
                    HullCtx,
                    aggr.get(),
                    snapshot,
                    std::move(yieldedState),
                    YieldPolicy);
            };
        }

        void ContinueTraversal(const TActorContext& ctx) {
            Y_ABORT_UNLESS(Snapshot);
            YieldedState = Traversal(*Snapshot, std::move(YieldedState));
            Snapshot->Destroy();
            Snapshot.reset();

            if (YieldedState) {
                ctx.Schedule(YieldPolicy.DelayBetweenQuants, new TEvents::TEvWakeup);
            } else {
                ReplyAndDie(ctx);
            }
        }

        void HandleWakeup(const TActorContext& ctx) {
            ctx.Send(ParentId, new TEvTakeHullSnapshot(true));
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr& ev, const TActorContext& ctx) {
            if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
                Snapshot.emplace(std::move(ev->Get()->Snap.LogoBlobsSnap));
            } else if constexpr (std::is_same_v<TKey, TKeyBlock>) {
                Snapshot.emplace(std::move(ev->Get()->Snap.BlocksSnap));
            } else if constexpr (std::is_same_v<TKey, TKeyBarrier>) {
                Snapshot.emplace(std::move(ev->Get()->Snap.BarriersSnap));
            } else {
                static_assert(!std::is_same_v<TKey, TKey>, "unsupported Hull database key");
            }
            ContinueTraversal(ctx);
        }

        void ReplyAndDie(const TActorContext& ctx) {
            if constexpr (std::is_same_v<TRequest, TEvBlobStorage::TEvVDbStat>) {
                Result->SetResult(Output.Str());
            }
            SendVDiskResponse(ctx, Ev->Sender, Result.release(), Ev->Cookie, HullCtx->VCtx, {});
            ctx.Send(ParentId, new TEvents::TEvGone);
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc, {
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            CFunc(TEvents::TSystem::PoisonPill, TBase::Die);
            HFunc(TEvTakeHullSnapshotResult, Handle);
        })

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TLevelIndexStatActor(
                const TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                TLevelIndexSnapshot &&snapshot,
                typename TRequest::TPtr &ev,
                std::unique_ptr<TResponse> result,
                TDbStatYieldPolicy yieldPolicy = {})
            : TActorBootstrapped<TThis>()
            , HullCtx(hullCtx)
            , ParentId(parentId)
            , Snapshot(std::in_place, std::move(snapshot))
            , Ev(ev)
            , Result(std::move(result))
            , YieldPolicy(std::move(yieldPolicy))
        {}

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        std::optional<TLevelIndexSnapshot> Snapshot;
        typename TRequest::TPtr Ev;
        std::unique_ptr<TResponse> Result;
        const TDbStatYieldPolicy YieldPolicy;
        TStringStream Output;
        TTraversal Traversal;
        std::optional<TYieldedState> YieldedState;
    };

} // NKikimr
