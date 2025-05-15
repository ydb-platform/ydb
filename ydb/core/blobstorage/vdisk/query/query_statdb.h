#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexStatActor
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec, class TRequest = TEvBlobStorage::TEvVDbStat, class TResponse = TEvBlobStorage::TEvVDbStatResult>
    class TLevelIndexStatActor : public TActorBootstrapped<TLevelIndexStatActor<TKey, TMemRec, TRequest, TResponse>> {

        using TThis = ::NKikimr::TLevelIndexStatActor<TKey, TMemRec, TRequest, TResponse>;
        using TLevelIndex = ::NKikimr::TLevelIndex<TKey, TMemRec>;
        using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
        using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
        using TMemIterator = typename TLevelSegment::TMemIterator;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

        friend class TActorBootstrapped<TThis>;

        void Bootstrap(const TActorContext &ctx) {
            TStringStream str;
            if constexpr (std::is_same_v<TRequest, TEvBlobStorage::TEvVDbStat>) {
                const bool prettyPrint = Ev->Get()->Record.GetPrettyPrint();
                CalculateStat(str, prettyPrint);
                Result->SetResult(str.Str());
                SendVDiskResponse(ctx, Ev->Sender, Result.release(), Ev->Cookie, HullCtx->VCtx, {});
            } else {
                CalculateStat(Result);
                SendVDiskResponse(ctx, Ev->Sender, Result.release(), Ev->Cookie, HullCtx->VCtx, {});
            }
            ctx.Send(ParentId, new TEvents::TEvActorDied);
            TThis::Die(ctx);
        }

        void CalculateStat(IOutputStream &str, bool pretty);

        void CalculateStat(std::unique_ptr<TResponse> &result);

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TLevelIndexStatActor(
                const TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                TLevelIndexSnapshot &&snapshot,
                typename TRequest::TPtr &ev,
                std::unique_ptr<TResponse> result)
            : TActorBootstrapped<TThis>()
            , HullCtx(hullCtx)
            , ParentId(parentId)
            , Snapshot(std::move(snapshot))
            , Ev(ev)
            , Result(std::move(result))
        {}

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        TLevelIndexSnapshot Snapshot;
        typename TRequest::TPtr Ev;
        std::unique_ptr<TResponse> Result;
    };

} // NKikimr
