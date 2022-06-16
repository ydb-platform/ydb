#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexStatActor
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexStatActor : public TActorBootstrapped<TLevelIndexStatActor<TKey, TMemRec>> {

        using TThis = ::NKikimr::TLevelIndexStatActor<TKey, TMemRec>;
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
            const bool prettyPrint = Ev->Get()->Record.GetPrettyPrint();
            CalculateStat(str, prettyPrint);
            Result->SetResult(str.Str());
            SendVDiskResponse(ctx, Ev->Sender, Result.release(), Ev->Cookie);
            ctx.Send(ParentId, new TEvents::TEvActorDied);
            TThis::Die(ctx);
        }

        void CalculateStat(IOutputStream &str, bool pretty);

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TLevelIndexStatActor(
                const TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                TLevelIndexSnapshot &&snapshot,
                TEvBlobStorage::TEvVDbStat::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
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
        TEvBlobStorage::TEvVDbStat::TPtr Ev;
        std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> Result;
    };

} // NKikimr
