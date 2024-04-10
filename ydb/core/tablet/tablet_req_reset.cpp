#include "tablet_impl.h"
#include "tablet_sys.h"
#include <ydb/core/base/logoblob.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {

class TTabletReqReset : public TActorBootstrapped<TTabletReqReset> {
    const TActorId Owner;
    const TIntrusivePtr<TTabletStorageInfo> TabletStorageInfo;
    ui32 Generation = 0;
    TActorId CurrentLeader;

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TActorContext& ctx) {
        ctx.Send(Owner, new TEvTablet::TEvResetTabletResult(status, TabletStorageInfo->TabletID));
        Die(ctx);
    }

    void Handle(TEvStateStorage::TEvInfo::TPtr& ev, const TActorContext&) {
        CurrentLeader = ev->Get()->CurrentLeader;
        Generation = std::max(Generation, ev->Get()->CurrentGeneration);
    }

    void FindLatestLogEntry(const TActorContext& ctx) {
        ctx.Register(CreateTabletFindLastEntry(ctx.SelfID, false, TabletStorageInfo.Get(), 0, true));
        Become(&TTabletReqReset::StateDiscover);
    }

    void Handle(TEvTabletBase::TEvFindLatestLogEntryResult::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            return ReplyAndDie(ev->Get()->Status, ctx);
        }
        Generation = std::max(Generation, ev->Get()->Latest.Generation());
        AdjustGeneration();
        BlockBlobStorage(ctx);
    }

    void AdjustGeneration() {
        if (Generation < TabletStorageInfo->Channels[0].LatestEntry()->FromGeneration) {
            Generation = TabletStorageInfo->Channels[0].LatestEntry()->FromGeneration;
        }
    }

    void BlockBlobStorage(const TActorContext& ctx) {
        ctx.Register(CreateTabletReqBlockBlobStorage(ctx.SelfID, TabletStorageInfo.Get(), Generation, false));
        Become(&TTabletReqReset::StateBlockBlobStorage);
    }

    void Handle(TEvTabletBase::TEvBlockBlobStorageResult::TPtr& ev, const TActorContext& ctx) {
        switch (ev->Get()->Status) {
            case NKikimrProto::OK:
                break;

            case NKikimrProto::RACE:
            case NKikimrProto::ALREADY:
                ++Generation;
                ctx.Register(CreateTabletReqBlockBlobStorage(ctx.SelfID, TabletStorageInfo.Get(), Generation, false));
                return;

            default:
                return ReplyAndDie(ev->Get()->Status, ctx);
        }

        TTablet::ExternalWriteZeroEntry(TabletStorageInfo.Get(), Generation + 1, SelfId());
        Become(&TTabletReqReset::StateWriteZeroEntry);
    }

    void Handle(TEvTabletBase::TEvWriteLogResult::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            return ReplyAndDie(ev->Get()->Status, ctx);
        }
        ctx.Register(CreateTabletReqDelete(ctx.SelfID, TabletStorageInfo, Generation));
        Become(&TTabletReqReset::StateDeleteTablet);
    }

    void Handle(TEvTabletBase::TEvDeleteTabletResult::TPtr& ev, const TActorContext& ctx) {
        if (CurrentLeader) {
            ctx.Send(CurrentLeader, new TEvents::TEvPoisonPill());
        }
        ReplyAndDie(ev->Get()->Status, ctx);
    }

    // 1
    STFUNC(StateDiscover) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletBase::TEvFindLatestLogEntryResult, Handle); // latest log entry discovered
            HFunc(TEvStateStorage::TEvInfo, Handle); // reply from state storage
        }
    }
    // 2
    STFUNC(StateBlockBlobStorage) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletBase::TEvBlockBlobStorageResult, Handle); // blob storage blocked
            HFunc(TEvStateStorage::TEvInfo, Handle); // reply from state storage
        }
    }
    // 3
    STFUNC(StateWriteZeroEntry) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletBase::TEvWriteLogResult, Handle); // zero entry written
            HFunc(TEvStateStorage::TEvInfo, Handle); // reply from state storage
        }
    }
    // 4
    STFUNC(StateDeleteTablet) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletBase::TEvDeleteTabletResult, Handle); // tablet data deleted
            HFunc(TEvStateStorage::TEvInfo, Handle); // reply from state storage
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_DELETE_TABLET;
    }

    TTabletReqReset(const TActorId& owner, const TIntrusivePtr<TTabletStorageInfo>& tabletStorageInfo, ui32 knownGeneration)
        : Owner(owner)
        , TabletStorageInfo(tabletStorageInfo)
        , Generation(knownGeneration)
    {
        Y_ABORT_UNLESS(!TabletStorageInfo->Channels.empty());
        Y_ABORT_UNLESS(TabletStorageInfo->Channels[0].LatestEntry() != nullptr);
    }

    void Bootstrap(const TActorContext& ctx) {
        TActorId stateStorageProxyId = MakeStateStorageProxyID();
        ctx.Send(stateStorageProxyId, new TEvStateStorage::TEvLookup(TabletStorageInfo->TabletID, 0));
        if (Generation == 0) {
            FindLatestLogEntry(ctx);
        } else {
            AdjustGeneration();
            BlockBlobStorage(ctx);
        }
    }
};

IActor* CreateTabletReqReset(const TActorId& owner, const TIntrusivePtr<NKikimr::TTabletStorageInfo>& info, ui32 knownGeneration) {
    return new TTabletReqReset(owner, info, knownGeneration);
}

}
