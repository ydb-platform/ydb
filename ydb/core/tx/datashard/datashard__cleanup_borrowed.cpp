#include "datashard_impl.h"
#include "operation.h"

#include <ydb/core/tablet_flat/flat_stat_table.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/mime/types/mime.h>
#include <library/cpp/resource/resource.h>

#include <library/cpp/html/pcdata/pcdata.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard::TTxMonitoringCleanupBorrowedPartsActor
    : public TActorBootstrapped<TDataShard::TTxMonitoringCleanupBorrowedPartsActor>
{
public:
    TTxMonitoringCleanupBorrowedPartsActor(
            const TActorId& owner,
            const TActorId& replyTo,
            THashMap<TLogoBlobID, TVector<ui64>> borrowedParts,
            bool dryRun)
        : Owner(owner)
        , ReplyTo(replyTo)
        , BorrowedParts(std::move(borrowedParts))
        , DryRun(dryRun)
    { }

    void Bootstrap(const TActorContext& ctx) {
        NTabletPipe::TClientConfig config({
            .RetryLimitCount = 5,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100)
        });
        config.CheckAliveness = true;

        for (const auto& kv : BorrowedParts) {
            for (const ui64 tabletId : kv.second) {
                auto res = WaitingTablets.insert(tabletId);
                if (res.second) {
                    ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, config));
                    ++TotalTablets;
                }
                ++TotalBorrows;
            }
        }

        Become(&TThis::StateDiscover);

        CheckDiscoverComplete(ctx);
    }

    STFUNC(StateDiscover) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            IgnoreFunc(TEvTabletPipe::TEvClientDestroyed);
            CFunc(TEvents::TEvPoison::EventType, HandlePoison);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();

        const ui64 tabletId = msg->TabletId;
        WaitingTablets.erase(tabletId);

        switch (msg->Status) {
            case NKikimrProto::OK:
                NTabletPipe::CloseClient(ctx, msg->ClientId);
                break;

            default:
                if (msg->Dead) {
                    DeadTablets.insert(tabletId);
                }
                break;
        }

        CheckDiscoverComplete(ctx);
    }

    void CheckDiscoverComplete(const TActorContext& ctx) {
        if (!WaitingTablets.empty()) {
            return;
        }

        for (const auto& kv : BorrowedParts) {
            for (const ui64 tabletId : kv.second) {
                if (DeadTablets.contains(tabletId)) {
                    DeadParts[tabletId].push_back(kv.first);
                }
            }
        }

        for (auto& kv : DeadParts) {
            std::sort(kv.second.begin(), kv.second.end());
            if (!DryRun) {
                // We use FlagTrackDelivery to catch cases where tablet dies before us
                // We use tablet id as cookie to distinguish different ack replies
                auto request = MakeHolder<TEvDataShard::TEvReturnBorrowedPart>(kv.first, kv.second);
                ctx.Send(Owner, request.Release(), IEventHandle::FlagTrackDelivery, kv.first);
                WaitingAcks.insert(kv.first);
            }
        }

        Become(&TThis::StateWaitAcks);

        CheckWaitComplete(ctx);
    }

    STFUNC(StateWaitAcks) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvDataShard::TEvReturnBorrowedPartAck, Handle);
            IgnoreFunc(TEvTabletPipe::TEvClientDestroyed);
            CFunc(TEvents::TEvPoison::EventType, HandlePoison);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
        switch (ev->Get()->SourceType) {
            case TEvDataShard::TEvReturnBorrowedPart::EventType: {
                if (WaitingAcks.erase(ev->Cookie)) {
                    DeadParts[ev->Cookie].clear();
                }
                CheckWaitComplete(ctx);
                break;
            }
        }
    }

    void Handle(TEvDataShard::TEvReturnBorrowedPartAck::TPtr& ev, const TActorContext& ctx) {
        WaitingAcks.erase(ev->Cookie);
        CheckWaitComplete(ctx);
    }

    void CheckWaitComplete(const TActorContext& ctx) {
        if (!WaitingAcks.empty()) {
            return;
        }

        TVector<ui64> deadTablets(DeadTablets.begin(), DeadTablets.end());
        std::sort(deadTablets.begin(), deadTablets.end());

        TStringStream str;
        HTML(str) {
            PRE() {
                if (DryRun) {
                    str << "Dry Run!" << Endl;
                    str << Endl;
                }
                str << "Total " << TotalBorrows << " borrows" << Endl;
                str << "Checked " << TotalTablets << " tablets" << Endl;
                str << Endl;
                for (const ui64 tabletId : deadTablets) {
                    str << "Dead tablet: " << tabletId << Endl;
                    for (const auto& part : DeadParts[tabletId]) {
                        str << "    Returned part: " << part << Endl;
                    }
                    str << Endl;
                }
            }
        }

        ctx.Send(ReplyTo, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        Die(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") { str << "Request cancelled"; }
            }
        }
        ctx.Send(ReplyTo, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        Die(ctx);
    }

    void Die(const TActorContext& ctx) override {
        ctx.Send(Owner, new TEvents::TEvGone);
        TActorBootstrapped::Die(ctx);
    }

private:
    const TActorId Owner;
    const TActorId ReplyTo;
    const THashMap<TLogoBlobID, TVector<ui64>> BorrowedParts;
    const bool DryRun;

    size_t TotalBorrows = 0;
    size_t TotalTablets = 0;
    THashSet<ui64> WaitingTablets;
    THashSet<ui64> DeadTablets;

    THashMap<ui64, TVector<TLogoBlobID>> DeadParts;
    THashSet<ui64> WaitingAcks;
};

class TDataShard::TTxMonitoringCleanupBorrowedParts
    : public NTabletFlatExecutor::TTransactionBase<TDataShard>
{
public:
    TTxMonitoringCleanupBorrowedParts(TDataShard *self, NMon::TEvRemoteHttpInfo::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext &, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                    "HTTP request at " << Self->TabletID() << " url="
                    << Ev->Get()->PathInfo());

        auto cgi = Ev->Get()->Cgi();

        if (cgi.Has("dry") && cgi.Get("dry") == "0") {
            DryRun = false;
        }

        BorrowedParts = Self->Executor()->GetBorrowedParts();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!BorrowedParts) {
            TStringStream str;
            HTML(str) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") { str << "Datashard doesn't have any borrowed parts"; }
                }
            }
            ctx.Send(Ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
            return;
        }

        auto actorId = ctx.RegisterWithSameMailbox(
            new TTxMonitoringCleanupBorrowedPartsActor(
                ctx.SelfID,
                Ev->Sender,
                std::move(BorrowedParts),
                DryRun));
        Self->Actors.insert(actorId);
    }

    TTxType GetTxType() const override { return TXTYPE_MONITORING; }

private:
    NMon::TEvRemoteHttpInfo::TPtr Ev;

    THashMap<TLogoBlobID, TVector<ui64>> BorrowedParts;
    bool DryRun = true;
};

ITransaction* TDataShard::CreateTxMonitoringCleanupBorrowedParts(
        TDataShard* self,
        NMon::TEvRemoteHttpInfo::TPtr ev)
{
    return new TTxMonitoringCleanupBorrowedParts(self, ev);
}

}
}
