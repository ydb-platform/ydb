#include "tablet_impl.h"
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <ydb/core/tablet/tablet_metrics.h>

#include <util/random/random.h>

namespace NKikimr {

class TTabletReqWriteLog : public TActorBootstrapped<TTabletReqWriteLog> {
    const TActorId Owner;
    const TLogoBlobID LogEntryID;
    TAutoPtr<NKikimrTabletBase::TTabletLogEntry> LogEntry;
    TVector<TEvTablet::TLogEntryReference> References;
    const TEvBlobStorage::TEvPut::ETactic CommitTactic;

    TIntrusivePtr<TTabletStorageInfo> Info;
    NMetrics::TTabletThroughputRawValue GroupWrittenBytes;
    NMetrics::TTabletIopsRawValue GroupWrittenOps;

    ui64 RequestCookies = 0;
    ui64 ResponseCookies = 0;

    ui32 RepliesToWait;
    TVector<ui32> YellowMoveChannels;
    TVector<ui32> YellowStopChannels;

    void Handle(TEvents::TEvUndelivered::TPtr&, const TActorContext &ctx) {
        return ReplyAndDie(NKikimrProto::ERROR, "BlobStorage proxy unavailable", ctx);
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr &ev, const TActorContext &ctx) {
        TEvBlobStorage::TEvPutResult *msg = ev->Get();

        if (msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
            YellowMoveChannels.push_back(msg->Id.Channel());
        }
        if (msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
            YellowStopChannels.push_back(msg->Id.Channel());
        }

        switch (msg->Status) {
        case NKikimrProto::OK:
            LOG_DEBUG_S(ctx, NKikimrServices::TABLET_MAIN, "Put Result: " << msg->Print(false));

            GroupWrittenBytes[std::make_pair(msg->Id.Channel(), msg->GroupId)] += msg->Id.BlobSize();
            GroupWrittenOps[std::make_pair(msg->Id.Channel(), msg->GroupId)] += 1;

            ResponseCookies ^= ev->Cookie;

            if (--RepliesToWait == 0) {
                if (Y_UNLIKELY(RequestCookies != ResponseCookies)) {
                    return ReplyAndDie(NKikimrProto::ERROR, "TEvPut and TEvPutResult cookies don't match", ctx);
                }

                return ReplyAndDie(NKikimrProto::OK, { }, ctx);
            }

            return;
        case NKikimrProto::RACE: // TODO: must be handled with retry
        case NKikimrProto::BLOCKED:
            return ReplyAndDie(NKikimrProto::BLOCKED, msg->ErrorReason, ctx);
        default:
            return ReplyAndDie(NKikimrProto::ERROR, msg->ErrorReason, ctx);
        }
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TString &reason, const TActorContext &ctx) {
        if (YellowMoveChannels) {
            SortUnique(YellowMoveChannels);
        }
        if (YellowStopChannels) {
            SortUnique(YellowStopChannels);
        }

        ctx.Send(Owner, new TEvTabletBase::TEvWriteLogResult(
            status,
            LogEntryID,
            std::move(YellowMoveChannels),
            std::move(YellowStopChannels),
            std::move(GroupWrittenBytes),
            std::move(GroupWrittenOps),
            reason));
        Die(ctx);
    }

    void SendToBS(const TLogoBlobID &id, const TString &buffer, const TActorContext &ctx,
                  const NKikimrBlobStorage::EPutHandleClass handleClass,
                  TEvBlobStorage::TEvPut::ETactic tactic) {
        Y_VERIFY(id.TabletID() == Info->TabletID);
        const TTabletChannelInfo *channelInfo = Info->ChannelInfo(id.Channel());
        Y_VERIFY(channelInfo);
        const TTabletChannelInfo::THistoryEntry *x = channelInfo->LatestEntry();
        Y_VERIFY(x->FromGeneration <= id.Generation());

        ui64 cookie = RandomNumber<ui64>();
        RequestCookies ^= cookie;

        SendPutToGroup(ctx, x->GroupID, Info.Get(), MakeHolder<TEvBlobStorage::TEvPut>(id, buffer, TInstant::Max(), handleClass, tactic), cookie);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_WRITE_LOG;
    }

    TTabletReqWriteLog(const TActorId &owner, const TLogoBlobID &logid, NKikimrTabletBase::TTabletLogEntry *entry, TVector<TEvTablet::TLogEntryReference> &refs, TEvBlobStorage::TEvPut::ETactic commitTactic, TTabletStorageInfo *info)
        : Owner(owner)
        , LogEntryID(logid)
        , LogEntry(entry)
        , CommitTactic(commitTactic)
        , Info(info)
        , RepliesToWait(Max<ui32>())
    {
        References.swap(refs);
        Y_VERIFY(Info);
    }

    void Bootstrap(const TActorContext &ctx) {
        TString logEntryBuffer = LogEntry->SerializeAsString();

        // todo: adaptive save-with-retry and timeouts
        // todo: cancelation

        const auto handleClass = NKikimrBlobStorage::TabletLog;
        for (const auto &ref : References) {
            SendToBS(ref.Id, ref.Buffer, ctx, handleClass, ref.Tactic ? *ref.Tactic : CommitTactic);
        }

        const TLogoBlobID actualLogEntryId = TLogoBlobID(
            LogEntryID.TabletID(),
            LogEntryID.Generation(),
            LogEntryID.Step(),
            LogEntryID.Channel(),
            logEntryBuffer.size(),
            LogEntryID.Cookie()
        );
        SendToBS(actualLogEntryId, logEntryBuffer, ctx, NKikimrBlobStorage::TabletLog, CommitTactic);

        RepliesToWait = References.size() + 1;
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvPutResult, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
        }
    }
};

IActor* CreateTabletReqWriteLog(const TActorId &owner, const TLogoBlobID &entryId, NKikimrTabletBase::TTabletLogEntry *entry, TVector<TEvTablet::TLogEntryReference> &refs, TEvBlobStorage::TEvPut::ETactic commitTactic, TTabletStorageInfo *info) {
    return new TTabletReqWriteLog(owner, entryId, entry, refs, commitTactic, info);
}

}
