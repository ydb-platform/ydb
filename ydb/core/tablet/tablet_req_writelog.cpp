#include "tablet_impl.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tablet/tablet_metrics.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

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

    NWilson::TSpan RequestSpan;
    TMap<TLogoBlobID, NWilson::TSpan> BlobSpans;

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

            GroupWrittenBytes[std::make_pair(msg->Id.Channel(), msg->GroupId.GetRawId())] += msg->Id.BlobSize();
            GroupWrittenOps[std::make_pair(msg->Id.Channel(), msg->GroupId.GetRawId())] += 1;

            ResponseCookies ^= ev->Cookie;

            if (--RepliesToWait == 0) {
                if (Y_UNLIKELY(RequestCookies != ResponseCookies)) {
                    TString err = "TEvPut and TEvPutResult cookies don't match";
                    EndInnerSpanError(msg->Id, err);
                    return ReplyAndDie(NKikimrProto::ERROR, err, ctx);
                }

                EndInnerSpanOk(msg->Id);
                return ReplyAndDie(NKikimrProto::OK, { }, ctx);
            }

            EndInnerSpanOk(msg->Id);
            return;
        case NKikimrProto::RACE: // TODO: must be handled with retry
        case NKikimrProto::BLOCKED:
            EndInnerSpanError(msg->Id, msg->ErrorReason);
            return ReplyAndDie(NKikimrProto::BLOCKED, msg->ErrorReason, ctx);
        default:
            EndInnerSpanError(msg->Id, msg->ErrorReason);
            return ReplyAndDie(NKikimrProto::ERROR, msg->ErrorReason, ctx);
        }
    }

    void EndInnerSpanOk(const TLogoBlobID& blobId) {
        if (Y_UNLIKELY(RequestSpan)) {
            auto span = BlobSpans.extract(blobId);
            if (!span.empty()) {
                span.mapped().EndOk();
            }
        }
    }

    void EndInnerSpanError(const TLogoBlobID& blobId, const TString& errorReason) {
        if (Y_UNLIKELY(RequestSpan)) {
            auto span = BlobSpans.extract(blobId);
            if (!span.empty()) {
                span.mapped().EndError(errorReason);
            }

            for (auto& other : BlobSpans) {
                other.second.EndError("Another request failed");
            }

            BlobSpans.clear();
        }
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TString &reason, const TActorContext &ctx) {
        if (YellowMoveChannels) {
            SortUnique(YellowMoveChannels);
        }
        if (YellowStopChannels) {
            SortUnique(YellowStopChannels);
        }

        if (status == NKikimrProto::OK) {
            RequestSpan.EndOk();
        } else {
            RequestSpan.EndError(reason);
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
                  TEvBlobStorage::TEvPut::ETactic tactic, NWilson::TTraceId traceId) {
        Y_ABORT_UNLESS(id.TabletID() == Info->TabletID);
        const TTabletChannelInfo *channelInfo = Info->ChannelInfo(id.Channel());
        Y_ABORT_UNLESS(channelInfo);
        const TTabletChannelInfo::THistoryEntry *x = channelInfo->LatestEntry();
        Y_ABORT_UNLESS(x->FromGeneration <= id.Generation());

        ui64 cookie = RandomNumber<ui64>();
        RequestCookies ^= cookie;

        SendPutToGroup(ctx, x->GroupID, Info.Get(), MakeHolder<TEvBlobStorage::TEvPut>(id, buffer, TInstant::Max(), handleClass, tactic), cookie, std::move(traceId));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_WRITE_LOG;
    }

    TTabletReqWriteLog(const TActorId &owner, const TLogoBlobID &logid, NKikimrTabletBase::TTabletLogEntry *entry, TVector<TEvTablet::TLogEntryReference> &refs,
        TEvBlobStorage::TEvPut::ETactic commitTactic, TTabletStorageInfo *info, NWilson::TTraceId traceId)
        : Owner(owner)
        , LogEntryID(logid)
        , LogEntry(entry)
        , CommitTactic(commitTactic)
        , Info(info)
        , RepliesToWait(Max<ui32>())
        , RequestSpan(TWilsonTablet::TabletDetailed, std::move(traceId), "Tablet.WriteLog")
    {
        References.swap(refs);
        Y_ABORT_UNLESS(Info);
    }

    void Bootstrap(const TActorContext &ctx) {
        TString logEntryBuffer = LogEntry->SerializeAsString();

        // todo: adaptive save-with-retry and timeouts
        // todo: cancelation

        const auto handleClass = NKikimrBlobStorage::TabletLog;
        for (const auto &ref : References) {
            NWilson::TTraceId innerTraceId;

            if (RequestSpan) {
                auto res = BlobSpans.try_emplace(ref.Id, TWilsonTablet::TabletDetailed, RequestSpan.GetTraceId(), "Tablet.WriteLog.Reference");

                innerTraceId = res.first->second.GetTraceId();
            }

            SendToBS(ref.Id, ref.Buffer, ctx, handleClass, ref.Tactic ? *ref.Tactic : CommitTactic, std::move(innerTraceId));
        }

        const TLogoBlobID actualLogEntryId = TLogoBlobID(
            LogEntryID.TabletID(),
            LogEntryID.Generation(),
            LogEntryID.Step(),
            LogEntryID.Channel(),
            logEntryBuffer.size(),
            LogEntryID.Cookie()
        );

        NWilson::TTraceId traceId;

        if (RequestSpan) {
            auto res = BlobSpans.try_emplace(actualLogEntryId, TWilsonTablet::TabletDetailed, RequestSpan.GetTraceId(), "Tablet.WriteLog.LogEntry");

            traceId = std::move(res.first->second.GetTraceId());
        }

        SendToBS(actualLogEntryId, logEntryBuffer, ctx, NKikimrBlobStorage::TabletLog, CommitTactic, std::move(traceId));

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

IActor* CreateTabletReqWriteLog(const TActorId &owner, const TLogoBlobID &entryId, NKikimrTabletBase::TTabletLogEntry *entry, TVector<TEvTablet::TLogEntryReference> &refs, TEvBlobStorage::TEvPut::ETactic commitTactic, TTabletStorageInfo *info, NWilson::TTraceId traceId) {
    return new TTabletReqWriteLog(owner, entryId, entry, refs, commitTactic, info, std::move(traceId));
}

}
