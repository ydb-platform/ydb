#include "tablet_impl.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, stream)

namespace NKikimr {

class TTabletReqFindLatestLogEntry : public TActorBootstrapped<TTabletReqFindLatestLogEntry> {
    const TActorId Owner;
    const bool ReadBody;
    const ui32 BlockedGeneration;
    const bool Leader;
    TIntrusivePtr<TTabletStorageInfo> Info;
    const TTabletChannelInfo *ChannelInfo;
    ui64 CurrentHistoryIndex;

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TString &reason) {
        Send(Owner, new TEvTabletBase::TEvFindLatestLogEntryResult(status, reason));
        PassAway();
    }

    void FindLastEntryNext() {
        if (CurrentHistoryIndex == 0)
            return ReplyAndDie(NKikimrProto::NODATA, "Tablet has no data");
        --CurrentHistoryIndex;

        const ui32 group = ChannelInfo->History[CurrentHistoryIndex].GroupID;
        const ui32 minGeneration = ChannelInfo->History[CurrentHistoryIndex].FromGeneration;
        auto request = MakeHolder<TEvBlobStorage::TEvDiscover>(Info->TabletID, minGeneration, ReadBody, true, TInstant::Max(), BlockedGeneration, Leader);
        SendToBSProxy(SelfId(), group, request.Release());
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        return ReplyAndDie(NKikimrProto::ERROR, "BlobStorage proxy unavailable");
    }

    void Handle(TEvBlobStorage::TEvDiscoverResult::TPtr &ev) {
        TEvBlobStorage::TEvDiscoverResult *msg = ev->Get();

        switch (msg->Status) {
        case NKikimrProto::OK:
            // as we can assign groups in ABA order, we must check that found entry is not from outdated generation
            if (ChannelInfo->History[CurrentHistoryIndex].FromGeneration > msg->Id.Generation()) {
                return FindLastEntryNext();
            } else {
                Send(Owner, new TEvTabletBase::TEvFindLatestLogEntryResult(msg->Id, msg->BlockedGeneration, msg->Buffer));
                return PassAway();
            }
        case NKikimrProto::NODATA:
            return FindLastEntryNext();
        case NKikimrProto::RACE:
        case NKikimrProto::ERROR:
        case NKikimrProto::TIMEOUT:
        case NKikimrProto::NO_GROUP:
        case NKikimrProto::BLOCKED:
            BLOG_ERROR("Handle::TEvDiscoverResult, result status " << NKikimrProto::EReplyStatus_Name(msg->Status));
            return ReplyAndDie(msg->Status, msg->ErrorReason);
        default:
            Y_ABORT_UNLESS(false, "default case status %s", NKikimrProto::EReplyStatus_Name(msg->Status).c_str());

            return ReplyAndDie(NKikimrProto::ERROR, msg->ErrorReason);
        }
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_FIND_LATEST;
    }

    TTabletReqFindLatestLogEntry(const TActorId &owner, bool readBody, TTabletStorageInfo *info, ui32 blockedGeneration, bool leader)
        : Owner(owner)
        , ReadBody(readBody)
        , BlockedGeneration(blockedGeneration)
        , Leader(leader)
        , Info(info)
        , ChannelInfo(Info->ChannelInfo(0))
        , CurrentHistoryIndex(ChannelInfo->History.size())
    {
        Y_ABORT_UNLESS(CurrentHistoryIndex > 0);
    }

    void Bootstrap() {
        Become(&TThis::StateInit);
        FindLastEntryNext();
    }

    STATEFN(StateInit) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvDiscoverResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
        }
    }
};

IActor* CreateTabletFindLastEntry(const TActorId &owner, bool readBody, TTabletStorageInfo *info, ui32 blockedGeneration, bool leader) {
    return new TTabletReqFindLatestLogEntry(owner, readBody, info, blockedGeneration, leader);
}

}
