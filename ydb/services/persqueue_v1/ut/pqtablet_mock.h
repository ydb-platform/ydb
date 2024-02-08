#pragma once

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/public/lib/base/msgbus_status.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

#include <memory>

namespace NKikimr::NPersQueueTests {

class TPQTabletMock : public TActor<TPQTabletMock>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    TPQTabletMock(const TActorId& tablet, TTabletStorageInfo* info);

    void AppendWriteReply(ui64 cookie);

private:
    using TEvRequestPtr = std::unique_ptr<TEvPersQueue::TEvRequest>;
    using TEvResponsePtr = std::unique_ptr<TEvPersQueue::TEvResponse>;

    struct TReply {
        TEvResponsePtr Response;
        TActorId Recipient;
    };

    struct TCommandReplies {
        TDeque<ui64> ExpectedRequests;
        THashMap<ui64, TReply> Replies;
    };

    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override;
    void DefaultSignalTabletActive(const TActorContext &ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;

    STFUNC(StateBoot);
    STFUNC(StateWork);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvRequest::TPtr& ev, const TActorContext& ctx);

    TMaybe<ui64> GetPartitionRequestCookie() const;

    void PrepareGetOwnershipResponse();
    void PrepareGetMaxSeqNoResponse();
    void PrepareReserveBytesResponse(const TActorId& recipient);
    void PrepareWriteResponse(const TActorId& recipient);

    static TEvResponsePtr MakeReserveBytesResponse(ui64 cookie);
    static TEvResponsePtr MakeWriteResponse(ui64 cookie);

    void TrySendResponses(const TActorContext& ctx, TCommandReplies& cmd);

    TEvRequestPtr Request;
    TEvResponsePtr Response;

    TString OwnerCookie = "owner-cookie";
    ui64 MaxSeqNo = 0;

    TCommandReplies CmdReserve;
    TCommandReplies CmdWrite;
};

}
