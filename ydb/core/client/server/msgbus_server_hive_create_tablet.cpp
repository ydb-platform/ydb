#include "msgbus_tabletreq.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/subdomain.h>

namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui32 DefaultTimeout = 90000;
}

template <typename ResponseType>
class TMessageBusHiveCreateTablet
        : public TActorBootstrapped<TMessageBusHiveCreateTablet<ResponseType>>
        , public TMessageBusSessionIdentHolder {
using TBase = TActorBootstrapped<TMessageBusHiveCreateTablet<ResponseType>>;

    struct TRequest {
        TEvHive::EEv Event;
        ui64 OwnerId;
        ui64 OwnerIdx;
        TTabletTypes::EType TabletType;
        TVector<ui32> AllowedNodeIDs;
        TVector<TSubDomainKey> AllowedDomains;
        TChannelsBindings BindedChannels;

        NKikimrProto::EReplyStatus Status;
        ui64 TabletId;

        TRequest(ui64 ownerId, ui64 ownerIdx, TTabletTypes::EType tabletType,
                 TVector<ui32> allowedNodeIDs,
                 TVector<TSubDomainKey> allowedDomains,
                 TChannelsBindings bindedChannels)
            : Event(TEvHive::EvCreateTablet)
            , OwnerId(ownerId)
            , OwnerIdx(ownerIdx)
            , TabletType(tabletType)
            , AllowedNodeIDs(std::move(allowedNodeIDs))
            , AllowedDomains(std::move(allowedDomains))
            , BindedChannels(std::move(bindedChannels))
            , Status(NKikimrProto::UNKNOWN)
            , TabletId(0)
        {}

        TRequest(ui64 ownerId, ui64 ownerIdx)
            : Event(TEvHive::EvLookupTablet)
            , OwnerId(ownerId)
            , OwnerIdx(ownerIdx)
            , TabletType()
            , Status(NKikimrProto::UNKNOWN)
            , TabletId(0)
        {}
    };

    TDuration Timeout;
    bool WithRetry;
    TActorId PipeClient;

    TDeque<TRequest> Requests;
    std::optional<ui64> HiveId;
    NKikimrProto::EReplyStatus Status;
    ui32 ResponsesReceived;
    ui32 DomainUid;
    TString ErrorReason;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TMessageBusHiveCreateTablet(TBusMessageContext &msg)
            : TMessageBusSessionIdentHolder(msg)
            , Status(NKikimrProto::UNKNOWN)
            , ResponsesReceived(0)
            , DomainUid(0)
    {
        const auto &record = static_cast<TBusHiveCreateTablet *>(msg.GetMessage())->Record;

        bool isOk = true;
        ui32 cmdCount = record.CmdCreateTabletSize();
        for (ui32 i = 0; i < cmdCount; ++i) {
            const auto &cmd = record.GetCmdCreateTablet(i);
            isOk = isOk && cmd.HasOwnerId() && cmd.HasOwnerIdx()
                && cmd.HasTabletType() && (cmd.BindedChannelsSize() > 0);
            if (isOk) {
                Requests.emplace_back(cmd.GetOwnerId(), cmd.GetOwnerIdx(), cmd.GetTabletType(),
                    TVector<ui32>(cmd.GetAllowedNodeIDs().begin(), cmd.GetAllowedNodeIDs().end()),
                    TVector<TSubDomainKey>(cmd.GetAllowedDomains().begin(), cmd.GetAllowedDomains().end()),
                    TChannelsBindings(cmd.GetBindedChannels().begin(), cmd.GetBindedChannels().end()));
            } else {
                ErrorReason = Sprintf("Missing arguments for CmdCreateTablet(%" PRIu32 ") call", i);
            }
        }
        cmdCount = record.CmdLookupTabletSize();
        for (ui32 i = 0; i < cmdCount; ++i) {
            const auto &cmd = record.GetCmdLookupTablet(i);
            isOk = isOk && cmd.HasOwnerId() && cmd.HasOwnerIdx();
            if (isOk) {
                Requests.emplace_back(cmd.GetOwnerId(), cmd.GetOwnerIdx());
            } else {
                ErrorReason = Sprintf("Missing arguments for CmdLookupTablet(%" PRIu32 ") call", i);
            }
        }
        if (isOk) {
            isOk = isOk && record.HasDomainUid();
            if (!isOk) {
                ErrorReason = Sprintf("Missing DomainUid for CmdCreateTablet/CmdLookupTablet call");
            }
        }

        DomainUid = isOk ? record.GetDomainUid() : 0;
        Status = isOk ? NKikimrProto::OK : NKikimrProto::ERROR;
        HiveId = record.HasHiveId() ? std::make_optional(record.GetHiveId()) : std::nullopt;
        WithRetry = true;
        Timeout = TDuration::MilliSeconds(DefaultTimeout);
    }

    void Handle(TEvHive::TEvCreateTabletReply::TPtr &ev, const TActorContext &ctx) {
        const NKikimrHive::TEvCreateTabletReply &record = ev->Get()->Record;

        ++ResponsesReceived;
        NKikimrProto::EReplyStatus status = record.GetStatus();
        if (ev->Cookie >= Requests.size()) {
            // Report the details
            ErrorReason = Sprintf("Unexpected reply from Hive with Cookie# %" PRIu64 ", Marker# HC2",
                (ui64)ev->Cookie);
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
        }

        TRequest &cmd = Requests[ev->Cookie];
        if (cmd.Status != NKikimrProto::UNKNOWN) {
            // Report the details
            ErrorReason = Sprintf("Duplicate reply from Hive with Cookie# %" PRIu64 ", Marker# HC3",
                (ui64)ev->Cookie);
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
        }

        if (!record.HasStatus()) {
            // Report the details
            ErrorReason = Sprintf("No status in reply from Hive with Cookie# %" PRIu64 ", Marker# HC4",
                (ui64)ev->Cookie);
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
        }

        cmd.Status = record.GetStatus();

        switch (status) {
            case NKikimrProto::OK:
            case NKikimrProto::ALREADY:
                if (!record.HasOwner() || !record.HasOwnerIdx() || !record.HasTabletID()) {
                    // Report the details
                    ErrorReason = Sprintf("Missing fields in reply from Hive, Marker# HC5");
                    return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
                }
                if (record.GetOwner() != cmd.OwnerId || record.GetOwnerIdx() != cmd.OwnerIdx) {
                    // Report the details
                    ErrorReason = Sprintf("Reply from Hive does not match the request, Marker# HC6");
                    return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
                }
                cmd.TabletId = record.GetTabletID();
                break;
            default:
                break;
        }

        if (ResponsesReceived == Requests.size()) {
            switch (Status) {
                case NKikimrProto::OK: {
                    THolder<ResponseType> result(new ResponseType());
                    auto &rec = result->Record;
                    rec.SetStatus(MSTATUS_OK);
                    for (ui32 i = 0; i < Requests.size(); ++i) {
                        TRequest &request = Requests[i];
                        switch (request.Event) {
                            case TEvHive::EvCreateTablet: {
                                auto* item = rec.AddCreateTabletResult();
                                item->SetStatus(request.Status);
                                item->SetTabletId(request.TabletId);
                                break;
                            }
                            case TEvHive::EvLookupTablet: {
                                auto* item = rec.AddLookupTabletResult();
                                item->SetStatus(request.Status);
                                item->SetTabletId(request.TabletId);
                                break;
                            }
                            default:
                                break;
                        }
                    }
                    return SendReplyAndDie(result.Release(), ctx);
                }
                default: {
                    if (ErrorReason.size() == 0) {
                        ErrorReason = Sprintf("Unexpected Status, Marker# HC7");
                    }
                    return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
                }
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            PipeClient = TActorId();
            ErrorReason = Sprintf("Client pipe to Hive connection error, Status# %s, Marker# HC10",
                NKikimrProto::EReplyStatus_Name(msg->Status).data());
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        PipeClient = TActorId();
        ErrorReason = Sprintf("Client pipe to Hive destroyed (connection lost), Marker# HC9");
        SendReplyMove(CreateErrorReply(MSTATUS_ERROR, ctx));
        return Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        ErrorReason = Sprintf("Timeout while waiting for hive response, may be just slow, Marker# HC11");
        return SendReplyAndDie(CreateErrorReply(MSTATUS_TIMEOUT, ctx), ctx);
    }

    void Die(const TActorContext &ctx) override {
        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = TActorId();
        }
        TActorBootstrapped<TMessageBusHiveCreateTablet>::Die(ctx);
    }

    virtual NBus::TBusMessage* CreateErrorReply(EResponseStatus status, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        THolder<ResponseType> result(new ResponseType());
        auto &rec = result->Record;
        rec.SetStatus(status);
        if (ErrorReason.size()) {
            rec.SetErrorReason(ErrorReason);
        } else {
            rec.SetErrorReason("Unknown, Marker# HC1");
        }
        return result.Release();
    }

    void SendReplyAndDie(NBus::TBusMessage *reply, const TActorContext &ctx) {
        SendReplyMove(reply);
        return Die(ctx);
    }

    void Bootstrap(const TActorContext &ctx) {
        // handle error from constructor
        if (!!ErrorReason) {
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
        }

        NTabletPipe::TClientConfig clientConfig;
        if (WithRetry) {
            clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        }

        auto &domainsInfo = AppData(ctx)->DomainsInfo;
        if (!domainsInfo->Domain || domainsInfo->GetDomain()->DomainUid != DomainUid) {
            // Report details
            ErrorReason = Sprintf("Incorrect DomainUid# %" PRIu64
                " or kikimr domian configuration, Marker# HC9", (ui64)DomainUid);
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
        }
        ui64 hiveTabletId = HiveId.value_or(domainsInfo->GetHive());

        if (Status == NKikimrProto::OK) {
            PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, hiveTabletId, clientConfig));

            for (ui64 i = 0; i < Requests.size(); ++i) {
                const TRequest &cmd = Requests[i];
                switch (cmd.Event) {
                    case TEvHive::EvCreateTablet: {
                        THolder<TEvHive::TEvCreateTablet> x(new TEvHive::TEvCreateTablet(cmd.OwnerId,
                                                                                         cmd.OwnerIdx,
                                                                                         cmd.TabletType,
                                                                                         cmd.BindedChannels,
                                                                                         cmd.AllowedNodeIDs));

                        for (const auto& domainKey: cmd.AllowedDomains) {
                           *x->Record.AddAllowedDomains() = domainKey;
                        }

                        NTabletPipe::SendData(ctx, PipeClient, x.Release(), i);
                        break;
                    }
                    case TEvHive::EvLookupTablet: {
                        THolder<TEvHive::TEvLookupTablet> x(new TEvHive::TEvLookupTablet(cmd.OwnerId, cmd.OwnerIdx));
                        NTabletPipe::SendData(ctx, PipeClient, x.Release(), i);
                        break;
                    }
                    default:
                        break;
                }
            }

            TBase::Become(&TMessageBusHiveCreateTablet<ResponseType>::StateWaiting, ctx, Timeout, new TEvents::TEvWakeup());
        } else {
            if (ErrorReason.size() == 0) {
                ErrorReason = Sprintf("Unexpected Status, Marker# HC8");
            }
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
        }
    }

    STFUNC(StateWaiting) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHive::TEvCreateTabletReply, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

IActor* CreateMessageBusHiveCreateTablet(TBusMessageContext &msg) {
    if (msg.GetMessage()->GetHeader()->Type == MTYPE_CLIENT_OLD_HIVE_CREATE_TABLET) {
        return new TMessageBusHiveCreateTablet<TBusHiveCreateTabletResult>(msg);
    } else {
        return new TMessageBusHiveCreateTablet<TBusResponse>(msg);
    }
}

}
}
