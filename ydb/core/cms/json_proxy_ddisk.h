#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/cms.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/protobuf/json/proto2json.h>
#include <util/string/cast.h>

namespace NKikimr::NCms {

class TJsonProxyDDisk : public TActorBootstrapped<TJsonProxyDDisk> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SERVICE_PROXY;
    }

    explicit TJsonProxyDDisk(NMon::TEvHttpInfo::TPtr& event)
        : RequestEvent(event)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& request = RequestEvent->Get()->Request;
        IsList = request.GetPathInfo() == "/api/json/ddisk/tablets";
        if (!IsList && request.GetPathInfo() != "/api/json/ddisk/tablet") {
            Reply("HTTP/1.1 404 Not Found\r\n\r\n", ctx);
            return;
        }

        auto pipeConfig = NTabletPipe::TClientConfig();
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        Pipe = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, MakeCmsID(), pipeConfig));

        if (IsList) {
            NTabletPipe::SendData(ctx, Pipe, new TEvCms::TEvDDiskInfoListRequest());
        } else {
            const auto& params = request.GetParams();
            if (!params.contains("tablet_id") || !TryFromString(params.Get("tablet_id"), TabletId)) {
                Reply("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nfield 'tablet_id' is required", ctx);
                return;
            }
            auto event = MakeHolder<TEvCms::TEvDDiskInfoGetRequest>();
            event->Record.SetTabletId(TabletId);
            NTabletPipe::SendData(ctx, Pipe, event.Release());
        }

        Become(&TThis::StateWork, ctx, TDuration::Seconds(30), new TEvents::TEvWakeup());
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvDDiskInfoListResponse, Handle);
            HFunc(TEvCms::TEvDDiskInfoGetResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    template <typename TRecord>
    void ReplyRecord(const TRecord& record, const TActorContext& ctx) {
        auto config = NProtobufJson::TProto2JsonConfig()
            .SetFormatOutput(false)
            .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName)
            .SetStringifyNumbers(NProtobufJson::TProto2JsonConfig::StringifyLongNumbersForDouble);
        Reply(TString(NMonitoring::HTTPOKJSON) + NProtobufJson::Proto2Json(record, config), ctx);
    }

    void Handle(TEvCms::TEvDDiskInfoListResponse::TPtr& ev, const TActorContext& ctx) {
        ReplyRecord(ev->Get()->Record, ctx);
    }

    void Handle(TEvCms::TEvDDiskInfoGetResponse::TPtr& ev, const TActorContext& ctx) {
        ReplyRecord(ev->Get()->Record, ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->ClientId == Pipe && ev->Get()->Status != NKikimrProto::OK) {
            Reply("HTTP/1.1 503 Service Unavailable\r\n\r\nCMS is unavailable", ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->ClientId == Pipe) {
            Reply("HTTP/1.1 503 Service Unavailable\r\n\r\nCMS connection was closed", ctx);
        }
    }

    void Timeout(const TActorContext& ctx) {
        Reply("HTTP/1.1 408 Request Timeout\r\n\r\nCMS request timeout", ctx);
    }

    void Reply(const TString& data, const TActorContext& ctx) {
        ctx.Send(RequestEvent->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        NTabletPipe::CloseClient(ctx, Pipe);
        PassAway();
    }

    NMon::TEvHttpInfo::TPtr RequestEvent;
    TActorId Pipe;
    ui64 TabletId = 0;
    bool IsList = false;
};

} // namespace NKikimr::NCms
