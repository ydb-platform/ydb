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

    enum class EMode {
        TabletList,
        TabletGet,
        DiskList,
    };

    explicit TJsonProxyDDisk(NMon::TEvHttpInfo::TPtr& event)
        : RequestEvent(event)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& request = RequestEvent->Get()->Request;
        const auto& path = request.GetPathInfo();
        if (path == "/api/json/ddisk/tablets") {
            Mode = EMode::TabletList;
        } else if (path == "/api/json/ddisk/tablet") {
            Mode = EMode::TabletGet;
        } else if (path == "/api/json/ddisk/disks") {
            Mode = EMode::DiskList;
        } else {
            Reply("HTTP/1.1 404 Not Found\r\n\r\n", ctx);
            return;
        }

        auto pipeConfig = NTabletPipe::TClientConfig();
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        Pipe = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, MakeCmsID(), pipeConfig));

        const auto& params = request.GetParams();
        switch (Mode) {
            case EMode::TabletList: {
                auto event = MakeHolder<TEvCms::TEvDDiskTabletListRequest>();
                FillOffsetAndLimit(params, event->Record);
                if (params.contains("filter")) {
                    event->Record.SetFilterTabletId(params.Get("filter"));
                }
                if (params.contains("sort_by")) {
                    const auto& sortBy = params.Get("sort_by");
                    if (sortBy == "last_changed_at") {
                        event->Record.SetSortBy(NKikimrCms::DDISK_TABLET_SORT_BY_LAST_CHANGED_AT);
                    } else if (sortBy == "groups_count") {
                        event->Record.SetSortBy(NKikimrCms::DDISK_TABLET_SORT_BY_GROUPS_COUNT);
                    } else {
                        event->Record.SetSortBy(NKikimrCms::DDISK_TABLET_SORT_BY_TABLET_ID);
                    }
                }
                if (params.contains("sort_desc")) {
                    event->Record.SetSortDescending(IsTrue(params.Get("sort_desc")));
                }
                if (params.contains("only_problems")) {
                    event->Record.SetOnlyProblems(IsTrue(params.Get("only_problems")));
                }
                NTabletPipe::SendData(ctx, Pipe, event.Release());
                break;
            }
            case EMode::TabletGet: {
                if (!params.contains("tablet_id") || !TryFromString(params.Get("tablet_id"), TabletId)) {
                    Reply("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nfield 'tablet_id' is required", ctx);
                    return;
                }
                auto event = MakeHolder<TEvCms::TEvDDiskInfoGetRequest>();
                event->Record.SetTabletId(TabletId);
                NTabletPipe::SendData(ctx, Pipe, event.Release());
                break;
            }
            case EMode::DiskList: {
                auto event = MakeHolder<TEvCms::TEvDDiskDiskListRequest>();
                FillOffsetAndLimit(params, event->Record);
                if (params.contains("filter")) {
                    event->Record.SetFilterDiskId(params.Get("filter"));
                }
                if (params.contains("filter_tablet_id")) {
                    event->Record.SetFilterTabletId(params.Get("filter_tablet_id"));
                }
                if (params.contains("sort_by")) {
                    const auto& sortBy = params.Get("sort_by");
                    if (sortBy == "tablets_count") {
                        event->Record.SetSortBy(NKikimrCms::DDISK_DISK_SORT_BY_TABLETS_COUNT);
                    } else {
                        event->Record.SetSortBy(NKikimrCms::DDISK_DISK_SORT_BY_DISK_ID);
                    }
                }
                if (params.contains("sort_desc")) {
                    event->Record.SetSortDescending(IsTrue(params.Get("sort_desc")));
                }
                if (params.contains("only_problems")) {
                    event->Record.SetOnlyProblems(IsTrue(params.Get("only_problems")));
                }
                NTabletPipe::SendData(ctx, Pipe, event.Release());
                break;
            }
        }

        Become(&TThis::StateWork, ctx, TDuration::Seconds(30), new TEvents::TEvWakeup());
    }

private:
    template <typename TParams, typename TRecord>
    static void FillOffsetAndLimit(const TParams& params, TRecord& record) {
        ui32 offset = 0;
        ui32 limit = 50;
        if (params.contains("offset") && TryFromString(params.Get("offset"), offset)) {
            record.SetOffset(offset);
        }
        if (params.contains("limit") && TryFromString(params.Get("limit"), limit)) {
            record.SetLimit(limit);
        } else {
            record.SetLimit(limit);
        }
    }

    static bool IsTrue(const TString& value) {
        return value == "1" || value == "true" || value == "yes";
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvDDiskTabletListResponse, Handle);
            HFunc(TEvCms::TEvDDiskInfoGetResponse, Handle);
            HFunc(TEvCms::TEvDDiskDiskListResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    template <typename TRecord>
    void ReplyRecord(const TRecord& record, const TActorContext& ctx) {
        // NOTE: StringifyNumbers only affects singular (optional) fields; it does
        // NOT cover repeated fields (e.g. DDiskTabletIds/PersistentBufferTabletIds,
        // or the DirectBlockGroupId/TabletId repeated fields elsewhere). Those are
        // controlled separately by StringifyNumbersRepeated. Without it, large
        // uint64 tablet ids (which routinely exceed Number.MAX_SAFE_INTEGER) are
        // emitted as raw JSON numbers and get silently rounded by the browser's
        // JSON parser, making distinct tablet ids collapse into a handful of
        // duplicate values in the UI. Both options must be set to keep singular
        // and repeated fields consistently safe for JS clients.
        auto config = NProtobufJson::TProto2JsonConfig()
            .SetFormatOutput(false)
            .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName)
            .SetStringifyNumbers(NProtobufJson::TProto2JsonConfig::StringifyLongNumbersForDouble)
            .SetStringifyNumbersRepeated(NProtobufJson::TProto2JsonConfig::StringifyLongNumbersForDouble);
        Reply(TString(NMonitoring::HTTPOKJSON) + NProtobufJson::Proto2Json(record, config), ctx);
    }

    void Handle(TEvCms::TEvDDiskTabletListResponse::TPtr& ev, const TActorContext& ctx) {
        ReplyRecord(ev->Get()->Record, ctx);
    }

    void Handle(TEvCms::TEvDDiskInfoGetResponse::TPtr& ev, const TActorContext& ctx) {
        ReplyRecord(ev->Get()->Record, ctx);
    }

    void Handle(TEvCms::TEvDDiskDiskListResponse::TPtr& ev, const TActorContext& ctx) {
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
    EMode Mode = EMode::TabletList;
};

} // namespace NKikimr::NCms
