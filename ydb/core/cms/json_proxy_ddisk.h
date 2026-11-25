#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/cms.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/protobuf/json/proto2json.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <vector>

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

        const auto& params = request.GetParams();
        ui32 timeout = 30'000;
        if (params.contains("timeout")) {
            if (!TryFromString(params.Get("timeout"), timeout) || timeout == 0) {
                ReplyBadParameter("timeout", ctx);
                return;
            }
        }
        if (Mode != EMode::TabletGet && params.contains("fields_required")) {
            const auto fields = params.Get("fields_required");
            if (fields && fields != "all") {
                StringSplitter(fields).Split(',').SkipEmpty().Collect(&FieldsRequired);
                if (FieldsRequired.empty()) {
                    ReplyBadParameter("fields_required", ctx);
                    return;
                }
            }
        }

        auto pipeConfig = NTabletPipe::TClientConfig();
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        Pipe = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, MakeCmsID(), pipeConfig));

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
                    } else if (sortBy == "degrade") {
                        event->Record.SetSortBy(NKikimrCms::DDISK_TABLET_SORT_BY_DEGRADE);
                    } else if (sortBy == "disk_usage") {
                        event->Record.SetSortBy(NKikimrCms::DDISK_TABLET_SORT_BY_DISK_USAGE);
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
                if (params.contains("group_by")) {
                    const auto& groupBy = params.Get("group_by");
                    if (groupBy == "degrade") {
                        event->Record.SetGroupBy(NKikimrCms::DDISK_TABLET_GROUP_BY_DEGRADE);
                    } else if (groupBy == "disk_usage") {
                        event->Record.SetGroupBy(NKikimrCms::DDISK_TABLET_GROUP_BY_DISK_USAGE);
                    }
                }
                if (params.contains("filter_group")) {
                    event->Record.SetFilterGroup(params.Get("filter_group"));
                }
                if (params.contains("group_by_degrade")) {
                    event->Record.SetGroupByDegrade(IsTrue(params.Get("group_by_degrade")));
                }
                event->Record.SetGroupsOnly(IsTrue(params.Get("groups_only")));
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
                event->Record.SetIncludeHealth(IsTrue(params.Get("include_health")));
                NTabletPipe::SendData(ctx, Pipe, event.Release());
                break;
            }
            case EMode::DiskList: {
                auto event = MakeHolder<TEvCms::TEvDDiskDiskListRequest>();
                FillOffsetAndLimit(params, event->Record);
                if (params.contains("include_tablet_ids")) {
                    event->Record.SetIncludeTabletIds(IsTrue(params.Get("include_tablet_ids")));
                }
                if (params.contains("filter")) {
                    event->Record.SetFilterDiskId(params.Get("filter"));
                }
                if (params.contains("filter_tablet_id")) {
                    event->Record.SetFilterTabletId(params.Get("filter_tablet_id"));
                }
                if (params.contains("node_id")) {
                    ui32 nodeId;
                    if (!TryFromString(params.Get("node_id"), nodeId)) {
                        ReplyBadParameter("node_id", ctx);
                        return;
                    }
                    event->Record.SetNodeId(nodeId);
                }
                if (params.contains("pdisk_id")) {
                    ui32 pdiskId;
                    if (!TryFromString(params.Get("pdisk_id"), pdiskId)) {
                        ReplyBadParameter("pdisk_id", ctx);
                        return;
                    }
                    event->Record.SetPDiskId(pdiskId);
                }
                if (params.contains("tablet_id")) {
                    ui64 tabletId;
                    if (!TryFromString(params.Get("tablet_id"), tabletId)) {
                        ReplyBadParameter("tablet_id", ctx);
                        return;
                    }
                    event->Record.SetTabletId(tabletId);
                }
                event->Record.SetInventory(IsTrue(params.Get("inventory")));
                event->Record.SetGroupsOnly(IsTrue(params.Get("groups_only")));
                if (params.Get("group_by") == "state") {
                    event->Record.SetGroupBy(NKikimrCms::DDISK_DISK_GROUP_BY_STATE);
                }
                if (params.contains("filter_group")) {
                    event->Record.SetFilterGroup(params.Get("filter_group"));
                }
                if (params.contains("sort_by")) {
                    const auto& sortBy = params.Get("sort_by");
                    if (sortBy == "tablets_count") {
                        event->Record.SetSortBy(NKikimrCms::DDISK_DISK_SORT_BY_TABLETS_COUNT);
                    } else if (sortBy == "ddisk_occupancy") {
                        event->Record.SetSortBy(NKikimrCms::DDISK_DISK_SORT_BY_DDISK_OCCUPANCY);
                    } else if (sortBy == "persistent_buffer_occupancy") {
                        event->Record.SetSortBy(NKikimrCms::DDISK_DISK_SORT_BY_PERSISTENT_BUFFER_OCCUPANCY);
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

        Become(&TThis::StateWork, ctx, TDuration::MilliSeconds(Min(timeout, 30'000u)), new TEvents::TEvWakeup());
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

    void ReplyBadParameter(TStringBuf name, const TActorContext& ctx) {
        TString reply = "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nInvalid field '";
        reply.append(name);
        reply.append("'");
        Reply(reply, ctx);
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

    template <typename TRecord, typename TRow>
    void SelectListFields(TRecord& record, NProtoBuf::RepeatedPtrField<TRow>& rows, TStringBuf identity) {
        if (FieldsRequired.empty()) {
            return;
        }
        const auto* descriptor = TRow::descriptor();
        std::vector<const NProtoBuf::FieldDescriptor*> omitted;
        for (int index = 0; index < descriptor->field_count(); ++index) {
            const auto* field = descriptor->field(index);
            const TStringBuf name(field->name().data(), field->name().size());
            if (name == identity || FieldsRequired.contains(TString(name))) {
                record.AddFieldsAvailable(TString(name));
            } else {
                omitted.push_back(field);
            }
        }
        for (auto& row : rows) {
            for (const auto* field : omitted) {
                row.GetReflection()->ClearField(&row, field);
            }
        }
    }

    void Handle(TEvCms::TEvDDiskTabletListResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        SelectListFields(record, *record.MutableTablets(), "TabletId");
        ReplyRecord(record, ctx);
    }

    void Handle(TEvCms::TEvDDiskInfoGetResponse::TPtr& ev, const TActorContext& ctx) {
        ReplyRecord(ev->Get()->Record, ctx);
    }

    void Handle(TEvCms::TEvDDiskDiskListResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        SelectListFields(record, *record.MutableDisks(), "DiskId");
        ReplyRecord(record, ctx);
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
    THashSet<TString> FieldsRequired;
};

} // namespace NKikimr::NCms
