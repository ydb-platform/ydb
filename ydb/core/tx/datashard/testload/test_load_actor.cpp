#include "actors.h"
#include "test_load_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/public/lib/base/msgbus.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr::NDataShardLoad {

class TLoadActor : public TActorBootstrapped<TLoadActor> {
    // per-actor HTTP info
    struct TActorInfo {
        ui64 Tag; // load tag
        TString Data; // HTML response
    };

    struct TRunningActorInfo {
        TActorId ActorId;
        TActorId Parent; // if set we notify parent when actor finishes

        explicit TRunningActorInfo(const TActorId& actorId, const TActorId& parent = {})
            : ActorId(actorId)
            , Parent(parent)
        {
        }
    };

    // per-request info
    struct THttpInfoRequest {
        TActorId Origin; // who asked for status
        int SubRequestId; // origin subrequest id
        THashMap<TActorId, TActorInfo> ActorMap; // per-actor status
        ui32 HttpInfoResPending; // number of requests pending
        TString ErrorMessage;
    };

    struct TFinishedTestInfo {
        ui64 Tag;
        TString ErrorReason;
        TInstant FinishTime;
        std::optional<TEvDataShardLoad::TLoadReport> Report;
    };

    // info about finished actors
    TVector<TFinishedTestInfo> FinishedTests;

    // currently running load actors
    TMap<ui64, TRunningActorInfo> LoadActors;
    ui64 LastTag = 0; // tags start from 1

    // next HTTP request identifier
    ui32 NextRequestId;

    // HTTP info requests being currently executed
    THashMap<ui32, THttpInfoRequest> InfoRequests;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DS_LOAD_ACTOR;
    }

    TLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : NextRequestId(1)
        , Counters(counters)
    {}

    void Bootstrap(const TActorContext& /*ctx*/) {
        Become(&TLoadActor::StateFunc);
    }

    void Handle(TEvDataShardLoad::TEvTestLoadRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        ui32 status = NMsgBusProxy::MSTATUS_OK;
        TString error;
        ui64 tag = 0;
        try {
            tag = ProcessCmd(ev, ctx);
        } catch (const TLoadActorException& ex) {
            LOG_ERROR_S(ctx, NKikimrServices::DS_LOAD_TEST, "Exception while creating load actor, what# "
                    << ex.what());
            status = NMsgBusProxy::MSTATUS_ERROR;
            error = ex.what();
        }
        auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadResponse>();
        response->Record.SetStatus(status);
        if (error) {
            response->Record.SetErrorReason(error);
        }
        if (record.HasCookie()) {
            response->Record.SetCookie(record.GetCookie());
        }
        if (tag) {
            response->Record.SetTag(tag);
        }
        ctx.Send(ev->Sender, response.release());
    }

    ui64 GetTag() {
        auto tag = ++LastTag;

        // just sanity check
        if (LoadActors.contains(tag)) {
            ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
        }

        return tag;
    }

    ui64 ProcessCmd(TEvDataShardLoad::TEvTestLoadRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        switch (record.Command_case()) {
        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertBulkStart: {
            const auto& cmd = record.GetUpsertBulkStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new bulk upsert load actor with tag# " << tag);

            auto* actor = CreateUpsertBulkActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));
            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertLocalMkqlStart: {
            const auto& cmd = record.GetUpsertLocalMkqlStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new local mkql upsert load actor with tag# " << tag);

            auto* actor = CreateLocalMkqlUpsertActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));

            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertKqpStart: {
            const auto& cmd = record.GetUpsertKqpStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new kqp upsert load actor with tag# " << tag);

            auto* actor = CreateKqpUpsertActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));

            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertProposeStart: {
            const auto& cmd = record.GetUpsertProposeStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new upsert load actor with tag# " << tag);

            auto* actor = CreateProposeUpsertActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));

            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kReadIteratorStart: {
            const auto& cmd = record.GetReadIteratorStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new read iterator load actor# " << tag);

            auto* actor = CreateReadIteratorActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));

            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kLoadStop: {
            const auto& cmd = record.GetLoadStop();
            if (cmd.HasRemoveAllTags() && cmd.GetRemoveAllTags()) {
                LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Delete all running load actors");
                for (auto& actorPair : LoadActors) {
                    ctx.Send(actorPair.second.ActorId, new TEvents::TEvPoisonPill);
                }
                LoadActors.clear();
            } else {
                if (!cmd.HasTag()) {
                    ythrow TLoadActorException() << "Either RemoveAllTags or Tag must present";
                }
                const ui64 tag = cmd.GetTag();
                auto it = LoadActors.find(tag);
                if (it == LoadActors.end()) {
                    ythrow TLoadActorException()
                        << Sprintf("load actor with Tag# %" PRIu64 " not found", tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Delete running load actor# " << tag);
                ctx.Send(it->second.ActorId, new TEvents::TEvPoisonPill);
                LoadActors.erase(it);
            }

            return 0;
        }

        default: {
            TString protoTxt;
            google::protobuf::TextFormat::PrintToString(record, &protoTxt);
            ythrow TLoadActorException() << (TStringBuilder()
                    << "TLoadActor::Handle(TEvDataShardLoad::TEvTestLoadRequest): unexpected command case: "
                    << ui32(record.Command_case())
                    << " protoTxt# " << protoTxt.Quote());
        }
        }
    }

    void Handle(TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get();
        auto it = LoadActors.find(msg->Tag);
        Y_VERIFY(it != LoadActors.end(), "%s", (TStringBuilder() << "failed to find actor with tag# " << msg->Tag
            << ", TEvTestLoadFinished from actor# " << ev->Sender).c_str());
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load actor# " << ev->Sender
            << " with tag# " << msg->Tag << " finished");

        if (it->second.Parent) {
            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>();
            response->Tag = ev->Get()->Tag;
            response->ErrorReason = ev->Get()->ErrorReason;
            response->Report = ev->Get()->Report;
            ctx.Send(it->second.Parent, response.release());
        }

        LoadActors.erase(it);

        FinishedTests.push_back({msg->Tag, msg->ErrorReason, TAppData::TimeProvider->Now(), msg->Report});

        auto infoIt = InfoRequests.begin();
        while (infoIt != InfoRequests.end()) {
            auto next = std::next(infoIt);

            THttpInfoRequest& info = infoIt->second;
            auto actorIt = info.ActorMap.find(ev->Sender);
            if (actorIt != info.ActorMap.end()) {
                const bool empty = !actorIt->second.Data;
                info.ActorMap.erase(actorIt);
                if (empty && !--info.HttpInfoResPending) {
                    GenerateHttpInfoRes(ctx, infoIt->first);
                }
            }

            infoIt = next;
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        // calculate ID of this request
        ui32 id = NextRequestId++;

        // get reference to request information
        THttpInfoRequest& info = InfoRequests[id];

        // fill in sender parameters
        info.Origin = ev->Sender;
        info.SubRequestId = ev->Get()->SubRequestId;

        info.ErrorMessage.clear();

        // send messages to subactors
        for (const auto& kv : LoadActors) {
            ctx.Send(kv.second.ActorId, new NMon::TEvHttpInfo(ev->Get()->Request, id));
            info.ActorMap[kv.second.ActorId].Tag = kv.first;
        }

        // record number of responses pending
        info.HttpInfoResPending = LoadActors.size();

        if (!info.HttpInfoResPending) {
            GenerateHttpInfoRes(ctx, id);
        }
    }

    void Handle(NMon::TEvHttpInfoRes::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get();
        ui32 id = static_cast<NMon::TEvHttpInfoRes *>(msg)->SubRequestId;

        auto it = InfoRequests.find(id);
        Y_VERIFY(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

        auto actorIt = info.ActorMap.find(ev->Sender);
        Y_VERIFY(actorIt != info.ActorMap.end());
        TActorInfo& perActorInfo = actorIt->second;

        TStringStream stream;
        msg->Output(stream);
        Y_VERIFY(!perActorInfo.Data);
        perActorInfo.Data = stream.Str();

        if (!--info.HttpInfoResPending) {
            GenerateHttpInfoRes(ctx, id);
        }
    }

    void GenerateHttpInfoRes(const TActorContext& ctx, ui32 id, bool nodata = false) {
        auto it = InfoRequests.find(id);
        Y_VERIFY(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

#define PROFILE(NAME) \
                        str << "<option value=\"" << ui32(NKikimrDataShardLoad::TEvTestLoadRequest::NAME) << "\">" << #NAME << "</option>";

#define PUT_HANDLE_CLASS(NAME) \
                        str << "<option value=\"" << ui32(NKikimrTxDataShard::NAME) << "\">" << #NAME << "</option>";

        TStringStream str;
        HTML(str) {
            if (info.ErrorMessage) {
                DIV() {
                    str << "<h1>" << info.ErrorMessage << "</h1>";
                }
            }

            if (!nodata) {
                for (const auto& pair : info.ActorMap) {
                    const TActorInfo& perActorInfo = pair.second;
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Tag# " << perActorInfo.Tag;
                        }
                        DIV_CLASS("panel-body") {
                            str << perActorInfo.Data;
                        }
                    }
                }

                COLLAPSED_BUTTON_CONTENT("finished_tests_info", "Finished tests") {
                    for (const auto& req : FinishedTests) {
                        DIV_CLASS("panel panel-info") {
                            DIV_CLASS("panel-heading") {
                                str << "Tag# " << req.Tag;
                            }
                            DIV_CLASS("panel-body") {
                                str << "<p>";
                                if (req.Report)
                                    str << "Report# " << req.Report->ToString() << "<br/>";
                                str << "Finish reason# " << req.ErrorReason << "<br/>";
                                str << "Finish time# " << req.FinishTime << "<br/>";
                                str << "</p>";
                            }
                        }
                    }
                }
            }
        }

        ctx.Send(info.Origin, new NMon::TEvHttpInfoRes(str.Str(), info.SubRequestId));

        InfoRequests.erase(it);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvDataShardLoad::TEvTestLoadRequest, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(NMon::TEvHttpInfoRes, Handle)
    )
};

NActors::IActor *CreateTestLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TLoadActor(counters);
}

} // NKikimr::NDataShardLoad