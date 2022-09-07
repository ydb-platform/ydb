#include "actors.h"
#include "test_load_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/public/lib/base/msgbus.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr::NDataShard {

class TLoadActor : public TActorBootstrapped<TLoadActor> {
    // per-actor HTTP info
    struct TActorInfo {
        ui64 Tag; // load tag
        TString Data; // HTML response
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
        std::optional<TLoadReport> Report;
    };

    // info about finished actors
    TVector<TFinishedTestInfo> FinishedTests;

    // currently running load actors
    TMap<ui64, TActorId> LoadActors;

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

    void Handle(TEvDataShard::TEvTestLoadRequest::TPtr& ev, const TActorContext& ctx) {
        ui32 status = NMsgBusProxy::MSTATUS_OK;
        TString error;
        const auto& record = ev->Get()->Record;
        try {
            ProcessCmd(record, ctx);
        } catch (const TLoadActorException& ex) {
            LOG_ERROR_S(ctx, NKikimrServices::DS_LOAD_TEST, "Exception while creating load actor, what# "
                    << ex.what());
            status = NMsgBusProxy::MSTATUS_ERROR;
            error = ex.what();
        }
        auto response = std::make_unique<TEvDataShard::TEvTestLoadResponse>();
        response->Record.SetStatus(status);
        if (error) {
            response->Record.SetErrorReason(error);
        }
        if (record.HasCookie()) {
            response->Record.SetCookie(record.GetCookie());
        }
        ctx.Send(ev->Sender, response.release());
    }

    template<typename T>
    ui64 GetOrGenerateTag(const T& cmd) {
        if (cmd.HasTag()) {
            return cmd.GetTag();
        } else {
            if (LoadActors.empty()) {
                return 1;
            } else {
                return LoadActors.rbegin()->first + 1;
            }
        }
    }

    void ProcessCmd(const NKikimrTxDataShard::TEvTestLoadRequest& record, const TActorContext& ctx) {
        switch (record.Command_case()) {
            case NKikimrTxDataShard::TEvTestLoadRequest::CommandCase::kBulkUpsertStart: {
                const auto& cmd = record.GetBulkUpsertStart();
                const ui64 tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new bulk upsert load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreateBulkUpsertActor(cmd, ctx.SelfID,
                                GetServiceCounters(Counters, "load_actor"), tag)));
                break;
            }

            case NKikimrTxDataShard::TEvTestLoadRequest::CommandCase::kUpsertMkqlStart: {
                const auto& cmd = record.GetUpsertMkqlStart();
                const ui64 tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new upsert load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreateUpsertActor(cmd, ctx.SelfID,
                                GetServiceCounters(Counters, "load_actor"), tag)));
                break;
            }

            case NKikimrTxDataShard::TEvTestLoadRequest::CommandCase::kLoadStop: {
                const auto& cmd = record.GetLoadStop();
                if (cmd.HasRemoveAllTags() && cmd.GetRemoveAllTags()) {
                    LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Delete all running load actors");
                    for (auto& actorPair : LoadActors) {
                        ctx.Send(actorPair.second, new TEvents::TEvPoisonPill);
                    }
                    LoadActors.clear();
                } else {
                    VERIFY_PARAM(Tag);
                    const ui64 tag = cmd.GetTag();
                    auto it = LoadActors.find(tag);
                    if (it == LoadActors.end()) {
                        ythrow TLoadActorException()
                            << Sprintf("load actor with Tag# %" PRIu64 " not found", tag);
                    }
                    LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Delete running load actor with tag# "
                            << tag);
                    ctx.Send(it->second, new TEvents::TEvPoisonPill);
                    LoadActors.erase(it);
                }
                break;
            }

            default: {
                TString protoTxt;
                google::protobuf::TextFormat::PrintToString(record, &protoTxt);
                ythrow TLoadActorException() << (TStringBuilder()
                        << "TLoadActor::Handle(TEvDataShard::TEvTestLoadRequest): unexpected command case: "
                        << ui32(record.Command_case())
                        << " protoTxt# " << protoTxt.Quote());
            }
        }
    }

    void Handle(TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get();
        auto it = LoadActors.find(msg->Tag);
        Y_VERIFY(it != LoadActors.end());
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load actor with tag# " << msg->Tag << " finished");
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

        const auto& params = ev->Get()->Request.GetParams();
        if (params.Has("protobuf")) {
            NKikimrTxDataShard::TEvTestLoadRequest record;
            bool status = google::protobuf::TextFormat::ParseFromString(params.Get("protobuf"), &record);
            if (status) {
                try {
                    ProcessCmd(record, ctx);
                } catch (const TLoadActorException& ex) {
                    info.ErrorMessage = ex.what();
                }
            } else {
                info.ErrorMessage = "bad protobuf";
            }

            GenerateHttpInfoRes(ctx, id, true);
            return;
        }

        // send messages to subactors
        for (const auto& kv : LoadActors) {
            ctx.Send(kv.second, new NMon::TEvHttpInfo(ev->Get()->Request, id));
            info.ActorMap[kv.second].Tag = kv.first;
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
                        str << "<option value=\"" << ui32(NKikimrTxDataShard::TEvTestLoadRequest::NAME) << "\">" << #NAME << "</option>";

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
        HFunc(TEvDataShard::TEvTestLoadRequest, Handle)
        HFunc(TEvTestLoadFinished, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(NMon::TEvHttpInfoRes, Handle)
    )
};

NActors::IActor *CreateTestLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TLoadActor(counters);
}

} // NKikimr::NDataShard