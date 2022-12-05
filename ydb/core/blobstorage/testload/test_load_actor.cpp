#include "test_load_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

namespace NKqpConstants {
    const TString DEFAULT_PROTO = R"_(
KqpLoadStart: {
    DurationSeconds: 30
    WindowDuration: 1
    WorkingDir: "/slice/db"
    NumOfSessions: 64
    UniformPartitionsCount: 1000
    DeleteTableOnFinish: 1
    WorkloadType: 0
    Kv: {
        InitRowCount: 1000
        PartitionsByLoad: true
        MaxFirstKey: 18446744073709551615
        StringLen: 8
        ColumnsCnt: 2
        RowsCnt: 1
    }
})_";

}

using namespace NActors;

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
        TMap<TActorId, TActorInfo> ActorMap; // per-actor status
        ui32 HttpInfoResPending; // number of requests pending
        TString ErrorMessage;
        TString Mode; // mode of page content
    };

    struct TFinishedTestInfo {
        ui64 Tag;
        TString ErrorReason;
        TInstant FinishTime;
        TString LastHtmlPage;
        NJson::TJsonValue JsonResult;
    };

    // info about finished actors
    TVector<TFinishedTestInfo> FinishedTests;

    // currently running load actors
    TMap<ui64, TActorId> LoadActors;

    // next HTTP request identifier
    ui32 NextRequestId;

    // HTTP info requests being currently executed
    THashMap<ui32, THttpInfoRequest> InfoRequests;

    // issure tags in ascending order
    ui64 NextTag = 1;

    // queue for all-nodes load
    TVector<NKikimrBlobStorage::TEvTestLoadRequest> AllNodesLoadConfigs;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_ACTOR;
    }

    TLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : NextRequestId(1)
        , Counters(counters)
    {}

    void Bootstrap(const TActorContext& /*ctx*/) {
        Become(&TLoadActor::StateFunc);
    }

    void Handle(TEvBlobStorage::TEvTestLoadRequest::TPtr& ev, const TActorContext& ctx) {
        ui32 status = NMsgBusProxy::MSTATUS_OK;
        TString error;
        ui64 tag = 0;
        const auto& record = ev->Get()->Record;
        try {
            tag = ProcessCmd(record, ctx);
        } catch (const TLoadActorException& ex) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_LOAD_TEST, "Exception while creating load actor, what# "
                    << ex.what());
            status = NMsgBusProxy::MSTATUS_ERROR;
            error = ex.what();
        }
        auto response = std::make_unique<TEvBlobStorage::TEvTestLoadResponse>();
        response->Record.SetStatus(status);
        if (error) {
            response->Record.SetErrorReason(error);
        }
        if (record.HasCookie()) {
            response->Record.SetCookie(record.GetCookie());
        }
        response->Record.SetTag(tag);
        ctx.Send(ev->Sender, response.release());
    }

    template<typename T>
    ui64 GetOrGenerateTag(const T& cmd) {
        if (cmd.HasTag()) {
            return cmd.GetTag();
        } else {
            return NextTag++;
        }
    }

    ui64 ProcessCmd(const NKikimrBlobStorage::TEvTestLoadRequest& record, const TActorContext& ctx) {
        ui64 tag = 0;
        switch (record.Command_case()) {
            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kLoadStart: {
                const auto& cmd = record.GetLoadStart();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreateWriterTestLoad(cmd, ctx.SelfID,
                                GetServiceCounters(Counters, "load_actor"), tag)));
                break;
            }

            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kLoadStop: {
                const auto& cmd = record.GetLoadStop();
                if (cmd.HasRemoveAllTags() && cmd.GetRemoveAllTags()) {
                    LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Delete all running load actors");
                    for (auto& actorPair : LoadActors) {
                        ctx.Send(actorPair.second, new TEvents::TEvPoisonPill);
                    }
                } else {
                    VERIFY_PARAM(Tag);
                    tag = cmd.GetTag();
                    auto iter = LoadActors.find(tag);
                    if (iter == LoadActors.end()) {
                        ythrow TLoadActorException()
                            << Sprintf("load actor with Tag# %" PRIu64 " not found", tag);
                    }
                    LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Delete running load actor with tag# "
                            << tag);
                    ctx.Send(iter->second, new TEvents::TEvPoisonPill);
                }
                break;
            }

            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kPDiskLoadStart: {
                const auto& cmd = record.GetPDiskLoadStart();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreatePDiskWriterTestLoad(
                                cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kPDiskReadLoadStart: {
                const auto& cmd = record.GetPDiskReadLoadStart();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreatePDiskReaderTestLoad(
                                cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kPDiskLogLoadStart: {
                const auto& cmd = record.GetPDiskLogLoadStart();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreatePDiskLogWriterTestLoad(
                                cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kVDiskLoadStart: {
                const auto& cmd = record.GetVDiskLoadStart();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreateVDiskWriterTestLoad(cmd, ctx.SelfID, tag)));
                break;
            }

            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kKeyValueLoadStart: {
                const auto& cmd = record.GetKeyValueLoadStart();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }

                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreateKeyValueWriterTestLoad(
                                cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kKqpLoadStart: {
                const auto& cmd = record.GetKqpLoadStart();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }

                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Create new Kqp load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreateKqpWriterTestLoad(
                            cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimrBlobStorage::TEvTestLoadRequest::CommandCase::kMemoryLoadStart: {
                const auto& cmd = record.GetMemoryLoadStart();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }

                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Create new memory load actor with tag# " << tag);
                LoadActors.emplace(tag, ctx.Register(CreateMemoryTestLoad(
                            cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            default: {
                TString protoTxt;
                google::protobuf::TextFormat::PrintToString(record, &protoTxt);
                ythrow TLoadActorException() << (TStringBuilder()
                        << "TLoadActor::Handle(TEvBlobStorage::TEvTestLoadRequest): unexpected command case: "
                        << ui32(record.Command_case())
                        << " protoTxt# " << protoTxt.Quote());
            }
        }
        return tag;
    }

    void Handle(TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get();
        auto iter = LoadActors.find(msg->Tag);
        Y_VERIFY(iter != LoadActors.end());
        LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Load actor with tag# " << msg->Tag << " finished");
        LoadActors.erase(iter);
        FinishedTests.push_back({msg->Tag, msg->ErrorReason, TAppData::TimeProvider->Now(), msg->LastHtmlPage,
            msg->JsonResult});
        {
            auto& val = FinishedTests.back().JsonResult;
            val["tag"] = msg->Tag;


            auto& build = val["build"];
            build["date"] = GetProgramBuildDate();
            build["timestamp"] = GetProgramBuildTimestamp();
            build["branch"] = GetBranch();
            build["last_author"] = GetArcadiaLastAuthor();
            build["build_user"] = GetProgramBuildUser();
            build["change_num"] = GetArcadiaLastChangeNum();
        }

        auto it = InfoRequests.begin();
        while (it != InfoRequests.end()) {
            auto next = std::next(it);

            THttpInfoRequest& info = it->second;
            auto actorIt = info.ActorMap.find(ev->Sender);
            if (actorIt != info.ActorMap.end()) {
                const bool empty = !actorIt->second.Data;
                info.ActorMap.erase(actorIt);
                if (empty && !--info.HttpInfoResPending) {
                    GenerateHttpInfoRes(ctx, "results", it->first);
                }
            }

            it = next;
        }
    }

    void RunRecordOnAllNodes(const auto& record, const TActorContext& ctx) {
        AllNodesLoadConfigs.push_back(record);
        const TActorId nameserviceId = GetNameserviceActorId();
        bool sendStatus = ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "send status: " << sendStatus);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::BS_LOAD_TEST, "Handle HttpInfo request");
        // calculate ID of this request
        ui32 id = NextRequestId++;

        // get reference to request information
        THttpInfoRequest& info = InfoRequests[id];

        const auto& params = ev->Get()->Request.GetParams();
        TString mode = params.Has("mode") ? params.Get("mode") : "start_load";

        // fill in sender parameters
        info.Origin = ev->Sender;
        info.SubRequestId = ev->Get()->SubRequestId;
        info.Mode = mode;
        info.ErrorMessage.clear();

        if (params.Has("protobuf")) {
            NKikimrBlobStorage::TEvTestLoadRequest record;
            bool status = google::protobuf::TextFormat::ParseFromString(params.Get("protobuf"), &record);
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST,
                "received protobuf: " << params.Get("protobuf") << " | "
                "proto parse status: " << std::to_string(status)
            );
            ui64 tag = 0;
            if (status) {
                if (params.Has("run_all") && params.Get("run_all") == "on") {
                    LOG_NOTICE_S(ctx, NKikimrServices::BS_LOAD_TEST, "running on all nodes");
                    RunRecordOnAllNodes(record, ctx);
                    tag = NextTag; // may be datarace here
                } else {
                    try {
                        LOG_NOTICE_S(ctx, NKikimrServices::BS_LOAD_TEST, "running on node: " << SelfId().NodeId());
                        tag = ProcessCmd(record, ctx);
                    } catch (const TLoadActorException& ex) {
                        info.ErrorMessage = ex.what();
                    }
                }
            } else {
                info.ErrorMessage = "bad protobuf";
            }

            GenerateJsonTagInfoRes(ctx, id, tag);
            return;
        } else if (params.Has("stop_request")) {
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "received stop request");
            NKikimrBlobStorage::TEvTestLoadRequest record;
            record.MutableLoadStop()->SetRemoveAllTags(true);
            if (params.Has("stop_all_nodes") && params.Get("stop_all_nodes") == "on") {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "stop load on all nodes");
                RunRecordOnAllNodes(record, ctx);
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "stop load on node: " << SelfId().NodeId());
                ProcessCmd(record, ctx);
            }
            GenerateJsonTagInfoRes(ctx, id, 0);
            return;
        } else if (mode == "results_json") {
            GenerateJsonInfoRes(ctx, id);
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
            GenerateHttpInfoRes(ctx, mode, id);
        }
    }

    void Handle(const TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        TAppData* appDataPtr = AppData();

        TVector<ui32> dyn_node_ids;
        for (const auto& nodeInfo : ev->Get()->Nodes) {
            if (nodeInfo.NodeId >= appDataPtr->DynamicNameserviceConfig->MaxStaticNodeId) {
                dyn_node_ids.push_back(nodeInfo.NodeId);
            }
        }

        for (const auto& cmd : AllNodesLoadConfigs) {
            for (const auto& id : dyn_node_ids) {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "sending load request to: " << id);
                auto msg = MakeHolder<TEvBlobStorage::TEvTestLoadRequest>();
                msg->Record = cmd;
                msg->Record.SetCookie(id);
                ctx.Send(MakeBlobStorageLoadID(id), msg.Release());
            }
        }

        AllNodesLoadConfigs.clear();
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
            GenerateHttpInfoRes(ctx, info.Mode, id);
        }
    }

    void GenerateJsonTagInfoRes(const TActorContext& ctx, ui32 id, ui64 tag) {
        auto it = InfoRequests.find(id);
        Y_VERIFY(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

        TStringStream str;
        str << NMonitoring::HTTPOKJSON;
        NJson::TJsonValue value;
        value["tag"] = tag;
        value["status"] = "OK";
        NJson::WriteJson(&str, &value);

        auto result = std::make_unique<NMon::TEvHttpInfoRes>(str.Str(), info.SubRequestId,
                NMon::IEvHttpInfoRes::EContentType::Custom);
        ctx.Send(info.Origin, result.release());

        InfoRequests.erase(it);
    }

    void GenerateJsonInfoRes(const TActorContext& ctx, ui32 id) {
        auto it = InfoRequests.find(id);
        Y_VERIFY(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

        NJson::TJsonArray array;

        for (auto it = FinishedTests.rbegin(); it != FinishedTests.rend(); ++it) {
            array.AppendValue(it->JsonResult);
        }

        TStringStream str;
        str << NMonitoring::HTTPOKJSON;
        NJson::WriteJson(&str, &array);

        auto result = std::make_unique<NMon::TEvHttpInfoRes>(str.Str(), info.SubRequestId,
                NMon::IEvHttpInfoRes::EContentType::Custom);
        ctx.Send(info.Origin, result.release());

        InfoRequests.erase(it);
    }

    void GenerateHttpInfoRes(const TActorContext& ctx, const TString& mode, ui32 id) {
        auto it = InfoRequests.find(id);
        Y_VERIFY(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

        TStringStream str;
        HTML(str) {
            if (info.ErrorMessage) {
                DIV() {
                    str << "<h1>" << info.ErrorMessage << "</h1>";
                }
            }
            auto printTabs = [&](TString link, TString name) {
                TString type = link == mode ? "btn-info" : "btn-default";
                str << "<a href='?mode=" << link << "' class='btn " << type << "'>" << name << "</a>\n";
            };

            printTabs("start_load", "Start load");
            printTabs("stop_load", "Stop load");
            printTabs("results", "Results");
            str << "<br>";

            str << "<div>";
            if (mode == "start_load") {
                str << R"___(
                    <script>
                        function sendStartRequest(button) {
                            $.ajax({
                                url: "",
                                data: {
                                    protobuf: $('#protobuf').val(),
                                    run_all: $('#runAllNodes:checked').val()
                                },
                                method: "GET",
                                dataType: "html",
                                success: function(result) {
                                    $(button).prop('disabled', true);
                                    $(button).text("started");
                                    $("#protobuf").text(result);
                                }
                            });
                        }
                    </script>
                )___";
                str << R"(<textarea id="protobuf" name="protobuf" rows="20" cols="50">)";
                str << NKqpConstants::DEFAULT_PROTO;
                str << "</textarea><br>";
                str << R"(<input type="checkbox" id="runAllNodes" name="runAll"> )";
                str << R"(<label for="runAllNodes"> Run on all nodes</label>)" << "<br><br>";
                str << R"(<button onClick='sendStartRequest(this)' name='startNewLoad' class='btn btn-default'>Start new load</button>)" << "<br><br>";
            } else if (mode == "stop_load") {
                str << R"___(
                    <script>
                        function sendStopRequest(button) {
                            $.ajax({
                                url: "",
                                data: {
                                    stop_request: true,
                                    stop_all_nodes: $('#stopAllNodes:checked').val()
                                },
                                method: "GET",
                                dataType: "html",
                                success: function(result) {
                                    $(button).prop('disabled', true);
                                    $(button).text("stopped");
                                }
                            });
                        }
                    </script>
                )___";
                str << R"(<input type="checkbox" id="stopAllNodes" name="stopAll"> )";
                str << R"(<label for="stopAllNodes"> Stop load on all nodes</label>)" << "<br><br>";
                str << R"(<button onClick='sendStopRequest(this)' name='stopNewLoad' class='btn btn-default'>Stop load</button>)" << "<br><br>";
            } else if (mode == "results") {
                for (auto it = info.ActorMap.rbegin(); it != info.ActorMap.rend(); ++it) {
                    const TActorInfo& perActorInfo = it->second;
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Tag# " << perActorInfo.Tag;
                        }
                        DIV_CLASS("panel-body") {
                            str << perActorInfo.Data;
                        }
                    }
                }

                for (auto it = FinishedTests.rbegin(); it != FinishedTests.rend(); ++it) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Tag# " << it->Tag;
                        }
                        DIV_CLASS("panel-body") {
                            str << "Finish reason# " << it->ErrorReason << "<br/>";
                            str << "Finish time# " << it->FinishTime << "<br/>";
                            str << it->LastHtmlPage;
                        }
                    }
                }
            }
            str << "</div>";
        }

        ctx.Send(info.Origin, new NMon::TEvHttpInfoRes(str.Str(), info.SubRequestId));
        // ctx.Send(info.Origin, new NMon::TEvHttpInfoRes(str.Str(), info.SubRequestId, NMon::IEvHttpInfoRes::EContentType::Custom));

        InfoRequests.erase(it);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvTestLoadRequest, Handle)
        HFunc(TEvTestLoadFinished, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(NMon::TEvHttpInfoRes, Handle)
        HFunc(TEvInterconnect::TEvNodesInfo, Handle)
    )
};

IActor *CreateTestLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TLoadActor(counters);
}

} // NKikimr
