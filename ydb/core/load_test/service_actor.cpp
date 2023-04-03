#include "service_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/load_test/ycsb/test_load_actor.h>

#include <ydb/public/lib/base/msgbus.h>

#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, stream)

namespace NKqpConstants {
    const char* DEFAULT_PROTO = R"_(
KqpLoad: {
    DurationSeconds: 30
    WindowDuration: 1
    WorkingDir: "%s"
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
    TVector<NKikimr::TEvLoadTestRequest> AllNodesLoadConfigs;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_ACTOR;
    }

    TLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : NextRequestId(1)
        , Counters(counters)
    {}

    void Bootstrap(const TActorContext&) {
        Become(&TLoadActor::StateFunc);
    }

    void Handle(TEvLoad::TEvLoadTestRequest::TPtr& ev) {
        ui32 status = NMsgBusProxy::MSTATUS_OK;
        TString error;
        ui64 tag = 0;
        const auto& record = ev->Get()->Record;
        try {
            tag = ProcessCmd(record);
        } catch (const TLoadActorException& ex) {
            LOG_E("Exception while creating load actor, what# " << ex.what());
            status = NMsgBusProxy::MSTATUS_ERROR;
            error = ex.what();
        }
        auto response = std::make_unique<TEvLoad::TEvLoadTestResponse>();
        response->Record.SetStatus(status);
        if (error) {
            response->Record.SetErrorReason(error);
        }
        if (record.HasCookie()) {
            response->Record.SetCookie(record.GetCookie());
        }
        response->Record.SetTag(tag);
        Send(ev->Sender, response.release());
    }

    template<typename T>
    ui64 GetOrGenerateTag(const T& cmd) {
        if (cmd.HasTag()) {
            return cmd.GetTag();
        } else {
            return NextTag++;
        }
    }

    ui64 ProcessCmd(const NKikimr::TEvLoadTestRequest& record) {
        ui64 tag = 0;
        switch (record.Command_case()) {
            case NKikimr::TEvLoadTestRequest::CommandCase::kStorageLoad: {
                const auto& cmd = record.GetStorageLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_D("Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreateWriterLoadTest(cmd, SelfId(),
                                GetServiceCounters(Counters, "load_actor"), tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kStop: {
                const auto& cmd = record.GetStop();
                if (cmd.HasRemoveAllTags() && cmd.GetRemoveAllTags()) {
                    LOG_D("Delete all running load actors");
                    for (auto& actorPair : LoadActors) {
                        Send(actorPair.second, new TEvents::TEvPoisonPill);
                    }
                } else {
                    VERIFY_PARAM(Tag);
                    tag = cmd.GetTag();
                    auto iter = LoadActors.find(tag);
                    if (iter == LoadActors.end()) {
                        ythrow TLoadActorException()
                            << Sprintf("load actor with Tag# %" PRIu64 " not found", tag);
                    }
                    LOG_D("Delete running load actor with tag# "
                            << tag);
                    Send(iter->second, new TEvents::TEvPoisonPill);
                }
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskWriteLoad: {
                const auto& cmd = record.GetPDiskWriteLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_D("Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreatePDiskWriterLoadTest(
                                cmd, SelfId(), GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskReadLoad: {
                const auto& cmd = record.GetPDiskReadLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_D("Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreatePDiskReaderLoadTest(
                                cmd, SelfId(), GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskLogLoad: {
                const auto& cmd = record.GetPDiskLogLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_D("Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreatePDiskLogWriterLoadTest(
                                cmd, SelfId(), GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kVDiskLoad: {
                const auto& cmd = record.GetVDiskLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_D("Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreateVDiskWriterLoadTest(cmd, SelfId(), tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kKeyValueLoad: {
                const auto& cmd = record.GetKeyValueLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }

                LOG_D("Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreateKeyValueWriterLoadTest(
                                cmd, SelfId(), GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kKqpLoad: {
                const auto& cmd = record.GetKqpLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }

                LOG_D("Create new Kqp load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreateKqpLoadActor(
                            cmd, SelfId(), GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kMemoryLoad: {
                const auto& cmd = record.GetMemoryLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }

                LOG_D("Create new memory load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreateMemoryLoadTest(
                            cmd, SelfId(), GetServiceCounters(Counters, "load_actor"), 0, tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kYCSBLoad: {
                const auto& cmd = record.GetYCSBLoad();
                tag = GetOrGenerateTag(cmd);
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }

                LOG_D("Create new YCSB load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(NDataShardLoad::CreateTestLoadActor(
                            cmd, SelfId(), GetServiceCounters(Counters, "load_actor"), tag)));
                break;
            }

            default: {
                TString protoTxt;
                google::protobuf::TextFormat::PrintToString(record, &protoTxt);
                ythrow TLoadActorException() << (TStringBuilder()
                        << "TLoadActor::Handle(TEvLoad::TEvLoadTestRequest): unexpected command case: "
                        << ui32(record.Command_case())
                        << " protoTxt# " << protoTxt.Quote());
            }
        }
        return tag;
    }

    void Handle(TEvLoad::TEvLoadTestFinished::TPtr& ev) {
        const auto& msg = ev->Get();
        auto iter = LoadActors.find(msg->Tag);
        Y_VERIFY(iter != LoadActors.end());
        LOG_D("Load actor with tag# " << msg->Tag << " finished");
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
                    GenerateHttpInfoRes("results", it->first);
                }
            }

            it = next;
        }
    }

    bool RunRecordOnAllNodes(const auto& record) {
        AllNodesLoadConfigs.push_back(record);

        if (AppData()->DomainsInfo->Domains.empty()) {
            return false;
        }
        auto domainInfo = AppData()->DomainsInfo->Domains.begin()->second;
        auto name = AppData()->TenantName;
        RegisterWithSameMailbox(CreateBoardLookupActor(MakeEndpointsBoardPath(name),
                                                        SelfId(),
                                                        domainInfo->DefaultStateStorageGroup,
                                                        EBoardLookupMode::Second,
                                                        false,
                                                        false));
        return true;
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (ev->Get()->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            LOG_E("Error status for TEvStateStorage::TEvBoardInfo");
            // TODO Reply error to user
            return;
        }
        TVector<ui32> dynNodesIds;
        for (const auto& [actorId, _] : ev->Get()->InfoEntries) {
            dynNodesIds.push_back(actorId.NodeId());
        }

        for (const auto& cmd : AllNodesLoadConfigs) {
            for (const auto& id : dynNodesIds) {
                LOG_D("sending load request to: " << id);
                auto msg = MakeHolder<TEvLoad::TEvLoadTestRequest>();
                msg->Record = cmd;
                msg->Record.SetCookie(id);
                Send(MakeLoadServiceID(id), msg.Release());
            }
        }

        AllNodesLoadConfigs.clear();
    }

    static TString GetAccept(const NMonitoring::IMonHttpRequest& request) {
        const auto& headers = request.GetHeaders();
        if (const THttpInputHeader* header = headers.FindHeader("Accept")) {
            return header->Value();
        } else {
            return "application/html";
        }
    }

    static TString GetContentType(const NMonitoring::IMonHttpRequest& request) {
        const auto& headers = request.GetHeaders();
        if (const THttpInputHeader* header = headers.FindHeader("Content-Type")) {
            return header->Value();
        } else {
            return "application/x-protobuf-text";
        }
    }

    void HandleGet(const NMonitoring::IMonHttpRequest& request, THttpInfoRequest& info, ui32 id) {
        const auto& params = request.GetParams();
        TString mode = params.Has("mode") ? params.Get("mode") : "start";
        info.Mode = mode;
        LOG_N("handle http GET request, mode: " << mode << " LoadActors.size(): " << LoadActors.size());

        if (mode == "results") {
            if (GetAccept(request) == "application/json") {
                GenerateJsonInfoRes(id);
                return;
            }

            // send messages to subactors
            for (const auto& kv : LoadActors) {
                Send(kv.second, new NMon::TEvHttpInfo(request, id));
                info.ActorMap[kv.second].Tag = kv.first;
            }

            // record number of responses pending
            info.HttpInfoResPending = LoadActors.size();
            if (!info.HttpInfoResPending) {
                GenerateHttpInfoRes(mode, id);
            }
        } else {
            GenerateHttpInfoRes(mode, id);
        }
    }

    template<class TRecord>
    std::optional<TRecord> ParseMessage(const NMonitoring::IMonHttpRequest& request, const TString& content) {
        std::optional<TRecord> record = TRecord{};

        bool success = false;
        auto contentType = GetContentType(request);
        if (contentType == "application/x-protobuf-text") {
            success = google::protobuf::TextFormat::ParseFromString(content, &*record);
        } else if (contentType == "application/json") {
            auto status = google::protobuf::util::JsonStringToMessage(content, &*record);
            success = status.ok();
        } else {
            Y_FAIL_S("content: " << content.Quote());
        }
        if (!success) {
            record.reset();
        }
        return record;
    }

    void HandlePost(const NMonitoring::IMonHttpRequest& request, const THttpInfoRequest& , ui32 id) {
        const auto& params = request.GetPostParams();
        TString content(request.GetPostContent());

        TString mode = params.Has("mode") ? params.Get("mode") : "start";

        LOG_N("handle http POST request, mode: " << mode);
        if (mode == "start") {
            TString errorMsg = "ok";
            auto record = ParseMessage<NKikimr::TEvLoadTestRequest>(request, params.Get("config"));
            LOG_I( "received config: " << params.Get("config").Quote() << "; proto parse success: " << std::to_string(bool{record}));

            ui64 tag = 0;
            if (record) {
                if (params.Has("all_nodes") && params.Get("all_nodes") == "true") {
                    LOG_N("running on all nodes");
                    bool ok = RunRecordOnAllNodes(*record);
                    if (ok) {
                        tag = NextTag; // may be datarace here
                    } else {
                        errorMsg = "error while retrieving domain nodes info";
                    }
                } else {
                    try {
                        LOG_N("running on node: " << SelfId().NodeId());
                        tag = ProcessCmd(*record);
                    } catch (const TLoadActorException& ex) {
                        errorMsg = ex.what();
                    }
                }
            } else {
                errorMsg = "bad protobuf";
                LOG_E(errorMsg);
            }

            GenerateJsonTagInfoRes(id, tag, errorMsg);
        } else if (mode = "stop") {
            auto record = ParseMessage<NKikimr::TEvLoadTestRequest::TStop>(request, content);
            if (!record) {
                record = NKikimr::TEvLoadTestRequest::TStop{};
                record->SetRemoveAllTags(true);
            }
            NKikimr::TEvLoadTestRequest loadReq;
            *loadReq.MutableStop() = *record;

            if (params.Has("all_nodes") && params.Get("all_nodes") == "true") {
                LOG_D("stop load on all nodes");
                RunRecordOnAllNodes(loadReq);
            } else {
                LOG_D("stop load on node: " << SelfId().NodeId());
                ProcessCmd(loadReq);
            }
            GenerateJsonTagInfoRes(id, 0, "OK");
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        // calculate ID of this request
        ui32 id = NextRequestId++;

        // get reference to request information
        THttpInfoRequest& info = InfoRequests[id];

        // fill in sender parameters
        info.Origin = ev->Sender;
        info.SubRequestId = ev->Get()->SubRequestId;

        auto& request = ev->Get()->Request;
        switch (request.GetMethod()) {
             case HTTP_METHOD_GET:
                HandleGet(request, info, id);
                break;
             case HTTP_METHOD_POST:
                HandlePost(request, info, id);
                break;
            default:
                Y_FAIL();
        }
    }

    void Handle(NMon::TEvHttpInfoRes::TPtr& ev) {
        const auto& msg = ev->Get();
        ui32 id = static_cast<NMon::TEvHttpInfoRes *>(msg)->SubRequestId;

        auto it = InfoRequests.find(id);
        Y_VERIFY(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;
        LOG_I("Handle TEvHttpInfoRes, pending: " << info.HttpInfoResPending);

        auto actorIt = info.ActorMap.find(ev->Sender);
        Y_VERIFY(actorIt != info.ActorMap.end());
        TActorInfo& perActorInfo = actorIt->second;

        TStringStream stream;
        msg->Output(stream);
        Y_VERIFY(!perActorInfo.Data);
        perActorInfo.Data = stream.Str();

        if (!--info.HttpInfoResPending) {
            GenerateHttpInfoRes(info.Mode, id);
        }
    }

    void GenerateJsonTagInfoRes(ui32 id, ui64 tag, TString errorMsg) {
        auto it = InfoRequests.find(id);
        Y_VERIFY(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

        TStringStream str;
        str << NMonitoring::HTTPOKJSON;
        NJson::TJsonValue value;
        if (tag) {
            value["tag"] = tag;
        }
        value["status"] = errorMsg;
        NJson::WriteJson(&str, &value);

        auto result = std::make_unique<NMon::TEvHttpInfoRes>(str.Str(), info.SubRequestId,
                NMon::IEvHttpInfoRes::EContentType::Custom);
        Send(info.Origin, result.release());

        InfoRequests.erase(it);
    }

    void GenerateJsonInfoRes(ui32 id) {
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
        Send(info.Origin, result.release());

        InfoRequests.erase(it);
    }

    void GenerateHttpInfoRes(const TString& mode, ui32 id) {
        auto it = InfoRequests.find(id);
        Y_VERIFY(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

        TStringStream str;
        HTML(str) {
            auto printTabs = [&](TString link, TString name) {
                TString type = link == mode ? "btn-info" : "btn-default";
                str << "<a href='?mode=" << link << "' class='btn " << type << "'>" << name << "</a>\n";
            };

            printTabs("start", "Start load");
            printTabs("stop", "Stop load");
            printTabs("results", "Results");
            str << "<br>";

            str << "<div>";
            if (mode == "start") {
                str << R"___(
                    <script>
                        function sendStartRequest(button, run_all) {
                            $.ajax({
                                url: "",
                                data: {
                                    mode: "start",
                                    all_nodes: run_all,
                                    config: $('#config').val()
                                },
                                method: "POST",
                                contentType: "application/x-protobuf-text",
                                success: function(result) {
                                    $(button).prop('disabled', true);
                                    $(button).text('started');
                                    $('#config').text('tag: ' + result.tag);
                                }
                            });
                        }
                    </script>
                )___";
                str << R"_(
                    <textarea id="config" name="config" rows="20" cols="50">)_" << Sprintf(NKqpConstants::DEFAULT_PROTO, AppData()->TenantName.data())
                    << R"_(
                    </textarea>
                    <br><br>
                    <button onClick='sendStartRequest(this, false)' name='startNewLoadOneNode' class='btn btn-default'>Start new load on current node</button>
                    <br>
                    <button onClick='sendStartRequest(this, true)' name='startNewLoadAllNodes' class='btn btn-default'>Start new load on all tenant nodes</button>
                )_";
            } else if (mode == "stop") {
                str << R"___(
                    <script>
                        function sendStopRequest(button, stop_all) {
                            $.ajax({
                                url: "",
                                data: {
                                    mode: "stop",
                                    all_nodes: stop_all
                                },
                                method: "POST",
                                contentType: "application/json",
                                success: function(result) {
                                    $(button).prop('disabled', true);
                                    $(button).text("stopped");
                                }
                            });
                        }
                    </script>
                )___";
                str << R"_(
                    <br><br>
                    <button onClick='sendStopRequest(this, false)' name='stopNewLoadOneNode' class='btn btn-default'>Stop load on current node</button>
                    <br>
                    <button onClick='sendStopRequest(this, true)' name='stopNewLoadAllNodes' class='btn btn-default'>Stop load on all tenant nodes</button>
                )_";
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

        Send(info.Origin, new NMon::TEvHttpInfoRes(str.Str(), info.SubRequestId));
        // Send(info.Origin, new NMon::TEvHttpInfoRes(str.Str(), info.SubRequestId, NMon::IEvHttpInfoRes::EContentType::Custom));

        InfoRequests.erase(it);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvLoad::TEvLoadTestRequest, Handle)
        hFunc(TEvLoad::TEvLoadTestFinished, Handle)
        hFunc(NMon::TEvHttpInfo, Handle)
        hFunc(NMon::TEvHttpInfoRes, Handle)
        hFunc(TEvStateStorage::TEvBoardInfo, Handle)
    )
};

IActor *CreateLoadTestActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TLoadActor(counters);
}

} // NKikimr
