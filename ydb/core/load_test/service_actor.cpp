#include "service_actor.h"

#include "aggregated_result.h"
#include "archive.h"
#include "config_examples.h"
#include "yql_single_query.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/load_test/ycsb/test_load_actor.h>

#include <ydb/public/lib/base/msgbus.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/util/json_util.h>

#include <util/generic/algorithm.h>
#include <util/generic/guid.h>
#include <util/string/type.h>

namespace NKikimr {

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, stream)

namespace {

bool IsJsonContentType(const TString& acceptFormat) {
    return acceptFormat == "application/json";
}

ui32 GetCgiParamNumber(const TCgiParameters& params, const TStringBuf name, ui32 minValue, ui32 maxValue, ui32 defaultValue) {
    if (params.Has(name)) {
        auto param = params.Get(name);
        if (IsNumber(param)) {
            i64 value = FromString(param);
            if (minValue <= value && value <= maxValue) {
                return static_cast<ui32>(value);
            }
        }
    }
    return defaultValue;
}

bool IsLegacyRequest(const TEvLoadTestRequest& request) {
    return !request.HasTag() || !request.HasUuid() || !request.HasTimestamp();
}

const google::protobuf::Message* GetCommandFromRequest(const TEvLoadTestRequest& request) {
    switch (request.Command_case()) {
    case TEvLoadTestRequest::CommandCase::kStorageLoad:
        return &request.GetStorageLoad();
    case TEvLoadTestRequest::CommandCase::kPDiskWriteLoad:
        return &request.GetPDiskWriteLoad();
    case TEvLoadTestRequest::CommandCase::kVDiskLoad:
        return &request.GetVDiskLoad();
    case TEvLoadTestRequest::CommandCase::kPDiskReadLoad:
        return &request.GetPDiskReadLoad();
    case TEvLoadTestRequest::CommandCase::kPDiskLogLoad:
        return &request.GetPDiskLogLoad();
    case TEvLoadTestRequest::CommandCase::kKeyValueLoad:
        return &request.GetKeyValueLoad();
    case TEvLoadTestRequest::CommandCase::kKqpLoad:
        return &request.GetKqpLoad();
    case TEvLoadTestRequest::CommandCase::kMemoryLoad:
        return &request.GetMemoryLoad();
    case TEvLoadTestRequest::CommandCase::kStop:
        return &request.GetStop();
    case TEvLoadTestRequest::CommandCase::kYCSBLoad:
        return &request.GetYCSBLoad();
    default:
        return nullptr;
    }
}

ui64 ExtractTagFromCommand(const TEvLoadTestRequest& request) {
    switch (request.Command_case()) {
    case TEvLoadTestRequest::CommandCase::kStorageLoad:
        return request.GetStorageLoad().GetTag();
    case TEvLoadTestRequest::CommandCase::kPDiskWriteLoad:
        return request.GetPDiskWriteLoad().GetTag();
    case TEvLoadTestRequest::CommandCase::kVDiskLoad:
        return request.GetVDiskLoad().GetTag();
    case TEvLoadTestRequest::CommandCase::kPDiskReadLoad:
        return request.GetPDiskReadLoad().GetTag();
    case TEvLoadTestRequest::CommandCase::kPDiskLogLoad:
        return request.GetPDiskLogLoad().GetTag();
    case TEvLoadTestRequest::CommandCase::kKeyValueLoad:
        return request.GetKeyValueLoad().GetTag();
    case TEvLoadTestRequest::CommandCase::kKqpLoad:
        return request.GetKqpLoad().GetTag();
    case TEvLoadTestRequest::CommandCase::kMemoryLoad:
        return request.GetMemoryLoad().GetTag();
    case TEvLoadTestRequest::CommandCase::kStop:
        return request.GetStop().GetTag();
    case TEvLoadTestRequest::CommandCase::kYCSBLoad:
        return request.GetYCSBLoad().GetTag();
    default:
        return Max<ui64>();
    }
}

template<typename T>
NJson::TJsonValue AggregatedFieldToJson(const TAggregatedField<T>& field) {
    NJson::TJsonValue value;
    value["min"] = field.MinValue;
    value["max"] = field.MaxValue;
    value["avg"] = field.AvgValue;
    return value;
}

NJson::TJsonValue AggregatedResultToJson(const TAggregatedResult& result) {
    NJson::TJsonValue value;

    value["uuid"] = result.Uuid;
    value["start"] = result.Start.ToStringUpToSeconds();
    value["finish"] = result.Finish.ToStringUpToSeconds();
    value["total_nodes"] = result.Stats.TotalNodes;
    value["success_nodes"] = result.Stats.SuccessNodes;
    value["transactions"] = AggregatedFieldToJson(result.Stats.Transactions);
    value["transactions_per_second"] = AggregatedFieldToJson(result.Stats.TransactionsPerSecond);
    value["errors_per_second"] = AggregatedFieldToJson(result.Stats.ErrorsPerSecond);
    for (ui32 level : xrange(EPL_COUNT_NUM)) {
        value["percentile_" + ToString(static_cast<EPercentileLevel>(level))] = AggregatedFieldToJson(result.Stats.Percentiles[level]);
    }
    value["config"] = result.Config;

    return value;
}

}  // anonymous namespace

using namespace NActors;

class TLoadActor : public TActorBootstrapped<TLoadActor> {
    // per-actor HTTP info
    struct TActorInfo {
        ui64 Tag; // load tag
        TString Uuid;
        ui64 Timestamp;
        TString Data; // HTML response
    };

    // per-request info
    struct THttpInfoRequest {
        TActorId Origin; // who asked for status
        int SubRequestId; // origin subrequest id
        TMap<TActorId, TActorInfo> ActorMap; // per-actor status
        ui32 HttpInfoResPending; // number of requests pending
        TString Mode; // mode of page content
        TString AcceptFormat;
        ui32 Offset = 0;
        ui32 Limit = 0;
    };

    struct TNodeFinishedTestInfo {
        ui32 NodeId;
        TString ErrorReason;
        TInstant Finish;
        TString LastHtmlPage;
        NJson::TJsonValue JsonResult;
    };

    struct TFinishedTestInfo {
        TString Uuid;
        ui64 Tag;
        TVector<TNodeFinishedTestInfo> Nodes;
    };

    struct TRequestedTestInfo {
        ui64 Tag;
        TString Uuid;
        ui64 Timestamp;
        NKikimr::TEvLoadTestRequest LoadTestRequest;
    };

    struct TRequestStatus {
        ui32 StartedCount;
        ui32 FinishedCount;
        ui32 FailedCount;
        THashMap<ui32, TEvNodeFinishResponse> NodeResponses;  // key is node id
    };

    TVector<TConfigExample> ConfigExamples;

    // info about finished actors
    TVector<TFinishedTestInfo> FinishedTests;

    TActorId TableCreationActor;
    TActorId RecordInsertionActor;

    // key is a tag, value is an actor that sent the request to this node
    THashMap<ui32, TActorId> RequestSender;

    // currently running load actors
    TMap<ui64, TActorId> LoadActors;

    // next HTTP request identifier
    ui32 NextRequestId;

    // HTTP info requests being currently executed
    THashMap<ui32, THttpInfoRequest> InfoRequests;

    // issure tags in ascending order
    ui64 NextTag = 1;
    // might be taken by legacy requests
    THashSet<ui64> TakenTags;

    // Requests in processing
    THashMap<TString, TEvLoadTestRequest> RequestsInProcessing;
    THashMap<ui64, TString> UuidByTag;
    // queue for all-nodes load
    TVector<TEvLoadTestRequest> AllNodesLoadConfigs;

    THashMap<TString, TRequestStatus> RequestStatus;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TVector<TAggregatedResult> TestResultsToStore;
    TVector<TAggregatedResult> ArchivedResults;

private:
    TConstArrayRef<TConfigExample> GetConfigExamples() {
        if (ConfigExamples.empty()) {
            const TString tenantName = AppData()->TenantName;
            for (const auto& templ : GetConfigTemplates()) {
                ConfigExamples.push_back(ApplyTemplateParams(templ, tenantName));
            }
        }
        return ConfigExamples;
    }

    ui64 GetTag(const TEvLoadTestRequest& origRequest, bool legacyRequest) {
        if (legacyRequest) {
            ui64 tag = ExtractTagFromCommand(origRequest);
            if (tag) {
                LOG_N("Received legacy request with tag# " << tag);
                Y_ENSURE(tag >= NextTag, "External tag# " << tag << " should not be less than NextTag = " << NextTag);
                Y_ENSURE(TakenTags.count(tag) == 0, "External tag# " << tag << " should not be taken");
                TakenTags.insert(tag);
                return tag;
            } else {
                LOG_N("Received legacy request with tag# 0, assigning it a proper tag in a regular way");
            }
        }
        while (TakenTags.count(NextTag)) {
            ++NextTag;
        }
        return NextTag++;
    }

    const TEvLoadTestRequest& AddRequestInProcessing(const TEvLoadTestRequest& origRequest, bool legacyRequest) {
        ui64 tag = GetTag(origRequest, legacyRequest);
        TEvLoadTestRequest request = origRequest;
        request.SetTag(tag);
        request.SetUuid(CreateGuidAsString());
        request.SetTimestamp(TInstant::Now().Seconds());
        UuidByTag[tag] = request.GetUuid();
        auto ret = RequestsInProcessing.emplace(request.GetUuid(), std::move(request));
        Y_ENSURE(ret.second);
        LOG_N("Added request info for tag# " << tag << ", uuid# " << UuidByTag[tag]);
        return ret.first->second;
    }

    const TEvLoadTestRequest& GetFixedRequest(TEvLoad::TEvLoadTestRequest::TPtr& ev) {
        const auto& origRequest = ev->Get()->Record;
        if (IsLegacyRequest(origRequest)) {
            LOG_N("Modifying legacy request to satisfy general expectations");
            const auto& modifiedRequest = AddRequestInProcessing(origRequest, /* legacyRequest */ true);
            return modifiedRequest;
        } else {
            return origRequest;
        }
    }

    void StoreResults() {
        if (TestResultsToStore.empty()) {
            return;
        }
        const TString query = MakeRecordInsertionYql(TestResultsToStore);
        LOG_D("YQL query to insert records: " << query);
        RecordInsertionActor = TlsActivationContext->Register(
            CreateYqlSingleQueryActor(
                SelfId(),
                AppData()->TenantName,
                query,
                NKikimrKqp::EQueryType::QUERY_TYPE_SQL_DML,
                /* readOnly */ false,
                TString(kRecordsInsertedResult)
            )
        );
        TestResultsToStore.clear();
        LOG_N("Created actor for record insertion " << RecordInsertionActor.ToString());
    }

    void AggregateNodeResponses(const TString& uuid) {
        TRequestStatus& status = RequestStatus[uuid];
        TStatsAggregator aggregator(status.StartedCount);
        ui64 latestFinish = 0;
        TVector<TNodeFinishedTestInfo> nodes;
        for (const auto& [_, result] : status.NodeResponses) {
            if (result.GetSuccess() && result.HasStats()) {
                aggregator.Add(result.GetStats());
            }
            latestFinish = Max(latestFinish, result.GetFinishTimestamp());
            TInstant finishTime = TInstant::Seconds(result.GetFinishTimestamp());

            NJson::TJsonValue jsonResult;
            NJson::ReadJsonTree(result.GetJsonResult(), &jsonResult, true);
            jsonResult["node_id"] = result.GetNodeId();
            jsonResult["finish"] = finishTime.ToStringLocalUpToSeconds();
            nodes.emplace_back(TNodeFinishedTestInfo{
                .NodeId = result.GetNodeId(),
                .ErrorReason = result.GetErrorReason(),
                .Finish = finishTime,
                .LastHtmlPage = result.GetLastHtmlPage(),
                .JsonResult = jsonResult
            });
        }
        const TEvLoadTestRequest& request = RequestsInProcessing[uuid];
        FinishedTests.emplace_back(TFinishedTestInfo{
            .Uuid = uuid,
            .Tag = request.GetTag(),
            .Nodes = std::move(nodes)
        });

        TString configString;
        auto messagePtr = GetCommandFromRequest(request);
        if (messagePtr) {
            google::protobuf::TextFormat::PrintToString(*messagePtr, &configString);
        }

        TestResultsToStore.emplace_back(TAggregatedResult{
            .Uuid = uuid,
            .Start = TInstant::Seconds(request.GetTimestamp()),
            .Finish = TInstant::Seconds(latestFinish),
            .Stats = aggregator.Get(),
            .Config = configString,
        });

        TableCreationActor = TlsActivationContext->Register(
            CreateYqlSingleQueryActor(
                SelfId(),
                AppData()->TenantName,
                MakeTableCreationYql(),
                NKikimrKqp::EQueryType::QUERY_TYPE_SQL_DDL,
                /* readOnly */ false,
                TString(kTableCreatedResult)
            )
        );
        LOG_N("Created actor for table creation " << TableCreationActor.ToString());
    }

    void StartReadingResultsFromTable(ui32 offset, ui32 limit) {
        const TString query = MakeRecordSelectionYql(offset, limit);
        LOG_D("YQL query to select records: " << query);
        auto recordSelectionActor = TlsActivationContext->Register(
            CreateYqlSingleQueryActor(
                SelfId(),
                AppData()->TenantName,
                query,
                NKikimrKqp::EQueryType::QUERY_TYPE_SQL_DML,
                /* readOnly */ true,
                TString(kRecordsSelectedResult)
            )
        );
        LOG_N("Created actor for record selection " << recordSelectionActor.ToString());
    }

    void GenerateArchiveJsonResponse(ui32 requestId) {
        auto it = InfoRequests.find(requestId);
        Y_ABORT_UNLESS(it != InfoRequests.end(), "failed to find request id %" PRIu32, requestId);
        THttpInfoRequest& info = it->second;

        NJson::TJsonArray array;

        for (const TAggregatedResult& result : ArchivedResults) {
            array.AppendValue(AggregatedResultToJson(result));
        }

        TStringStream str;
        str << NMonitoring::HTTPOKJSON;
        NJson::WriteJson(&str, &array);

        auto result = std::make_unique<NMon::TEvHttpInfoRes>(str.Str(), info.SubRequestId,
                NMon::IEvHttpInfoRes::EContentType::Custom);
        Send(info.Origin, result.release());

        InfoRequests.erase(it);
    }

    void RespondToArchiveRequests() {
        TVector<ui32> archiveRequestIds;
        for (const auto& [id, req] : InfoRequests) {
            if (req.Mode == "archive") {
                archiveRequestIds.push_back(id);
            }
        }
        for (ui32 id : archiveRequestIds) {
            if (IsJsonContentType(InfoRequests[id].AcceptFormat)) {
                GenerateArchiveJsonResponse(id);
            } else {
                GenerateHttpInfoRes("archive", id);
            }
        }
    }

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
        const auto& record = GetFixedRequest(ev);
        LOG_N("Load test request arrived from " << ev->Sender.ToString() <<
            ": tag# " << record.GetTag() <<
            ", uuid# " << record.GetUuid());
        ui32 status = NMsgBusProxy::MSTATUS_OK;
        TString error;
        try {
            Y_ENSURE(!RequestSender.contains(record.GetTag()),
                "node is currently handling another request with tag# " << record.GetTag());
            RequestSender[record.GetTag()] = ev->Sender;
            UuidByTag[record.GetTag()] = record.GetUuid();
            ProcessCmd(record);
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
        response->Record.SetTag(record.HasTag() ? record.GetTag() : 0);
        Send(ev->Sender, response.release());
    }

    void Handle(TEvLoad::TEvLoadTestResponse::TPtr& ev) {
        if (ev->Get()->Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
            LOG_E("Receieved non-OK LoadTestResponse from another node, Record# " << ev->ToString());
        } else {
            LOG_N("Receieved OK LoadTestResponse from another node# " << ev->ToString());
        }
    }

    ui64 ProcessCmd(const NKikimr::TEvLoadTestRequest& record) {
        ui64 tag = 0;
        if (record.Command_case() != TEvLoadTestRequest::CommandCase::kStop) {
            Y_ENSURE(record.HasTag());
            tag = record.GetTag();
        }
        switch (record.Command_case()) {
            case NKikimr::TEvLoadTestRequest::CommandCase::kStorageLoad: {
                const auto& cmd = record.GetStorageLoad();
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
                if (LoadActors.count(tag) != 0) {
                    ythrow TLoadActorException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
                }
                LOG_D("Create new load actor with tag# " << tag);
                LoadActors.emplace(tag, TlsActivationContext->Register(CreateVDiskWriterLoadTest(cmd, SelfId(), tag)));
                break;
            }

            case NKikimr::TEvLoadTestRequest::CommandCase::kKeyValueLoad: {
                const auto& cmd = record.GetKeyValueLoad();
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
        Y_ABORT_UNLESS(iter != LoadActors.end());
        LOG_D("Load actor with tag# " << msg->Tag << " finished");
        LoadActors.erase(iter);
        const TInstant finishTime = TAppData::TimeProvider->Now();

        Y_ENSURE(UuidByTag.contains(msg->Tag), "Not found uuid corresponding for tag# " << msg->Tag);
        {
            auto nodeFinishResponse = MakeHolder<TEvLoad::TEvNodeFinishResponse>();
            TEvNodeFinishResponse& record = nodeFinishResponse->Record;
            const TString& uuid = UuidByTag.at(msg->Tag);
            record.SetUuid(uuid);
            record.SetNodeId(SelfId().NodeId());
            record.SetSuccess(msg->Report != nullptr);
            record.SetFinishTimestamp(finishTime.Seconds());
            record.SetErrorReason(msg->ErrorReason);

            auto* stats = record.MutableStats();
            const NJson::TJsonValue& jsonResult = msg->JsonResult;

            stats->SetTransactions(jsonResult["txs"].GetUInteger());
            stats->SetTransactionsPerSecond(jsonResult["rps"].GetDouble());
            stats->SetErrorsPerSecond(jsonResult["errors"].GetDouble());
            for (ui32 level : xrange(EPL_COUNT_NUM)) {
                stats->AddPercentiles(jsonResult["percentile"][ToString((EPercentileLevel) level)].GetDouble());
            }
            record.SetLastHtmlPage(msg->LastHtmlPage);
            {
                TStringStream str;
                NJson::WriteJson(&str, &jsonResult);
                record.SetJsonResult(str.Str());
            }
            auto requestSender = RequestSender[msg->Tag];
            LOG_N("Sending TEvNodeFinishResponse back to sender# " << requestSender.ToString());
            Send(requestSender, nodeFinishResponse.Release());
            RequestSender.erase(msg->Tag);
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

    void Handle(TEvLoad::TEvNodeFinishResponse::TPtr& ev) {
        const auto& msg = ev->Get();
        TEvNodeFinishResponse& record = msg->Record;
        const TString& uuid = record.GetUuid();
        const ui32 nodeId = record.GetNodeId();

        auto requestIt = RequestStatus.find(uuid);
        if (requestIt == RequestStatus.end()) {
            LOG_E("Node finish response has arrived for unknown request uuid# " << uuid);
            return;
        }

        TRequestStatus& status = requestIt->second;
        auto nodeIt = status.NodeResponses.find(nodeId);
        if (nodeIt != status.NodeResponses.end()) {
            LOG_E("Node finish response has arrived for already finished node# " << nodeId << ", uuid# " << uuid);
            return;
        }
        status.NodeResponses.emplace(nodeId, record);
        if (record.GetSuccess()) {
            ++status.FinishedCount;
        } else {
            ++status.FailedCount;
        }
        LOG_N("Received responses from " << status.FinishedCount << " finished and " <<
            status.FailedCount << " failed nodes out of " << status.StartedCount);
        if (status.FinishedCount + status.FailedCount >= status.StartedCount) {
            AggregateNodeResponses(uuid);
        }
    }

    void RunRecordOnAllNodes(const TEvLoadTestRequest& record, ui64& tag, TString& uuid, TString& /*msg*/) {
        const auto& modifiedRequest = AddRequestInProcessing(record, /* legacyRequest */ false);
        AllNodesLoadConfigs.push_back(modifiedRequest);
        auto name = AppData()->TenantName;
        RegisterWithSameMailbox(CreateBoardLookupActor(MakeEndpointsBoardPath(name),
                                                        SelfId(),
                                                        EBoardLookupMode::Second));
        tag = modifiedRequest.GetTag();
        uuid = modifiedRequest.GetUuid();
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
        SortUnique(dynNodesIds);

        for (const auto& cmd : AllNodesLoadConfigs) {
            SendLoadTestRequestToNodes(cmd, dynNodesIds);
        }

        AllNodesLoadConfigs.clear();
    }

    void SendLoadTestRequestToNodes(const NKikimr::TEvLoadTestRequest& request, const TConstArrayRef<ui32>& dynNodesIds) {
        const TString& uuid = request.GetUuid();
        Y_ENSURE(!uuid.empty());
        RequestStatus.emplace(uuid, TRequestStatus{
            .StartedCount = static_cast<ui32>(dynNodesIds.size()),
            .FinishedCount = 0,
            .FailedCount = 0,
        });
        for (const auto& id : dynNodesIds) {
            LOG_D("sending load request to: " << id);
            auto msg = MakeHolder<TEvLoad::TEvLoadTestRequest>();
            msg->Record = request;
            msg->Record.SetCookie(id);
            Send(MakeLoadServiceID(id), msg.Release());
        }
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
            if (IsJsonContentType(info.AcceptFormat)) {
                GenerateJsonInfoRes(id);
                return;
            }

            // send messages to subactors
            for (const auto& [tag, actorId] : LoadActors) {
                Send(actorId, new NMon::TEvHttpInfo(request, id));
                info.ActorMap[actorId].Tag = tag;
                auto reqIt = RequestsInProcessing.find(UuidByTag.at(tag));
                if (reqIt != RequestsInProcessing.end()) {
                    const TEvLoadTestRequest& req = reqIt->second;
                    info.ActorMap[actorId].Uuid = req.GetUuid();
                    info.ActorMap[actorId].Timestamp = req.GetTimestamp();
                }
            }

            // record number of responses pending
            info.HttpInfoResPending = LoadActors.size();
            if (!info.HttpInfoResPending) {
                GenerateHttpInfoRes(mode, id);
            }
        } else if (mode == "archive") {
            ui32 offset = GetCgiParamNumber(params, "offset", 0, Max<ui32>(), 0);
            ui32 limit = GetCgiParamNumber(params, "limit", 1, 100, 10);
            info.Offset = offset;
            info.Limit = limit;
            if (AppData()->TenantName.empty()) {
                RespondToArchiveRequests();
            } else {
                StartReadingResultsFromTable(offset, limit);
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
        } else if (IsJsonContentType(contentType)) {
            auto status = google::protobuf::util::JsonStringToMessage(content, &*record);
            success = status.ok();
        } else {
            LOG_D("Unable to parse request, content: " << content.Quote());
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
            TString uuid;
            if (record) {
                if (params.Has("all_nodes") && params.Get("all_nodes") == "true") {
                    LOG_N("running on all nodes");
                    RunRecordOnAllNodes(*record, tag, uuid, errorMsg);
                } else {
                    try {
                        LOG_N("running on single node");
                        const auto& modifiedRequest = AddRequestInProcessing(record.value(), /* legacyRequest */ false);
                        tag = modifiedRequest.GetTag();
                        uuid = modifiedRequest.GetUuid();
                        const TVector<ui32> dynNodesIds = {SelfId().NodeId()};
                        SendLoadTestRequestToNodes(modifiedRequest, dynNodesIds);
                    } catch (const TLoadActorException& ex) {
                        errorMsg = ex.what();
                    }
                }
            } else {
                errorMsg = "bad protobuf";
                LOG_E(errorMsg);
            }

            GenerateJsonTagInfoRes(id, tag, uuid, errorMsg);
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
                ui64 dummyTag;
                TString dummyUuid;
                TString dummyMsg;
                RunRecordOnAllNodes(loadReq, dummyTag, dummyUuid, dummyMsg);
            } else {
                LOG_D("stop load on node: " << SelfId().NodeId());
                ProcessCmd(loadReq);
            }
            GenerateJsonTagInfoRes(id, 0, "", "OK");
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
        info.AcceptFormat = GetAccept(request);

        switch (request.GetMethod()) {
             case HTTP_METHOD_GET:
                HandleGet(request, info, id);
                break;
             case HTTP_METHOD_POST:
                HandlePost(request, info, id);
                break;
            default:
                Y_ABORT();
        }
    }

    void Handle(const TEvLoad::TEvYqlSingleQueryResponse::TPtr& ev) {
        const auto* response = ev->Get();
        if (response->Result == kTableCreatedResult) {
            if (response->ErrorMessage.Defined()) {
                LOG_E("Failed to create test results table: " << response->ErrorMessage.GetRef());
            } else {
                LOG_N("Created test results table");
                StoreResults();
            }
        } else if (response->Result == kRecordsInsertedResult) {
            if (response->ErrorMessage.Defined()) {
                LOG_E("Failed to save test results into table: " << response->ErrorMessage.GetRef());
            } else {
                LOG_N("Inserted records with test results");
            }
        } else if (response->Result == kRecordsSelectedResult) {
            if (response->ErrorMessage.Defined()) {
                LOG_E("Failed to select test results from table: " << response->ErrorMessage.GetRef());
            } else {
                LOG_N("Selected records from table");
                Y_ENSURE(response->Response.Defined());
                if (!LoadResultFromResponseProto(response->Response.GetRef(), ArchivedResults)) {
                    LOG_E("Failed to parse results from table");
                    ArchivedResults.clear();
                } else {
                    LOG_N("Got results from table: " << ArchivedResults.size());
                }
            }
            RespondToArchiveRequests();
        } else {
            LOG_E("Unsupported result from YQL query: " << response->Result);
        }
    }

    void Handle(NMon::TEvHttpInfoRes::TPtr& ev) {
        const auto& msg = ev->Get();
        ui32 id = static_cast<NMon::TEvHttpInfoRes *>(msg)->SubRequestId;

        auto it = InfoRequests.find(id);
        Y_ABORT_UNLESS(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;
        LOG_I("Handle TEvHttpInfoRes, pending: " << info.HttpInfoResPending);

        auto actorIt = info.ActorMap.find(ev->Sender);
        Y_ABORT_UNLESS(actorIt != info.ActorMap.end());
        TActorInfo& perActorInfo = actorIt->second;

        TStringStream stream;
        msg->Output(stream);
        Y_ABORT_UNLESS(!perActorInfo.Data);
        perActorInfo.Data = stream.Str();

        if (!--info.HttpInfoResPending) {
            GenerateHttpInfoRes(info.Mode, id);
        }
    }

    void GenerateJsonTagInfoRes(ui32 id, ui64 tag, TString uuid, TString errorMsg) {
        auto it = InfoRequests.find(id);
        Y_ABORT_UNLESS(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

        TStringStream str;
        str << NMonitoring::HTTPOKJSON;
        NJson::TJsonValue value;
        if (tag) {
            value["tag"] = tag;
        }
        value["uuid"] = std::move(uuid);
        value["status"] = std::move(errorMsg);
        NJson::WriteJson(&str, &value);

        auto result = std::make_unique<NMon::TEvHttpInfoRes>(str.Str(), info.SubRequestId,
                NMon::IEvHttpInfoRes::EContentType::Custom);
        Send(info.Origin, result.release());

        InfoRequests.erase(it);
    }

    void GenerateJsonInfoRes(ui32 id) {
        auto it = InfoRequests.find(id);
        Y_ABORT_UNLESS(it != InfoRequests.end());
        THttpInfoRequest& info = it->second;

        NJson::TJsonArray array;
        for (auto it = FinishedTests.rbegin(); it != FinishedTests.rend(); ++it) {
            NJson::TJsonValue value;
            const TFinishedTestInfo& testInfo = *it;
            value["uuid"] = testInfo.Uuid;
            value["tag"] = testInfo.Tag;

            auto& build = value["build"];
            build["date"] = GetProgramBuildDate();
            build["timestamp"] = GetProgramBuildTimestamp();
            build["branch"] = GetBranch();
            build["last_author"] = GetArcadiaLastAuthor();
            build["build_user"] = GetProgramBuildUser();
            build["change_num"] = GetArcadiaLastChangeNum();

            NJson::TJsonArray nodeArray;
            for (const TNodeFinishedTestInfo& nodeInfo : testInfo.Nodes) {
                nodeArray.AppendValue(nodeInfo.JsonResult);
            }
            value["nodes"] = nodeArray;

            array.AppendValue(value);
        }

        TStringStream str;
        str << NMonitoring::HTTPOKJSON;
        NJson::WriteJson(&str, &array);

        auto result = std::make_unique<NMon::TEvHttpInfoRes>(str.Str(), info.SubRequestId,
                NMon::IEvHttpInfoRes::EContentType::Custom);
        Send(info.Origin, result.release());

        InfoRequests.erase(it);
    }

    void RenderStartForm(IOutputStream& str) {
        str << R"___(
            <script>
                function updateStatus(new_class, new_text) {
                    let line = $("#start-status-line");
                    line.removeClass();
                    if (new_class) {
                        line.addClass(new_class);
                    }
                    line.text(new_text);
                }
                function sendStartRequest(button, run_all) {
                    updateStatus("", "Starting..");
                    $.ajax({
                        url: "",
                        data: {
                            mode: "start",
                            all_nodes: run_all,
                            config: $("#config").val()
                        },
                        method: "POST",
                        contentType: "application/x-protobuf-text",
                        success: function(result) {
                            if (result.status == "ok") {
                                updateStatus(
                                    "text-success",
                                    "Starting: UUID# " + result.uuid + ", node tag# " + result.tag + ", status# " + result.status
                                );
                            } else {
                                updateStatus(
                                    "text-danger",
                                    "Status# " + result.status
                                );
                            }
                        }
                    });
                })___";

            str << "let kPresetConfigs=[";
            bool first = true;
            for (const auto& example : GetConfigExamples()) {
                if (first) {
                    first = false;
                } else {
                    str << ", ";
                }
                str << '"' << example.Escaped << '"';
            }
            str << "];";
            str << R"___(
                function loadConfigExample() {
                    let selectValue = $("#example-select").get(0).value;
                    let pos = Number(selectValue);
                    $("#config").val(kPresetConfigs[pos]);
                }
            </script>
        )___";

        HTML(str) {
            FORM() {
                DIV_CLASS("form-group") {
                    LABEL_CLASS_FOR("", "example-select") {
                        str << "Load example:";
                    }
                    str << "<select name='examples' id='example-select' onChange='loadConfigExample()'>\n";
                    ui32 pos = 0;
                    for (const auto& example : GetConfigExamples()) {
                        str << "<option value='" << ToString(pos) << "'>" << example.LoadName << "</option>\n";
                        ++pos;
                    }
                    str << "</select>\n";
                }
                DIV_CLASS("form-group") {
                    str << "<textarea id='config' name='config' rows='20' cols='50'>";
                    str << GetConfigExamples()[0].Text;
                    str << "</textarea>\n";
                }
                DIV_CLASS("form-group") {
                    str << "<button type='button' onClick='sendStartRequest(this, false)' name='startNewLoadOneNode' class='btn btn-default'>\n";
                    str << "Start new load on current node\n";
                    str << "</button>\n";
                }
                DIV_CLASS("form-group") {
                    str << "<button type='button' onClick='sendStartRequest(this, true)' name='startNewLoadAllNodes' class='btn btn-default'>\n";
                    str << "Start new load on all tenant nodes\n";
                    str << "</button>\n";
                }
                DIV_CLASS("form-group") {
                    str << "<p id='start-status-line'></p>";
                }
            }
        }
    }

    void GenerateHttpInfoRes(const TString& mode, ui32 id) {
        auto it = InfoRequests.find(id);
        Y_ABORT_UNLESS(it != InfoRequests.end());
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
            printTabs("archive", "Archive");
            str << "<br>";

            str << "<div>";
            if (mode == "start") {
                RenderStartForm(str);
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
                auto printUuidTag = [&str](const TString& uuid, ui64 tag) {
                    str << "UUID# " << uuid << " (node tag# " << tag << ")";
                };
                for (auto it = info.ActorMap.rbegin(); it != info.ActorMap.rend(); ++it) {
                    const TActorInfo& perActorInfo = it->second;
                    auto uuidIter = UuidByTag.find(perActorInfo.Tag);
                    const TString uuid = uuidIter != UuidByTag.end() ? uuidIter->second : "";
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            printUuidTag(uuid, perActorInfo.Tag);
                        }
                        DIV_CLASS("panel-body") {
                            str << perActorInfo.Data;
                        }
                    }
                }

                for (auto it = FinishedTests.rbegin(); it != FinishedTests.rend(); ++it) {
                    const TFinishedTestInfo& testInfo = *it;
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            printUuidTag(testInfo.Uuid, testInfo.Tag);
                        }
                        for (const TNodeFinishedTestInfo& nodeInfo : testInfo.Nodes) {
                            DIV_CLASS("panel-body") {
                                str << "Node# " << nodeInfo.NodeId << ". ";
                                str << "Finish reason# " << nodeInfo.ErrorReason << "<br/>";
                                str << "Finish time# " << nodeInfo.Finish.ToStringUpToSeconds() << "<br/>";
                                str << nodeInfo.LastHtmlPage;
                            }
                        }
                    }
                }
            } else if (mode == "archive") {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Archived load test results";
                    }
                    if (AppData()->TenantName.empty()) {
                        DIV_CLASS("panel-body text-warning") {
                            str << "Table is not available because TenantName is not set";
                        }
                    }
                    TABLE_CLASS("table-bordered table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "UUID"; }
                                TABLEH() { str << "Time"; }
                                TABLEH() {
                                    str << "<span title=\"Success nodes / total nodes\">Ok / nodes</span>";
                                }
                                TABLEH() { str << "Txs"; }
                                TABLEH() { str << "Txs/Sec"; }
                                TABLEH() { str << "Errors/Sec"; }
                                for (ui32 level : xrange(EPL_COUNT_NUM)) {
                                    TABLEH() {
                                        str << "p";
                                        if (level == EPL_100) {
                                            str << "Max";
                                        } else {
                                            str << ToString(static_cast<EPercentileLevel>(level));
                                        }
                                        str << "(ms)";
                                    }
                                }
                                TABLEH() { str << "Config"; }
                            }
                        }
                        TABLEBODY() {
                            ui32 rowNumber = 0;
                            for (const TAggregatedResult& result : ArchivedResults) {
                                TABLER() {
                                    TABLED() {
                                        PrintUuidToHtml(result.Uuid, str);
                                    }
                                    TABLED() {
                                        PrintStartFinishToHtml(result.Start, result.Finish, str);
                                    }
                                    const TAggregatedStats& stats = result.Stats;
                                    TABLED() {
                                        str << stats.SuccessNodes << " / " << stats.TotalNodes;
                                    }
                                    TABLED() {
                                        PrintFieldToHtml(stats.Transactions, str);
                                    }
                                    TABLED() {
                                        PrintFieldToHtml(stats.TransactionsPerSecond, str);
                                    }
                                    TABLED() {
                                        PrintFieldToHtml(stats.ErrorsPerSecond, str);
                                    }
                                    for (ui32 level : xrange(EPL_COUNT_NUM)) {
                                        TABLED() {
                                            PrintFieldToHtml(stats.Percentiles[level], str);
                                        }
                                    }
                                    TABLED() {
                                        COLLAPSED_BUTTON_CONTENT(TString("configProtobuf") + ToString(rowNumber), "Config") {
                                            str << "<pre>" << result.Config << "</pre>";
                                        }
                                    }
                                }
                                ++rowNumber;
                            }
                        }
                    }
                    str << "<div>\n";
                    {
                        str << "<span>";
                        if (info.Offset) {
                            ui32 prevOffset = info.Offset - Min(info.Limit, info.Offset);
                            str << "<a href='?mode=archive&offset=" << prevOffset << "&limit=" << info.Limit << "' ";

                        } else {
                            str << "<a href='#' disabled ";
                        }
                        str << "class='btn btn-default'>&lt; prev</a></span>\n";
                    }
                    {
                        str << "<span>";
                        if (ArchivedResults.size() >= info.Limit) {
                            ui32 nextOffset = info.Offset + info.Limit;
                            str << "<a href='?mode=archive&offset=" << nextOffset << "&limit=" << info.Limit << "' ";
                        } else {
                            str << "<a href='#' disabled ";
                        }
                        str << "class='btn btn-default'>next &gt;</a></span>\n";
                    }
                    str << "</div>\n";
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
        hFunc(TEvLoad::TEvLoadTestResponse, Handle)
        hFunc(TEvLoad::TEvLoadTestFinished, Handle)
        hFunc(TEvLoad::TEvNodeFinishResponse, Handle)
        hFunc(NMon::TEvHttpInfo, Handle)
        hFunc(NMon::TEvHttpInfoRes, Handle)
        hFunc(TEvStateStorage::TEvBoardInfo, Handle)
        hFunc(TEvLoad::TEvYqlSingleQueryResponse, Handle)
    )
};

IActor *CreateLoadTestActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TLoadActor(counters);
}

} // NKikimr
