#include "base_compute_actor.h"

#include <ydb/core/fq/libs/common/rows_proto_splitter.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/metrics.h>
#include <ydb/core/fq/libs/compute/common/retry_actor.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/private_client/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <queue>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TResultSetWriterActor : public TBaseComputeActor<TResultSetWriterActor> {
    struct TWriterMeta {
        int64_t Offset = 0;
        TInstant StartTime;
    };

public:
    enum ERequestType {
        RT_FETCH_SCRIPT_RESULT,
        RT_WRITE_RESULT_SET,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("FetchScriptResult") },
            { MakeIntrusive<TComputeRequestCounters>("WriteResultSet") }
        });

        ::NMonitoring::TDynamicCounterPtr Counters;

    public:
        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: Requests) {
                request->Register(Counters);
            }
        }

        TComputeRequestCountersPtr GetCounters(ERequestType type) {
            return Requests[type];
        }
    };

    TResultSetWriterActor(const TRunActorParams& params, int64_t resultSetId, const TActorId& parent, const TActorId& connector, const NKikimr::NOperationId::TOperationId& operationId, const ::NMonitoring::TDynamicCounterPtr& counters)
        : TBaseComputeActor(counters, "ResultSetWriter")
        , Params(params)
        , ResultSetId(resultSetId)
        , Parent(parent)
        , Connector(connector)
        , OperationId(operationId)
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_RESULT_SET_WRITER_ACTOR";

    void Start() {
        LOG_I("ResultSetId: " << ResultSetId << " Start result set writer actor");
        Become(&TResultSetWriterActor::StateFunc);
        SendFetchScriptResultRequest();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvFetchScriptResultResponse, Handle);
        hFunc(NFq::TEvInternalService::TEvWriteResultResponse, Handle);
    )

    void Handle(const TEvYdbCompute::TEvFetchScriptResultResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status == NYdb::EStatus::BAD_REQUEST && FetchRowsLimit == 0) {
            LOG_W("ResultSetId: " << ResultSetId << " Got bad request: " << ev->Get()->Issues.ToOneLineString() << ", try to fallback to old behaviour");
            FetchRowsLimit = 1000;
            SendFetchScriptResultRequest();
            return;
        }

        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("ResultSetId: " << ResultSetId << " Can't fetch script result: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvResultSetWriterResponse(ResultSetId, ev->Get()->Issues, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
            return;
        }

        LOG_I("ResultSetId: " << ResultSetId << " FetchToken: " << FetchToken << " Successfully fetched " << response.ResultSet->RowsCount() << " rows");
        Truncated |= response.ResultSet->Truncated();
        FetchToken = response.NextFetchToken;
        auto emptyResultSet = response.ResultSet->RowsCount() == 0;
        auto resultSetProto = NYdb::TProtoAccessor::GetProto(*response.ResultSet);

        if (!emptyResultSet) {
            NFq::TRowsProtoSplitter rowsSplitter(std::move(resultSetProto), ProtoMessageLimit, BaseProtoByteSize, MaxRowsCountPerChunk);
            auto splittedResultSets = rowsSplitter.Split();

            if (!splittedResultSets.Success) {
                LOG_E("ResultSetId: " << ResultSetId << " Can't split script result: " << splittedResultSets.Issues.ToOneLineString());
                Send(Parent, new TEvYdbCompute::TEvResultSetWriterResponse(ResultSetId, splittedResultSets.Issues, NYdb::EStatus::INTERNAL_ERROR));
                FailedAndPassAway();
                return;
            }

            for (auto& resultSet : splittedResultSets.ResultSets) {
                auto protoReq = CreateProtoRequestWithoutResultSet(Offset);
                Offset += resultSet.rows().size();
                protoReq.mutable_result_set()->Swap(&resultSet);
                ResultChunks.emplace(std::move(protoReq));
            }

            TryStartResultWriters();
        }

        if (WriterInflight.empty()) {
            SendReplyAndPassAway();
        } else if (FetchToken && (WriterInflight.size() + ResultChunks.size()) < 2 * MAX_WRITER_INFLIGHT) {
            SendFetchScriptResultRequest();
        }
    }

    void Handle(const NFq::TEvInternalService::TEvWriteResultResponse::TPtr& ev) {
        auto writeResultCounters = Counters.GetCounters(ERequestType::RT_WRITE_RESULT_SET);
        writeResultCounters->InFly->Dec();
        
        auto cookie = ev->Cookie;
        auto it = WriterInflight.find(cookie);
        if (it == WriterInflight.end()) {
            auto errorMsg = TStringBuilder{} << "ResultSetId: " << ResultSetId << " Cookie: " << cookie << " Internal error. Cookie wasn't found in case of write result reponse";
            LOG_E(errorMsg);
            Send(Parent, new TEvYdbCompute::TEvResultSetWriterResponse(ResultSetId, NYql::TIssues{NYql::TIssue{errorMsg}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
            return;
        }
        auto meta = it->second;
        WriterInflight.erase(it);
        writeResultCounters->LatencyMs->Collect((TInstant::Now() - meta.StartTime).MilliSeconds());

        if (!ev.Get()->Get()->Status.IsSuccess()) {
            writeResultCounters->Error->Inc();
            TString errorMsg = TStringBuilder{} << "ResultSetId: " << ResultSetId << " Cookie: " << cookie << " Error writing result for offset " << meta.Offset;
            LOG_E(errorMsg);
            Send(Parent, new TEvYdbCompute::TEvResultSetWriterResponse(ResultSetId, NYql::TIssues{NYql::TIssue{errorMsg}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
            return;
        }

        TryStartResultWriters();

        writeResultCounters->Ok->Inc();
        LOG_I("ResultSetId: " << ResultSetId << " Cookie: " << cookie << " Result successfully written for offset " << meta.Offset);
        if (FetchToken) {
            if (FetchToken != LastProcessedToken && (WriterInflight.size() + ResultChunks.size()) < 2 * MAX_WRITER_INFLIGHT) {
                SendFetchScriptResultRequest();
            }
        } else if (WriterInflight.empty()) {
            SendReplyAndPassAway();
        }
    }

    void TryStartResultWriters() {
        auto writeResultCounters = Counters.GetCounters(ERequestType::RT_WRITE_RESULT_SET);
        while (!ResultChunks.empty() && WriterInflight.size() < MAX_WRITER_INFLIGHT) {
            auto chunk = std::move(ResultChunks.front());
            ResultChunks.pop();

            WriterInflight[Cookie] = {static_cast<int64_t>(chunk.offset()), TInstant::Now()};
            writeResultCounters->InFly->Inc();
            Send(NFq::MakeInternalServiceActorId(), new NFq::TEvInternalService::TEvWriteResultRequest(std::move(chunk)), 0, Cookie++);
        }
    }

    void SendFetchScriptResultRequest() {
        LastProcessedToken = FetchToken;
        Register(new TRetryActor<TEvYdbCompute::TEvFetchScriptResultRequest, TEvYdbCompute::TEvFetchScriptResultResponse, NKikimr::NOperationId::TOperationId, int64_t, TString, uint64_t>(Counters.GetCounters(ERequestType::RT_FETCH_SCRIPT_RESULT), SelfId(), Connector, OperationId, ResultSetId, FetchToken, FetchRowsLimit));
    }

    void SendReplyAndPassAway() {
        Send(Parent, new TEvYdbCompute::TEvResultSetWriterResponse(ResultSetId, Offset, Truncated));
        PassAway();
    }

    Fq::Private::WriteTaskResultRequest CreateProtoRequestWithoutResultSet(ui64 startRowIndex) {
        Fq::Private::WriteTaskResultRequest protoReq;
        protoReq.set_owner_id(Params.Owner);
        protoReq.mutable_result_id()->set_value(Params.ResultId);
        protoReq.set_result_set_id(ResultSetId);
        protoReq.set_offset(startRowIndex);
        *protoReq.mutable_deadline() = NProtoInterop::CastToProto(Params.Deadline);
        return protoReq;
    }

private:
    ui64 Cookie = 0;
    static constexpr int MAX_WRITER_INFLIGHT = 3;
    TMap<ui64, TWriterMeta> WriterInflight;
    TRunActorParams Params;
    uint32_t ResultSetId = 0;
    TActorId Parent;
    TActorId Connector;
    NKikimr::NOperationId::TOperationId OperationId;
    TCounters Counters;
    int64_t Offset = 0;
    bool Truncated = false;
    TString FetchToken;
    TString LastProcessedToken;
    ui64 FetchRowsLimit = 0;
    const size_t ProtoMessageLimit = 10_MB;
    const size_t MaxRowsCountPerChunk = 100'000;
    const size_t BaseProtoByteSize = CreateProtoRequestWithoutResultSet(0).ByteSizeLong();
    std::queue<Fq::Private::WriteTaskResultRequest> ResultChunks;
};

class TResultWriterActor : public TBaseComputeActor<TResultWriterActor> {
public:
    enum ERequestType {
        RT_GET_OPERATION,
        RT_PING,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("GetOperation") },
            { MakeIntrusive<TComputeRequestCounters>("Ping") }
        });

        ::NMonitoring::TDynamicCounterPtr Counters;

    public:
        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: Requests) {
                request->Register(Counters);
            }
        }

        TComputeRequestCountersPtr GetCounters(ERequestType type) {
            return Requests[type];
        }
    };

    TResultWriterActor(const TRunActorParams& params, const TActorId& parent, const TActorId& connector, const TActorId& pinger, const NKikimr::NOperationId::TOperationId& operationId, bool operationEntryExpected, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "ResultWriter")
        , Params(params)
        , Parent(parent)
        , Connector(connector)
        , Pinger(pinger)
        , OperationId(operationId)
        , OperationEntryExpected(operationEntryExpected)
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_RESULT_WRITER_ACTOR";

    void Start() {
        LOG_I("Start result writer actor. Compute state: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        Become(&TResultWriterActor::StateFunc);
        SendGetOperation();
    }

    void SendGetOperation() {
        Register(new TRetryActor<TEvYdbCompute::TEvGetOperationRequest, TEvYdbCompute::TEvGetOperationResponse, NYdb::TOperation::TOperationId>(Counters.GetCounters(ERequestType::RT_GET_OPERATION), SelfId(), Connector, OperationId));
    }

    void WriteNextResultSet() {
        if (CurrentResultSetId < (int64_t)PingTaskRequest.result_set_meta_size()) {
            Register(new TResultSetWriterActor(Params,
                                                    CurrentResultSetId++,
                                                    SelfId(),
                                                    Connector,
                                                    OperationId,
                                                    GetBaseCounters()));
            return;
        }

        SendFinalPingRequest();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvGetOperationResponse, Handle);
        hFunc(TEvYdbCompute::TEvResultSetWriterResponse, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
    )

    void Handle(const TEvYdbCompute::TEvGetOperationResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (!OperationEntryExpected && response.Status == NYdb::EStatus::NOT_FOUND) {
            LOG_I("Operation has been already removed");
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse({}, NYdb::EStatus::SUCCESS));
            CompleteAndPassAway();
            return;
        }

        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't get operation: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(ev->Get()->Issues, ev->Get()->Status));
            FailedAndPassAway();
            return;
        }

        for (const auto& resultSetMeta: ev.Get()->Get()->ResultSetsMeta) {
            auto& meta = *PingTaskRequest.add_result_set_meta();
            for (const auto& column: resultSetMeta.Columns) {
                auto& new_column = *meta.add_column();
                new_column.set_name(column.Name);
                *new_column.mutable_type() = column.Type.GetProto();
            }
        }

        WriteNextResultSet();
    }

    void Handle(const TEvents::TEvForwardPingResponse::TPtr& ev) {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Dec();
        pingCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev.Get()->Get()->Success) {
            pingCounters->Ok->Inc();
            LOG_I("The result has been moved");
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse({}, NYdb::EStatus::SUCCESS));
            CompleteAndPassAway();
        } else {
            pingCounters->Error->Inc();
            LOG_E("Move result error");
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Move result error. OperationId: " << ProtoToString(OperationId)}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
        }
    }

    void Handle(const TEvYdbCompute::TEvResultSetWriterResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't fetch script result: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(ev->Get()->Issues, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
            return;
        }
        if (response.ResultSetId >= static_cast<ui64>(PingTaskRequest.result_set_meta_size())) {
            LOG_E("Can't fetch script result: internal error");
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(ev->Get()->Issues, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
            return;
        }
        auto* meta = PingTaskRequest.mutable_result_set_meta(response.ResultSetId);
        meta->set_rows_count(response.RowsCount);
        meta->set_truncated(response.Truncated);
        WriteNextResultSet();
    }

    void SendFinalPingRequest() {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Inc();
        PingTaskRequest.set_result_set_count(PingTaskRequest.result_set_meta_size());
        PingTaskRequest.mutable_result_id()->set_value(Params.ResultId);
        Send(Pinger, new TEvents::TEvForwardPingRequest(PingTaskRequest));
    }

private:
    TRunActorParams Params;
    int64_t CurrentResultSetId = 0;
    TActorId Parent;
    TActorId Connector;
    TActorId Pinger;
    NKikimr::NOperationId::TOperationId OperationId;
    const bool OperationEntryExpected;
    TCounters Counters;
    TInstant StartTime;
    TString FetchToken;
    Fq::Private::PingTaskRequest PingTaskRequest;
};

std::unique_ptr<NActors::IActor> CreateResultWriterActor(const TRunActorParams& params,
                                                         const TActorId& parent,
                                                         const TActorId& connector,
                                                         const TActorId& pinger,
                                                         const NKikimr::NOperationId::TOperationId& operationId,
                                                         bool operationEntryExpected,
                                                         const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TResultWriterActor>(params, parent, connector, pinger, operationId, operationEntryExpected, queryCounters);
}

}
