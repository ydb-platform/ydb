#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include "proxy.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/fq/libs/common/rows_proto_splitter.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/yson/node/node_io.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/private_client/internal_service.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RESULT_WRITER, "TraceId: " << TraceId << " " << stream)
#define LOG_I(stream) LOG_INFO_S (*TlsActivationContext, NKikimrServices::FQ_RESULT_WRITER, "TraceId: " << TraceId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RESULT_WRITER, "TraceId: " << TraceId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RESULT_WRITER, "TraceId: " << TraceId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NYql;
using namespace NDqs;

class TResultWriter : public NActors::TActorBootstrapped<TResultWriter> {
public:
    TResultWriter(
        const NActors::TActorId& executerId,
        const TString& resultType,
        const TResultId& resultId,
        const TVector<TString>& columns,
        const TString& traceId,
        const TInstant& deadline,
        ui64 resultBytesLimit)
        : ExecuterId(executerId)
        , ResultBuilder(MakeHolder<TProtoBuilder>(resultType, columns))
        , ResultId({resultId})
        , TraceId(traceId)
        , Deadline(deadline)
        , ResultBytesLimit(resultBytesLimit)
        , InternalServiceId(NFq::MakeInternalServiceActorId())
    {
        if (!ResultBytesLimit) {
            ResultBytesLimit = 20_MB;
        }
    }

    static constexpr char ActorName[] = "YQ_RESULT_WRITER";

    void Bootstrap() {
        LOG_I("Bootstrap");
        Become(&TResultWriter::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)

        hFunc(NDq::TEvDqCompute::TEvChannelData, OnChannelData)
        hFunc(TEvReadyState, OnReadyState);
        hFunc(TEvQueryResponse, OnQueryResult);

        hFunc(NFq::TEvInternalService::TEvWriteResultResponse, HandleResponse);
    )

    void PassAway() {
        auto duration = (TInstant::Now()-StartTime);
        LOG_I("FinishWrite, Records: " << RowIndex
                << " HasError: " << HasError
                << " Size: " << Size
                << " Rows: " << Rows
                << " FreeSpace: " << FreeSpace
                << " Duration: " << duration
                << " AvgSpeed: " << Size/(duration.Seconds()+1)/1024/1024
                << " ResultChunks.size(): " << ResultChunks.size());
        NActors::IActor::PassAway();
    }

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&) {
        auto req = MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::UNAVAILABLE, TIssue("Undelivered").SetCode(NYql::DEFAULT_ERROR, TSeverityIds::S_ERROR));
        Send(ExecuterId, req.Release());
        HasError = true;
    }

    void SendIssuesAndSetErrorFlag(const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode statusCode = NYql::NDqProto::StatusIds::INTERNAL_ERROR) {
        LOG_E("ControlPlane WriteResult Issues: " << issues.ToString());
        Issues.AddIssues(issues);
        HasError = true;
        auto req = MakeHolder<TEvDqFailure>(statusCode, Issues);
        Send(ExecuterId, req.Release());
    }

    void MaybeFinish() {
        if (Finished && Requests.empty()) {
            Send(ExecuterId, new TEvGraphFinished());
        }
    }

    void OnQueryResult(TEvQueryResponse::TPtr& ev) {
        Finished = true;
        NYql::NDqProto::TQueryResponse queryResult(ev->Get()->Record);

        for (const auto& x : Head) {
            queryResult.AddSample()->CopyFrom(x.Proto);
        }

        Head.clear();
        if (!Issues.Empty()) {
            IssuesToMessage(Issues, queryResult.MutableIssues());
        }
        queryResult.SetTruncated(Truncated);
        queryResult.SetRowsCount(Rows);

        LOG_D("Send response to executer");
        Send(ExecuterId, new TEvQueryResponse(std::move(queryResult)));
    }

    void OnReadyState(TEvReadyState::TPtr&) { }

    void HandleResponse(NFq::TEvInternalService::TEvWriteResultResponse::TPtr& ev) {
        auto statusCode = ev->Get()->Result.status_code();
        const auto& issues = ev->Get()->Status.GetIssues();
        if (issues && statusCode == NYql::NDqProto::StatusIds::UNSPECIFIED) {
            statusCode = NYql::NDqProto::StatusIds::EXTERNAL_ERROR;
        }
        if (statusCode != NYql::NDqProto::StatusIds::UNSPECIFIED) {
            SendIssuesAndSetErrorFlag(issues, statusCode);
            return;
        }

        auto it = Requests.find(ev->Get()->Result.request_id());
        if (it == Requests.end()) {
            HasError = true;
            auto req = MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::BAD_REQUEST, TIssue("Unknown RequestId").SetCode(NYql::DEFAULT_ERROR, TSeverityIds::S_ERROR));
            Send(ExecuterId, req.Release());
            return;
        }
        auto& request = it->second;
        if (request.MessagesNum > 0)
            --request.MessagesNum;

        --InflightCounter;

        if (request.MessagesNum == 0) {
            FreeSpace += request.Size;

            if (FreeSpace > 0) {
                auto res = MakeHolder<NDq::TEvDqCompute::TEvChannelDataAck>();
                res->Record.SetChannelId(request.ChannelId);
                res->Record.SetFreeSpace(FreeSpace);
                res->Record.SetSeqNo(request.SeqNo);
                res->Record.SetFinish(HasError);
                Send(request.Sender, res.Release());

                auto duration = (TInstant::Now()-StartTime);

                LOG_T("ChannelData, Records: " << RowIndex
                    << " HasError: " << HasError
                    << " Size: " << Size
                    << " Rows: " << Rows
                    << " FreeSpace: " << FreeSpace
                    << " Duration: " << duration
                    << " AvgSpeed: " << Size/(duration.Seconds()+1)/1024/1024);
            }

            Requests.erase(it);
            MaybeFinish();
        }
        SendResult(); // Send remaining rows
    }

    void StopChannel(NDq::TEvDqCompute::TEvChannelData::TPtr& ev) {
        auto res = MakeHolder<NDq::TEvDqCompute::TEvChannelDataAck>();
        res->Record.SetChannelId(ev->Get()->Record.GetChannelData().GetChannelId());
        res->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        res->Record.SetFreeSpace(0);
        res->Record.SetFinish(true);
        Send(ev->Sender, res.Release());
    }

    Fq::Private::WriteTaskResultRequest CreateProtoRequestWithoutResultSet(ui64 startRowIndex) {
        Fq::Private::WriteTaskResultRequest protoReq;
        protoReq.set_owner_id(ResultId.Owner);
        protoReq.mutable_result_id()->set_value(ResultId.Id);
        protoReq.set_offset(startRowIndex);
        protoReq.set_result_set_id(ResultId.SetId);
        protoReq.set_request_id(Cookie);
        *protoReq.mutable_deadline() = NProtoInterop::CastToProto(Deadline);
        return protoReq;
    }

    void SendResult() {
        if (InflightCounter || !ResultChunks) {
            return;
        }
        while (ResultChunks) {
            const auto& chunk = ResultChunks.front();
            // if owner is not empty, then there is data to send to storage, otherwise just shift seqno
            if (chunk.owner_id()) {
                break;
            }
            const auto& request = Requests[chunk.request_id()];
            auto res = MakeHolder<NDq::TEvDqCompute::TEvChannelDataAck>();
            res->Record.SetChannelId(request.ChannelId);
            res->Record.SetFreeSpace(FreeSpace);
            res->Record.SetSeqNo(request.SeqNo);
            res->Record.SetFinish(HasError);
            Send(request.Sender, res.Release());
            Requests.erase(chunk.request_id());

            auto duration = (TInstant::Now()-StartTime);

            LOG_T("ChannelData Shift, Records: " << RowIndex
                << " HasError: " << HasError
                << " Size: " << Size
                << " Rows: " << Rows
                << " FreeSpace: " << FreeSpace
                << " Duration: " << duration
                << " AvgSpeed: " << Size/(duration.Seconds()+1)/1024/1024);
            ResultChunks.pop_front();
        }

        if (!ResultChunks) {
            MaybeFinish();
            return;
        }
        ++InflightCounter;
        auto chunk = std::move(ResultChunks.front());
        ResultChunks.pop_front();
        Send(InternalServiceId, new NFq::TEvInternalService::TEvWriteResultRequest(std::move(chunk)));
    }

    void ConstructResults(const Ydb::ResultSet& resultSet, ui64 startRowIndex) {
        const size_t baseProtoByteSize = CreateProtoRequestWithoutResultSet(startRowIndex).ByteSizeLong();

        NFq::TRowsProtoSplitter rowsSplitter(resultSet, ProtoMessageLimit, baseProtoByteSize, MaxRowsCountPerChunk);
        const auto splittedResultSets = rowsSplitter.Split();

        if (!splittedResultSets.Success) {
            SendIssuesAndSetErrorFlag(splittedResultSets.Issues, NYql::NDqProto::StatusIds::LIMIT_EXCEEDED);
            return;
        }

        for (const auto& resultSet : splittedResultSets.ResultSets) {
            auto protoReq = CreateProtoRequestWithoutResultSet(startRowIndex);
            startRowIndex += resultSet.rows().size();
            *protoReq.mutable_result_set() = resultSet;
            ResultChunks.emplace_back(std::move(protoReq));
        }
        Requests[Cookie].MessagesNum = splittedResultSets.ResultSets.size();
    }

    void ProcessData(NDq::TEvDqCompute::TEvChannelData::TPtr& ev) {
        if (!ev->Get()->Record.GetChannelData().HasData()) {
            auto& request = Requests[Cookie];
            request.Sender = ev->Sender;
            request.ChannelId = ev->Get()->Record.GetChannelData().GetChannelId();
            request.SeqNo = ev->Get()->Record.GetSeqNo();
            request.Size = 0;
            ResultChunks.emplace_back();
            ResultChunks.back().set_request_id(Cookie);
            SendResult();
            Cookie++;
            return;
        }

        NDq::TDqSerializedBatch data;
        data.Proto = std::move(*ev->Get()->Record.MutableChannelData()->MutableData());
        if (data.Proto.HasPayloadId()) {
            data.Payload = ev->Get()->GetPayload(data.Proto.GetPayloadId());
        }

        FreeSpace -= data.Size();
        OccupiedSpace += data.Size();
        auto resultSet = ResultBuilder->BuildResultSet({ data });

        if (OccupiedSpace > ResultBytesLimit) {
            TIssues issues;
            issues.AddIssue(TStringBuilder() << "Can not write results with size > " << ResultBytesLimit << " byte(s), please write results to Object Storage by INSERT INTO binding.'' statement");
            SendIssuesAndSetErrorFlag(issues, NYql::NDqProto::StatusIds::LIMIT_EXCEEDED);
            return;
        }

        ui64 startRowIndex = RowIndex;
        RowIndex += resultSet.rows().size();

        auto& request = Requests[Cookie];
        request.Sender = ev->Sender;
        request.ChannelId = ev->Get()->Record.GetChannelData().GetChannelId();
        request.SeqNo = ev->Get()->Record.GetSeqNo();
        request.Size = data.Size();

        ConstructResults(resultSet, startRowIndex);
        SendResult();

        Size += data.Size();
        Rows += data.RowCount();

        if (!Truncated &&
            (!AllResultsBytesLimit || Size + data.Size() < *AllResultsBytesLimit)
            && (!RowsLimitPerWrite || Rows + data.RowCount() < *RowsLimitPerWrite)) {
            Head.push_back(std::move(data));
        } else {
            Truncated = true;
        }

        Cookie++;
    }

    void OnChannelData(NDq::TEvDqCompute::TEvChannelData::TPtr& ev) {
        if (HasError) {
            StopChannel(ev);
            return;
        }

        try {
            ProcessData(ev);
        } catch (...) {
            LOG_E(CurrentExceptionMessage());
            auto req = MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssue("Internal error on data write").SetCode(NYql::DEFAULT_ERROR, TSeverityIds::S_ERROR));
            Send(ExecuterId, req.Release());
            HasError = true;
        }
    }

    struct TRequest {
        TActorId Sender;
        ui64 ChannelId;
        ui64 SeqNo;
        i64 Size;
        size_t MessagesNum;
    };
    THashMap<ui64, TRequest> Requests;

    TVector<NDq::TDqSerializedBatch> Head;
    bool Truncated = false;
    TMaybe<ui64> AllResultsBytesLimit = 10000;
    TMaybe<ui64> RowsLimitPerWrite = 1000;
    ui64 Size = 0;
    ui64 Rows = 0;

    const TActorId ExecuterId;
    THolder<TProtoBuilder> ResultBuilder;
    const TResultId ResultId;
    const TString TraceId;
    TInstant Deadline;
    const TInstant StartTime = TInstant::Now();

    ui64 RowIndex = 0;
    ui64 Cookie = 0;
    i64 FreeSpace = 64_MB; // TODO: make global free space
    const size_t ProtoMessageLimit = 10_MB;
    const size_t MaxRowsCountPerChunk = 100'000;
    bool HasError = false;
    bool Finished = false;
    NYql::TIssues Issues;
    ui64 ResultBytesLimit;
    ui64 OccupiedSpace = 0;

    TDeque<Fq::Private::WriteTaskResultRequest> ResultChunks;
    ui32 InflightCounter = 0;
    TActorId InternalServiceId;
};

NActors::IActor* CreateResultWriter(
    const NActors::TActorId& executerId,
    const TString& resultType,
    const TResultId& resultId,
    const TVector<TString>& columns,
    const TString& traceId,
    const TInstant& deadline,
    ui64 resultBytesLimit)
{
    return new TResultWriter(executerId, resultType, resultId, columns, traceId, deadline, resultBytesLimit);
}

} // namespace NFq
