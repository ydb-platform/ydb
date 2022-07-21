#include <ydb/core/yq/libs/config/protos/yq_config.pb.h>
#include "proxy.h"

#include <ydb/core/protos/services.pb.h>
#include <ydb/core/yq/libs/common/rows_proto_splitter.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/private_client/internal_service.h>

#define LOG_E(stream)                                                        \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Writer: " << TraceId << ": " << stream)
#define LOG_I(stream)                                                        \
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Writer: " << TraceId << ": " << stream)
#define LOG_D(stream)                                                        \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Writer: " << TraceId << ": " << stream)

namespace NYq {

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

    void Bootstrap(const TActorContext&) {
        LOG_I("Bootstrap");
        Become(&TResultWriter::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        HFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)

        HFunc(NDq::TEvDqCompute::TEvChannelData, OnChannelData)
        HFunc(TEvReadyState, OnReadyState);
        HFunc(TEvQueryResponse, OnQueryResult);

        hFunc(NFq::TEvInternalService::TEvWriteResultResponse, HandleResponse);
    )

    void PassAway() {
        auto duration = (TInstant::Now()-StartTime);
        LOG_I("FinishWrite, Records: " << RowIndex << " HasError: " << HasError << " Size: " << Size << " Rows: " << Rows << " FreeSpace: " << FreeSpace << " Duration: " << duration << " AvgSpeed: " << Size/(duration.Seconds()+1)/1024/1024);
        NActors::IActor::PassAway();
    }

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&, const NActors::TActorContext& ) {
        auto req = MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::UNAVAILABLE, TIssue("Undelivered").SetCode(NYql::DEFAULT_ERROR, TSeverityIds::S_ERROR));
        Send(ExecuterId, req.Release());
        HasError = true;
    }

    void SendIssuesAndSetErrorFlag(const TIssues& issues) {
        LOG_E("ControlPlane WriteResult Issues: " << issues.ToString());
        Issues.AddIssues(issues);
        HasError = true;
        auto req = MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::INTERNAL_ERROR, Issues);
        Send(ExecuterId, req.Release());
    }

    void MaybeFinish() {
        if (Finished && Requests.empty()) {
            Send(ExecuterId, new TEvGraphFinished());
        }
    }

    void OnQueryResult(TEvQueryResponse::TPtr& ev, const TActorContext&) {
        Finished = true;
        NYql::NDqProto::TQueryResponse queryResult(ev->Get()->Record);

        *queryResult.MutableYson() = ResultBuilder->BuildYson(Head);
        if (!Issues.Empty()) {
            IssuesToMessage(Issues, queryResult.MutableIssues());
        }
        queryResult.SetTruncated(Truncated);
        queryResult.SetRowsCount(Rows);

        Send(ExecuterId, new TEvQueryResponse(std::move(queryResult)));
    }

    void OnReadyState(TEvReadyState::TPtr&, const TActorContext&) { }

    void HandleResponse(NFq::TEvInternalService::TEvWriteResultResponse::TPtr& ev) {
        const auto& issues = ev->Get()->Status.GetIssues();
        if (issues) {
            SendIssuesAndSetErrorFlag(issues);
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

                LOG_D("ChannelData, Records: " << RowIndex
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
        if (InflightCounter || CurChunkInd >= ResultChunks.size()) {
            return;
        }
        ++InflightCounter;
        Send(InternalServiceId, new NFq::TEvInternalService::TEvWriteResultRequest(std::move(ResultChunks[CurChunkInd++])));
    }

    void ConstructResults(const Ydb::ResultSet& resultSet, ui64 startRowIndex) {
        const size_t baseProtoByteSize = CreateProtoRequestWithoutResultSet(startRowIndex).ByteSizeLong();

        NYq::TRowsProtoSplitter rowsSplitter(resultSet, ProtoMessageLimit, baseProtoByteSize, MaxRowsCountPerChunk);
        const auto splittedResultSets = rowsSplitter.Split();

        if (!splittedResultSets.Success) {
            SendIssuesAndSetErrorFlag(splittedResultSets.Issues);
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
        auto& data = ev->Get()->Record.GetChannelData().GetData();
        auto resultSet = ResultBuilder->BuildResultSet({data});
        FreeSpace -= data.GetRaw().size();
        OccupiedSpace += data.GetRaw().size();

        if (OccupiedSpace > ResultBytesLimit) {
            TIssues issues;
            issues.AddIssue(TStringBuilder() << "Can not write results with size > " << ResultBytesLimit << " byte(s)");
            SendIssuesAndSetErrorFlag(issues);
            return;
        }

        ui64 startRowIndex = RowIndex;
        RowIndex += resultSet.rows().size();

        auto& request = Requests[Cookie];
        request.Sender = ev->Sender;
        request.ChannelId = ev->Get()->Record.GetChannelData().GetChannelId();
        request.SeqNo = ev->Get()->Record.GetSeqNo();
        request.Size = data.GetRaw().size();

        ConstructResults(resultSet, startRowIndex);
        SendResult();

        if (!Truncated &&
            (!AllResultsBytesLimit || Size + data.GetRaw().size() < *AllResultsBytesLimit)
            && (!RowsLimitPerWrite || Rows + data.GetRows() < *RowsLimitPerWrite)) {
            Head.push_back(data);
        } else {
            Truncated = true;
        }

        Size += data.GetRaw().size();
        Rows += data.GetRows();
        Cookie++;
    }

    void OnChannelData(NDq::TEvDqCompute::TEvChannelData::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        if (HasError) {
            StopChannel(ev);
            return;
        }

        try {
            if (ev->Get()->Record.GetChannelData().HasData()) {
                ProcessData(ev);
            } else {
                auto res = MakeHolder<NDq::TEvDqCompute::TEvChannelDataAck>();
                res->Record.SetChannelId(ev->Get()->Record.GetChannelData().GetChannelId());
                res->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
                res->Record.SetFreeSpace(FreeSpace);
                res->Record.SetFinish(HasError);
                Send(ev->Sender, res.Release());

                auto duration = (TInstant::Now()-StartTime);

                LOG_D("ChannelData, Records: " << RowIndex
                    << " HasError: " << HasError
                    << " Size: " << Size
                    << " Rows: " << Rows
                    << " FreeSpace: " << FreeSpace
                    << " Duration: " << duration
                    << " AvgSpeed: " << Size/(duration.Seconds()+1)/1024/1024);
            }
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

    TVector<NYql::NDqProto::TData> Head;
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

    TVector<Fq::Private::WriteTaskResultRequest> ResultChunks;
    size_t CurChunkInd = 0;
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

} // namespace NYq
