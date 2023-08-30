#include "source_id_encoding.h"
#include "writer.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NKikimr::NPQ {

#if defined(LOG_PREFIX) || defined(TRACE) || defined(DEBUG) || defined(INFO) || defined(ERROR)
#error "Already defined LOG_PREFIX or TRACE or DEBUG or INFO or ERROR"
#endif


#define LOG_PREFIX "TPartitionWriter " << TabletId << " (partition=" << PartitionId << ") "
#define TRACE(message) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PQ_WRITE_PROXY, LOG_PREFIX << message);
#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_WRITE_PROXY, LOG_PREFIX << message);
#define INFO(message)  LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_WRITE_PROXY, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_WRITE_PROXY, LOG_PREFIX << message);

static const ui64 WRITE_BLOCK_SIZE = 4_KB;    

TString TEvPartitionWriter::TEvInitResult::TSuccess::ToString() const {
    auto out = TStringBuilder() << "Success {"
        << " OwnerCookie: " << OwnerCookie
        << " SourceIdInfo: " << SourceIdInfo.ShortDebugString();
    if (WriteId != INVALID_WRITE_ID) {
        out << " WriteId: " << WriteId;
    }
    out << " }";
    return out;
}

TString TEvPartitionWriter::TEvInitResult::TError::ToString() const {
    return TStringBuilder() << "Error {"
        << " Reason: " << Reason
        << " Response: " << Response.ShortDebugString()
    << " }";
}

TString TEvPartitionWriter::TEvInitResult::ToString() const {
    auto out = TStringBuilder() << ToStringHeader() << " {";

    out << " SessionId: " << SessionId;
    out << " TxId: " << TxId;
    if (IsSuccess()) {
        out << " " << GetResult().ToString();
    } else {
        out << " " << GetError().ToString();
    }

    out << " }";
    return out;
}

TString TEvPartitionWriter::TEvWriteAccepted::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " SessionId: " << SessionId
        << " TxId: " << TxId
        << " Cookie: " << Cookie
    << " }";
}

TString TEvPartitionWriter::TEvWriteResponse::DumpError() const {
    Y_VERIFY(!IsSuccess());

    return TStringBuilder() << "Error {"
        << " SessionId: " << SessionId
        << " TxId: " << TxId
        << " Reason: " << GetError().Reason
        << " Response: " << Record.ShortDebugString()
    << " }";
}

TString TEvPartitionWriter::TEvWriteResponse::ToString() const {
    auto out = TStringBuilder() << ToStringHeader() << " {";

    out << " SessionId: " << SessionId;
    out << " TxId: " << TxId;
    if (IsSuccess()) {
        out << " Success { Response: " << Record.ShortDebugString() << " }";
    } else {
        out << " " << DumpError();
    }

    out << " }";
    return out;
}

class TPartitionWriter: public TActorBootstrapped<TPartitionWriter>, private TRlHelpers {

    static constexpr size_t MAX_QUOTA_INFLIGHT = 3;
    
    static void FillHeader(NKikimrClient::TPersQueuePartitionRequest& request,
            ui32 partitionId, const TActorId& pipeClient)
    {
        request.SetPartition(partitionId);
        ActorIdToProto(pipeClient, request.MutablePipeClient());
    }

    static void FillHeader(NKikimrClient::TPersQueuePartitionRequest& request,
            ui32 partitionId, const TActorId& pipeClient, const TString& ownerCookie, ui64 cookie)
    {
        FillHeader(request, partitionId, pipeClient);
        request.SetOwnerCookie(ownerCookie);
        request.SetCookie(cookie);
    }

    template <typename... Args>
    static THolder<TEvPersQueue::TEvRequest> MakeRequest(Args&&... args) {
        auto ev = MakeHolder<TEvPersQueue::TEvRequest>();
        FillHeader(*ev->Record.MutablePartitionRequest(), std::forward<Args>(args)...);

        return ev;
    }

    static bool BasicCheck(const NKikimrClient::TResponse& response, TString& error, bool mustHaveResponse = true) {
        if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
            error = TStringBuilder() << "Status is not ok"
                << ": status# " << static_cast<ui32>(response.GetStatus());
            return false;
        }

        if (response.GetErrorCode() != NPersQueue::NErrorCode::OK) {
            error = TStringBuilder() << "Error code is not ok"
                << ": code# " << static_cast<ui32>(response.GetErrorCode());
            return false;
        }

        if (mustHaveResponse && !response.HasPartitionResponse()) {
            error = "Absent partition response";
            return false;
        }

        return true;
    }

    static NKikimrClient::TResponse MakeResponse(ui64 cookie) {
        NKikimrClient::TResponse response;
        response.MutablePartitionResponse()->SetCookie(cookie);
        return response;
    }

    void BecomeZombie(const TEvPartitionWriter::TEvWriteResponse::EErrors errorCode, const TString& error) {
        ErrorCode = errorCode;

        SendError(error);
        Become(&TThis::StateZombie);
    }

    void SendError(const TString& error) {
        for (auto cookie : std::exchange(PendingWrite, {})) {
            SendWriteResult(ErrorCode, error, MakeResponse(cookie));
        }
        for (const auto& [cookie, _] : std::exchange(PendingReserve, {})) {
            SendWriteResult(ErrorCode, error, MakeResponse(cookie));
        }
        for (const auto& [cookie, _] : std::exchange(ReceivedReserve, {})) {
            SendWriteResult(ErrorCode, error, MakeResponse(cookie));
        }
        for (const auto& [cookie, _] : std::exchange(Pending, {})) {
            SendWriteResult(ErrorCode, error, MakeResponse(cookie));
        }
    }

    template <typename... Args>
    void SendInitResult(Args&&... args) {
        Send(Client, new TEvPartitionWriter::TEvInitResult(Opts.SessionId, Opts.TxId, std::forward<Args>(args)...));
    }

    void InitResult(const TString& reason, NKikimrClient::TResponse&& response) {
        SendInitResult(reason, std::move(response));
        BecomeZombie(TEvPartitionWriter::TEvWriteResponse::EErrors::InternalError, "Init error");
    }

    void InitResult(const TString& ownerCookie, const TEvPartitionWriter::TEvInitResult::TSourceIdInfo& sourceIdInfo, ui64 writeId) {
        SendInitResult(ownerCookie, sourceIdInfo, writeId);
    }

    template <typename... Args>
    void SendWriteResult(Args&&... args) {
        Send(Client, new TEvPartitionWriter::TEvWriteResponse(Opts.SessionId, Opts.TxId, std::forward<Args>(args)...));
    }

    void WriteResult(TEvPartitionWriter::TEvWriteResponse::EErrors errorCode, const TString& reason, NKikimrClient::TResponse&& response) {
        SendWriteResult(errorCode, reason, std::move(response));
        BecomeZombie(errorCode, "Write error");
    }

    void WriteResult(NKikimrClient::TResponse&& response) {
        SendWriteResult(std::move(response));
        PendingWrite.pop_front();
    }

    void WriteAccepted(ui64 cookie) {
        Send(Client, new TEvPartitionWriter::TEvWriteAccepted(Opts.SessionId, Opts.TxId, cookie));
    }

    void Disconnected(TEvPartitionWriter::TEvWriteResponse::EErrors errorCode) {
        Send(Client, new TEvPartitionWriter::TEvDisconnected());
        BecomeZombie(errorCode, "Disconnected");
    }

    /// GetWriteId

    void GetWriteId(const TActorContext& ctx) {
        auto ev = MakeWriteIdRequest();
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
        Become(&TThis::StateGetWriteId);
    }

    STATEFN(StateGetWriteId) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleWriteId);
            hFunc(TEvPartitionWriter::TEvWriteRequest, HoldPending);
        default:
            return StateBase(ev);
        }
    }

    void HandleWriteId(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto& record = ev->Get()->Record.GetRef();
        WriteId = record.GetResponse().GetTopicOperations().GetWriteId();

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY,
                    "SessionId: " << Opts.SessionId <<
                    " TxId: " << Opts.TxId <<
                    " WriteId: " << WriteId);

        GetOwnership();
    }

    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeWriteIdRequest() {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        if (Opts.Token) {
            ev->Record.SetUserToken(Opts.Token);
        }
        //ev->Record.SetRequestActorId(???);

        ev->Record.MutableRequest()->SetDatabase(CanonizePath(Opts.Database));
        ev->Record.MutableRequest()->SetSessionId(Opts.SessionId);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_UNDEFINED);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_TOPIC);

        if (Opts.TraceId) {
            ev->Record.SetTraceId(Opts.TraceId);
        }

        if (Opts.RequestType) {
            ev->Record.SetRequestType(Opts.RequestType);
        }

        //ev->Record.MutableRequest()->SetCancelAfterMs(???);
        //ev->Record.MutableRequest()->SetTimeoutMs(???);

        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(Opts.TxId);

        auto* topics = ev->Record.MutableRequest()->MutableTopicOperations()->AddTopics();
        topics->set_path(Opts.TopicPath);
        auto* partitions = topics->add_partitions();
        partitions->set_partition_id(PartitionId);

        return ev;
    }

    void SetWriteId(NKikimrClient::TPersQueuePartitionRequest& request) {
        if (WriteId != INVALID_WRITE_ID) {
            request.SetWriteId(WriteId);
        }
    }

    /// GetOwnership

    void GetOwnership() {
        auto ev = MakeRequest(PartitionId, PipeClient);

        auto& cmd = *ev->Record.MutablePartitionRequest()->MutableCmdGetOwnership();
        if (Opts.UseDeduplication) {
            cmd.SetOwner(SourceId);
        } else {
            cmd.SetOwner(CreateGuidAsString());
        }
        cmd.SetForce(true);

        NTabletPipe::SendData(SelfId(), PipeClient, ev.Release());
        Become(&TThis::StateGetOwnership);
    }

    STATEFN(StateGetOwnership) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvResponse, HandleOwnership);
            hFunc(TEvPartitionWriter::TEvWriteRequest, HoldPending);
        default:
            return StateBase(ev);
        }
    }

    void HandleOwnership(TEvPersQueue::TEvResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;

        TString error;
        if (!BasicCheck(record, error)) {
            return InitResult(error, std::move(record));
        }

        const auto& response = record.GetPartitionResponse();
        if (!response.HasCmdGetOwnershipResult()) {
            return InitResult("Absent Ownership result", std::move(record));
        }

        OwnerCookie = response.GetCmdGetOwnershipResult().GetOwnerCookie();
        GetMaxSeqNo();
    }

    /// GetMaxSeqNo

    void GetMaxSeqNo() {
        auto ev = MakeRequest(PartitionId, PipeClient);

        auto& cmd = *ev->Record.MutablePartitionRequest()->MutableCmdGetMaxSeqNo();
        cmd.AddSourceId(NSourceIdEncoding::EncodeSimple(SourceId));

        NTabletPipe::SendData(SelfId(), PipeClient, ev.Release());
        Become(&TThis::StateGetMaxSeqNo);
    }

    STATEFN(StateGetMaxSeqNo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, HandleMaxSeqNo);
            hFunc(TEvPartitionWriter::TEvWriteRequest, HoldPending);
        default:
            return StateBase(ev);
        }
    }

    void HandleMaxSeqNo(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;

        TString error;
        if (!BasicCheck(record, error)) {
            return InitResult(error, std::move(record));
        }

        const auto& response = record.GetPartitionResponse();
        if (!response.HasCmdGetMaxSeqNoResult()) {
            return InitResult("Absent MaxSeqNo result", std::move(record));
        }

        const auto& result = response.GetCmdGetMaxSeqNoResult();
        if (result.SourceIdInfoSize() < 1) {
            return InitResult("Empty source id info", std::move(record));
        }

        const auto& sourceIdInfo = result.GetSourceIdInfo(0);
        if (Opts.CheckState) {
            switch (sourceIdInfo.GetState()) {
            case NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED:
                Registered = true;
                break;
            case NKikimrPQ::TMessageGroupInfo::STATE_PENDING_REGISTRATION:
                if (Opts.AutoRegister) {
                    return RegisterMessageGroup();
                } else {
                    return InitResult("Source is not registered", std::move(record));
                }
            default:
                return InitResult("Unknown source state", std::move(record));
            }
        }

        InitResult(OwnerCookie, sourceIdInfo, WriteId);
        Become(&TThis::StateWork);

        if (Pending) {
            ReserveBytes(ctx);
        }
    }

    /// RegisterMessageGroup

    void RegisterMessageGroup() {
        if (Registered) {
            Y_VERIFY_DEBUG(false);
            return InitResult("Already registered", NKikimrClient::TResponse());
        }

        auto ev = MakeRequest(PartitionId, PipeClient);

        auto& cmd = *ev->Record.MutablePartitionRequest()->MutableCmdRegisterMessageGroup();
        cmd.SetId(NSourceIdEncoding::EncodeSimple(SourceId));
        // TODO cmd.SetPartitionKeyRange()
        cmd.SetAfterSplit(true);

        NTabletPipe::SendData(SelfId(), PipeClient, ev.Release());
        Become(&TThis::StateRegisterMessageGroup);
    }

    STATEFN(StateRegisterMessageGroup) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvResponse, HandleRegisterMessageGroup);
            hFunc(TEvPartitionWriter::TEvWriteRequest, HoldPending);
        default:
            return StateBase(ev);
        }
    }

    void HandleRegisterMessageGroup(TEvPersQueue::TEvResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;

        TString error;
        if (!BasicCheck(record, error, false)) {
            return InitResult(error, std::move(record));
        }

        Registered = true;
        GetMaxSeqNo();
    }

    /// Work

    STATEFN(StateWork) {
        DEBUG("Received event: " << (*ev.Get()).GetTypeName())
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPartitionWriter::TEvWriteRequest, Handle);
            hFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Reject(TEvPartitionWriter::TEvWriteRequest::TPtr& ev) {
        const auto cookie = ev->Get()->Record.GetPartitionRequest().GetCookie();
        return WriteResult(ErrorCode, "Rejected by writer", MakeResponse(cookie));
    }

    void HoldPending(TEvPartitionWriter::TEvWriteRequest::TPtr& ev) {
        auto& record = ev->Get()->Record;
        const auto cookie = record.GetPartitionRequest().GetCookie();

        Y_VERIFY(Pending.empty() || Pending.rbegin()->first < cookie);
        Y_VERIFY(PendingReserve.empty() || PendingReserve.rbegin()->first < cookie);
        Y_VERIFY(PendingWrite.empty() || PendingWrite.back() < cookie);

        Pending.emplace(cookie, std::move(ev->Get()->Record));
    }

    void Handle(TEvPartitionWriter::TEvWriteRequest::TPtr& ev, const TActorContext& ctx) {
        HoldPending(ev);
        ReserveBytes(ctx);
    }

    void ReserveBytes(const TActorContext& ctx) {
        if (IsQuotaInflight()) {
            return;
        }

        const bool checkQuota = Opts.CheckRequestUnits() && IsQuotaRequired();

        size_t processed = 0;
        PendingQuotaAmount = 0;

        while (Pending) {
            auto it = Pending.begin();

            FillHeader(*it->second.MutablePartitionRequest(), PartitionId, PipeClient, OwnerCookie, it->first);
            auto ev = MakeRequest(PartitionId, PipeClient, OwnerCookie, it->first);

            auto& request = *ev->Record.MutablePartitionRequest();
            request.SetMessageNo(MessageNo++);

            SetWriteId(request);

            auto& cmd = *request.MutableCmdReserveBytes();
            cmd.SetSize(it->second.ByteSize());
            cmd.SetLastRequest(false);

            if (checkQuota) {
                ++processed;
                PendingQuotaAmount += CalcRuConsumption(it->second.ByteSize());
                PendingQuota.emplace_back(it->first);
            }

            NTabletPipe::SendData(SelfId(), PipeClient, ev.Release());

            PendingReserve.emplace(it->first, RequestHolder{ std::move(it->second), checkQuota });
            Pending.erase(it);

            if (checkQuota && processed == MAX_QUOTA_INFLIGHT) {
                break;
            }
        }

        if (checkQuota) {
            RequestDataQuota(PendingQuotaAmount, ctx);
        }
    }

    void EnqueueReservedAndProcess(ui64 cookie) {
        Y_VERIFY(!PendingReserve.empty());
        auto it = PendingReserve.begin();

        Y_VERIFY(it->first == cookie);

        ReceivedReserve.emplace(it->first, std::move(it->second));

        ProcessQuota();
    }

    void ProcessQuota() {
        auto rit = ReceivedReserve.begin();
        auto qit = ReceivedQuota.begin();

        while(rit != ReceivedReserve.end() && qit != ReceivedQuota.end()) {
            auto& request = rit->second;
            const auto cookie = rit->first;
            TRACE("processing quota for request cookie=" << cookie << ", QuotaChecked=" << request.QuotaChecked << ", QuotaAccepted=" << request.QuotaAccepted);
            if (!request.QuotaChecked || request.QuotaAccepted) {
                // A situation when a quota was not requested or was received while waiting for a reserve
                Write(cookie, std::move(request.Request));
                ReceivedReserve.erase(rit++);
                continue;
            }

            if (cookie != *qit) {
                ERROR("The order of reservation and quota requests should be the same. ReserveCookie=" << cookie << ", QuotaCookie=" << *qit);
                Disconnected(TEvPartitionWriter::TEvWriteResponse::InternalError);
                return;
            }

            Write(cookie, std::move(request.Request));
            ReceivedReserve.erase(rit++);
            ++qit;
        }

        while(rit != ReceivedReserve.end()) {
            auto& request = rit->second;
            const auto cookie = rit->first;
            TRACE("processing quota for request cookie=" << cookie << ", QuotaChecked=" << request.QuotaChecked << ", QuotaAccepted=" << request.QuotaAccepted);
            if (request.QuotaChecked && !request.QuotaAccepted) {
                break;
            }

            // A situation when a quota was not requested or was received while waiting for a reserve
            Write(cookie, std::move(request.Request));
            ReceivedReserve.erase(rit++);
        }

        while(qit != ReceivedQuota.end()) {
            auto cookie = *qit;
            TRACE("processing quota for request cookie=" << cookie);
            auto pit = PendingReserve.find(cookie);

            if (pit == PendingReserve.end()) {
                ERROR("The received quota does not apply to any request. Cookie=" << *qit);
                Disconnected(TEvPartitionWriter::TEvWriteResponse::InternalError);
                return;
            }

            pit->second.QuotaAccepted = true;
            ++qit;
        }

        ReceivedQuota.clear();
    }

    void Write(ui64 cookie) {
        Y_VERIFY(!PendingReserve.empty());
        auto it = PendingReserve.begin();

        Y_VERIFY(it->first == cookie);
        Y_VERIFY(PendingWrite.empty() || PendingWrite.back() < cookie);

        Write(cookie, std::move(it->second.Request));

        PendingReserve.erase(it);
    }

    void Write(ui64 cookie, NKikimrClient::TPersQueueRequest&& req) {
        auto ev = MakeHolder<TEvPersQueue::TEvRequest>();
        ev->Record = req;

        auto& request = *ev->Record.MutablePartitionRequest();
        request.SetMessageNo(MessageNo++);

        SetWriteId(request);

        if (!Opts.UseDeduplication) {
            request.SetPartition(PartitionId);
        }
        NTabletPipe::SendData(SelfId(), PipeClient, ev.Release());

        PendingWrite.emplace_back(cookie);
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;

        TString error;
        if (!BasicCheck(record, error)) {
            return WriteResult(TEvPartitionWriter::TEvWriteResponse::InternalError, error, std::move(record));
        }

        const auto& response = record.GetPartitionResponse();
        if (!response.CmdWriteResultSize()) {
            if (PendingReserve.empty()) {
                return WriteResult(TEvPartitionWriter::TEvWriteResponse::InternalError, "Unexpected ReserveBytes response", std::move(record));
            }

            const auto cookie = PendingReserve.begin()->first;
            if (cookie != response.GetCookie()) {
                error = TStringBuilder() << "Unexpected cookie at ReserveBytes"
                    << ": expected# " << cookie
                    << ", got# " << response.GetCookie();
                return WriteResult(TEvPartitionWriter::TEvWriteResponse::InternalError, error, std::move(record));
            }

            WriteAccepted(cookie);

            Y_VERIFY(!PendingReserve.empty());
            auto it = PendingReserve.begin();
            auto& holder = it->second;

            if ((holder.QuotaChecked && !holder.QuotaAccepted)|| !ReceivedReserve.empty()) {
                // There may be two situations:
                // - a quota has been requested, and the quota has not been received yet
                // - the quota was not requested, for example, due to a change in the metering option, but the previous quota requests have not yet been processed
                EnqueueReservedAndProcess(cookie);
            } else {
                Write(cookie);
            }
        } else {
            if (PendingWrite.empty()) {
                return WriteResult(TEvPartitionWriter::TEvWriteResponse::InternalError, "Unexpected Write response", std::move(record));
            }

            const auto cookie = PendingWrite.front();
            if (cookie != response.GetCookie()) {
                error = TStringBuilder() << "Unexpected cookie at Write"
                    << ": expected# " << cookie
                    << ", got# " << response.GetCookie();
                return WriteResult(TEvPartitionWriter::TEvWriteResponse::InternalError, error, std::move(record));
            }

            WriteResult(std::move(record));
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        auto msg = ev->Get();
        DEBUG("TEvClientConnected Status " << msg->Status << ", TabletId: " << msg->TabletId << ", NodeId " << msg->ServerId.NodeId() << ", Generation: " << msg->Generation);
        Y_VERIFY_DEBUG(msg->TabletId == TabletId);

        if (msg->Status != NKikimrProto::OK) {
            ERROR("received TEvClientConnected with status " << ev->Get()->Status);
            Disconnected(TEvPartitionWriter::TEvWriteResponse::InternalError);
            return;
        }

        Y_VERIFY_DEBUG_S(msg->Generation, "Tablet generation should be greater than 0");

        if (ExpectedGeneration)
        {
            if(*ExpectedGeneration != msg->Generation)
            {
                INFO("received TEvClientConnected with wrong generation. Expected: " << *ExpectedGeneration << ", received " << msg->Generation);
                Disconnected(TEvPartitionWriter::TEvWriteResponse::PartitionNotLocal);
                PassAway();
            }
            if (NActors::TActivationContext::ActorSystem()->NodeId != msg->ServerId.NodeId())
            {
                INFO("received TEvClientConnected with wrong NodeId. Expected: " << NActors::TActivationContext::ActorSystem()->NodeId << ", received " << msg->ServerId.NodeId());
                Disconnected(TEvPartitionWriter::TEvWriteResponse::PartitionNotLocal);
                PassAway();
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->TabletId == TabletId) {
            DEBUG("received TEvClientDestroyed");
            Disconnected(TEvPartitionWriter::TEvWriteResponse::PartitionDisconnected);
        }
    }

    void PassAway() override {
        if (PipeClient) {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeClient);
        }
        SendError("Unexpected termination");
        TActorBootstrapped::PassAway();
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
        OnWakeup(tag);
        switch (tag) {
            case EWakeupTag::RlAllowed:
                ReceivedQuota.insert(ReceivedQuota.end(), PendingQuota.begin(), PendingQuota.end());
                PendingQuota.clear();

                ProcessQuota();                

                break;

            case EWakeupTag::RlNoResource:
                // Re-requesting the quota. We do this until we get a quota. 
                // We do not request a quota with a long waiting time because the writer may already be a destroyer, and the quota will still be waiting to be received.
                RequestDataQuota(PendingQuotaAmount, ctx);
                break;
            
            default:
                Y_VERIFY_DEBUG_S(false, "Unsupported tag: " << static_cast<ui64>(tag));
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_PARTITION_WRITER_ACTOR;
    }

    explicit TPartitionWriter(
            const TActorId& client,
            const std::optional<TString>& topicPath,
            ui64 tabletId,
            ui32 partitionId,
            const std::optional<ui32> expectedGeneration,
            const TString& sourceId,
            const TPartitionWriterOpts& opts)
        : TRlHelpers(topicPath, opts.RlCtx, WRITE_BLOCK_SIZE, !!opts.RlCtx)
        , Client(client)
        , TabletId(tabletId)
        , PartitionId(partitionId)
        , ExpectedGeneration(expectedGeneration)
        , SourceId(sourceId)
        , Opts(opts)
    {
        if (Opts.MeteringMode) {
            SetMeteringMode(*Opts.MeteringMode);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };

        PipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), TabletId, config));

        if (Opts.Database && Opts.SessionId && Opts.TxId) {
            GetWriteId(ctx);
        } else {
            GetOwnership();
        }
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvNotifyUpdate, TRlHelpers::Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    STATEFN(StateZombie) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPartitionWriter::TEvWriteRequest, Reject);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:

    const TActorId Client;
    const ui64 TabletId;
    const ui32 PartitionId;
    const std::optional<ui32> ExpectedGeneration;
    const TString SourceId;
    const TPartitionWriterOpts Opts;

    TActorId PipeClient;
    TString OwnerCookie;
    bool Registered = false;
    ui64 MessageNo = 0;

    struct RequestHolder {
        NKikimrClient::TPersQueueRequest Request;
        bool QuotaChecked;
        bool QuotaAccepted;

        RequestHolder(NKikimrClient::TPersQueueRequest&& request, bool quotaChecked)
            : Request(request)
            , QuotaChecked(quotaChecked)
            , QuotaAccepted(false) {
        }
    };

    TMap<ui64, NKikimrClient::TPersQueueRequest> Pending;
    TMap<ui64, RequestHolder> PendingReserve;
    TMap<ui64, RequestHolder> ReceivedReserve;
    TDeque<ui64> PendingQuota;
    ui64 PendingQuotaAmount;
    TDeque<ui64> ReceivedQuota;
    TDeque<ui64> PendingWrite;

    TEvPartitionWriter::TEvWriteResponse::EErrors ErrorCode = TEvPartitionWriter::TEvWriteResponse::EErrors::InternalError;

    ui64 WriteId = INVALID_WRITE_ID;
}; // TPartitionWriter

IActor* CreatePartitionWriter(const TActorId& client, const std::optional<TString>& topicPath, ui64 tabletId, ui32 partitionId, 
                              const std::optional<ui32> expectedGeneration, const TString& sourceId,
                              const TPartitionWriterOpts& opts) {
    return new TPartitionWriter(client, topicPath, tabletId, partitionId, expectedGeneration, sourceId, opts);
}

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

}
