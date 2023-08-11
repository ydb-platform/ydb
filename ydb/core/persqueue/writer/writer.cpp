#include "source_id_encoding.h"
#include "writer.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NKikimr::NPQ {

TString TEvPartitionWriter::TEvInitResult::TSuccess::ToString() const {
    return TStringBuilder() << "Success {"
        << " OwnerCookie: " << OwnerCookie
        << " SourceIdInfo: " << SourceIdInfo.ShortDebugString()
    << " }";
}

TString TEvPartitionWriter::TEvInitResult::TError::ToString() const {
    return TStringBuilder() << "Error {"
        << " Reason: " << Reason
        << " Response: " << Response.ShortDebugString()
    << " }";
}

TString TEvPartitionWriter::TEvInitResult::ToString() const {
    auto out = TStringBuilder() << ToStringHeader() << " {";

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
        << " Cookie: " << Cookie
    << " }";
}

TString TEvPartitionWriter::TEvWriteResponse::DumpError() const {
    Y_VERIFY(!IsSuccess());

    return TStringBuilder() << "Error {"
        << " Reason: " << GetError().Reason
        << " Response: " << Record.ShortDebugString()
    << " }";
}

TString TEvPartitionWriter::TEvWriteResponse::ToString() const {
    auto out = TStringBuilder() << ToStringHeader() << " {";

    if (IsSuccess()) {
        out << " Success { Response: " << Record.ShortDebugString() << " }";
    } else {
        out << " " << DumpError();
    }

    out << " }";
    return out;
}

class TPartitionWriter: public TActorBootstrapped<TPartitionWriter> {
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

    void BecomeZombie(const TString& error) {
        SendError(error);
        Become(&TThis::StateZombie);
    }

    void SendError(const TString& error) {
        for (auto cookie : std::exchange(PendingWrite, {})) {
            SendWriteResult(error, MakeResponse(cookie));
        }
        for (const auto& [cookie, _] : std::exchange(PendingReserve, {})) {
            SendWriteResult(error, MakeResponse(cookie));
        }
        for (const auto& [cookie, _] : std::exchange(Pending, {})) {
            SendWriteResult(error, MakeResponse(cookie));
        }
    }

    template <typename... Args>
    void SendInitResult(Args&&... args) {
        Send(Client, new TEvPartitionWriter::TEvInitResult(std::forward<Args>(args)...));
    }

    void InitResult(const TString& reason, NKikimrClient::TResponse&& response) {
        SendInitResult(reason, std::move(response));
        BecomeZombie("Init error");
    }

    void InitResult(const TString& ownerCookie, const TEvPartitionWriter::TEvInitResult::TSourceIdInfo& sourceIdInfo) {
        SendInitResult(ownerCookie, sourceIdInfo);
    }

    template <typename... Args>
    void SendWriteResult(Args&&... args) {
        Send(Client, new TEvPartitionWriter::TEvWriteResponse(std::forward<Args>(args)...));
    }

    void WriteResult(const TString& reason, NKikimrClient::TResponse&& response) {
        SendWriteResult(reason, std::move(response));
        BecomeZombie("Write error");
    }

    void WriteResult(NKikimrClient::TResponse&& response) {
        SendWriteResult(std::move(response));
        PendingWrite.pop_front();
    }

    void WriteAccepted(ui64 cookie) {
        Send(Client, new TEvPartitionWriter::TEvWriteAccepted(cookie));
    }

    void Disconnected() {
        Send(Client, new TEvPartitionWriter::TEvDisconnected());
        BecomeZombie("Disconnected");
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
            hFunc(TEvPersQueue::TEvResponse, HandleMaxSeqNo);
            hFunc(TEvPartitionWriter::TEvWriteRequest, HoldPending);
        default:
            return StateBase(ev);
        }
    }

    void HandleMaxSeqNo(TEvPersQueue::TEvResponse::TPtr& ev) {
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

        InitResult(OwnerCookie, sourceIdInfo);
        Become(&TThis::StateWork);

        if (Pending) {
            ReserveBytes();
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
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPartitionWriter::TEvWriteRequest, Handle);
            hFunc(TEvPersQueue::TEvResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Reject(TEvPartitionWriter::TEvWriteRequest::TPtr& ev) {
        const auto cookie = ev->Get()->Record.GetPartitionRequest().GetCookie();
        return WriteResult("Rejected by writer", MakeResponse(cookie));
    }

    void HoldPending(TEvPartitionWriter::TEvWriteRequest::TPtr& ev) {
        auto& record = ev->Get()->Record;
        const auto cookie = record.GetPartitionRequest().GetCookie();

        Y_VERIFY(Pending.empty() || Pending.rbegin()->first < cookie);
        Y_VERIFY(PendingReserve.empty() || PendingReserve.rbegin()->first < cookie);
        Y_VERIFY(PendingWrite.empty() || PendingWrite.back() < cookie);

        Pending.emplace(cookie, std::move(ev->Get()->Record));
    }

    void Handle(TEvPartitionWriter::TEvWriteRequest::TPtr& ev) {
        HoldPending(ev);
        ReserveBytes();
    }

    void ReserveBytes() {
        while (Pending) {
            auto it = Pending.begin();

            FillHeader(*it->second.MutablePartitionRequest(), PartitionId, PipeClient, OwnerCookie, it->first);
            auto ev = MakeRequest(PartitionId, PipeClient, OwnerCookie, it->first);

            auto& request = *ev->Record.MutablePartitionRequest();
            request.SetMessageNo(MessageNo++);

            auto& cmd = *request.MutableCmdReserveBytes();
            cmd.SetSize(it->second.ByteSize());
            cmd.SetLastRequest(false);

            NTabletPipe::SendData(SelfId(), PipeClient, ev.Release());

            PendingReserve.emplace(it->first, std::move(it->second));
            Pending.erase(it);
        }
    }

    void Write(ui64 cookie) {
        Y_VERIFY(!PendingReserve.empty());
        auto it = PendingReserve.begin();

        Y_VERIFY(it->first == cookie);
        Y_VERIFY(PendingWrite.empty() || PendingWrite.back() < cookie);

        auto ev = MakeHolder<TEvPersQueue::TEvRequest>();
        ev->Record = std::move(it->second);

        auto& request = *ev->Record.MutablePartitionRequest();
        request.SetMessageNo(MessageNo++);

        if (!Opts.UseDeduplication) {
            request.SetPartition(PartitionId);
        }
        NTabletPipe::SendData(SelfId(), PipeClient, ev.Release());

        PendingWrite.emplace_back(cookie);
        PendingReserve.erase(it);
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;

        TString error;
        if (!BasicCheck(record, error)) {
            return WriteResult(error, std::move(record));
        }

        const auto& response = record.GetPartitionResponse();
        if (!response.CmdWriteResultSize()) {
            if (PendingReserve.empty()) {
                return WriteResult("Unexpected ReserveBytes response", std::move(record));
            }

            const auto cookie = PendingReserve.begin()->first;
            if (cookie != response.GetCookie()) {
                error = TStringBuilder() << "Unexpected cookie at ReserveBytes"
                    << ": expected# " << cookie
                    << ", got# " << response.GetCookie();
                return WriteResult(error, std::move(record));
            }

            WriteAccepted(cookie);
            Write(cookie);
        } else {
            if (PendingWrite.empty()) {
                return WriteResult("Unexpected Write response", std::move(record));
            }

            const auto cookie = PendingWrite.front();
            if (cookie != response.GetCookie()) {
                error = TStringBuilder() << "Unexpected cookie at Write"
                    << ": expected# " << cookie
                    << ", got# " << response.GetCookie();
                return WriteResult(error, std::move(record));
            }

            WriteResult(std::move(record));
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "TEvClientConnected Status " << msg->Status << ", TabletId: " << msg->TabletId << ", NodeId " << msg->ServerId.NodeId() << ", Generation: " << msg->Generation);
        Y_VERIFY_DEBUG(msg->TabletId == TabletId);

        if (msg->Status != NKikimrProto::OK) {
            LOG_ERROR_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "TPartitionWriter " << TabletId << " (partition=" << PartitionId << ") received TEvClientConnected with status " << ev->Get()->Status);
            Disconnected();
            return;
        }

        Y_VERIFY_DEBUG_S(msg->Generation, "Tablet generation should be greater than 0");

        if (ExpectedGeneration)
        {
            if(*ExpectedGeneration != msg->Generation)
            {
                LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "TPartitionWriter " << TabletId << " (partition=" << PartitionId << ") received TEvClientConnected with wrong generation. Expected: " << *ExpectedGeneration << ", received " << msg->Generation);
                Disconnected();
                PassAway();
            }
            if (NActors::TActivationContext::ActorSystem()->NodeId != msg->ServerId.NodeId())
            {
                LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "TPartitionWriter " << TabletId << " (partition=" << PartitionId << ") received TEvClientConnected with wrong NodeId. Expected: " << NActors::TActivationContext::ActorSystem()->NodeId << ", received " << msg->ServerId.NodeId());
                Disconnected();
                PassAway();
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->TabletId == TabletId) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "TPartitionWriter " << TabletId << " (partition=" << PartitionId << ") received TEvClientDestroyed");
            Disconnected();
        }
    }

    void PassAway() override {
        if (PipeClient) {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeClient);
        }
        SendError("Unexpected termination");
        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_PARTITION_WRITER_ACTOR;
    }

    explicit TPartitionWriter(
            const TActorId& client,
            ui64 tabletId,
            ui32 partitionId,
            TMaybe<ui32> expectedGeneration,
            const TString& sourceId,
            const TPartitionWriterOpts& opts)
        : Client(client)
        , TabletId(tabletId)
        , PartitionId(partitionId)
        , ExpectedGeneration(expectedGeneration)
        , SourceId(sourceId)
        , Opts(opts)
    {
    }

    void Bootstrap() {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };

        PipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), TabletId, config));
        GetOwnership();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
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
    const TMaybe<ui32> ExpectedGeneration;
    const TString SourceId;
    const TPartitionWriterOpts Opts;

    TActorId PipeClient;
    TString OwnerCookie;
    bool Registered = false;
    ui64 MessageNo = 0;

    TMap<ui64, NKikimrClient::TPersQueueRequest> Pending;
    TMap<ui64, NKikimrClient::TPersQueueRequest> PendingReserve;
    TDeque<ui64> PendingWrite;

}; // TPartitionWriter

IActor* CreatePartitionWriter(const TActorId& client, ui64 tabletId, ui32 partitionId, TMaybe<ui32> expectedGeneration, const TString& sourceId, const TPartitionWriterOpts& opts) {
    return new TPartitionWriter(client, tabletId, partitionId, expectedGeneration, sourceId, opts);
}
}
