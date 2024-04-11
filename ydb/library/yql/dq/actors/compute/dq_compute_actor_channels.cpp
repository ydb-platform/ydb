#include "dq_compute_actor_channels.h"

#include <util/string/join.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/dq.h>


#define LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)


namespace NYql::NDq {

using namespace NActors;
using namespace NKikimr;

namespace {

template <typename TCollection>
TString InFlightMessagesStr(const TCollection& inFlight) {
    TVector<ui64> ids;
    ids.reserve(inFlight.size());
    for (auto& kv : inFlight) {
        ids.push_back(kv.first);
    }
    return TStringBuilder() << '[' << JoinSeq(", ", ids) << ']';
}

} // anonymous namespace

TDqComputeActorChannels::TDqComputeActorChannels(TActorId owner, const TTxId& txId, const TDqTaskSettings& task,
    bool retryOnUndelivery, NDqProto::EDqStatsMode statsMode, ui64 channelBufferSize, ICallbacks* cbs, ui32 actorActivityType)
    : TActor(&TDqComputeActorChannels::WorkState, actorActivityType)
    , Owner(owner)
    , TxId(txId)
    , TaskId(task.GetId())
    , RetryOnUndelivery(retryOnUndelivery)
    , Cbs(cbs)
{
    for (ui32 i = 0; i < task.InputsSize(); ++i) {
        for (auto& channel : task.GetInputs(i).GetChannels()) {
            TInputChannelState inputChannel;
            inputChannel.ChannelId = channel.GetId();

            if (Y_UNLIKELY(statsMode >= NDqProto::DQ_STATS_MODE_PROFILE)) {
                inputChannel.Stats = std::make_unique<TInputChannelStats>();
            }

            auto result = InputChannelsMap.emplace(channel.GetId(), std::move(inputChannel));
            YQL_ENSURE(result.second);
        }
    }

    for (ui32 i = 0; i < task.OutputsSize(); ++i) {
        for (auto& channel : task.GetOutputs(i).GetChannels()) {
            TOutputChannelState outputChannel;
            outputChannel.ChannelId = channel.GetId();
            outputChannel.PeerState.ActualizeFreeSpace(channelBufferSize);

            if (channel.GetDstEndpoint().HasActorId()) {
                outputChannel.Peer = ActorIdFromProto(channel.GetDstEndpoint().GetActorId());
            }

            if (Y_UNLIKELY(statsMode >= NDqProto::DQ_STATS_MODE_PROFILE)) {
                outputChannel.Stats = std::make_unique<TOutputChannelStats>();
            }

            auto result = OutputChannelsMap.emplace(channel.GetId(), std::move(outputChannel));
            YQL_ENSURE(result.second);
        }
    }
}

STATEFN(TDqComputeActorChannels::WorkState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvDqCompute::TEvChannelData, HandleWork);
        hFunc(TEvDqCompute::TEvChannelDataAck, HandleWork);
        hFunc(TEvDqCompute::TEvRetryChannelData, HandleWork);
        hFunc(TEvDqCompute::TEvRetryChannelDataAck, HandleWork);
        hFunc(TEvents::TEvPoison, HandlePoison);
        hFunc(TEvents::TEvUndelivered, HandleWork);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleWork);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
        default: {
            InternalError(TStringBuilder() << "TDqComputeActorChannels: unexpected event " << ev->GetTypeRewrite()
                << " at WorkState");
        }
    }
}

void TDqComputeActorChannels::HandleWork(TEvDqCompute::TEvChannelData::TPtr& ev) {
    NDqProto::TEvComputeChannelData& record = ev->Get()->Record;

    TChannelDataOOB channelData;
    channelData.Proto = std::move(*record.MutableChannelData());
    if (channelData.Proto.GetData().HasPayloadId()) {
        channelData.Payload = ev->Get()->GetPayload(channelData.Proto.GetData().GetPayloadId());
    }

    ui64 channelId = channelData.Proto.GetChannelId();

    TInputChannelState& inputChannel = InCh(channelId);

    LOG_T("Received input for channelId: " << channelId
        << ", seqNo: " << record.GetSeqNo()
        << ", size: " << channelData.Proto.GetData().GetRaw().size()
        << ", rows: " << channelData.Proto.GetData().GetRows()
        << ", watermark: " << channelData.Proto.HasWatermark()
        << ", checkpoint: " << channelData.Proto.HasCheckpoint()
        << ", finished: " << channelData.Proto.GetFinished()
        << ", from: " << ev->Sender
        << ", expected seqNo: " << (inputChannel.LastRecvSeqNo + 1));

    if (record.GetSeqNo() != inputChannel.LastRecvSeqNo + 1) {
        LOG_E("Unexpected input channelId: " << channelId << " seqNo: " << record.GetSeqNo()
            << ", expected: " << (inputChannel.LastRecvSeqNo + 1));
        return;
    }

    if (record.GetSeqNo() == 1) {
        YQL_ENSURE(inputChannel.InFlight.empty(), "" << InFlightMessagesStr(inputChannel.InFlight));
        if (!inputChannel.Peer) {
            inputChannel.Peer = ev->Sender;
        }
    }

    YQL_ENSURE(inputChannel.Peer);
    inputChannel.LastRecvSeqNo++;

    // remove all ack messages with seqNo <= seqNo
    while (!inputChannel.InFlight.empty() && inputChannel.InFlight.begin()->first <= record.GetSeqNo()) {
        inputChannel.InFlight.erase(inputChannel.InFlight.begin());
    }

    if (inputChannel.RetryState && !inputChannel.InFlight.empty()) {
        LOG_D("Received all retried messages for input channelId: " << channelId);
        inputChannel.RetryState.reset();
    }

    if (inputChannel.RetryState) {
        LOG_D("Waiting for input channelId: " << channelId
            << " messages: " << InFlightMessagesStr(inputChannel.InFlight));
    }

    if (channelData.Proto.GetFinished()) {
        LOG_D("Finish input channelId: " << channelId << ", from: " << *inputChannel.Peer);
        inputChannel.Finished = true;
    }

    Cbs->TakeInputChannelData(std::move(channelData), !record.GetNoAck());
}

void TDqComputeActorChannels::HandleWork(TEvDqCompute::TEvChannelDataAck::TPtr& ev) {
    auto& record = ev->Get()->Record;

    TOutputChannelState& outputChannel = OutCh(record.GetChannelId());

    LOG_T("Received channel data ack for channelId: " << record.GetChannelId()
        << ", seqNo: " << record.GetSeqNo()
        << ", lastSentSeqNo: " << outputChannel.LastSentSeqNo
        << ", freeSpace: " << record.GetFreeSpace()
        << ", early finish: " << record.GetFinish());

    YQL_ENSURE(record.GetSeqNo() <= outputChannel.LastSentSeqNo);

    if (record.GetFinish()) {
        auto it = outputChannel.InFlight.begin();
        while (it != outputChannel.InFlight.end()) {
            outputChannel.PeerState.RemoveInFlight(it->second.Data.PayloadSize(), it->second.Data.RowCount());
            it = outputChannel.InFlight.erase(it);
        }
        outputChannel.RetryState.reset();
        outputChannel.Finished = true;
        outputChannel.EarlyFinish = true;
        Cbs->PeerFinished(record.GetChannelId());
        return;
    }

    // remove all messages with seqNo <= ackSeqNo
    auto it = outputChannel.InFlight.begin();
    while (it != outputChannel.InFlight.end() && it->first <= record.GetSeqNo()) {
        outputChannel.PeerState.RemoveInFlight(it->second.Data.PayloadSize(), it->second.Data.RowCount());
        it = outputChannel.InFlight.erase(it);
    }

    outputChannel.PeerState.ActualizeFreeSpace(record.GetFreeSpace());

    LOG_T("PeerState, peerState:(" << outputChannel.PeerState.DebugString() << ")"
        << ", sentSeqNo: " << outputChannel.LastSentSeqNo
        << ", ackSeqNo: " << record.GetSeqNo());

    if (outputChannel.RetryState && outputChannel.InFlight.empty()) {
        LOG_D("Received all retried messages for output channelId: " << record.GetChannelId());
        outputChannel.RetryState.reset();
    }

    if (outputChannel.RetryState) {
        LOG_D("Waiting for output channelId: " << record.GetChannelId()
            << " messages: " << InFlightMessagesStr(outputChannel.InFlight));
        return;
    }

    LOG_T("Resume compute actor");
    Cbs->ResumeExecution(EResumeSource::ChannelsHandleWork);
}

void TDqComputeActorChannels::HandleWork(TEvDqCompute::TEvRetryChannelData::TPtr& ev) {
    auto* msg = ev->Get();
    LOG_T("Resend channel data events for output channelId: " << msg->ChannelId
        << ", seqNo: [" << msg->FromSeqNo << ".." << msg->ToSeqNo << ']');

    TOutputChannelState& outputChannel = OutCh(msg->ChannelId);

    if (!outputChannel.RetryState) {
        LOG_D("Output channelId: " << msg->ChannelId << " does not have retry state");
        return;
    }

    if (!outputChannel.RetryState->RetryScheduled) {
        LOG_E("Output channelId: " << msg->ChannelId << " does not have scheduled retry event");
        return;
    }

    outputChannel.RetryState->RetryScheduled = false;

    auto now = Now();

    for (auto& inFlight : outputChannel.InFlight) {
        ui64 seqNo = inFlight.first;

        if (seqNo < msg->FromSeqNo || seqNo > msg->ToSeqNo) {
            LOG_E("Output channelId: " << msg->ChannelId << " has unexpected inflight message seqNo: " << seqNo);
        }

        auto retryEv = MakeHolder<TEvDqCompute::TEvChannelData>();
        retryEv->Record.SetSeqNo(seqNo);
        retryEv->Record.SetSendTime(now.MilliSeconds());

        auto* data = retryEv->Record.MutableChannelData();
        data->CopyFrom(inFlight.second.Data.Proto);
        data->MutableData()->ClearPayloadId();
        if (!inFlight.second.Data.Payload.IsEmpty()) {
            data->MutableData()->SetPayloadId(retryEv->AddPayload(TRope(inFlight.second.Data.Payload)));
        }
        data->SetChannelId(msg->ChannelId);
        data->SetFinished(inFlight.second.Finished);

        LOG_D("Resending data chunk, channelId: " << msg->ChannelId
            << ", peer: " << *outputChannel.Peer
            << ", data size: " << inFlight.second.Data.PayloadSize()
            << ", seqNo: " << seqNo
            << ", finished: " << inFlight.second.Finished);

        ui32 flags = CalcMessageFlags(*outputChannel.Peer);
        Send(*outputChannel.Peer, retryEv.Release(), flags, /* cookie */ msg->ChannelId);
    }
}

void TDqComputeActorChannels::HandleWork(TEvDqCompute::TEvRetryChannelDataAck::TPtr& ev) {
    auto* msg = ev->Get();
    LOG_D("Resend channel data ack events for input channelId: " << msg->ChannelId
        << ", seqNo: [" << msg->FromSeqNo << ".." << msg->ToSeqNo << ']');

    TInputChannelState& inputChannel = InCh(msg->ChannelId);

    if (!inputChannel.RetryState) {
        LOG_E("Input channelId: " << msg->ChannelId << " does not have retry state");
        return;
    }

    if (!inputChannel.RetryState->RetryScheduled) {
        LOG_E("Input channelId: " << msg->ChannelId << " does not have scheduled retry event");
        return;
    }

    inputChannel.RetryState->RetryScheduled = false;

    for (auto& inFlight : inputChannel.InFlight) {
        ui64 seqNo = inFlight.first;
        if (seqNo < msg->FromSeqNo || seqNo > msg->ToSeqNo) {
            LOG_E("Input channelId: " << msg->ChannelId << " has unexpected inflight message seqNo: " << seqNo);
        }

        if (Y_UNLIKELY(inputChannel.Stats)) {
            inputChannel.Stats->ResentMessages++;
        }

        auto retryEv = MakeHolder<TEvDqCompute::TEvChannelDataAck>();
        retryEv->Record.SetSeqNo(seqNo);
        retryEv->Record.SetChannelId(msg->ChannelId);
        retryEv->Record.SetFreeSpace(Cbs->GetInputChannelFreeSpace(msg->ChannelId));

        LOG_D("Resending data chunk ack, channelId: " << msg->ChannelId
            << ", peer: " << *inputChannel.Peer
            << ", seqNo: " << seqNo
            << ", freeSpace: " << retryEv->Record.GetFreeSpace());

        ui32 flags = CalcMessageFlags(*inputChannel.Peer);
        Send(*inputChannel.Peer, retryEv.Release(), flags, /* cookie */ msg->ChannelId);
    }
}

void TDqComputeActorChannels::HandleWork(TEvents::TEvUndelivered::TPtr& ev) {
    auto sourceType = ev->Get()->SourceType;
    auto reason = ev->Get()->Reason;

    if (sourceType == TEvDqCompute::TEvChannelData::EventType) {
        return HandleUndeliveredEvChannelData(ev->Cookie, reason);
    }

    if (sourceType == TEvDqCompute::TEvChannelDataAck::EventType) {
        return HandleUndeliveredEvChannelDataAck(ev->Cookie, reason);
    }

    if (sourceType == TEvDq::TEvAbortExecution::EventType) {
        LOG_N("Ignoring undelivered event: " << sourceType << ", cookie: " << ev->Cookie << ", reason: " << reason);
        return;
    }

    auto message = TStringBuilder() << "Undelivered event: " << sourceType
        << ", cookie: " << ev->Cookie
        << ", reason: " << reason;

    LOG_E(message);
    return InternalError(message);
}

void TDqComputeActorChannels::HandleWork(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    ui32 nodeId = ev->Get()->NodeId;
    LOG_E("Handle node disconnect: " << nodeId);

    if (TrackingNodes.erase(nodeId) == 0) {
        return;
    }

    if (!RetryOnUndelivery) {
        return RuntimeError("detected disconnected node");
    }

    auto now = Now();

    for (auto& inputChannel : InputChannelsMap) {
        if (!inputChannel.second.IsFromNode(nodeId)) {
            continue;
        }

        inputChannel.second.PollRequest.reset();

        LOG_E("Disconnected node for input channelId: " << inputChannel.first
            << ", retry inflight events: " << InFlightMessagesStr(inputChannel.second.InFlight));

        if (!inputChannel.second.InFlight.empty()) {
            bool success = ScheduleRetryForChannel<TInputChannelState, TEvDqCompute::TEvRetryChannelDataAck>(
                inputChannel.second, now);
            if (!success) {
                return;
            }
        }
    }

    for (auto& outputChannel : OutputChannelsMap) {
        if (!outputChannel.second.IsToNode(nodeId)) {
            continue;
        }

        LOG_E("Disconnected node for output channelId: " << outputChannel.first
            << ", retry inflight events: " << InFlightMessagesStr(outputChannel.second.InFlight));

        if (!outputChannel.second.InFlight.empty()) {
            bool success = ScheduleRetryForChannel<TOutputChannelState, TEvDqCompute::TEvRetryChannelData>(
                outputChannel.second, now);
            if (!success) {
                return;
            }
        }
    }
}

void TDqComputeActorChannels::HandleUndeliveredEvChannelData(ui64 channelId, NActors::TEvents::TEvUndelivered::EReason reason) {
    TOutputChannelState& outputChannel = OutCh(channelId);

    if (outputChannel.Finished && outputChannel.EarlyFinish && !SupportCheckpoints) {
        LOG_I("Ignore undelivered TEvChannelData event due to early finish, channelId: " << channelId);
        outputChannel.InFlight.clear();
        Cbs->ResumeExecution(EResumeSource::ChannelsHandleUndeliveredData);
        return;
    }

    LOG_N("Handle undelivered event: TEvChannelData, channelId: " << channelId << ", reason: " << reason);

    if (reason == TEvents::TEvUndelivered::ReasonActorUnknown) {
        return RuntimeError("Output channel actor is unavailable");
    }

    if (outputChannel.InFlight.empty()) {
        return;
    }

    if (!RetryOnUndelivery) {
        auto message = TStringBuilder() << "Failed to deliver output channel data, channelId: "
            << channelId << ", reason: " << reason;
        return RuntimeError(message);
    }

    ScheduleRetryForChannel<TOutputChannelState, TEvDqCompute::TEvRetryChannelData>(outputChannel, Now());
}

void TDqComputeActorChannels::HandleUndeliveredEvChannelDataAck(ui64 channelId, NActors::TEvents::TEvUndelivered::EReason reason) {
    TInputChannelState& inputChannel = InCh(channelId);
    inputChannel.PollRequest.reset();

    if (inputChannel.Finished && !SupportCheckpoints) {
        LOG_I("Handle undelivered event: TEvChannelDataAck, channelId: " << channelId << ", reason: " << reason
            << ". Ignore, channel is finished.");
        inputChannel.InFlight.clear();
        Cbs->ResumeExecution(EResumeSource::ChannelsHandleUndeliveredAck);
        return;
    }

    LOG_N("Handle undelivered event: TEvChannelDataAck, channelId: " << channelId << ", reason: " << reason);

    if (reason == TEvents::TEvUndelivered::ReasonActorUnknown) {
        return RuntimeError("Input channel actor is unavailable");
    }

    if (inputChannel.InFlight.empty()) {
        return;
    }

    if (!RetryOnUndelivery) {
        auto message = TStringBuilder() << "Failed to deliver input channel ack, channelId: "
            << channelId << ", reason: " << reason;
        return RuntimeError(message);
    }

    ScheduleRetryForChannel<TInputChannelState, TEvDqCompute::TEvRetryChannelDataAck>(inputChannel, Now());
}

template <typename TChannelState, typename TRetryEvent>
bool TDqComputeActorChannels::ScheduleRetryForChannel(TChannelState& channel, TInstant now) {
    if (!channel.RetryState) {
        channel.RetryState.emplace(now);
    }

    auto retryAt = channel.RetryState->CalcNextRetry(now);
    if (!retryAt) {
        LOG_E("Delivery interval exceeded for channelId: " << channel.ChannelId);
        RuntimeError("channel delivery interval exceeded");
        return false;
    }

    // every InFlight message can generate undelivery event,
    // but we schedule retry event only once
    if (!channel.RetryState->RetryScheduled) {
        channel.RetryState->RetryScheduled = true;
        auto retryAfter = *retryAt - now;
        LOG_D("Retry #" << channel.RetryState->AttemptNo << " scheduled on " << retryAt
            << ", deadline: " << channel.RetryState->Deadline);
        Schedule(retryAfter, new TRetryEvent(channel.ChannelId, channel.InFlight.begin()->first,
            channel.InFlight.rbegin()->first));
    }

    return true;
}

STATEFN(TDqComputeActorChannels::DeadState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvents::TEvPoison, HandlePoison);
        default: {
            LOG_E("Skip unexpected event " << ev->GetTypeRewrite() << " at DeadState");
        }
    }
}

void TDqComputeActorChannels::HandlePoison(TEvents::TEvPoison::TPtr&) {
    LOG_D("pass away");
    PassAway();
}

TInstant TDqComputeActorChannels::Now() const {
    return TInstant::Now();
}

void TDqComputeActorChannels::RuntimeError(const TString& message) {
    LOG_E(message);

    auto ev = TEvDq::TEvAbortExecution::Unavailable(message);
    Send(Owner, ev.Release());

    Become(&TDqComputeActorChannels::DeadState);
}

void TDqComputeActorChannels::InternalError(const TString& message) {
    LOG_C(message);

    auto ev = TEvDq::TEvAbortExecution::InternalError(message);
    Send(Owner, ev.Release());

    Become(&TDqComputeActorChannels::DeadState);
}

void TDqComputeActorChannels::PassAway() {
    for (ui32 nodeId : TrackingNodes) {
        Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
    }
    IActor::PassAway();
}

void TDqComputeActorChannels::SetInputChannelPeer(ui64 channelId, const TActorId& peer) {
    TInputChannelState& inputChannel = InCh(channelId);
    inputChannel.Peer = peer;
}

void TDqComputeActorChannels::SetOutputChannelPeer(ui64 channelId, const TActorId& peer) {
    TOutputChannelState& outputChannel = OutCh(channelId);
    outputChannel.Peer = peer;
}

bool TDqComputeActorChannels::HasFreeMemoryInChannel(const ui64 channelId) const {
    const TOutputChannelState& outputChannel = OutCh(channelId);
    return outputChannel.PeerState.HasFreeMemory();
}

bool TDqComputeActorChannels::CanSendChannelData(const ui64 channelId) const {
    const TOutputChannelState& outputChannel = OutCh(channelId);
    return outputChannel.Peer && (!outputChannel.Finished || SupportCheckpoints) && !outputChannel.RetryState;
}

bool TDqComputeActorChannels::ShouldSkipData(ui64 channelId) {
    TOutputChannelState& outputChannel = OutCh(channelId);
    return outputChannel.Finished && !SupportCheckpoints;
}

void TDqComputeActorChannels::SendChannelData(TChannelDataOOB&& channelData, const bool needAck) {
    TOutputChannelState& outputChannel = OutCh(channelData.Proto.GetChannelId());

    YQL_ENSURE(!outputChannel.Finished || SupportCheckpoints);
    YQL_ENSURE(!outputChannel.RetryState);

    const ui64 seqNo = ++outputChannel.LastSentSeqNo;
    const ui32 chunkBytes = channelData.PayloadSize();
    const ui32 chunkRows = channelData.RowCount();
    const bool finished = channelData.Proto.GetFinished();

    LOG_T("SendChannelData, channelId: " << channelData.Proto.GetChannelId()
        << ", peer: " << *outputChannel.Peer
        << ", rows: " << chunkRows
        << ", bytes: " << chunkBytes
        << ", watermark: " << channelData.Proto.HasWatermark()
        << ", checkpoint: " << channelData.Proto.HasCheckpoint()
        << ", seqNo: " << seqNo
        << ", finished: " << finished);

    auto dataEv = MakeHolder<TEvDqCompute::TEvChannelData>();
    dataEv->Record.SetSeqNo(seqNo);
    dataEv->Record.SetSendTime(Now().MilliSeconds());
    // copying here since we need to save channelData in InFlight
    *dataEv->Record.MutableChannelData() = channelData.Proto;
    if (channelData.Proto.HasData()) {
        dataEv->Record.MutableChannelData()->MutableData()->ClearPayloadId();
        if (!channelData.Payload.IsEmpty()) {
            dataEv->Record.MutableChannelData()->MutableData()->SetPayloadId(dataEv->AddPayload(TRope(channelData.Payload)));
        }
    }

    outputChannel.InFlight.emplace(
        seqNo,
        TOutputChannelState::TInFlightMessage(
            seqNo,
            std::move(channelData),
            finished
        )
    );
    outputChannel.Finished = finished;

    ui32 flags = CalcMessageFlags(*outputChannel.Peer);
    dataEv->Record.SetNoAck(!needAck);
    Send(*outputChannel.Peer, dataEv.Release(), flags, /* cookie */ outputChannel.ChannelId);

    outputChannel.PeerState.AddInFlight(chunkBytes, chunkRows);
}

bool TDqComputeActorChannels::PollChannel(ui64 channelId, i64 freeSpace) {
    TInputChannelState& inputChannel = InCh(channelId);

    if (!inputChannel.Peer || (inputChannel.Finished && !SupportCheckpoints) || inputChannel.RetryState ||
        inputChannel.LastRecvSeqNo == 0 || freeSpace <= 0)
    {
        LOG_T("no poll, channelId: " << channelId << ", hasPeer: " << inputChannel.Peer.has_value()
            << ", finished: " << inputChannel.Finished << ", retry: " << inputChannel.RetryState.has_value()
            << ", lastSeqNo: " << inputChannel.LastRecvSeqNo << ", freeSpace: " << freeSpace);
        return false;
    }

    if (inputChannel.PollRequest) {
        if (inputChannel.PollRequest->SeqNo == inputChannel.LastRecvSeqNo &&
            inputChannel.PollRequest->FreeSpace == freeSpace)
        {
            LOG_T("no poll, channelId: " << channelId << ", has poll request"
                << " with same seqNo: " << inputChannel.PollRequest->SeqNo
                << " and freeSpace: " << inputChannel.PollRequest->FreeSpace);
            return false;
        }
    } else {
        if (!inputChannel.InFlight.empty()) {
            i64 lastFreeSpace = inputChannel.InFlight.rbegin()->second.FreeSpace;
            if (lastFreeSpace >= freeSpace) {
                LOG_T("no poll, channelId: " << channelId
                    << ", has inflight messages: " << InFlightMessagesStr(inputChannel.InFlight)
                    << ", last with freeSpace: " << lastFreeSpace
                    << ", current freeSpace: " << freeSpace);
                return false;
            } else {
                LOG_T("do poll, channelId: " << channelId << ", last freeSpace: " << lastFreeSpace
                    << ", current freeSpace: " << freeSpace << ", seqNo: " << inputChannel.LastRecvSeqNo);
            }
        }
    }

    inputChannel.PollRequest.emplace(inputChannel.LastRecvSeqNo, freeSpace);

    LOG_T("Poll input channelId: " << channelId
        << ", from: " << *inputChannel.Peer
        << ", seqNo: " << inputChannel.LastRecvSeqNo
        << ", freeSpace: " << freeSpace);

    if (Y_UNLIKELY(inputChannel.Stats)) {
        inputChannel.Stats->PollRequests++;
        if (!inputChannel.StartPollTime) {
            inputChannel.StartPollTime = TInstant::Now();
        }
    }

    SendChannelDataAck(inputChannel, freeSpace);
    return true;
}

bool TDqComputeActorChannels::CheckInFlight(const TString& prefix) {
    for (auto& inputChannel: InputChannelsMap) {
        if (!inputChannel.second.InFlight.empty()) {
            if (inputChannel.second.Finished) {
                LOG_D(prefix << ", don't wait for ack delivery in input channelId: "
                    << inputChannel.first << ", seqNo: " << InFlightMessagesStr(inputChannel.second.InFlight));
                continue;
            }
            LOG_D(prefix << ", waiting for ack delivery in input channelId: "
                << inputChannel.first << ", seqNo: " << InFlightMessagesStr(inputChannel.second.InFlight));
            return false;
        }
    }

    for (auto& outputChannel : OutputChannelsMap) {
        if (!outputChannel.second.InFlight.empty()) {
            LOG_D(prefix << ", waiting for chunk delivery in output channelId: "
                << outputChannel.first << ", seqNo: " << InFlightMessagesStr(outputChannel.second.InFlight));
            return false;
        }
    }

    LOG_D(prefix);
    return true;
}

bool TDqComputeActorChannels::FinishInputChannels() {
    bool result = true;

    for (auto& pair : InputChannelsMap) {
        ui64 channelId = pair.first;
        auto& inputChannel = pair.second;

        if (!inputChannel.Peer) {
            LOG_D("Can not finish input channelId: " << channelId << " prematurely, peer not set yet");
            result = false;
            continue;
        }

        if (inputChannel.Finished) {
            continue;
        }

        LOG_D("Finish input channelId: " << channelId << " prematurely");

        inputChannel.Finished = true;

        auto ackEv = MakeHolder<TEvDqCompute::TEvChannelDataAck>();
        ackEv->Record.SetSeqNo(inputChannel.LastRecvSeqNo);
        ackEv->Record.SetChannelId(inputChannel.ChannelId);
        ackEv->Record.SetFreeSpace(0);
        ackEv->Record.SetFinish(true);

        ui32 flags = (inputChannel.Finished && !SupportCheckpoints) ? 0 : CalcMessageFlags(*inputChannel.Peer);
        Send(*inputChannel.Peer, ackEv.Release(), flags, /* cookie */ inputChannel.ChannelId);
    }

    return result;
}

const TDqComputeActorChannels::TPeerState& TDqComputeActorChannels::GetOutputChannelInFlightState(ui64 channelId) {
    TOutputChannelState& outputChannel = OutCh(channelId);
    return outputChannel.PeerState;
}

const TDqComputeActorChannels::TInputChannelStats* TDqComputeActorChannels::GetInputChannelStats(ui64 channelId) {
    return InCh(channelId).Stats.get();
}

const TDqComputeActorChannels::TOutputChannelStats* TDqComputeActorChannels::GetOutputChannelStats(ui64 channelId) {
    return OutCh(channelId).Stats.get();
}

void TDqComputeActorChannels::SendChannelDataAck(i64 channelId, i64 freeSpace) {
    TInputChannelState& inputChannel = InCh(channelId);
    SendChannelDataAck(inputChannel, freeSpace);
}

void TDqComputeActorChannels::SendChannelDataAck(TInputChannelState& inputChannel, i64 freeSpace) {
    LOG_T("Sending channel data ack to"
        << " channelId: " << inputChannel.ChannelId
        << ", peer: " << *inputChannel.Peer
        << ", from: " << Owner
        << ", freeSpace: " << freeSpace
        << ", seqNo: " << inputChannel.LastRecvSeqNo
        << ", finished: " << inputChannel.Finished);

    inputChannel.InFlight.emplace(
        inputChannel.LastRecvSeqNo,
        TInputChannelState::TInFlightMessage(
            inputChannel.LastRecvSeqNo,
            freeSpace
        )
    );

    auto ackEv = MakeHolder<TEvDqCompute::TEvChannelDataAck>();
    ackEv->Record.SetSeqNo(inputChannel.LastRecvSeqNo);
    ackEv->Record.SetChannelId(inputChannel.ChannelId);
    ackEv->Record.SetFreeSpace(freeSpace);

    ui32 flags = (inputChannel.Finished && !SupportCheckpoints) ? 0 : CalcMessageFlags(*inputChannel.Peer);
    Send(*inputChannel.Peer, ackEv.Release(), flags, /* cookie */ inputChannel.ChannelId);
}

ui32 TDqComputeActorChannels::CalcMessageFlags(const TActorId& peer) {
    ui32 flags = IEventHandle::FlagTrackDelivery;
    if (TrackingNodes.insert(peer.NodeId()).second) {
        flags |= IEventHandle::FlagSubscribeOnSession;
    }
    return flags;
}

TDqComputeActorChannels::TInputChannelState& TDqComputeActorChannels::InCh(ui64 channelId) {
    auto ch = InputChannelsMap.FindPtr(channelId);
    YQL_ENSURE(ch, "task: " << TaskId << ", unknown input channelId: " << channelId);
    return *ch;
}

const TDqComputeActorChannels::TOutputChannelState& TDqComputeActorChannels::OutCh(const ui64 channelId) const {
    auto ch = OutputChannelsMap.FindPtr(channelId);
    YQL_ENSURE(ch, "task: " << TaskId << ", unknown output channelId: " << channelId);
    return *ch;
}

TDqComputeActorChannels::TOutputChannelState& TDqComputeActorChannels::OutCh(const ui64 channelId) {
    auto ch = OutputChannelsMap.FindPtr(channelId);
    YQL_ENSURE(ch, "task: " << TaskId << ", unknown output channelId: " << channelId);
    return *ch;
}

void TDqComputeActorChannels::SetCheckpointsSupport() {
    SupportCheckpoints = true;
}

} // namespace NYql::NDq
