#pragma once

#include "dq_compute_actor.h"

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <ydb/core/base/events.h>


namespace NYql::NDq {

class TDqComputeActorChannels : public NActors::TActor<TDqComputeActorChannels> {
public:
    struct TPeerState {
        i64 PeerFreeSpace = 0;
        ui64 InFlightBytes = 0;
        ui64 InFlightRows = 0;
        ui32 InFlightCount = 0;
        i64 PrevPeerFreeSpace = 0;
    };

    struct ICallbacks {
        virtual i64 GetInputChannelFreeSpace(ui64 channelId) const = 0;
        virtual void TakeInputChannelData(NDqProto::TChannelData&& channelData, bool ack) = 0;
        virtual void PeerFinished(ui64 channelId) = 0;
        virtual void ResumeExecution() = 0;

        virtual ~ICallbacks() = default;
    };

    struct TInputChannelStats {
        ui64 PollRequests = 0;
        ui64 ResentMessages = 0;
        TDuration IdleTime; // wait time until 1st message received
        TDuration WaitTime; // wait time after 1st message received
        TInstant FirstMessageTs;
        TInstant LastMessageTs;
    };

    struct TOutputChannelStats {
        ui64 ResentMessages = 0;
        TInstant FirstMessageTs;
        TInstant LastMessageTs;
    };

public:
    TDqComputeActorChannels(NActors::TActorId owner, const TTxId& txId, const TDqTaskSettings& task, bool retryOnUndelivery,
        NDqProto::EDqStatsMode statsMode, ui64 channelBufferSize, ICallbacks* cbs, ui32 actorActivityType);

private:
    STATEFN(WorkState);
    void HandleWork(TEvDqCompute::TEvChannelData::TPtr& ev);
    void HandleWork(TEvDqCompute::TEvChannelDataAck::TPtr& ev);
    void HandleWork(TEvDqCompute::TEvRetryChannelData::TPtr& ev);
    void HandleWork(TEvDqCompute::TEvRetryChannelDataAck::TPtr& ev);
    void HandleWork(NKikimr::TEvents::TEvUndelivered::TPtr& ev);
    void HandleWork(NKikimr::TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleUndeliveredEvChannelData(ui64 channelId, NKikimr::TEvents::TEvUndelivered::EReason reason);
    void HandleUndeliveredEvChannelDataAck(ui64 channelId, NKikimr::TEvents::TEvUndelivered::EReason reason);
    template <typename TChannelState, typename TRetryEvent>
    bool ScheduleRetryForChannel(TChannelState& channel, TInstant now);

private:
    STATEFN(DeadState);
    void HandlePoison(NKikimr::TEvents::TEvPoison::TPtr&);

private:
    TInstant Now() const;
    void RuntimeError(const TString& message);
    void InternalError(const TString& message);
    void PassAway() override;

public:
    void SetCheckpointsSupport(); // Finished channels will be polled for checkpoints.
    void SetInputChannelPeer(ui64 channelId, const NActors::TActorId& peer);
    void SetOutputChannelPeer(ui64 channelId, const NActors::TActorId& peer);
    bool CanSendChannelData(ui64 channelId);
    void SendChannelData(NDqProto::TChannelData&& channelData);
    void SendChannelDataAck(i64 channelId, i64 freeSpace);
    bool PollChannel(ui64 channelId, i64 freeSpace);
    bool CheckInFlight(const TString& prefix);
    bool FinishInputChannels();
    bool ShouldSkipData(ui64 channelId);
    const TPeerState& GetOutputChannelInFlightState(ui64 channelId);
    const TInputChannelStats* GetInputChannelStats(ui64 channelId);
    const TOutputChannelStats* GetOutputChannelStats(ui64 channelId);

private:
    struct TChannelRetryState {
        const TInstant Deadline;
        TInstant RetryAt;
        ui32 AttemptNo = 0;
        bool RetryScheduled = false;

        explicit TChannelRetryState(TInstant now)
            : Deadline(now + GetDqExecutionSettings().FlowControl.OutputChannelDeliveryInterval)
            , RetryAt(now + GetDqExecutionSettings().FlowControl.OutputChannelRetryInterval)
            , AttemptNo(0) {}

        TMaybe<TInstant> CalcNextRetry(TInstant now) {
            if (!RetryScheduled) {
                ++AttemptNo;
                RetryAt = now + GetDqExecutionSettings().FlowControl.OutputChannelRetryInterval * AttemptNo;
            }
            if (RetryAt < Deadline) {
                return RetryAt;
            }
            return Nothing();
        }
    };

    struct TInputChannelState {
        ui64 ChannelId = 0;
        std::optional<NActors::TActorId> Peer;
        ui64 LastRecvSeqNo = 0;
        bool Finished = false;

        struct TInFlightMessage {
            TInFlightMessage(ui64 seqNo, i64 freeSpace)
                : SeqNo(seqNo)
                , FreeSpace(freeSpace) {}

            const ui64 SeqNo;
            const i64 FreeSpace;
        };
        TMap<ui64, TInFlightMessage> InFlight;

        std::optional<TChannelRetryState> RetryState;

        struct TPollRequest {
            TPollRequest(ui64 seqNo, i64 freeSpace)
                : SeqNo(seqNo)
                , FreeSpace(freeSpace) {}

            const ui64 SeqNo;
            const i64 FreeSpace;
        };
        std::optional<TPollRequest> PollRequest;
        std::optional<TInstant> StartPollTime; // used only if Stats defined

        bool IsFromNode(ui32 nodeId) const {
            return Peer && Peer->NodeId() == nodeId;
        }

        std::unique_ptr<TInputChannelStats> Stats;
    };

    struct TOutputChannelState {
        ui64 ChannelId = 0;
        std::optional<NActors::TActorId> Peer;
        ui64 LastSentSeqNo = 0;
        bool Finished = false;
        bool EarlyFinish = false;

        struct TInFlightMessage {
            TInFlightMessage(ui64 seqNo, NYql::NDqProto::TChannelData&& data, bool finished)
                : SeqNo(seqNo)
                , Data(std::move(data))
                , Finished(finished) {}

            const ui64 SeqNo;
            const NYql::NDqProto::TChannelData Data;
            const bool Finished;
        };
        TMap<ui64, TInFlightMessage> InFlight;
        TPeerState PeerState;

        std::optional<TChannelRetryState> RetryState;

        bool IsToNode(ui32 nodeId) const {
            return Peer && Peer->NodeId() == nodeId;
        }

        std::unique_ptr<TOutputChannelStats> Stats;
    };

    void SendChannelDataAck(TInputChannelState& inputChannel, i64 freeSpace);
    ui32 CalcMessageFlags(const NActors::TActorId& peer);
    TInputChannelState& InCh(ui64 channelId);
    TOutputChannelState& OutCh(ui64 channelId);

private:
    const NActors::TActorId Owner;
    const TTxId TxId;
    const ui64 TaskId;
    const bool RetryOnUndelivery;
    bool SupportCheckpoints = false;
    ICallbacks* const Cbs;
    THashSet<ui32> TrackingNodes;
    THashMap<ui64, TInputChannelState> InputChannelsMap;
    THashMap<ui64, TOutputChannelState> OutputChannelsMap;
};

} // namespace NYql::NDq
