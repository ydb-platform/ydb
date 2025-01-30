#pragma once

#include "dq_compute_actor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <util/generic/noncopyable.h>

namespace NYql::NDq {

class TDqComputeActorChannels : public NActors::TActor<TDqComputeActorChannels> {
public:
    friend struct TChannelsTestFixture;
    struct TPeerState: NNonCopyable::TMoveOnly {
    private:
        static const ui32 InterconnectHeadersSize = 96;
        i64 InFlightBytes = 0;
        i64 InFlightRows = 0;
        i32 InFlightCount = 0;
        i64 PeerFreeSpace = 0;
    public:
        i64 GetFreeMemory() const {
            return PeerFreeSpace - InFlightBytes;
        }

        bool NeedAck() const {
            return (InFlightCount % 16 == 0) || !HasFreeMemory();
        }

        TString DebugString() const {
            return TStringBuilder() <<
                "freeSpace:" << PeerFreeSpace << ";" <<
                "inFlightBytes:" << InFlightBytes << ";" <<
                "inFlightCount:" << InFlightCount << ";"
                ;
        }

        bool HasFreeMemory() const {
            return PeerFreeSpace >= InFlightBytes;
        }

        void ActualizeFreeSpace(const i64 actual) {
            PeerFreeSpace = actual;
        }

        void AddInFlight(const ui64 bytes, const ui64 rows) {
            InFlightBytes += bytes + InterconnectHeadersSize;
            InFlightRows += rows;
            InFlightCount += 1;
        }

        void RemoveInFlight(const ui64 bytes, const ui64 rows) {
            InFlightBytes -= (bytes + InterconnectHeadersSize);
            Y_ABORT_UNLESS(InFlightBytes >= 0);
            InFlightRows -= rows;
            Y_ABORT_UNLESS(InFlightRows >= 0);
            InFlightCount -= 1;
            Y_ABORT_UNLESS(InFlightCount >= 0);
        }
    };

    struct ICallbacks {
        virtual i64 GetInputChannelFreeSpace(ui64 channelId) const = 0;
        virtual void TakeInputChannelData(TChannelDataOOB&& channelData, bool ack) = 0;
        virtual void PeerFinished(ui64 channelId) = 0;
        virtual void ResumeExecution(EResumeSource source) = 0;

        virtual ~ICallbacks() = default;
    };

    struct TInputChannelStats {
        ui64 PollRequests = 0;
        ui64 ResentMessages = 0;
    };

    struct TOutputChannelStats {
        ui64 ResentMessages = 0;
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
    void HandleWork(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void HandleWork(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleUndeliveredEvChannelData(ui64 channelId, NActors::TEvents::TEvUndelivered::EReason reason);
    void HandleUndeliveredEvChannelDataAck(ui64 channelId, NActors::TEvents::TEvUndelivered::EReason reason);
    template <typename TChannelState, typename TRetryEvent>
    bool ScheduleRetryForChannel(TChannelState& channel, TInstant now);

private:
    STATEFN(DeadState);
    void HandlePoison(NActors::TEvents::TEvPoison::TPtr&);

private:
    TInstant Now() const;
    void RuntimeError(const TString& message);
    void InternalError(const TString& message);
    void PassAway() override;

public:
    void SetCheckpointsSupport(); // Finished channels will be polled for checkpoints.
    void SetInputChannelPeer(ui64 channelId, const NActors::TActorId& peer);
    void SetOutputChannelPeer(ui64 channelId, const NActors::TActorId& peer);
    bool CanSendChannelData(const ui64 channelId) const;
    bool HasFreeMemoryInChannel(const ui64 channelId) const;
    void SendChannelData(TChannelDataOOB&& channelData, const bool needAck);
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
            TInFlightMessage(ui64 seqNo, TChannelDataOOB&& data, bool finished)
                : SeqNo(seqNo)
                , Data(std::move(data))
                , Finished(finished) {}

            const ui64 SeqNo;
            const TChannelDataOOB Data;
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
    const TOutputChannelState& OutCh(const ui64 channelId) const;

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
