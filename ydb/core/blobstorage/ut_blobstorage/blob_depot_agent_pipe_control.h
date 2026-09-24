#pragma once

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blob_depot/events.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <functional>
#include <map>

// Lets a test cut the pipe between a BlobDepot agent and its tablet the way a real network failure does, and keep
// it cut for as long as it likes.
//
// Poisoning the agent's pipe client makes the tablet see TEvServerDisconnected and the agent see TEvClientDestroyed
// *without* changing the tablet generation. That distinction matters: a tablet restart bumps the generation, and
// the agent already drops everything below it on reconnect, so a restart cannot reach the reconnect-ordering paths
// at all. Everything interesting -- id ranges the tablet still holds, writes that outlive the disconnect, expiry
// watermarks -- only happens within one generation.
//
// Installs itself into TTestActorSystem::FilterFunction, chaining to whatever was there before, and removes itself
// on destruction.
class TAgentPipeControl {
public:
    TAgentPipeControl(TEnvironmentSetup& env, ui64 blobDepotTabletId)
        : Env(env)
        , TabletId(blobDepotTabletId)
        , ChainedFilter(std::move(Env.Runtime->FilterFunction))
    {
        Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return Filter(nodeId, ev);
        };
    }

    ~TAgentPipeControl() {
        Env.Runtime->FilterFunction = std::move(ChainedFilter);
    }

    // Severs the agent's current pipe. The agent reconnects on its own straight away, so use HoldDown() when the
    // tablet should keep seeing the agent as gone.
    void Break(ui32 nodeId) {
        const auto it = Nodes.find(nodeId);
        UNIT_ASSERT_C(it != Nodes.end() && it->second.ClientId,
            "no BlobDepot agent pipe seen yet on node " << nodeId << "; put something through it first");
        Env.Runtime->Send(new IEventHandle(it->second.ClientId, TActorId(), new TEvents::TEvPoisonPill), nodeId);
        Env.Sim(TDuration::Seconds(1));
    }

    // Cuts the pipe and keeps the agent from registering again, so from the tablet's point of view it stays gone
    // and its expiry timer runs. The agent itself keeps a pipe up and sits in Registering, which is what a real
    // agent does when the tablet stops answering.
    void HoldDown(ui32 nodeId) {
        auto& state = Nodes[nodeId];
        state.HeldDown = true;
        state.RegisteredSinceRelease = false;
        Break(nodeId);
    }

    // Lets the agent register again, cutting the pipe once more so it retries immediately rather than sitting in
    // Registering until something else disturbs it.
    void Release(ui32 nodeId, TDuration settle = TDuration::Seconds(5)) {
        const auto it = Nodes.find(nodeId);
        UNIT_ASSERT(it != Nodes.end());
        it->second.HeldDown = false;
        Break(nodeId);
        Env.Sim(settle);
        UNIT_ASSERT_C(it->second.RegisteredSinceRelease,
            "agent on node " << nodeId << " did not register within " << settle);
    }

    ui32 ConnectCount(ui32 nodeId) const { return Get(nodeId).ConnectCount; }
    ui32 RegisterCount(ui32 nodeId) const { return Get(nodeId).RegisterCount; }

    // How many times this node's agent asked the tablet for blob sequence numbers. Zero means it holds no ranges
    // and so cannot be what is pinning GetLeastExpectedBlobId on a channel.
    ui32 AllocateIdsCount(ui32 nodeId) const { return Get(nodeId).AllocateIdsCount; }

    // Blob sequence ids handed back to the tablet in TEvDiscardSpoiledBlobSeq, counted as the tablet sees them
    ui32 DiscardedBlobSeqIds() const { return DiscardedBlobSeqIds_; }

    // Collect-garbage requests the BlobDepot tablet has issued for its own data channels (channel >= 2), split by
    // barrier kind. System channels are ignored: they are not given-id-range traffic. Reclaiming an expired
    // agent's ranges advances GetLeastExpectedBlobId and lets a barrier through on a data channel that still
    // had trash in the pinned step.
    ui32 TabletCollectGarbageCount(bool hard) const { return hard ? HardCollects : SoftCollects; }

    // Rewrites the status of the next TEvPutResult delivered to the agent on this node, so a test can fail one
    // underlying blobstorage write without touching the group itself
    void FailNextBackingPut(ui32 nodeId) { Nodes[nodeId].FailNextPut = true; }

    // Withholds the agent's next backing TEvPutResult until DeliverHeldBackingPut(). The put query stays alive
    // waiting on the blobstorage proxy -- which is the one queue a disconnect does not drain -- so this is how a
    // test builds a write that outlives a disconnect and lands after the tablet has moved on.
    void HoldNextBackingPut(ui32 nodeId) { Nodes[nodeId].HoldNextPut = true; }

    bool BackingPutHeld(ui32 nodeId) const { return bool(Get(nodeId).HeldPut); }

    // Puts the withheld result back on the queue and returns. Does not run the actor system: processing it makes
    // the agent answer the user put, and that TEvPutResult aborts the edge actor unless it is already capturing.
    // The caller captures on the next wait (AwaitPut / WaitForEdgeActorEvent), which is what drains the queue.
    void DeliverHeldBackingPut(ui32 nodeId) {
        auto& state = Nodes[nodeId];
        UNIT_ASSERT_C(state.HeldPut, "no backing TEvPutResult was withheld on node " << nodeId);
        Env.Runtime->Send(state.HeldPut.release(), nodeId);
    }

    // The reclaimed step watermark the tablet handed this agent in TEvRegisterAgentResult, by channel. Non-empty
    // only once the tablet has actually expired the agent -- nothing else populates that field.
    const std::map<ui8, ui32>& InvalidatedSteps(ui32 nodeId) const { return Get(nodeId).InvalidatedSteps; }

    ui32 MaxInvalidatedStep(ui32 nodeId) const {
        ui32 result = 0;
        for (const auto& [channel, step] : Get(nodeId).InvalidatedSteps) {
            result = Max(result, step);
        }
        return result;
    }

    // Step of the last blob sequence id the tablet saw this agent commit, so a test can show that ids issued after
    // a reclamation really do sit above the watermark the agent was told to drop.
    ui32 LastCommittedStep(ui32 nodeId) const { return Get(nodeId).LastCommittedStep; }

    // Whether that last commit sits above the watermark for its own channel. Each channel carries its own
    // watermark and its own NextBlobSeqId bump, so comparing against the maximum across channels would fail
    // whenever the commit happened to land on the channel with the lower one.
    bool LastCommitIsAboveWatermark(ui32 nodeId) const {
        const auto& state = Get(nodeId);
        const auto it = state.InvalidatedSteps.find(state.LastCommittedChannel);
        return it == state.InvalidatedSteps.end() || state.LastCommittedStep > it->second;
    }

private:
    struct TNodeState {
        TActorId AgentId;                // pinned the first time we see this node register
        TActorId ClientId;               // the agent's pipe client, as last announced by TEvClientConnected
        ui32 ConnectCount = 0;
        ui32 RegisterCount = 0;
        ui32 AllocateIdsCount = 0;
        bool HeldDown = false;
        bool RegisteredSinceRelease = false;
        bool FailNextPut = false;
        bool HoldNextPut = false;
        std::unique_ptr<IEventHandle> HeldPut;
        std::map<ui8, ui32> InvalidatedSteps;
        ui8 LastCommittedChannel = 0;
        ui32 LastCommittedStep = 0;
    };

    const TNodeState& Get(ui32 nodeId) const {
        const auto it = Nodes.find(nodeId);
        UNIT_ASSERT_C(it != Nodes.end(), "no BlobDepot agent seen on node " << nodeId);
        return it->second;
    }

    bool Filter(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvTabletPipe::TEvClientConnected::EventType:
                // local: a pipe client on this node telling its owner the pipe to our tablet is up. Until we have
                // seen the node register we cannot tell the agent's own pipe from anyone else's to the same tablet,
                // so take it as a candidate and let the registration below confirm who the agent really is.
                if (auto *msg = ev->Get<TEvTabletPipe::TEvClientConnected>();
                        msg && msg->TabletId == TabletId && msg->Status == NKikimrProto::OK) {
                    auto& state = Nodes[ev->Recipient.NodeId()];
                    if (!state.AgentId || state.AgentId == ev->Recipient) {
                        state.ClientId = msg->ClientId;
                        ++state.ConnectCount;
                    }
                }
                break;

            case TEvBlobDepot::EvRegisterAgent: {
                // seen at the tablet, after the pipe server has unwrapped it; Sender is still the agent actor
                auto& state = Nodes[ev->Sender.NodeId()];
                state.AgentId = ev->Sender;
                ++state.RegisterCount;
                if (state.HeldDown) {
                    return false; // the tablet never learns this agent came back
                }
                state.RegisteredSinceRelease = true;
                break;
            }

            case TEvBlobDepot::EvRegisterAgentResult:
                // sent by the tablet straight to the agent actor, carrying whatever it reclaimed while we were away
                if (const auto it = Nodes.find(ev->Recipient.NodeId());
                        it != Nodes.end() && ev->Recipient == it->second.AgentId) {
                    if (auto *msg = ev->Get<TEvBlobDepot::TEvRegisterAgentResult>()) {
                        it->second.InvalidatedSteps.clear();
                        for (const auto& item : msg->Record.GetInvalidatedSteps()) {
                            it->second.InvalidatedSteps[item.GetChannel()] = item.GetInvalidatedStep();
                        }
                    }
                }
                break;

            case TEvBlobDepot::EvAllocateIds:
                if (const auto it = Nodes.find(ev->Sender.NodeId()); it != Nodes.end()) {
                    ++it->second.AllocateIdsCount;
                }
                break;

            case TEvBlobDepot::EvCommitBlobSeq:
                if (const auto it = Nodes.find(ev->Sender.NodeId()); it != Nodes.end()) {
                    if (auto *msg = ev->Get<TEvBlobDepot::TEvCommitBlobSeq>()) {
                        for (const auto& item : msg->Record.GetItems()) {
                            if (item.HasBlobLocator()) {
                                const auto& blobSeqId = item.GetBlobLocator().GetBlobSeqId();
                                it->second.LastCommittedChannel = blobSeqId.GetChannel();
                                it->second.LastCommittedStep = blobSeqId.GetStep();
                            }
                        }
                    }
                }
                break;

            case TEvBlobDepot::EvDiscardSpoiledBlobSeq:
                if (auto *msg = ev->Get<TEvBlobDepot::TEvDiscardSpoiledBlobSeq>()) {
                    DiscardedBlobSeqIds_ += msg->Record.ItemsSize();
                }
                break;

            case TEvBlobStorage::EvCollectGarbage:
                if (auto *msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                        msg && msg->TabletId == TabletId && msg->Channel >= 2) {
                    (msg->Hard ? HardCollects : SoftCollects) += 1;
                }
                break;

            case TEvBlobStorage::EvPutResult:
                // Only the agent's own backing write, never the user-facing result the test itself is waiting for:
                // both land on this node, so match the pinned agent actor rather than just the node.
                if (const auto it = Nodes.find(ev->Recipient.NodeId());
                        it != Nodes.end() && ev->Recipient == it->second.AgentId) {
                    if (it->second.FailNextPut) {
                        if (auto *msg = ev->Get<TEvBlobStorage::TEvPutResult>()) {
                            it->second.FailNextPut = false;
                            msg->Status = NKikimrProto::ERROR;
                            msg->ErrorReason = "injected by TAgentPipeControl";
                        }
                    } else if (it->second.HoldNextPut) {
                        it->second.HoldNextPut = false;
                        it->second.HeldPut = std::move(ev);
                        return false; // withheld, not lost -- DeliverHeldBackingPut() puts it back
                    }
                }
                break;
        }

        return ChainedFilter ? ChainedFilter(nodeId, ev) : true;
    }

private:
    TEnvironmentSetup& Env;
    const ui64 TabletId;
    std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> ChainedFilter;
    std::map<ui32, TNodeState> Nodes;
    ui32 DiscardedBlobSeqIds_ = 0;
    ui32 SoftCollects = 0;
    ui32 HardCollects = 0;
};
