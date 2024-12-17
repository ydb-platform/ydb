#pragma once

#include "common.h"
#include "defs.h"
#include "env.h"

using TFlowRecord = TIntrusivePtr<NBackpressure::TFlowRecord>;
using TQueueId = NKikimrBlobStorage::EVDiskQueueId;

struct TDiskDelay {
    TWeightedRandom<TDuration> Delays;
    TDuration Max;
    TString Tag;

    TDiskDelay(TDuration delay = TDuration::Zero(), TString tag = "")
        : Max(delay)
        , Tag(tag)
    {
        Delays.AddValue(delay, 1);
    }

    TDiskDelay(TDuration min, ui64 minWeight, TDuration max, ui64 maxWeight, TString tag = "")
        : Max(max)
        , Tag(tag)
    {
        Delays.AddValue(min, minWeight);
        Delays.AddValue(max, maxWeight);
    }

    TDiskDelay(const TDiskDelay&) = default;
    TDiskDelay(TDiskDelay&&) = default;
    TDiskDelay& operator=(const TDiskDelay&) = default;
    TDiskDelay& operator=(TDiskDelay&&) = default;

    TDuration GetRandom() {
        return Delays.GetRandom();
    }
};

struct TEvDelayedMessageWrapper : public TEventLocal<TEvDelayedMessageWrapper, TEvBlobStorage::EvDelayedMessageWrapper> {
public:
    std::unique_ptr<IEventHandle> Event;

    TEvDelayedMessageWrapper(std::unique_ptr<IEventHandle>& ev)
        : Event(ev.release())
    {}
};

struct TVDiskDelayEmulator {
    TVDiskDelayEmulator(const std::shared_ptr<TEnvironmentSetup>& env)
        : Env(env)
    {}

    using TFlowKey = std::pair<ui32, TQueueId>;  // { nodeId, queueId }

    std::shared_ptr<TEnvironmentSetup> Env;
    TActorId Edge;
    // assuming there is only one disk per node
    std::unordered_map<TFlowKey, TFlowRecord> FlowRecords;

    std::unordered_map<ui32, TDiskDelay> DelayByNode;
    std::deque<TDiskDelay> DelayByResponseOrder;
    TDiskDelay DefaultDelay = TDuration::Seconds(1);
    bool LogUnwrap = false;

    using TEventHandler = std::function<bool(std::unique_ptr<IEventHandle>&)>;

    std::unordered_map<ui32, TEventHandler> EventHandlers;

    void AddHandler(ui32 eventType, TEventHandler handler) {
        EventHandlers[eventType] = handler;
    }

    bool Filter(ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvDelayedMessageWrapper::EventType) {
            std::unique_ptr<IEventHandle> delayedMsg(std::move(ev));
            ev.reset(delayedMsg->Get<TEvDelayedMessageWrapper>()->Event.release());
            if (LogUnwrap) {
                Cerr << TAppData::TimeProvider->Now() << " Unwrap " << ev->ToString() << Endl;
            }
            return true;
        }
        
        ui32 type = ev->GetTypeRewrite();
        auto it = EventHandlers.find(type);
        if (it != EventHandlers.end() && it->second) {
            return (it->second)(ev);
        }
        return true;
    }

    TDuration GetMsgDelay(ui32 vdiskNodeId) {
        TDiskDelay& delay = DefaultDelay;
        auto it = DelayByNode.find(vdiskNodeId);
        if (it == DelayByNode.end()) {
            if (!DelayByResponseOrder.empty()) {
                delay = DelayByResponseOrder.front();
                DelayByResponseOrder.pop_front();
            }
            DelayByNode[vdiskNodeId] = delay;
        } else {
            delay = it->second;
        }
        TDuration rand = delay.GetRandom();
        return rand;
    }

    TDuration DelayMsg(std::unique_ptr<IEventHandle>& ev) {
        TDuration delay = GetMsgDelay(ev->Sender.NodeId());

        Env->Runtime->WrapInActorContext(Edge, [&] {
            TActivationContext::Schedule(delay, new IEventHandle(
                    ev->Sender,
                    ev->Recipient,
                    new TEvDelayedMessageWrapper(ev))
            );
        });
        return delay;
    }

    void SetDelayByResponseOrder(const std::deque<TDiskDelay>& delays) {
        DelayByResponseOrder = delays;
        DelayByNode = {};
    }
};

struct TDelayFilterFunctor {
    std::shared_ptr<TVDiskDelayEmulator> VDiskDelayEmulator;

    bool operator()(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        return VDiskDelayEmulator->Filter(nodeId, ev);
    }
};
