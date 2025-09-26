#pragma once

#include "node.h"
#include "interrupter.h"

#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/generic/noncopyable.h>

class TTestICCluster: public TNonCopyable {
public:
    struct TTrafficInterrupterSettings {
        TDuration RejectingTrafficTimeout;
        double BandWidth;
        bool Disconnect;
    };

    enum Flags : ui64 {
        EMPTY = 0,
        USE_ZC = 1,
        USE_TLS = 1 << 1
    };

private:
    const ui32 NumNodes;
    const TString Address = "::1";
    TDuration DeadPeerTimeout = TDuration::Seconds(2);
    NMonitoring::TDynamicCounterPtr Counters;
    THashMap<ui32, THolder<TNode>> Nodes;
    TList<TTrafficInterrupter> interrupters;
    NActors::TChannelsConfig ChannelsConfig;
    TPortManager PortManager;
    TIntrusivePtr<NLog::TSettings> LoggerSettings;

public:
    TTestICCluster(ui32 numNodes = 1, NActors::TChannelsConfig channelsConfig = NActors::TChannelsConfig(),
                   TTrafficInterrupterSettings* tiSettings = nullptr, TIntrusivePtr<NLog::TSettings> loggerSettings = nullptr, Flags flags = EMPTY)
        : NumNodes(numNodes)
        , Counters(new NMonitoring::TDynamicCounters)
        , ChannelsConfig(channelsConfig)
        , LoggerSettings(loggerSettings)
    {
        THashMap<ui32, ui16> nodeToPortMap;
        THashMap<ui32, THashMap<ui32, ui16>> specificNodePortMap;

        for (ui32 i = 1; i <= NumNodes; ++i) {
            nodeToPortMap.emplace(i, PortManager.GetPort());
        }

        if (tiSettings) {
            ui32 nodeId;
            ui16 listenPort;
            ui16 forwardPort;
            for (auto& item : nodeToPortMap) {
                nodeId = item.first;
                listenPort = item.second;
                forwardPort = PortManager.GetPort();

                specificNodePortMap[nodeId] = nodeToPortMap;
                specificNodePortMap[nodeId].at(nodeId) = forwardPort;
                interrupters.emplace_back(Address, listenPort, forwardPort, tiSettings->RejectingTrafficTimeout, tiSettings->BandWidth, tiSettings->Disconnect);
                interrupters.back().Start();
            }
        }

        for (ui32 i = 1; i <= NumNodes; ++i) {
            auto& portMap = tiSettings ? specificNodePortMap[i] : nodeToPortMap;
            Nodes.emplace(i, MakeHolder<TNode>(i, NumNodes, portMap, Address, Counters, DeadPeerTimeout, ChannelsConfig,
                /*numDynamicNodes=*/0, /*numThreads=*/1, LoggerSettings, TNode::DefaultInflight(),
                flags & USE_ZC ? ESocketSendOptimization::IC_MSG_ZEROCOPY : ESocketSendOptimization::DISABLED,
                flags & USE_TLS));
        }
    }

    TNode* GetNode(ui32 id) {
        return Nodes[id].Get();
    }

    ~TTestICCluster() {
    }

    TActorId RegisterActor(NActors::IActor* actor, ui32 nodeId) {
        return Nodes[nodeId]->RegisterActor(actor);
    }

    TActorId InterconnectProxy(ui32 peerNodeId, ui32 nodeId) {
        return Nodes[nodeId]->InterconnectProxy(peerNodeId);
    }

    void KillActor(ui32 nodeId, const TActorId& id) {
        Nodes[nodeId]->Send(id, new NActors::TEvents::TEvPoisonPill);
    }

    NThreading::TFuture<TString> GetSessionDbg(ui32 me, ui32 peer) {
        NThreading::TPromise<TString> promise = NThreading::NewPromise<TString>();

        class TGetHttpInfoActor : public NActors::TActorBootstrapped<TGetHttpInfoActor> {
        public:
            TGetHttpInfoActor(const TActorId& id, NThreading::TPromise<TString> promise)
                : IcProxy(id)
                , Promise(promise)
                {}

            void Bootstrap() {
                NMonitoring::TMonService2HttpRequest monReq(nullptr, nullptr, nullptr, nullptr, "", nullptr);
                Send(IcProxy, new NMon::TEvHttpInfo(monReq));
                Become(&TGetHttpInfoActor::StateFunc);
            }

            STRICT_STFUNC(StateFunc,
                hFunc(NMon::TEvHttpInfoRes, Handle);
            )
        private:
            void Handle(NMon::TEvHttpInfoRes::TPtr& ev) {
                TStringStream str;
                ev->Get()->Output(str);
                Promise.SetValue(str.Str());
                PassAway();
            }
            const TActorId IcProxy;
            NThreading::TPromise<TString> Promise;
        };

        IActor* actor = new TGetHttpInfoActor(Nodes[me]->InterconnectProxy(peer), promise); 
        Nodes[me]->RegisterActor(actor);

        return promise.GetFuture();
    }
};
