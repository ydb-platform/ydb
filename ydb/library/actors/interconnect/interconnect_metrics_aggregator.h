#pragma once

#include <ydb/library/actors/core/actor.h>

#include "interconnect_common.h"

namespace NActors::NInterconnectMetricsAggregator {

enum EMetricsAggregatorEvents {
    EvRegisterPeer = EventSpaceBegin(TEvents::ES_PRIVATE) + 0x900,
    EvUpdateConnected,
    EvUpdateClockSkew,
    EvUnregisterPeer,
};

struct TEvRegisterPeer : TEventLocal<TEvRegisterPeer, EvRegisterPeer> {
    TString PeerLabel;
    TString PeerName;

    TEvRegisterPeer(TString peerLabel, TString peerName)
        : PeerLabel(std::move(peerLabel))
        , PeerName(std::move(peerName))
    {}
};

struct TEvUpdateConnected : TEventLocal<TEvUpdateConnected, EvUpdateConnected> {
    TString PeerLabel;
    TString PeerName;
    ui32 Connected = 0;

    TEvUpdateConnected(TString peerLabel, TString peerName, ui32 connected)
        : PeerLabel(std::move(peerLabel))
        , PeerName(std::move(peerName))
        , Connected(connected)
    {}
};

struct TEvUpdateClockSkew : TEventLocal<TEvUpdateClockSkew, EvUpdateClockSkew> {
    TString PeerLabel;
    TString PeerName;
    i64 ClockSkew = 0;

    TEvUpdateClockSkew(TString peerLabel, TString peerName, i64 clockSkew)
        : PeerLabel(std::move(peerLabel))
        , PeerName(std::move(peerName))
        , ClockSkew(clockSkew)
    {}
};

struct TEvUnregisterPeer : TEventLocal<TEvUnregisterPeer, EvUnregisterPeer> {
    TString PeerLabel;
    TString PeerName;

    TEvUnregisterPeer(TString peerLabel, TString peerName)
        : PeerLabel(std::move(peerLabel))
        , PeerName(std::move(peerName))
    {}
};

NActors::IActor* CreateInterconnectMetricsAggregatorActor(TIntrusivePtr<NActors::TInterconnectProxyCommon> common);

static inline NActors::TActorId MakeInterconnectMetricsAggregatorId(ui32 nodeId) {
    char x[12] = {'I', 'C', 'M', 'e', 't', 'r', 'i', 'c', 'A', 'g', 'g', 'r'};
    return NActors::TActorId(nodeId, TStringBuf(x, 12));
}

} // namespace NActors::NInterconnectMetricsAggregator
