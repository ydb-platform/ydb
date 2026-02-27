#pragma once

#include <ydb/library/actors/core/actor.h>

#include "interconnect_common.h"

namespace NActors::NInterconnectHostMetrics {

enum EHostMetricsAggregatorEvents {
    EvRegisterPeer = EventSpaceBegin(TEvents::ES_PRIVATE) + 0x900,
    EvUpdateConnected,
    EvUpdateClockSkew,
    EvUnregisterPeer,
};

struct TEvRegisterPeer : TEventLocal<TEvRegisterPeer, EvRegisterPeer> {
    TString PeerHost;
    TString PeerHumanName;

    TEvRegisterPeer(TString peerHost, TString peerHumanName)
        : PeerHost(std::move(peerHost))
        , PeerHumanName(std::move(peerHumanName))
    {}
};

struct TEvUpdateConnected : TEventLocal<TEvUpdateConnected, EvUpdateConnected> {
    TString PeerHost;
    TString PeerHumanName;
    ui32 Connected = 0;

    TEvUpdateConnected(TString peerHost, TString peerHumanName, ui32 connected)
        : PeerHost(std::move(peerHost))
        , PeerHumanName(std::move(peerHumanName))
        , Connected(connected)
    {}
};

struct TEvUpdateClockSkew : TEventLocal<TEvUpdateClockSkew, EvUpdateClockSkew> {
    TString PeerHost;
    TString PeerHumanName;
    i64 ClockSkew = 0;

    TEvUpdateClockSkew(TString peerHost, TString peerHumanName, i64 clockSkew)
        : PeerHost(std::move(peerHost))
        , PeerHumanName(std::move(peerHumanName))
        , ClockSkew(clockSkew)
    {}
};

struct TEvUnregisterPeer : TEventLocal<TEvUnregisterPeer, EvUnregisterPeer> {
    TString PeerHost;
    TString PeerHumanName;

    TEvUnregisterPeer(TString peerHost, TString peerHumanName)
        : PeerHost(std::move(peerHost))
        , PeerHumanName(std::move(peerHumanName))
    {}
};

NActors::IActor* CreateInterconnectHostMetricsAggregatorActor(TIntrusivePtr<NActors::TInterconnectProxyCommon> common);

static inline NActors::TActorId MakeInterconnectHostMetricsAggregatorId(ui32 nodeId) {
    char x[12] = {'I', 'C', 'H', 'o', 's', 't', 'M', 'e', 't', 'r', 'i', 'c'};
    return NActors::TActorId(nodeId, TStringBuf(x, 12));
}

} // namespace NActors::NInterconnectHostMetrics
