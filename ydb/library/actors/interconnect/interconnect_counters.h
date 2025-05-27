#pragma once

#include <util/system/valgrind.h>

#include "types.h"

#include "interconnect_common.h"

#include <memory>
#include <optional>

namespace NActors {

class IInterconnectMetrics {
public:
    virtual ~IInterconnectMetrics() = default;

    virtual void AddInflightDataAmount(ui64 value) = 0;
    virtual void SubInflightDataAmount(ui64 value) = 0;
    virtual void AddTotalBytesWritten(ui64 value) = 0;
    virtual void SetClockSkewMicrosec(i64 value) = 0;
    virtual void IncSessionDeaths() = 0;
    virtual void IncHandshakeFails() = 0;
    virtual void SetConnected(ui32 value) = 0;
    virtual void IncSubscribersCount() = 0;
    virtual void SubSubscribersCount(ui32 value) = 0;
    virtual void SubOutputBuffersTotalSize(ui64 value) = 0;
    virtual void AddOutputBuffersTotalSize(ui64 value) = 0;
    virtual ui64 GetOutputBuffersTotalSize() const = 0;
    virtual void IncDisconnections() = 0;
    virtual void IncUsefulWriteWakeups() = 0;
    virtual void IncSpuriousWriteWakeups() = 0;
    virtual void IncSendSyscalls(ui64 ns) = 0;
    virtual void IncInflyLimitReach() = 0;
    virtual void IncDisconnectByReason(const TString& s) = 0;
    virtual void IncUsefulReadWakeups() = 0;
    virtual void IncSpuriousReadWakeups() = 0;
    virtual void SetPeerInfo(ui32 nodeId, const TString& name, const TString& dataCenterId) = 0;
    virtual void AddInputChannelsIncomingTraffic(ui16 channel, ui64 incomingTraffic) = 0;
    virtual void IncInputChannelsIncomingEvents(ui16 channel) = 0;
    virtual void IncRecvSyscalls(ui64 ns) = 0;
    virtual void AddTotalBytesRead(ui64 value) = 0;
    virtual void UpdatePingTimeHistogram(ui64 value) = 0;
    virtual void UpdateOutputChannelTraffic(ui16 channel, ui64 value) = 0;
    virtual void UpdateOutputChannelEvents(ui16 channel) = 0;
    virtual void SetUtilization(ui32 total, ui32 starvation) = 0;
    TString GetHumanFriendlyPeerHostName() const {
        return HumanFriendlyPeerHostName.value_or(TString());
    }

protected:
    std::optional<TString> DataCenterId;
    std::optional<ui32> PeerNodeId;
    std::optional<TString> HumanFriendlyPeerHostName;
};

std::unique_ptr<IInterconnectMetrics> CreateInterconnectCounters(const NActors::TInterconnectProxyCommon::TPtr& common);
std::unique_ptr<IInterconnectMetrics> CreateInterconnectMetrics(const NActors::TInterconnectProxyCommon::TPtr& common);
} // NActors
