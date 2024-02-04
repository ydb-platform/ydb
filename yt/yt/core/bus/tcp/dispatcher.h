#pragma once

#include "public.h"

#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TBusNetworkBandCounters
{
    #define XX(camelCaseField, snakeCaseField) std::atomic<i64> camelCaseField = 0;
    ITERATE_BUS_NETWORK_STATISTICS_FIELDS(XX)
    #undef XX
};

struct TBusNetworkCounters final
{
    static constexpr bool EnableHazard = true;

    TEnumIndexedArray<EMultiplexingBand, TBusNetworkBandCounters> PerBandCounters;

    TBusNetworkStatistics ToStatistics() const;
};

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher
{
public:
    static TTcpDispatcher* Get();

    const TBusNetworkCountersPtr& GetCounters(const TString& networkName, bool encrypted);

    //! Returns the poller used by TCP transport.
    NConcurrency::IPollerPtr GetXferPoller();

    //! Reconfigures the dispatcher.
    void Configure(const TTcpDispatcherConfigPtr& config);

    //! Disables all networking. Safety measure for local runs and snapshot validation.
    void DisableNetworking();

    //! Returns true if networking is disabled.
    bool IsNetworkingDisabled();

    //! Returns the network name for a given #address.
    const TString& GetNetworkNameForAddress(const NNet::TNetworkAddress& address);

    //! Returns the TOS level configured for a band.
    TTosLevel GetTosLevelForBand(EMultiplexingBand band);

    //! Provides diagnostics for the whole TCP bus subsystem.
    NYTree::IYPathServicePtr GetOrchidService();

private:
    TTcpDispatcher();

    DECLARE_LEAKY_SINGLETON_FRIEND()
    friend class TTcpConnection;
    friend class TTcpBusClient;
    friend class TTcpBusServerBase;
    template <class TServer>
    friend class TTcpBusServerProxy;

    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
