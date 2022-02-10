#pragma once

#include "defs.h"

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/network/interface.h>
#include <util/system/mutex.h>

namespace NBus {
    ///////////////////////////////////////////////
    /// \brief Client interface to locator service

    /// This interface abstracts clustering/location service that
    /// allows clients find servers (address, port) using "name" and "key".
    /// The instance lives in TBusMessageQueue-object, but can be shared by different queues.
    class TBusLocator: public TAtomicRefCount<TBusLocator>, public TNonCopyable {
    private:
        typedef ui64 TServiceId;
        typedef TSet<TString> TServiceIdSet;
        TServiceIdSet ServiceIdSet;
        TServiceId GetServiceId(const char* name);

        typedef TMap<TNetAddr, TString> THostAddrMap;
        THostAddrMap HostAddrMap;

        TNetworkInterfaceList MyInterfaces;

        struct TItem {
            TServiceId ServiceId;
            TBusKey Start;
            TBusKey End;
            TNetAddr Addr;

            bool operator<(const TItem& y) const;

            bool operator==(const TItem& y) const;

            TItem(TServiceId serviceId, TBusKey start, TBusKey end, const TNetAddr& addr);
        };

        typedef TMultiSet<TItem> TItems;
        TItems Items;
        TMutex Lock;

        int RegisterBreak(TServiceId serviceId, const TBusKey start, const TNetAddr& addr);
        int UnregisterBreak(TServiceId serviceId, const TNetAddr& addr);

        void NormalizeBreaks(TServiceId serviceId);

    private:
        int Register(TBusService service, TBusKey start, TBusKey end, const TNetAddr& addr);

    public:
        /// creates instance that obtains location table from locator server (not implemented)
        TBusLocator();

        /// returns true if this address is on the same node for YBUS_KEYLOCAL
        bool IsLocal(const TNetAddr& addr);

        /// returns first address for service and key
        int Locate(TBusService service, TBusKey key, TNetAddr* addr);

        /// returns all addresses mathing service and key
        int LocateAll(TBusService service, TBusKey key, TVector<TNetAddr>& addrs);

        /// returns actual host name for service and key
        int LocateHost(TBusService service, TBusKey key, TString* host, int* port, bool* isLocal = nullptr);

        /// returns all key ranges for the given service
        int LocateKeys(TBusService service, TBusKeyVec& keys, bool onlyLocal = false);

        /// returns port on the local node for the service
        int GetLocalPort(TBusService service);

        /// returns addresses of the local node for the service
        int GetLocalAddresses(TBusService service, TVector<TNetAddr>& addrs);

        /// register service instance
        int Register(TBusService service, TBusKey start, TBusKey end, const TNetworkAddress& addr, EIpVersion requireVersion = EIP_VERSION_4, EIpVersion preferVersion = EIP_VERSION_ANY);
        /// @throws yexception
        int Register(TBusService service, const char* host, int port, TBusKey start = YBUS_KEYMIN, TBusKey end = YBUS_KEYMAX, EIpVersion requireVersion = EIP_VERSION_4, EIpVersion preferVersion = EIP_VERSION_ANY);

        /// unregister service instance
        int Unregister(TBusService service, TBusKey start, TBusKey end);

        int RegisterBreak(TBusService service, const TVector<TBusKey>& starts, const TNetAddr& addr);
        int UnregisterBreak(TBusService service, const TNetAddr& addr);
    };

}
