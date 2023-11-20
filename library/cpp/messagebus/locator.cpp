////////////////////////////////////////////////////////////////////////////
/// \file
/// \brief Implementation of locator service

#include "locator.h"

#include "ybus.h"

#include <util/generic/hash_set.h>
#include <util/system/hostname.h>

namespace NBus {
    using namespace NAddr;

    static TIpPort GetAddrPort(const IRemoteAddr& addr) {
        switch (addr.Addr()->sa_family) {
            case AF_INET: {
                return ntohs(((const sockaddr_in*)addr.Addr())->sin_port);
            }

            case AF_INET6: {
                return ntohs(((const sockaddr_in6*)addr.Addr())->sin6_port);
            }

            default: {
                ythrow yexception() << "not implemented";
                break;
            }
        }
    }

    static inline bool GetIp6AddressFromVector(const TVector<TNetAddr>& addrs, TNetAddr* addr) {
        for (size_t i = 1; i < addrs.size(); ++i) {
            if (addrs[i - 1].Addr()->sa_family == addrs[i].Addr()->sa_family) {
                return false;
            }

            if (GetAddrPort(addrs[i - 1]) != GetAddrPort(addrs[i])) {
                return false;
            }
        }

        for (size_t i = 0; i < addrs.size(); ++i) {
            if (addrs[i].Addr()->sa_family == AF_INET6) {
                *addr = addrs[i];
                return true;
            }
        }

        return false;
    }

    EMessageStatus TBusProtocol::GetDestination(const TBusClientSession*, TBusMessage* mess, TBusLocator* locator, TNetAddr* addr) {
        TBusService service = GetService();
        TBusKey key = GetKey(mess);
        TVector<TNetAddr> addrs;

        /// check for special local key
        if (key == YBUS_KEYLOCAL) {
            locator->GetLocalAddresses(service, addrs);
        } else {
            /// lookup address/port in the locator table
            locator->LocateAll(service, key, addrs);
        }

        if (addrs.size() == 0) {
            return MESSAGE_SERVICE_UNKNOWN;
        } else if (addrs.size() == 1) {
            *addr = addrs[0];
        } else {
            if (!GetIp6AddressFromVector(addrs, addr)) {
                /// default policy can't make choice for you here, overide GetDestination() function
                /// to implement custom routing strategy for your service.
                return MESSAGE_SERVICE_TOOMANY;
            }
        }

        return MESSAGE_OK;
    }

    static const sockaddr_in* SockAddrIpV4(const IRemoteAddr& a) {
        return (const sockaddr_in*)a.Addr();
    }

    static const sockaddr_in6* SockAddrIpV6(const IRemoteAddr& a) {
        return (const sockaddr_in6*)a.Addr();
    }

    static bool IsAddressEqual(const IRemoteAddr& a1, const IRemoteAddr& a2) {
        if (a1.Addr()->sa_family == a2.Addr()->sa_family) {
            if (a1.Addr()->sa_family == AF_INET) {
                return memcmp(&SockAddrIpV4(a1)->sin_addr, &SockAddrIpV4(a2)->sin_addr, sizeof(in_addr)) == 0;
            } else {
                return memcmp(&SockAddrIpV6(a1)->sin6_addr, &SockAddrIpV6(a2)->sin6_addr, sizeof(in6_addr)) == 0;
            }
        }
        return false;
    }

    TBusLocator::TBusLocator()
        : MyInterfaces(GetNetworkInterfaces())
    {
    }

    bool TBusLocator::TItem::operator<(const TItem& y) const {
        const TItem& x = *this;

        if (x.ServiceId == y.ServiceId) {
            return (x.End < y.End) || ((x.End == y.End) && CompareByHost(x.Addr, y.Addr) < 0);
        }
        return x.ServiceId < y.ServiceId;
    }

    bool TBusLocator::TItem::operator==(const TItem& y) const {
        return ServiceId == y.ServiceId && Start == y.Start && End == y.End && Addr == y.Addr;
    }

    TBusLocator::TItem::TItem(TServiceId serviceId, TBusKey start, TBusKey end, const TNetAddr& addr)
        : ServiceId(serviceId)
        , Start(start)
        , End(end)
        , Addr(addr)
    {
    }

    bool TBusLocator::IsLocal(const TNetAddr& addr) {
        for (const auto& myInterface : MyInterfaces) {
            if (IsAddressEqual(addr, *myInterface.Address)) {
                return true;
            }
        }

        return false;
    }

    TBusLocator::TServiceId TBusLocator::GetServiceId(const char* name) {
        const char* c = ServiceIdSet.insert(name).first->c_str();
        return (ui64)c;
    }

    int TBusLocator::RegisterBreak(TBusService service, const TVector<TBusKey>& starts, const TNetAddr& addr) {
        TGuard<TMutex> G(Lock);

        TServiceId serviceId = GetServiceId(service);
        for (size_t i = 0; i < starts.size(); ++i) {
            RegisterBreak(serviceId, starts[i], addr);
        }
        return 0;
    }

    int TBusLocator::RegisterBreak(TServiceId serviceId, const TBusKey start, const TNetAddr& addr) {
        TItems::const_iterator it = Items.lower_bound(TItem(serviceId, 0, start, addr));
        TItems::const_iterator service_it =
            Items.lower_bound(TItem(serviceId, 0, 0, TNetAddr()));

        THolder<TItem> left;
        THolder<TItem> right;
        if ((it != Items.end() || Items.begin() != Items.end()) && service_it != Items.end() && service_it->ServiceId == serviceId) {
            if (it == Items.end()) {
                --it;
            }
            const TItem& item = *it;
            left.Reset(new TItem(serviceId, item.Start,
                                 Max<TBusKey>(item.Start, start - 1), item.Addr));
            right.Reset(new TItem(serviceId, start, item.End, addr));
            Items.erase(*it);
        } else {
            left.Reset(new TItem(serviceId, YBUS_KEYMIN, start, addr));
            if (start < YBUS_KEYMAX) {
                right.Reset(new TItem(serviceId, start + 1, YBUS_KEYMAX, addr));
            }
        }
        Items.insert(*left);
        Items.insert(*right);
        NormalizeBreaks(serviceId);
        return 0;
    }

    int TBusLocator::UnregisterBreak(TBusService service, const TNetAddr& addr) {
        TGuard<TMutex> G(Lock);

        TServiceId serviceId = GetServiceId(service);
        return UnregisterBreak(serviceId, addr);
    }

    int TBusLocator::UnregisterBreak(TServiceId serviceId, const TNetAddr& addr) {
        int deleted = 0;
        TItems::iterator it = Items.begin();
        while (it != Items.end()) {
            const TItem& item = *it;
            if (item.ServiceId != serviceId) {
                ++it;
                continue;
            }
            TItems::iterator itErase = it++;
            if (item.ServiceId == serviceId && item.Addr == addr) {
                Items.erase(itErase);
                deleted += 1;
            }
        }

        if (Items.begin() == Items.end()) {
            return deleted;
        }
        TBusKey keyItem = YBUS_KEYMAX;
        it = Items.end();
        TItems::iterator first = it;
        do {
            --it;
            // item.Start is not used in set comparison function
            // so you can't violate set sort order by changing it
            // hence const_cast()
            TItem& item = const_cast<TItem&>(*it);
            if (item.ServiceId != serviceId) {
                continue;
            }
            first = it;
            if (item.End < keyItem) {
                item.End = keyItem;
            }
            keyItem = item.Start - 1;
        } while (it != Items.begin());

        if (first != Items.end() && first->Start != 0) {
            TItem item(serviceId, YBUS_KEYMIN, first->Start - 1, first->Addr);
            Items.insert(item);
        }

        NormalizeBreaks(serviceId);
        return deleted;
    }

    void TBusLocator::NormalizeBreaks(TServiceId serviceId) {
        TItems::const_iterator first = Items.lower_bound(TItem(serviceId, YBUS_KEYMIN, YBUS_KEYMIN, TNetAddr()));
        TItems::const_iterator last = Items.end();

        if ((Items.end() != first) && (first->ServiceId == serviceId)) {
            if (serviceId != Max<TServiceId>()) {
                last = Items.lower_bound(TItem(serviceId + 1, YBUS_KEYMIN, YBUS_KEYMIN, TNetAddr()));
            }

            --last;
            Y_ASSERT(Items.end() != last);
            Y_ASSERT(last->ServiceId == serviceId);

            TItem& beg = const_cast<TItem&>(*first);
            beg.Addr = last->Addr;
        }
    }

    int TBusLocator::LocateAll(TBusService service, TBusKey key, TVector<TNetAddr>& addrs) {
        TGuard<TMutex> G(Lock);
        Y_ABORT_UNLESS(addrs.empty(), "Non emtpy addresses");

        TServiceId serviceId = GetServiceId(service);
        TItems::const_iterator it;

        for (it = Items.lower_bound(TItem(serviceId, 0, key, TNetAddr()));
             it != Items.end() && it->ServiceId == serviceId && it->Start <= key && key <= it->End;
             ++it) {
            const TItem& item = *it;
            addrs.push_back(item.Addr);
        }

        if (addrs.size() == 0) {
            return -1;
        }
        return (int)addrs.size();
    }

    int TBusLocator::Locate(TBusService service, TBusKey key, TNetAddr* addr) {
        TGuard<TMutex> G(Lock);

        TServiceId serviceId = GetServiceId(service);
        TItems::const_iterator it;

        it = Items.lower_bound(TItem(serviceId, 0, key, TNetAddr()));

        if (it != Items.end()) {
            const TItem& item = *it;
            if (item.ServiceId == serviceId && item.Start <= key && key < item.End) {
                *addr = item.Addr;

                return 0;
            }
        }

        return -1;
    }

    int TBusLocator::GetLocalPort(TBusService service) {
        TGuard<TMutex> G(Lock);
        TServiceId serviceId = GetServiceId(service);
        TItems::const_iterator it;
        int port = 0;

        for (it = Items.lower_bound(TItem(serviceId, 0, 0, TNetAddr())); it != Items.end(); ++it) {
            const TItem& item = *it;
            if (item.ServiceId != serviceId) {
                break;
            }

            if (IsLocal(item.Addr)) {
                if (port != 0 && port != GetAddrPort(item.Addr)) {
                    Y_ASSERT(0 && "Can't decide which port to use.");
                    return 0;
                }
                port = GetAddrPort(item.Addr);
            }
        }

        return port;
    }

    int TBusLocator::GetLocalAddresses(TBusService service, TVector<TNetAddr>& addrs) {
        TGuard<TMutex> G(Lock);
        TServiceId serviceId = GetServiceId(service);
        TItems::const_iterator it;

        for (it = Items.lower_bound(TItem(serviceId, 0, 0, TNetAddr())); it != Items.end(); ++it) {
            const TItem& item = *it;
            if (item.ServiceId != serviceId) {
                break;
            }

            if (IsLocal(item.Addr)) {
                addrs.push_back(item.Addr);
            }
        }

        if (addrs.size() == 0) {
            return -1;
        }

        return (int)addrs.size();
    }

    int TBusLocator::LocateHost(TBusService service, TBusKey key, TString* host, int* port, bool* isLocal) {
        int ret;
        TNetAddr addr;
        ret = Locate(service, key, &addr);
        if (ret != 0) {
            return ret;
        }

        {
            TGuard<TMutex> G(Lock);
            THostAddrMap::const_iterator it = HostAddrMap.find(addr);
            if (it == HostAddrMap.end()) {
                return -1;
            }
            *host = it->second;
        }

        *port = GetAddrPort(addr);
        if (isLocal != nullptr) {
            *isLocal = IsLocal(addr);
        }
        return 0;
    }

    int TBusLocator::LocateKeys(TBusService service, TBusKeyVec& keys, bool onlyLocal) {
        TGuard<TMutex> G(Lock);
        Y_ABORT_UNLESS(keys.empty(), "Non empty keys");

        TServiceId serviceId = GetServiceId(service);
        TItems::const_iterator it;
        for (it = Items.begin(); it != Items.end(); ++it) {
            const TItem& item = *it;
            if (item.ServiceId != serviceId) {
                continue;
            }
            if (onlyLocal && !IsLocal(item.Addr)) {
                continue;
            }
            keys.push_back(std::make_pair(item.Start, item.End));
        }
        return (int)keys.size();
    }

    int TBusLocator::Register(TBusService service, const char* hostName, int port, TBusKey start /*= YBUS_KEYMIN*/, TBusKey end /*= YBUS_KEYMAX*/, EIpVersion requireVersion /*= EIP_VERSION_4*/, EIpVersion preferVersion /*= EIP_VERSION_ANY*/) {
        TNetAddr addr(hostName, port, requireVersion, preferVersion); // throws
        {
            TGuard<TMutex> G(Lock);
            HostAddrMap[addr] = hostName;
        }
        Register(service, start, end, addr);
        return 0;
    }

    int TBusLocator::Register(TBusService service, TBusKey start, TBusKey end, const TNetworkAddress& na, EIpVersion requireVersion /*= EIP_VERSION_4*/, EIpVersion preferVersion /*= EIP_VERSION_ANY*/) {
        TNetAddr addr(na, requireVersion, preferVersion); // throws
        Register(service, start, end, addr);
        return 0;
    }

    int TBusLocator::Register(TBusService service, TBusKey start, TBusKey end, const TNetAddr& addr) {
        TGuard<TMutex> G(Lock);

        TServiceId serviceId = GetServiceId(service);
        TItems::const_iterator it;

        TItem itemToReg(serviceId, start, end, addr);
        for (it = Items.lower_bound(TItem(serviceId, 0, start, TNetAddr()));
             it != Items.end() && it->ServiceId == serviceId;
             ++it) {
            const TItem& item = *it;
            if (item == itemToReg) {
                return 0;
            }
            if ((item.Start < start && start < item.End) || (item.Start < end && end < item.End)) {
                Y_ABORT("Overlap in registered keys with non-identical range");
            }
        }

        Items.insert(itemToReg);
        return 0;
    }

    int TBusLocator::Unregister(TBusService service, TBusKey start, TBusKey end) {
        TGuard<TMutex> G(Lock);
        TServiceId serviceId = GetServiceId(service);
        Items.erase(TItem(serviceId, start, end, TNetAddr()));
        return 0;
    }

}
