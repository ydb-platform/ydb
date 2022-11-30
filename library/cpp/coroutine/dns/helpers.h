#pragma once

#include "iface.h"

#include <util/network/address.h>
#include <util/generic/vector.h>

namespace NAsyncDns {
    class TContResolver;
    class TContDnsCache;
    using NAddr::IRemoteAddrRef;

    typedef TVector<IRemoteAddrRef> TAddrs;

    struct TResolveAddr: public IHostResult {
        inline TResolveAddr(ui16 port)
            : Port(port)
            , Status(0)
        {
        }

        void OnComplete(const TResult& result) override;

        TAddrs Result;
        ui16 Port;
        TVector<int> Status;
    };

    //resolve addr(host + port), like TNetworkAddress
    void ResolveAddr(TContResolver& resolver, const TString& addr, TAddrs& result);
    void ResolveAddr(TContResolver& resolver, const TString& host, ui16 port, TAddrs& result);
    void ResolveAddr(TContResolver& resolver, const TString& addr, TAddrs& result, TContDnsCache* cache);
    void ResolveAddr(TContResolver& resolver, const TString& host, ui16 port, TAddrs& result, TContDnsCache* cache);
}
