#pragma once

#include "iface.h"

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/network/address.h>

class TContExecutor;

namespace NAsyncDns {
    class TContResolver;

    struct TCacheOptions {
        inline TCacheOptions() {
        }

        inline TCacheOptions& SetEntryLifetime(const TDuration& val) noexcept {
            EntryLifetime = val;

            return *this;
        }

        inline TCacheOptions& SetNotFoundLifetime(const TDuration& val) noexcept {
            NotFoundLifetime = val;

            return *this;
        }

        inline TCacheOptions& SetMaxSize(size_t val) noexcept {
            MaxSize = val;

            return *this;
        }

        size_t MaxSize = 512;
        TDuration EntryLifetime = TDuration::Seconds(1800);
        TDuration NotFoundLifetime = TDuration::Seconds(1);
    };

    // MT-unsafe LRU DNS cache
    class TContDnsCache {
    public:
        TContDnsCache(TContExecutor* e, const TCacheOptions& opts = {});
        ~TContDnsCache();

        void LookupOrResolve(TContResolver& resolver, const TString& addr, ui16 port, TVector<NAddr::IRemoteAddrRef>& result);
        void LookupOrResolve(TContResolver& resolver, const TString& addr, TVector<NAddr::IRemoteAddrRef>& result);

    private:
        class TImpl;
        THolder<TImpl> I_;
    };
}
