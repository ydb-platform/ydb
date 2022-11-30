#pragma once

#include <util/datetime/base.h>
#include <util/generic/yexception.h>

struct hostent;

namespace NAsyncDns {
    struct TOptions {
        inline TOptions()
            : MaxRequests(Max())
            , Retries(5)
            , TimeOut(TDuration::MilliSeconds(50))
        {
        }

        inline TOptions& SetMaxRequests(size_t val) noexcept {
            MaxRequests = val;

            return *this;
        }

        inline TOptions& SetRetries(size_t val) noexcept {
            Retries = val;

            return *this;
        }

        inline TOptions& SetTimeOut(const TDuration& val) noexcept {
            TimeOut = val;

            return *this;
        }

        size_t MaxRequests;
        size_t Retries;
        TDuration TimeOut;
    };

    struct IHostResult {
        struct TResult {
            int Status;
            int Timeouts;
            const hostent* Result;
        };

        virtual ~IHostResult() = default;

        virtual void OnComplete(const TResult& result) = 0;
    };

    struct TNameRequest {
        inline TNameRequest(const char* addr, int family, IHostResult* cb)
            : Addr(addr)
            , Family(family)
            , CB(cb)
        {
        }

        inline TNameRequest Copy(IHostResult* cb) const noexcept {
            return TNameRequest(Addr, Family, cb);
        }

        inline TNameRequest Copy(int family) const noexcept {
            return TNameRequest(Addr, family, CB);
        }

        const char* Addr;
        int Family;
        IHostResult* CB;
    };

    struct TDnsError: public yexception {
    };
}
