#pragma once

#include "iface.h"

#include <util/network/socket.h>
#include <util/datetime/base.h>
#include <util/generic/ptr.h>

namespace NAsyncDns {
    struct IPoller {
        virtual void OnStateChange(SOCKET s, bool read, bool write) = 0;
    };

    class TAsyncDns {
    public:
        TAsyncDns(IPoller* poller, const TOptions& opts = TOptions());
        ~TAsyncDns();

        void AsyncResolve(const TNameRequest& req);

        TDuration Timeout();
        void ProcessSocket(SOCKET s);
        void ProcessNone();

    private:
        class TImpl;
        THolder<TImpl> I_;
    };

    void CheckAsyncStatus(int status);
    void CheckPartialAsyncStatus(int status);
}
