#pragma once

#include <library/cpp/actors/interconnect/poller_actor.h>
#include "pg_proxy_config.h"
#include "pg_sock64.h"

namespace NPG {

struct TSocketDescriptor : public NActors::TSharedDescriptor, public TNetworkConfig {
    TSocketType Socket;

    TSocketDescriptor() = default;

    TSocketDescriptor(int af)
        : Socket(af)
    {
    }

    TSocketDescriptor(TSocketType&& s)
        : Socket(std::move(s))
    {}

    int GetDescriptor() override {
        return static_cast<SOCKET>(Socket);
    }
};

class TSocketBuffer : public TBuffer, public TNetworkConfig {
public:
    TSocketBuffer()
        : TBuffer(BUFFER_SIZE)
    {}

    bool EnsureEnoughSpaceAvailable(size_t need) {
        size_t avail = Avail();
        if (avail < need) {
            Reserve(Capacity() + need);
            return true;
        }
        return true;
    }

    // non-destructive variant of AsString
    TString AsString() const {
        return TString(Data(), Size());
    }
};

}
