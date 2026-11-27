#pragma once

#include <util/generic/hash.h>
#include <util/network/poller.h>
#include <util/system/pipe.h>

#include <functional>
#include <mutex>
#include <thread>

namespace NFq::NWasmServices {

// One socket waiter per executor, never per transform. It only publishes
// readiness: curl, credentials, response processing and guest work stay on actors.
class TSocketReadiness {
public:
    using TNotify = std::function<void()>;
    TSocketReadiness();
    ~TSocketReadiness();
    ui64 Watch(SOCKET socket, TNotify notify);
    void Arm(ui64 id, bool read, bool write);
    void Remove(ui64 id);
    void Stop();   // only at hard actor-system teardown, after user workers stop

private:
    struct TWatch {
        SOCKET Socket;
        TNotify Notify;
    };

    void Run();
    TSocketPoller Poller;
    TPipeHandle ReadEnd, WriteEnd;
    std::mutex Mutex;
    THashMap<ui64, TWatch> Watches;
    ui64 NextId = 0;
    bool Stopping = false;
    std::thread Thread;
};

}   // namespace NFq::NWasmServices
