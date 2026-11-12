#include "readiness.h"

#include <util/generic/yexception.h>

namespace NFq::NWasmServices {

TSocketReadiness::TSocketReadiness() {
    TPipeHandle::Pipe(ReadEnd, WriteEnd, CloseOnExec);
    Poller.WaitRead(ReadEnd, nullptr);
    Thread = std::thread([this] {
        Run();
    });
}

TSocketReadiness::~TSocketReadiness() {
    Stop();
}

ui64 TSocketReadiness::Watch(SOCKET socket, TNotify notify) {
    std::lock_guard lock(Mutex);
    Y_ENSURE(!Stopping, "Socket readiness is stopped");
    const auto id = ++NextId;
    Y_ENSURE(id, "Socket readiness ID exhausted");
    Watches.emplace(id, TWatch{ socket, std::move(notify) });
    return id;
}

void TSocketReadiness::Arm(ui64 id, bool read, bool write) {
    std::lock_guard lock(Mutex);
    auto it = Watches.find(id);
    if (it == Watches.end() || Stopping) {
        return;
    }
    const auto socket = it->second.Socket;
    auto* cookie = reinterpret_cast<void*>(static_cast<uintptr_t>(id));
    if (read && write) {
        Poller.WaitReadWriteOneShot(socket, cookie);
    } else if (read) {
        Poller.WaitReadOneShot(socket, cookie);
    } else if (write) {
        Poller.WaitWriteOneShot(socket, cookie);
    } else {
        Poller.Unwait(socket);
    }
}

void TSocketReadiness::Remove(ui64 id) {
    std::lock_guard lock(Mutex);
    if (auto it = Watches.find(id); it != Watches.end()) {
        // libcurl invokes REMOVE before closing/reusing the descriptor. Events
        // already returned by the OS carry a unique ID and cannot hit a new fd.
        Poller.Unwait(it->second.Socket);
        Watches.erase(it);
    }
}

void TSocketReadiness::Run() {
    void* events[64];
    for (;;) {
        const auto count = Poller.WaitI(events, std::size(events));
        for (size_t i = 0; i < count; ++i) {
            if (!events[i]) {
                return;
            }
            TNotify notify;
            {
                std::lock_guard lock(Mutex);
                if (Stopping) {
                    return;
                }
                if (auto it = Watches.find(reinterpret_cast<uintptr_t>(events[i])); it != Watches.end()) {
                    notify = it->second.Notify;
                }
            }
            if (notify) {
                notify();
            }
        }
    }
}

void TSocketReadiness::Stop() {
    {
        std::lock_guard lock(Mutex);
        if (std::exchange(Stopping, true)) {
            return;
        }
    }
    // One byte, written once to an empty pipe; no work or polling timeout.
    const char wake = 0;
    Y_ABORT_UNLESS(WriteEnd.Write(&wake, 1) == 1);
    Thread.join();
    std::lock_guard lock(Mutex);
    for (const auto& [_, watch] : Watches) {
        Poller.Unwait(watch.Socket);
    }
    Watches.clear();
}

}   // namespace NFq::NWasmServices
