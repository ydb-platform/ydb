#include "poller_tcp_unit_select.h"

#include <csignal>

#if defined(_win_)
#include <winsock2.h>
#define SOCKET_ERROR_SOURCE ::WSAGetLastError()
#elif defined(_darwin_)
#include <cerrno>
#define SOCKET_ERROR_SOURCE errno
typedef timeval TIMEVAL;
#else
#include <cerrno>
#define SOCKET_ERROR_SOURCE errno
#endif

namespace NInterconnect {
    TPollerUnitSelect::TPollerUnitSelect() {
    }

    TPollerUnitSelect::~TPollerUnitSelect() {
    }

    template <bool IsWrite>
    void
    TPollerUnitSelect::Process() {
        auto& side = GetSide<IsWrite>();
        side.ProcessInput();

        enum : size_t { R,
                        W,
                        E };
        static const auto O = IsWrite ? W : R;

        ::fd_set sets[3];

        FD_ZERO(&sets[R]);
        FD_ZERO(&sets[W]);
        FD_ZERO(&sets[E]);

        for (const auto& operation : side.Operations) {
            FD_SET(operation.first, &sets[O]);
            FD_SET(operation.first, &sets[E]);
        }

#if defined(_win_)
        ::TIMEVAL timeout = {0L, 99991L};
        const auto numberEvents = !side.Operations.empty() ? ::select(FD_SETSIZE, &sets[R], &sets[W], &sets[E], &timeout)
                                                           : (::Sleep(100), 0);
#elif defined(_darwin_)
        ::TIMEVAL timeout = {0L, 99991L};
        const auto numberEvents = ::select(FD_SETSIZE, &sets[R], &sets[W], &sets[E], &timeout);
#else
        ::sigset_t sigmask;
        ::sigemptyset(&sigmask);
        ::sigaddset(&sigmask, SIGPIPE);
        ::sigaddset(&sigmask, SIGTERM);

        struct ::timespec timeout = {0L, 99999989L};
        const auto numberEvents = ::pselect(FD_SETSIZE, &sets[R], &sets[W], &sets[E], &timeout, &sigmask);
#endif

        Y_DEBUG_ABORT_UNLESS(numberEvents >= 0);

        for (auto it = side.Operations.cbegin(); side.Operations.cend() != it;) {
            if (FD_ISSET(it->first, &sets[O]) || FD_ISSET(it->first, &sets[E]))
                if (const auto& finalizer = it->second.second(it->second.first)) {
                    side.Operations.erase(it++);
                    finalizer();
                    continue;
                }
            ++it;
        }
    }

    void
    TPollerUnitSelect::ProcessRead() {
        Process<false>();
    }

    void
    TPollerUnitSelect::ProcessWrite() {
        Process<true>();
    }

}
