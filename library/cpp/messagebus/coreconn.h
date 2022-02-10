#pragma once

//////////////////////////////////////////////////////////////
/// \file
/// \brief Definitions for asynchonous connection queue

#include "base.h"
#include "event_loop.h"
#include "netaddr.h"

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/list.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/network/address.h>
#include <util/network/ip.h>
#include <util/network/poller.h>
#include <util/string/util.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

#include <deque>
#include <utility>

#ifdef NO_ERROR
#undef NO_ERROR
#endif

#define BUS_WORKER_CONDVAR
//#define BUS_WORKER_MIXED

namespace NBus {
    class TBusConnection;
    class TBusConnectionFactory;
    class TBusServerFactory;

    using TBusConnectionList = TList<TBusConnection*>;

    /// @throw yexception
    EIpVersion MakeIpVersion(bool allowIpv4, bool allowIpv6);

    inline bool WouldBlock() {
        int syserr = LastSystemError();
        return syserr == EAGAIN || syserr == EINPROGRESS || syserr == EWOULDBLOCK || syserr == EINTR;
    }

    class TBusSession;

    struct TMaxConnectedException: public yexception {
        TMaxConnectedException(unsigned maxConnect) {
            yexception& exc = *this;
            exc << TStringBuf("Exceeded maximum number of outstanding connections: ");
            exc << maxConnect;
        }
    };

    enum EPollType {
        POLL_READ,
        POLL_WRITE
    };

}
