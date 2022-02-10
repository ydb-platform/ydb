#include "coreconn.h"

#include "remote_connection.h"

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/network/socket.h>
#include <util/string/util.h>
#include <util/system/thread.h>

namespace NBus {
    TBusInstant Now() {
        return millisec();
    }

    EIpVersion MakeIpVersion(bool allowIpv4, bool allowIpv6) {
        if (allowIpv4) {
            if (allowIpv6) {
                return EIP_VERSION_ANY;
            } else {
                return EIP_VERSION_4;
            }
        } else if (allowIpv6) {
            return EIP_VERSION_6;
        }

        ythrow yexception() << "Neither of IPv4/IPv6 is allowed.";
    }

}
