#pragma once

#include "netaddr.h"

#include <util/network/init.h>

namespace NBus {
    namespace NPrivate {
        struct TAcceptorStatus {
            bool Summary;

            ui64 AcceptorId;

            SOCKET Fd;

            TNetAddr ListenAddr;

            unsigned AcceptSuccessCount;
            TInstant LastAcceptSuccessInstant;

            unsigned AcceptErrorCount;
            TInstant LastAcceptErrorInstant;
            int LastAcceptErrorErrno;

            void ResetIncremental();

            TAcceptorStatus();

            TAcceptorStatus& operator+=(const TAcceptorStatus& that);

            TString PrintToString() const;
        };

    }
}
