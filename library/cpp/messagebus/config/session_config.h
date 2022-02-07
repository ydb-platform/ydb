#pragma once

#include "codegen.h"
#include "defs.h"

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/string.h>

namespace NBus {
#define BUS_SESSION_CONFIG_MAP(XX, comma)                                                  \
    XX(Name, TString, "")                                                                  \
    comma                                                                                  \
    XX(NumRetries, int, 0) comma                                                           \
    XX(RetryInterval, int, 1000) comma                                                     \
    XX(ReconnectWhenIdle, bool, false) comma                                               \
        XX(MaxInFlight, i64, 1000) comma                                                   \
        XX(PerConnectionMaxInFlight, unsigned, 0) comma                                    \
        XX(PerConnectionMaxInFlightBySize, unsigned, 0) comma                              \
            XX(MaxInFlightBySize, i64, -1) comma                                           \
                XX(TotalTimeout, i64, 0) comma                                             \
                    XX(SendTimeout, i64, 0) comma                                          \
                        XX(ConnectTimeout, i64, 0) comma                                   \
                            XX(DefaultBufferSize, size_t, 10 * 1024) comma                 \
                                XX(MaxBufferSize, size_t, 1024 * 1024) comma               \
                                XX(SocketRecvBufferSize, unsigned, 0) comma                \
                                XX(SocketSendBufferSize, unsigned, 0) comma                \
                                XX(SocketToS, int, -1) comma                               \
                                    XX(SendThreshold, size_t, 10 * 1024) comma             \
                                        XX(Cork, TDuration, TDuration::Zero()) comma       \
                                        XX(MaxMessageSize, unsigned, 26 << 20) comma       \
                                        XX(TcpNoDelay, bool, false) comma                  \
                                        XX(TcpCork, bool, false) comma                     \
                                        XX(ExecuteOnMessageInWorkerPool, bool, true) comma \
                                        XX(ExecuteOnReplyInWorkerPool, bool, true) comma   \
                                        XX(ReusePort, bool, false) comma                   \
                                        XX(ListenPort, unsigned, 0) /* TODO: server only */

    ////////////////////////////////////////////////////////////////////
    /// \brief Configuration for client and server session
    struct TBusSessionConfig {
        BUS_SESSION_CONFIG_MAP(STRUCT_FIELD_GEN, )

        struct TSecret {
            TDuration TimeoutPeriod;
            TDuration StatusFlushPeriod;

            TSecret();
        };

        // secret options are available, but you shouldn't probably use them
        TSecret Secret;

        /// initialized with default settings
        TBusSessionConfig();

        TString PrintToString() const;

        void ConfigureLastGetopt(NLastGetopt::TOpts&, const TString& prefix = "mb-");
    };

    using TBusClientSessionConfig = TBusSessionConfig;
    using TBusServerSessionConfig = TBusSessionConfig;

} // NBus
