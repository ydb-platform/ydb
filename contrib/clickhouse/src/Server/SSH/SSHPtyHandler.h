#pragma once

#include "clickhouse_config.h"

#if USE_SSH && defined(OS_LINUX)

#include <DBPoco/Net/StreamSocket.h>
#include <DBPoco/Net/TCPServerConnection.h>
#include <Server/IServer.h>
#include <Server/SSH/SSHSession.h>

namespace DB
{

class SSHPtyHandler : public DBPoco::Net::TCPServerConnection
{
public:
    struct Options
    {
        size_t max_auth_attempts;
        size_t auth_timeout_seconds;
        size_t finish_timeout_seconds;
        size_t event_poll_interval_milliseconds;
        bool enable_client_options_passing;
    };

    explicit SSHPtyHandler
    (
        IServer & server_,
        ::ssh::SSHSession session_,
        const DBPoco::Net::StreamSocket & socket_,
        const Options & options
    );

    ~SSHPtyHandler() override;

    void run() override;

private:
    IServer & server;
    DBPoco::Logger * log;
    ::ssh::SSHSession session;
    Options options;
};

}

#endif
