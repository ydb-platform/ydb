#pragma once

#include <CHDBPoco/Net/StreamSocket.h>
#include <CHDBPoco/Timespan.h>


namespace DB_CHDB
{
/// Temporarily overrides socket send/receive timeouts and reset them back into destructor (or manually by calling reset method)
/// If "limit_max_timeout" is true, timeouts could be only decreased (maxed by previous value).
struct TimeoutSetter
{
    TimeoutSetter(CHDBPoco::Net::StreamSocket & socket_,
        CHDBPoco::Timespan send_timeout_,
        CHDBPoco::Timespan receive_timeout_,
        bool limit_max_timeout = false);

    TimeoutSetter(CHDBPoco::Net::StreamSocket & socket_, CHDBPoco::Timespan timeout_, bool limit_max_timeout = false);

    ~TimeoutSetter();

    /// Reset timeouts back.
    void reset();

    CHDBPoco::Net::StreamSocket & socket;

    CHDBPoco::Timespan send_timeout;
    CHDBPoco::Timespan receive_timeout;

    CHDBPoco::Timespan old_send_timeout;
    CHDBPoco::Timespan old_receive_timeout;
    bool was_reset = false;
};
}
