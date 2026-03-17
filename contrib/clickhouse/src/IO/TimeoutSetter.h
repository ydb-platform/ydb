#pragma once

#include <DBPoco/Net/StreamSocket.h>
#include <DBPoco/Timespan.h>


namespace DB
{
/// Temporarily overrides socket send/receive timeouts and reset them back into destructor (or manually by calling reset method)
/// If "limit_max_timeout" is true, timeouts could be only decreased (maxed by previous value).
struct TimeoutSetter
{
    TimeoutSetter(DBPoco::Net::StreamSocket & socket_,
        DBPoco::Timespan send_timeout_,
        DBPoco::Timespan receive_timeout_,
        bool limit_max_timeout = false);

    TimeoutSetter(DBPoco::Net::StreamSocket & socket_, DBPoco::Timespan timeout_, bool limit_max_timeout = false);

    ~TimeoutSetter();

    /// Reset timeouts back.
    void reset();

    DBPoco::Net::StreamSocket & socket;

    DBPoco::Timespan send_timeout;
    DBPoco::Timespan receive_timeout;

    DBPoco::Timespan old_send_timeout;
    DBPoco::Timespan old_receive_timeout;
    bool was_reset = false;
};
}
