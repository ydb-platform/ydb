#pragma once

#include <Core/Defines.h>
#include <Interpreters/Context_fwd.h>

#include <DBPoco/Net/HTTPClientSession.h>
#include <DBPoco/Timespan.h>

namespace DB
{

struct ServerSettings;
struct Settings;

#define APPLY_FOR_ALL_CONNECTION_TIMEOUT_MEMBERS(M) \
    M(connection_timeout, withUnsecureConnectionTimeout) \
    M(secure_connection_timeout, withSecureConnectionTimeout) \
    M(send_timeout, withSendTimeout) \
    M(receive_timeout, withReceiveTimeout) \
    M(tcp_keep_alive_timeout, withTCPKeepAliveTimeout) \
    M(http_keep_alive_timeout, withHTTPKeepAliveTimeout) \
    M(hedged_connection_timeout, withHedgedConnectionTimeout) \
    M(receive_data_timeout, withReceiveDataTimeout) \
    M(handshake_timeout, withHandshakeTimeout) \
    M(sync_request_timeout, withSyncRequestTimeout) \


struct ConnectionTimeouts
{
    DBPoco::Timespan connection_timeout = DBPoco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0);
    DBPoco::Timespan secure_connection_timeout = DBPoco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0);

    DBPoco::Timespan send_timeout = DBPoco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0);
    DBPoco::Timespan receive_timeout = DBPoco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0);

    DBPoco::Timespan tcp_keep_alive_timeout = DBPoco::Timespan(DEFAULT_TCP_KEEP_ALIVE_TIMEOUT, 0);
    DBPoco::Timespan http_keep_alive_timeout = DBPoco::Timespan(DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT, 0);
    size_t http_keep_alive_max_requests = DEFAULT_HTTP_KEEP_ALIVE_MAX_REQUEST;


    /// Timeouts for HedgedConnections
    DBPoco::Timespan hedged_connection_timeout = DBPoco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0);
    DBPoco::Timespan receive_data_timeout = DBPoco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0);
    /// Timeout for receiving HELLO packet
    DBPoco::Timespan handshake_timeout = DBPoco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0);
    /// Timeout for synchronous request-result protocol call (like Ping or TablesStatus)
    DBPoco::Timespan sync_request_timeout = DBPoco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0);

    ConnectionTimeouts() = default;

    static DBPoco::Timespan saturate(DBPoco::Timespan timespan, DBPoco::Timespan limit);
    ConnectionTimeouts getSaturated(DBPoco::Timespan limit) const;

    /// Timeouts for the case when we have just single attempt to connect.
    static ConnectionTimeouts getTCPTimeoutsWithoutFailover(const Settings & settings);

    /// Timeouts for the case when we will try many addresses in a loop.
    static ConnectionTimeouts getTCPTimeoutsWithFailover(const Settings & settings);
    static ConnectionTimeouts getHTTPTimeouts(const Settings & settings, const ServerSettings & server_settings);

    static ConnectionTimeouts getFetchPartHTTPTimeouts(const ServerSettings & server_settings, const Settings & user_settings);

    ConnectionTimeouts getAdaptiveTimeouts(const String & method, bool first_attempt, bool first_byte) const;

#define DECLARE_BUILDER_FOR_MEMBER(member, setter_func) \
    ConnectionTimeouts & setter_func(size_t seconds); \
    ConnectionTimeouts & setter_func(DBPoco::Timespan span); \

APPLY_FOR_ALL_CONNECTION_TIMEOUT_MEMBERS(DECLARE_BUILDER_FOR_MEMBER)
#undef DECLARE_BUILDER_FOR_MEMBER

    ConnectionTimeouts & withConnectionTimeout(size_t seconds);
    ConnectionTimeouts & withConnectionTimeout(DBPoco::Timespan span);
    ConnectionTimeouts & withHTTPKeepAliveMaxRequests(size_t requests);
};

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define DEFINE_BUILDER_FOR_MEMBER(member, setter_func) \
    inline ConnectionTimeouts & ConnectionTimeouts::setter_func(size_t seconds) \
    { \
        return setter_func(DBPoco::Timespan(seconds, 0)); \
    } \
    inline ConnectionTimeouts & ConnectionTimeouts::setter_func(DBPoco::Timespan span) \
    { \
        member = span; \
        return *this; \
    } \

    APPLY_FOR_ALL_CONNECTION_TIMEOUT_MEMBERS(DEFINE_BUILDER_FOR_MEMBER)
/// NOLINTEND(bugprone-macro-parentheses)

#undef DEFINE_BUILDER_FOR_MEMBER


inline ConnectionTimeouts ConnectionTimeouts::getSaturated(DBPoco::Timespan limit) const
{
#define SATURATE_MEMBER(member, setter_func) \
    .setter_func(saturate(member, limit))

    return ConnectionTimeouts(*this)
APPLY_FOR_ALL_CONNECTION_TIMEOUT_MEMBERS(SATURATE_MEMBER);

#undef SATURETE_MEMBER
}

#undef APPLY_FOR_ALL_CONNECTION_TIMEOUT_MEMBERS

inline ConnectionTimeouts & ConnectionTimeouts::withConnectionTimeout(size_t seconds)
{
    return withConnectionTimeout(DBPoco::Timespan(seconds, 0));
}

inline ConnectionTimeouts & ConnectionTimeouts::withConnectionTimeout(DBPoco::Timespan span)
{
    connection_timeout = span;
    secure_connection_timeout = span;
    return *this;
}

inline ConnectionTimeouts & ConnectionTimeouts::withHTTPKeepAliveMaxRequests(size_t requests)
{
    http_keep_alive_max_requests = requests;
    return *this;
}

void setTimeouts(DBPoco::Net::HTTPClientSession & session, const ConnectionTimeouts & timeouts);
ConnectionTimeouts getTimeouts(const DBPoco::Net::HTTPClientSession & session);

}
