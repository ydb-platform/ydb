#include "config.h"

#include <yt/yt/core/net/config.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

void THttpIOConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("read_buffer_size", &TThis::ReadBufferSize)
        .Default(128_KB);

    registrar.Parameter("max_redirect_count", &TThis::MaxRedirectCount)
        .Default(0);

    registrar.Parameter("connection_idle_timeout", &TThis::ConnectionIdleTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("header_read_timeout", &TThis::HeaderReadTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("body_read_idle_timeout", &TThis::BodyReadIdleTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("write_idle_timeout", &TThis::WriteIdleTimeout)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

void TServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default(80);

    registrar.Parameter("max_simultaneous_connections", &TThis::MaxSimultaneousConnections)
        .Default(50000);

    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(8192);

    registrar.Parameter("bind_retry_count", &TThis::BindRetryCount)
        .Default(5);

    registrar.Parameter("bind_retry_backoff", &TThis::BindRetryBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("enable_keep_alive", &TThis::EnableKeepAlive)
        .Default(true);

    registrar.Parameter("cancel_fiber_on_connection_close", &TThis::CancelFiberOnConnectionClose)
        .Default(false);

    registrar.Parameter("nodelay", &TThis::NoDelay)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_idle_connections", &TThis::MaxIdleConnections)
        .Default(0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("dialer", &TThis::Dialer)
        .DefaultNew();
    registrar.Parameter("omit_question_mark_for_empty_query", &TThis::OmitQuestionMarkForEmptyQuery)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TRetryingClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("attempt_timeout", &TThis::AttemptTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("backoff_timeout", &TThis::BackoffTimeout)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_attempt_count", &TThis::MaxAttemptCount)
        .Default(3);
}

////////////////////////////////////////////////////////////////////////////////

void TCorsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disable_cors_check", &TThis::DisableCorsCheck)
        .Default(false);
    registrar.Parameter("host_allow_list", &TThis::HostAllowList)
        .Default({"localhost"});
    registrar.Parameter("host_suffix_allow_list", &TThis::HostSuffixAllowList)
        .Default({".yandex-team.ru"});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
