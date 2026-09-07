#include "init.h"

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/support/log.h>
#include <contrib/libs/grpc/src/core/lib/surface/init.h>

#include <library/cpp/logger/log.h>

#include <util/system/atexit.h>
#include <util/system/src_location.h>

#include <atomic>
#include <memory>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////
// We have to manage the lifetime of the GRPC custom logger to make sure it is
// alive as long as the GRPC is running.
std::atomic<TLog*> GrpcLog;

////////////////////////////////////////////////////////////////////////////////

gpr_log_severity LogPriorityToSeverity(ELogPriority priority)
{
    if (priority <= TLOG_ERR) {
        return GPR_LOG_SEVERITY_ERROR;
    }

    if (priority <= TLOG_INFO) {
        return GPR_LOG_SEVERITY_INFO;
    }

    return GPR_LOG_SEVERITY_DEBUG;
}

ELogPriority LogSeverityToPriority(gpr_log_severity severity)
{
    switch (severity) {
        default:
        case GPR_LOG_SEVERITY_DEBUG:
            return TLOG_DEBUG;
        case GPR_LOG_SEVERITY_INFO:
            return TLOG_INFO;
        case GPR_LOG_SEVERITY_ERROR:
            return TLOG_ERR;
    }
}

void AddLog(gpr_log_func_args* args)
{
    auto file = ::NPrivate::StripRoot(
        {args->file, static_cast<ui32>(strlen(args->file))});

    *GrpcLog.load(std::memory_order_acquire)
        << LogSeverityToPriority(args->severity)
        << TSourceLocation(file.As<TStringBuf>(), args->line) << ": "
        << args->message;
}

void EnableGrpcTracing()
{
    const char* tracers[] = {
        "api",
        "channel",
        "client_channel_call",
        "client_channel_routing",
        "connectivity_state",
        "handshaker",
        "http",
        "http2_stream_state",
        "op_failure",
        "tcp",
        "timer",
    };

    for (const char* tracer: tracers) {
        grpc_tracer_set_enabled(tracer, 1);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TGrpcInitializer::TGrpcInitializer()
{
    grpc_init();
}

TGrpcInitializer::~TGrpcInitializer()
{
    grpc_shutdown_blocking();

    if (!grpc_is_initialized()) {
        // Restore the default log function. Just in case.
        gpr_set_log_function(nullptr);

        // Once GRPC is stopped, we can safely destroy the custom logger
        delete GrpcLog.exchange(nullptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

void GrpcLoggerInit(TLog log, bool enableTracing)
{
    const auto severity = LogPriorityToSeverity(log.FiltrationLevel());

    auto tmp = std::make_unique<TLog>(std::move(log));
    TLog* expected = nullptr;
    if (!GrpcLog.compare_exchange_strong(expected, tmp.get())) {
        // Logger already initialized. Ignore subsequent invocations.
        return;
    }
    Y_UNUSED(tmp.release());

    // Prevent race condition on asynchronous gRPC shutdown:
    // grpc_shutdown_internal writes to the GrpcLog, but the TTempBufManager
    // singletone is in the process of being deleted. Priority must be higher
    // than that of TTempBufManager, which is equal to 2
    AtExit(grpc_maybe_wait_for_async_shutdown, 3);

    gpr_set_log_verbosity(severity);
    gpr_set_log_function(AddLog);

    if (enableTracing) {
        EnableGrpcTracing();
    }
}

}   // namespace NYdb::NBS
