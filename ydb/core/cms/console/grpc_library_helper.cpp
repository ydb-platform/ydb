#include "grpc_library_helper.h"

namespace NKikimr::NConsole {

void SetGRpcLibraryFunction() {
    auto logFn = [](gpr_log_func_args* args) {
        auto severity = args->severity;
        if (severity == GPR_LOG_SEVERITY_DEBUG) {
            fprintf(stderr, ":GRPC_LIBRARY DEBUG: %s\n", args->message);
        } else if (severity == GPR_LOG_SEVERITY_INFO) {
            fprintf(stderr, ":GRPC_LIBRARY INFO: %s\n", args->message);
        } else {
            fprintf(stderr, ":GRPC_LIBRARY ERROR: %s\n", args->message);
        }
    };
    gpr_set_log_function(logFn);
}

void EnableGRpcTracersEnable() {
    grpc_tracer_set_enabled("cares_resolver", true);
    grpc_tracer_set_enabled("channel", true);
    grpc_tracer_set_enabled("connectivity_state", true);
    grpc_tracer_set_enabled("sdk_authz", true);
    grpc_tracer_set_enabled("http", true);
    grpc_tracer_set_enabled("http1", true);
    grpc_tracer_set_enabled("tcp", true);
}

void SetGRpcLibraryLogVerbosity(NActors::NLog::EPriority prio) {
    if (prio >= NActors::NLog::EPriority::PRI_DEBUG) {
        gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);
        EnableGRpcTracersEnable();
    } else if (prio >= NActors::NLog::EPriority::PRI_INFO) {
        gpr_set_log_verbosity(GPR_LOG_SEVERITY_INFO);
        EnableGRpcTracersEnable();
    } else if (prio >= NActors::NLog::EPriority::PRI_ERROR) {
        gpr_set_log_verbosity(GPR_LOG_SEVERITY_ERROR);
        EnableGRpcTracersEnable();
    } else {
        gpr_set_log_verbosity(GPR_LOG_SEVERITY_ERROR);
        grpc_tracer_set_enabled("all", false);
    }
}

} // namespace NKikimr::NGRpcService
