#pragma once

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/utils/actor_log/log.h>

namespace NYql::NTestHelpers {

///
/// Program-lifetime holder for IHTTPGateway (+ its YqlLoggerScope prerequisite).
///
/// IHTTPGateway is a refcounted singleton whose destruction runs
/// curl_global_cleanup, which in turn tears down c-ares. In tests that races
/// with gRPC threads still using c-ares (see ares_library_cleanup_unsafe).
/// Referencing this holder from a test binary keeps the refcount >= 1 for the
/// lifetime of the process, so no destroy/create cycles happen during
/// test tear-down.
///
/// IHTTPGateway::Make() requires logging to be initialized, so YqlLoggerScope
/// is constructed first and destroyed last.
///
struct THttpGatewayHolder {
    std::shared_ptr<NYql::NLog::YqlLoggerScope> LoggerScope;
    NYql::IHTTPGateway::TPtr HttpGateway;

    THttpGatewayHolder();
    ~THttpGatewayHolder();
};

const THttpGatewayHolder& GetGlobalHttpGatewayHolder();

}  // namespace NYql::NTestHelpers
