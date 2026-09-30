#include "http_gateway_holder.h"

#include <library/cpp/logger/null.h>
#include <util/generic/yexception.h>

namespace NYql::NTestHelpers {

THttpGatewayHolder::THttpGatewayHolder()
    : LoggerScope(std::make_shared<NYql::NLog::YqlLoggerScope>(
        new NYql::NLog::TTlsLogBackend(new TNullLogBackend())
      ))
    , HttpGateway(NYql::IHTTPGateway::Make())
{}

THttpGatewayHolder::~THttpGatewayHolder() {
    // By this point, all threads using c-ares must be stopped.
    // Use this line to set a breakpoint while debugging c-ares races.
    HttpGateway.reset();
    LoggerScope.reset();
}

const THttpGatewayHolder& GetGlobalHttpGatewayHolder() {
    static const THttpGatewayHolder holder;
    Y_ENSURE(holder.HttpGateway);
    return holder;
}

}  // namespace NYql::NTestHelpers
