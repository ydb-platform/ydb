#include <util/system/env.h>
#include <util/string/cast.h>

#include "logging_resolver.h"

namespace NYql {
    NThreading::TFuture<ILoggingResolver::TResponse>

    TLoggingResolverEnvMock::Resolve(const ILoggingResolver::TRequest& _) const {
        auto promise = NThreading::NewPromise<ILoggingResolver::TResponse>();

        promise.SetValue(ILoggingResolver::TResponse{
            GetEnv("LOGGING_YDB_HOST"),
            FromString<ui32>(GetEnv("LOGGING_YDB_PORT")),
            GetEnv("LOGGING_YDB_TABLE")
        });

        return promise.GetFuture();
    }

    ILoggingResolver::TPtr MakeLoggingResolverEnvMock() {
        return std::make_shared<TLoggingResolverEnvMock>();
    }
} //namespace NYql