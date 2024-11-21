#include <util/system/env.h>
#include <util/string/cast.h>

#include "logging_resolver.h"

namespace NYql {
    // TLoggingResolverEnvMock is a mock implementation of ILoggingResolver that
    // reads the configuration from the environment variables.
    class TLoggingResolverEnvMock: public ILoggingResolver {
    public:
        TLoggingResolverEnvMock(){};

        virtual NThreading::TFuture<ILoggingResolver::TResponse>
        Resolve(const ILoggingResolver::TRequest& _) const override {
            auto promise = NThreading::NewPromise<ILoggingResolver::TResponse>();

            auto host = GetEnv("LOGGING_YDB_HOST");
            Y_ENSURE(host, "LOGGING_YDB_HOST is not set");

            auto portStr = GetEnv("LOGGING_YDB_PORT");
            Y_ENSURE(portStr, "LOGGING_YDB_PORT is not set");
            auto port = FromString<ui32>(portStr);

            auto table = GetEnv("LOGGING_YDB_TABLE");
            Y_ENSURE(table, "LOGGING_YDB_TABLE is not set");

            promise.SetValue(ILoggingResolver::TResponse{host, port, table});

            return promise.GetFuture();
        }

        ~TLoggingResolverEnvMock(){};
    };

    ILoggingResolver::TPtr MakeLoggingResolverEnvMock() {
        return std::make_shared<TLoggingResolverEnvMock>();
    }

} //namespace NYql