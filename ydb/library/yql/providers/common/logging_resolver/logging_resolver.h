#pragma once

#include <library/cpp/threading/future/future.h>

namespace NYql {
    // ILoggingResolver determines the reading endpoint for the YDB database
    // underlying the Logging service.
    class ILoggingResolver {
    public:
        using TPtr = std::shared_ptr<ILoggingResolver>;

        struct TRequest {
            TString FolderId;
            TString LogGroupName;
        };

        struct TResponse {
            TString Host;  // database hostname
            ui32 Port;     // database port
            TString Table; // table to read from
        };

        virtual NThreading::TFuture<TResponse> Resolve(const TRequest& request) const;

        virtual ~ILoggingResolver() = default;
    };

    // TLoggingResolverEnvMock is a mock implementation of ILoggingResolver that
    // reads the configuration from the environment variables.
    class TLoggingResolverEnvMock: public ILoggingResolver {
        TLoggingResolverEnvMock() {};

        virtual NThreading::TFuture<TResponse> Resolve(const TRequest& request) const override;
    };

    ILoggingResolver::TPtr MakeLoggingResolverEnvMock();

} //namespace NYql