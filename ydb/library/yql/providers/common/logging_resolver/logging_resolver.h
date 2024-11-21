#pragma once

#include <library/cpp/threading/future/future.h>

namespace NYql {
    // ILoggingResolver determines the reading endpoint for the YDB database
    // underlying the Logging service.
    class ILoggingResolver {
    public:
        using TPtr = std::shared_ptr<ILoggingResolver>;

        // TAuth contains credentials to access logging API
        struct TAuth {
            TString StructuredToken; // Serialized token value used to access MDB API
            bool AddBearerToToken = false;
        };

        // folder_id -> credentials to access logging API;
        using TAuthMap = THashMap<TString, TAuth>;

        struct TRequest {
            TString FolderId;
            TString LogGroupName;
        };

        struct TResponse {
            TString Host;  // database hostname
            ui32 Port;     // database port
            TString Table; // table to read from
        };

        virtual NThreading::TFuture<TResponse> Resolve(const TRequest& request) const = 0;

        virtual ~ILoggingResolver() = default;
    };

    ILoggingResolver::TPtr MakeLoggingResolverEnvMock();

} //namespace NYql