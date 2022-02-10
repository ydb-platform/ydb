#pragma once

#include "mon_service_http_request.h"

namespace NMonitoring {
    enum class EAuthType {
        None = 0,
        Tvm = 1,
    };

    struct TAuthResult {
        enum class EStatus {
            NoCredentials = 0,
            Denied,
            Ok,
        };

        TAuthResult(EStatus status)
            : Status{status}
        {
        }

        static TAuthResult Denied() {
            return TAuthResult(EStatus::Denied);
        }

        static TAuthResult NoCredentials() {
            return TAuthResult(EStatus::NoCredentials);
        }

        static TAuthResult Ok() {
            return TAuthResult(EStatus::Ok);
        }

        explicit operator bool() const {
            return Status == EStatus::Ok;
        }

        EStatus Status{EStatus::NoCredentials};
    };

    struct IAuthProvider {
        virtual ~IAuthProvider() = default;
        virtual TAuthResult Check(const IHttpRequest& req) = 0;
    };

    THolder<IAuthProvider> CreateFakeAuth();
} // namespace NMonitoring
