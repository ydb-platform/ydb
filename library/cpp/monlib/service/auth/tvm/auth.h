#pragma once

#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/monlib/service/auth.h>
#include <library/cpp/tvmauth/client/facade.h>

namespace NMonitoring {
    struct ITvmManager {
        virtual ~ITvmManager() = default;
        virtual bool IsAllowedClient(NTvmAuth::TTvmId clientId) = 0;
        virtual NTvmAuth::TCheckedServiceTicket CheckServiceTicket(TStringBuf ticket) = 0;
    };

    THolder<ITvmManager> CreateDefaultTvmManager(
        NTvmAuth::NTvmApi::TClientSettings settings,
        TVector<NTvmAuth::TTvmId> allowedClients,
        NTvmAuth::TLoggerPtr logger = NTvmAuth::TDevNullLogger::IAmBrave());

    THolder<ITvmManager> CreateDefaultTvmManager(
        NTvmAuth::NTvmTool::TClientSettings settings,
        TVector<NTvmAuth::TTvmId> allowedClients,
        NTvmAuth::TLoggerPtr logger = NTvmAuth::TDevNullLogger::IAmBrave());

    THolder<ITvmManager> CreateDefaultTvmManager(
        TAtomicSharedPtr<NTvmAuth::TTvmClient> client,
        TVector<NTvmAuth::TTvmId> allowedClients);

    THolder<ITvmManager> CreateDefaultTvmManager(
        std::shared_ptr<NTvmAuth::TTvmClient> client,
        TVector<NTvmAuth::TTvmId> allowedClients);

    THolder<IAuthProvider> CreateTvmAuth(THolder<ITvmManager> tvmManager);
} // namespace NMonitoring
