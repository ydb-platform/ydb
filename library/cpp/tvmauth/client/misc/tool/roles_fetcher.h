#pragma once

#include <library/cpp/tvmauth/client/misc/fetch_result.h>
#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/client/misc/roles/roles.h>

#include <library/cpp/tvmauth/client/logger.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NTvmAuth::NTvmTool {
    struct TRolesFetcherSettings {
        TString SelfAlias;
        TDuration UpdatePeriod = TDuration::Minutes(1);
        TDuration WarnPeriod = TDuration::Minutes(20);
    };

    class TRolesFetcher {
    public:
        TRolesFetcher(const TRolesFetcherSettings& settings, TLoggerPtr logger);

        bool IsTimeToUpdate(TDuration sinceUpdate) const;
        bool ShouldWarn(TDuration sinceUpdate) const;
        bool AreRolesOk() const;

        NUtils::TFetchResult FetchActualRoles(const TKeepAliveHttpClient::THeaders& authHeader,
                                              TKeepAliveHttpClient& client) const;
        void Update(NUtils::TFetchResult&& fetchResult);

        NTvmAuth::NRoles::TRolesPtr GetCurrentRoles() const;

    protected:
        struct TRequest {
            TString Url;
            TKeepAliveHttpClient::THeaders Headers;
        };

    protected:
        TRequest CreateRequest(const TKeepAliveHttpClient::THeaders& authHeader) const;

    private:
        const TRolesFetcherSettings Settings_;
        const TLoggerPtr Logger_;
        const TString IfNoneMatch_ = "If-None-Match";

        NUtils::TProtectedValue<NTvmAuth::NRoles::TRolesPtr> CurrentRoles_;
    };
}
