#pragma once

#include "protocol.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include <util/system/spinlock.h>

namespace NYdb::inline Dev::NOidc {

    class TOidcProvider
        : public ICredentialsProvider,
          public std::enable_shared_from_this<TOidcProvider> {
    public:
        TOidcProvider(
            std::string issuer,
            std::shared_ptr<IOidcTokenCache> tokenCache,
            std::weak_ptr<ICoreFacility> facility);
        ~TOidcProvider() override;

        std::string GetAuthInfo() const override;
        NThreading::TFuture<std::string> GetAuthInfoAsync() const override;
        bool IsValid() const override;

        virtual void Bootstrap() = 0;
        void Start();

    protected:
        virtual void Refresh() = 0;

        TOidcProtocolClient& Protocol();
        std::optional<TOidcTokenSet> ReadCache() const;
        TOidcTokenSet GetTokens() const;
        void Restore(TOidcTokenSet tokens);
        void Publish(TOidcTokenSet tokens);
        void Schedule(TInstant when);
        void ScheduleAfter(TDuration delay);
        void StopRefreshing();

        static TInstant RefreshAt(const TOidcToken& accessToken);

    private:
        bool OnPeriodicTick(EStatus status);
        void Fail(std::exception_ptr error);
        void HandleRefreshError(std::exception_ptr error);
        static std::string BuildAuthInfo(const std::string& accessToken);

    private:
        TOidcProtocolClient Protocol_;
        std::shared_ptr<IOidcTokenCache> TokenCache_;
        std::weak_ptr<ICoreFacility> Facility_;

        mutable TAdaptiveLock Lock_;
        TOidcTokenSet Tokens_;
        TInstant NextRefresh_ = TInstant::Zero();
        bool Requesting_ = false;
        bool Stopped_ = false;

        mutable NThreading::TPromise<std::string> AuthInfo_;
    };

} // namespace NYdb::inline Dev::NOidc
