#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>

#include <ydb/public/api/client/yc_public/iam/iam_token_service.pb.h>
#include <ydb/public/api/client/yc_public/iam/iam_token_service.grpc.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/http/simple/http_client.h>

#include <mutex>

using namespace yandex::cloud::iam::v1;

namespace NYdb::inline Dev {

class TIAMCredentialsProvider : public ICredentialsProvider {
public:
    TIAMCredentialsProvider(const TIamHost& params)
        : HttpClient_(TSimpleHttpClient(TString(params.Host), params.Port))
        , Request_("/computeMetadata/v1/instance/service-accounts/default/token")
        , NextTicketUpdate_(TInstant::Zero())
        , RefreshPeriod_(params.RefreshPeriod)
    {
        GetTicket();
    }

    std::string GetAuthInfo() const override {
        std::string ticket;
        TInstant nextTicketUpdate;
        {
            std::lock_guard lock(Lock_);
            ticket = Ticket_;
            nextTicketUpdate = NextTicketUpdate_;
        }
        if (TInstant::Now() >= nextTicketUpdate) {
            GetTicket();
            {
                std::lock_guard lock(Lock_);
                ticket = Ticket_;
            }
        }
        return ticket;
    }

    bool IsValid() const override {
        return true;
    }

private:
    TSimpleHttpClient HttpClient_;
    std::string Request_;
    mutable std::mutex Lock_;
    mutable std::string Ticket_;
    mutable TInstant NextTicketUpdate_;
    TDuration RefreshPeriod_;

    void GetTicket() const {
        try {
            TStringStream out;
            TSimpleHttpClient::THeaders headers;
            headers["Metadata-Flavor"] = "Google";
            HttpClient_.DoGet(Request_, &out, headers);
            NJson::TJsonValue resp;
            NJson::ReadJsonTree(&out, &resp, true);

            auto respMap = resp.GetMap();

            std::string ticket;
            if (auto it = respMap.find("access_token"); it == respMap.end())
                ythrow yexception() << "Result doesn't contain access_token";
            else if (ticket = it->second.GetStringSafe(); ticket.empty())
                ythrow yexception() << "Got empty ticket";

            const auto now = TInstant::Now();
            TInstant nextUpdate;
            TDuration expiresIn;
            if (auto it = respMap.find("expires_in"); it != respMap.end()) {
                auto seconds = it->second.GetUInteger();
                if (seconds > 0) {
                    expiresIn = TDuration::Seconds(seconds);
                }
            } else if (auto it = respMap.find("expiry"); it != respMap.end()) {
                try {
                    TInstant expiry;
                    if (TInstant::TryParseIso8601(it->second.GetStringSafe(), expiry) && expiry > now) {
                        expiresIn = expiry - now;
                    }
                } catch (...) {
                }
            }
            if (expiresIn > TDuration::Zero()) {
                const auto halfLife = expiresIn / 2;
                const auto interval = std::max(std::min(halfLife, RefreshPeriod_), TDuration::MilliSeconds(100));
                nextUpdate = now + interval;
            } else {
                nextUpdate = now + std::min(RefreshPeriod_, TDuration::Minutes(30));
            }

            {
                std::lock_guard lock(Lock_);
                Ticket_ = std::move(ticket);
                NextTicketUpdate_ = nextUpdate;
            }
        } catch (...) {
        }
    }
};

class TIamCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamCredentialsProviderFactory(const TIamHost& params): Params_(params) {}

    TCredentialsProviderPtr CreateProvider() const final {
        return std::make_shared<TIAMCredentialsProvider>(Params_);
    }

private:
    TIamHost Params_;
};

/// Acquire an IAM token using a local metadata service on a virtual machine.
TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(const TIamHost& params ) {
    return std::make_shared<TIamCredentialsProviderFactory>(params);
}

TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactory(const TIamJwtFilename& params) {
    TIamJwtParams jwtParams = { params, ReadJwtKeyFile(params.JwtFilename) };
    return std::make_shared<TIamJwtCredentialsProviderFactory<CreateIamTokenRequest,
                                                              CreateIamTokenResponse,
                                                              IamTokenService>>(std::move(jwtParams));
}

TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactory(const TIamJwtContent& params) {
    TIamJwtParams jwtParams = { params, ParseJwtParams(params.JwtContent) };
    return std::make_shared<TIamJwtCredentialsProviderFactory<CreateIamTokenRequest,
                                                              CreateIamTokenResponse,
                                                              IamTokenService>>(std::move(jwtParams));
}

TCredentialsProviderFactoryPtr CreateIamOAuthCredentialsProviderFactory(const TIamOAuth& params) {
    return std::make_shared<TIamOAuthCredentialsProviderFactory<CreateIamTokenRequest,
                                                                CreateIamTokenResponse,
                                                                IamTokenService>>(params);
}

}
