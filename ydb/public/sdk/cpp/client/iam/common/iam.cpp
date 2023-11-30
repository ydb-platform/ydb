#include <ydb/public/sdk/cpp/client/iam/common/iam.h>

#include <library/cpp/http/simple/http_client.h>

using namespace NYdbGrpc;

namespace NYdb {

class TIAMCredentialsProvider : public ICredentialsProvider {
public:
    TIAMCredentialsProvider(const TIamHost& params)
        : HttpClient_(TSimpleHttpClient(params.Host, params.Port))
        , Request_("/computeMetadata/v1/instance/service-accounts/default/token")
        , NextTicketUpdate_(TInstant::Zero())
        , RefreshPeriod_(params.RefreshPeriod)
    {
        GetTicket();
    }

    TStringType GetAuthInfo() const override {
        if (TInstant::Now() >= NextTicketUpdate_) {
            GetTicket();
        }
        return Ticket_;
    }

    bool IsValid() const override {
        return true;
    }

private:
    TSimpleHttpClient HttpClient_;
    TStringType Request_;
    mutable TStringType Ticket_;
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

            if (auto it = respMap.find("access_token"); it == respMap.end())
                ythrow yexception() << "Result doesn't contain access_token";
            else if (TString ticket = it->second.GetStringSafe(); ticket.empty())
                ythrow yexception() << "Got empty ticket";
            else
                Ticket_ = std::move(ticket);

            if (auto it = respMap.find("expires_in"); it == respMap.end())
                ythrow yexception() << "Result doesn't contain expires_in";
            else {
                const TDuration expiresIn = TDuration::Seconds(it->second.GetUInteger());

                NextTicketUpdate_ = TInstant::Now() + std::max(expiresIn, RefreshPeriod_);
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

}