#include "credentials.h"

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/uri/uri.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/spinlock.h>

#include <condition_variable>
#include <mutex>
#include <thread>

#define PROV_ERR "Oauth 2 token exchange credentials provider: "
#define INV_ARG "Invalid argument for " PROV_ERR
#define EXCH_ERR "Exchange token error in " PROV_ERR

namespace NYdb {

namespace {

// compares str to already lowered lowerStr
bool LowerAsciiEqual(TStringBuf str, TStringBuf lowerStr) {
    if (str.size() != lowerStr.size()) {
        return false;
    }
    for (size_t i = 0; i < lowerStr.size(); ++i) {
        char c = str[i];
        if (c >= 'A' && c <= 'Z') {
            c += 'a' - 'A';
        }
        if (c != lowerStr[i]) {
            return false;
        }
    }
    return true;
}

using TChronoDuration = std::chrono::duration<TDuration::TValue, std::micro>;

class TFixedTokenSource: public ITokenSource {
public:
    TFixedTokenSource(const TString& token, const TString& tokenType)
        : Token{token, tokenType}
    {
    }

    TToken GetToken() const override {
        return Token;
    }

    TToken Token;
};

class TTokenExchangeError: public std::runtime_error {
public:
    explicit TTokenExchangeError(const std::string& s, TKeepAliveHttpClient::THttpCode code = 0)
        : std::runtime_error(s)
        , HttpCode(code)
    {
    }

    explicit TTokenExchangeError(const char* s, TKeepAliveHttpClient::THttpCode code = 0)
        : std::runtime_error(s)
        , HttpCode(code)
    {
    }

public:
    TKeepAliveHttpClient::THttpCode HttpCode = 0;
};

bool IsRetryableError(TKeepAliveHttpClient::THttpCode code) {
    return code == HTTP_REQUEST_TIME_OUT
        || code == HTTP_AUTHENTICATION_TIMEOUT
        || code == HTTP_TOO_MANY_REQUESTS
        || code == HTTP_INTERNAL_SERVER_ERROR
        || code == HTTP_BAD_GATEWAY
        || code == HTTP_SERVICE_UNAVAILABLE
        || code == HTTP_GATEWAY_TIME_OUT;
}

ERetryErrorClass RetryPolicyClass(TKeepAliveHttpClient::THttpCode code) {
    return IsRetryableError(code) ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry; // In case when we already have token we know that all params are correct and all errors are temporary
}

ERetryErrorClass SyncRetryPolicyClass(const std::exception* ex, bool retryAllErrors) {
    if (const TTokenExchangeError* err = dynamic_cast<const TTokenExchangeError*>(ex)) {
        if (IsRetryableError(err->HttpCode)) {
            return ERetryErrorClass::LongRetry;
        }
        if (retryAllErrors) {
            return ERetryErrorClass::ShortRetry;
        }
    }
    if (const TSystemError* err = dynamic_cast<const TSystemError*>(ex)) {
        return ERetryErrorClass::ShortRetry;
    }

    // Else this is a usual error like bad response
    return ERetryErrorClass::NoRetry;
}

using TRetryPolicy = IRetryPolicy<TKeepAliveHttpClient::THttpCode>;
using TSyncRetryPolicy = IRetryPolicy<const std::exception*, bool>;

struct TPrivateOauth2TokenExchangeParams: public TOauth2TokenExchangeParams {
    TPrivateOauth2TokenExchangeParams(const TOauth2TokenExchangeParams& params)
        : TOauth2TokenExchangeParams(params)
    {
        if (!TokenEndpoint_) {
            throw std::invalid_argument(INV_ARG "no token endpoint");
        }

        for (const TString& aud : Audience_) {
            if (!aud) {
                throw std::invalid_argument(INV_ARG "empty audience");
            }
        }

        for (const TString& scope : Scope_) {
            if (!scope) {
                throw std::invalid_argument(INV_ARG "empty scope");
            }
        }

        ParseTokenEndpoint();
    }

    TPrivateOauth2TokenExchangeParams(const TPrivateOauth2TokenExchangeParams&) = default;

public:
    TString TokenHost_; // https://host
    ui16 TokenPort_;
    TString TokenRequest_; // Path without host:port

private:
    void ParseTokenEndpoint() {
        if (TokenEndpoint_.empty()) {
            throw std::invalid_argument(INV_ARG "token endpoint not set");
        }
        NUri::TUri url;
        NUri::TUri::TState::EParsed parseStatus = url.Parse(TokenEndpoint_, NUri::TFeature::FeaturesAll);
        if (parseStatus != NUri::TUri::TState::EParsed::ParsedOK) {
            throw std::invalid_argument(INV_ARG "failed to parse url");
        }
        if (url.IsNull(NUri::TUri::FieldScheme)) {
            throw std::invalid_argument(TStringBuilder() << INV_ARG "token url without scheme: " << TokenEndpoint_);
        }
        TokenHost_ = TStringBuilder() << url.GetField(NUri::TUri::FieldScheme) << "://" << url.GetHost();

        if (url.IsNull(NUri::TUri::FieldPort)) {
            TokenPort_ = 80;
            if (url.GetScheme() == NUri::TScheme::SchemeHTTPS) {
                TokenPort_ = 443;
            }
        } else {
            TokenPort_ = FromString(url.GetField(NUri::TUri::FieldPort));
        }

        TStringBuilder req;
        req << url.GetField(NUri::TUri::FieldPath);
        if (!url.IsNull(NUri::TUri::FieldQuery)) {
            req << '?' << url.GetField(NUri::TUri::FieldQuery);
        }
        TokenRequest_ = std::move(req);
    }
};

class TOauth2TokenExchangeProviderImpl: public std::enable_shared_from_this<TOauth2TokenExchangeProviderImpl>
{
    struct TTokenExchangeResult {
        TString Token;
        TInstant TokenDeadline;
        TInstant TokenRefreshTime;
    };

public:
    explicit TOauth2TokenExchangeProviderImpl(const TPrivateOauth2TokenExchangeParams& params)
        : Params(params)
    {
        ExchangeTokenSync(TInstant::Now());
    }

    void Stop() {
        {
            std::unique_lock<std::mutex> lock(StopMutex);
            Stopping = true;
            StopVar.notify_all();
        }

        if (RefreshTokenThread.joinable()) {
            RefreshTokenThread.join();
        }
    }

    TStringType GetAuthInfo() const {
        const TInstant now = TInstant::Now();
        TString token;
        with_lock (Lock) {
            if (!Token || now >= TokenDeadline) { // Update sync. This can be if we have repeating error during token refresh process. In this case we will try for the last time and throw an error
                ExchangeTokenSync(now, true);
                token = Token;
            } else {
                if (now >= TokenRefreshTime) {
                    TryRefreshToken();
                }
                token = Token; // Still valid
            }
        }
        return token;
    }

private:
    TString GetScopeParam() const {
        TStringBuilder scope;
        for (const TString& s : Params.Scope_) {
            if (scope) {
                scope << ' ';
            }
            scope << s;
        }
        return std::move(scope);
    }

    TString GetRequestParams() const {
        TCgiParameters params;
        params.emplace("grant_type", Params.GrantType_);
        params.emplace("requested_token_type", Params.RequestedTokenType_);

        auto addIfNotEmpty = [&](TStringBuf name, const TString& value) {
            if (value) {
                params.emplace(name, value);
            }
        };

        for (const TString& res : Params.Resource_) {
            params.emplace("resource", res);
        }
        for (const TString& aud : Params.Audience_) {
            params.emplace("audience", aud);
        }
        addIfNotEmpty("scope", GetScopeParam());

        auto addTokenSource = [&](TStringBuf tokenParamName, TStringBuf tokenTypeParamName, const std::shared_ptr<ITokenSource>& tokenSource) {
            if (tokenSource) {
                const ITokenSource::TToken token = tokenSource->GetToken();
                params.emplace(tokenParamName, token.Token);
                params.emplace(tokenTypeParamName, token.TokenType);
            }
        };

        addTokenSource("subject_token", "subject_token_type", Params.SubjectTokenSource_);
        addTokenSource("actor_token", "actor_token_type", Params.ActorTokenSource_);

        return params.Print();
    }

    void RaiseError(TKeepAliveHttpClient::THttpCode statusCode, TStringStream& responseStream) const {
        TStringBuilder err;
        err << EXCH_ERR << HttpCodeStrEx(statusCode);

        try {
            NJson::TJsonValue responseJson;
            NJson::ReadJsonTree(&responseStream, &responseJson, true);
            const auto& resp = responseJson.GetMap();

            auto optionalStringParam = [&](TStringBuf name) -> TStringBuf {
                auto iter = resp.find(name);
                if (iter == resp.end()) {
                    return {};
                }
                return iter->second.GetString();
            };

            if (TStringBuf error = optionalStringParam("error")) {
                err << ", error: " << error;
            }

            if (TStringBuf description = optionalStringParam("error_description")) {
                err << ", description: " << description;
            }

            if (TStringBuf uri = optionalStringParam("error_uri")) {
                err << ", error_uri: " << uri;
            }
        } catch (const std::exception& ex) {
            err << ", could not parse response: " << ex.what();
        }

        throw TTokenExchangeError(err, statusCode);
    }

    TTokenExchangeResult ProcessExchangeTokenResponse(TInstant now, TKeepAliveHttpClient::THttpCode statusCode, TStringStream& responseStream) const {
        if (statusCode != HTTP_OK) {
            RaiseError(statusCode, responseStream);
        }

        NJson::TJsonValue responseJson;
        try {
            NJson::ReadJsonTree(&responseStream, &responseJson, true);
        } catch (const std::exception& ex) {
            throw std::runtime_error(TStringBuilder() << EXCH_ERR "json parsing error: " << ex.what());
        }

        const auto& resp = responseJson.GetMap();

        auto requiredParam = [&](TStringBuf name) -> const NJson::TJsonValue& {
            auto iter = resp.find(name);
            if (iter == resp.end()) {
                throw std::runtime_error(TStringBuilder() << EXCH_ERR "no field \"" << name << "\" in response");
            }
            return iter->second;
        };

        const TString& tokenType = requiredParam("token_type").GetString();
        if (!LowerAsciiEqual(tokenType, "bearer")) {
            throw std::runtime_error(TStringBuilder() << EXCH_ERR "unsupported token type: \"" << tokenType << "\"");
        }

        const long long expireTime = requiredParam("expires_in").GetIntegerRobust(); // also converts from string/double
        if (expireTime <= 0) {
            throw std::runtime_error(TStringBuilder() << EXCH_ERR "incorrect expiration time: " << expireTime);
        }

        auto scopeIt = resp.find("scope");
        if (scopeIt != resp.end() && scopeIt->second.GetString() != GetScopeParam()) {
            throw std::runtime_error(TStringBuilder() << EXCH_ERR "different scope. Expected \""
                << GetScopeParam() << "\", but got \"" << scopeIt->second.GetString() << "\"");
        }

        const TString& token = requiredParam("access_token").GetString();
        if (token.empty()) {
            throw std::runtime_error(EXCH_ERR "got empty token");
        }

        const TDuration expireDelta = TDuration::Seconds(expireTime);
        TTokenExchangeResult result;
        result.TokenDeadline = now + expireDelta;
        result.TokenRefreshTime = now + expireDelta / 2;
        result.Token = TStringBuilder() << "Bearer " << token;
        return result;
    }

    // May be run without lock
    // Can throw exceptions
    TTokenExchangeResult ExchangeToken(TInstant now) const {
        TKeepAliveHttpClient client(Params.TokenHost_, Params.TokenPort_, Params.SocketTimeout_, Params.ConnectTimeout_);
        TStringStream responseStream;
        TKeepAliveHttpClient::THeaders headers;
        headers["Content-Type"] = "application/x-www-form-urlencoded";
        const TKeepAliveHttpClient::THttpCode statusCode = client.DoPost(Params.TokenRequest_, GetRequestParams(), &responseStream, headers);
        return ProcessExchangeTokenResponse(now, statusCode, responseStream);
    }

    void ExchangeTokenSync(TInstant now, bool retryAllErrors = false) const { // Is run under lock
        TSyncRetryPolicy::IRetryState::TPtr retryState;
        while (true) {
            try {
                TTokenExchangeResult result = ExchangeToken(now);
                Token = result.Token;
                TokenDeadline = result.TokenDeadline;
                TokenRefreshTime = result.TokenRefreshTime;
                break;
            } catch (const std::exception& ex) {
                if (!retryState) {
                    retryState = TSyncRetryPolicy::GetFixedIntervalPolicy( // retry more aggresively when we are in sync mode
                        SyncRetryPolicyClass,
                        TDuration::MilliSeconds(100), // delay // default
                        TDuration::MilliSeconds(300), // long delay // default
                        std::numeric_limits<size_t>::max(), // max retries // default
                        Params.SyncUpdateTimeout_ // max time
                    )->CreateRetryState();
                }
                if (auto interval = retryState->GetNextRetryDelay(&ex, retryAllErrors)) {
                    Sleep(*interval);
                } else {
                    throw;
                }
            }
        }
    }

    bool IsStopping() const {
        std::unique_lock<std::mutex> lock(StopMutex);
        return Stopping;
    }

    void RefreshToken() const {
        TRetryPolicy::IRetryState::TPtr retryState;
        TInstant deadline;
        with_lock (Lock) {
            deadline = TokenDeadline;
        }
        while (!IsStopping()) {
            const TInstant now = TInstant::Now();
            try {
                TTokenExchangeResult result = ExchangeToken(now);
                with_lock (Lock) {
                    Token = result.Token;
                    TokenDeadline = result.TokenDeadline;
                    TokenRefreshTime = result.TokenRefreshTime;
                    break;
                }
            } catch (const std::exception& ex) { // If this error will repeat, we finally will get it syncronously in GetAuthInfo() and pass to client
                if (!retryState) {
                    retryState = TRetryPolicy::GetExponentialBackoffPolicy(
                        RetryPolicyClass,
                        TDuration::MilliSeconds(10), // min delay // default
                        TDuration::MilliSeconds(200), // min long delay // default
                        TDuration::Seconds(30), // max delay // default
                        std::numeric_limits<size_t>::max(), // max retries // default
                        deadline - TInstant::Now() // max time
                    )->CreateRetryState();
                } else {
                    TKeepAliveHttpClient::THttpCode code = HTTP_CODE_MAX;
                    if (const auto* err = dynamic_cast<const TTokenExchangeError*>(&ex)) {
                        code = err->HttpCode;
                    }
                    if (auto delay = retryState->GetNextRetryDelay(code)) {
                        std::unique_lock<std::mutex> lock(StopMutex);
                        const bool stopping = StopVar.wait_for(
                            lock,
                            TChronoDuration(delay->GetValue()),
                            [this]() {
                                return Stopping;
                            }
                        );
                        if (stopping) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        TokenIsRefreshing = false;
    }

    void TryRefreshToken() const { // Is run under lock
        if (TokenIsRefreshing) {
            return;
        }
        if (RefreshTokenThread.joinable()) {
            RefreshTokenThread.join();
        }

        TokenIsRefreshing = true;
        RefreshTokenThread = std::thread(
            [w = weak_from_this()]() {
                if (auto p = w.lock()) {
                    p->RefreshToken();
                }
            }
        );
    }

private:
    TPrivateOauth2TokenExchangeParams Params;

    TAdaptiveLock Lock;
    mutable bool TokenIsRefreshing = false;
    mutable std::thread RefreshTokenThread;
    mutable TString Token;
    mutable TInstant TokenDeadline;
    mutable TInstant TokenRefreshTime;

    // Stop
    bool Stopping = false;
    mutable std::mutex StopMutex;
    mutable std::condition_variable StopVar;
};

class TOauth2TokenExchangeProvider: public ICredentialsProvider {
public:
    explicit TOauth2TokenExchangeProvider(const TPrivateOauth2TokenExchangeParams& params)
        : Impl(std::make_shared<TOauth2TokenExchangeProviderImpl>(params))
    {
    }

    TStringType GetAuthInfo() const override {
        return Impl->GetAuthInfo();
    }

    bool IsValid() const override {
        return true;
    }

    ~TOauth2TokenExchangeProvider() { // The last link tp provider is gone
        Impl->Stop();
    }

private:
    std::shared_ptr<TOauth2TokenExchangeProviderImpl> Impl;
};

class TOauth2TokenExchangeFactory: public ICredentialsProviderFactory {
public:
    explicit TOauth2TokenExchangeFactory(const TOauth2TokenExchangeParams& params)
        : Provider(std::make_shared<TOauth2TokenExchangeProvider>(params))
    {
    }

    TCredentialsProviderPtr CreateProvider() const override {
        return Provider;
    }

private:
    std::shared_ptr<TOauth2TokenExchangeProvider> Provider;
};

} // namespace

std::shared_ptr<ICredentialsProviderFactory> CreateOauth2TokenExchangeCredentialsProviderFactory(const TOauth2TokenExchangeParams& params) {
    return std::make_shared<TOauth2TokenExchangeFactory>(params);
}

std::shared_ptr<ITokenSource> CreateFixedTokenSource(const TString& token, const TString& tokenType) {
    return std::make_shared<TFixedTokenSource>(token, tokenType);
}

} // namespace NYdb
