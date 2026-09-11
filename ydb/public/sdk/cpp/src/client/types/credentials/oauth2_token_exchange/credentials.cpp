#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/runtime/runtime.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/uri/uri.h>
#include <library/cpp/retry/retry_policy.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/spinlock.h>

#include <mutex>

#define PROV_ERR "Oauth 2 token exchange credentials provider: "
#define INV_ARG "Invalid argument for " PROV_ERR
#define EXCH_ERR "Exchange token error in " PROV_ERR

namespace NYdb::inline Dev {

namespace {

// compares str to already lowered lowerStr
bool LowerAsciiEqual(std::string_view str, std::string_view lowerStr) {
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

class TFixedTokenSource: public ITokenSource {
public:
    TFixedTokenSource(const std::string& token, const std::string& tokenType)
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

ERetryErrorClass RetryPolicyClass(const std::exception* ex) {
    if (const TTokenExchangeError* err = dynamic_cast<const TTokenExchangeError*>(ex)) {
        if (IsRetryableError(err->HttpCode)) {
            return ERetryErrorClass::LongRetry;
        }
    }
    if (dynamic_cast<const TSystemError*>(ex)) {
        return ERetryErrorClass::ShortRetry;
    }

    // Else this is a usual error like bad response
    return ERetryErrorClass::NoRetry;
}

using TRetryPolicy = IRetryPolicy<const std::exception*>;

struct TPrivateOauth2TokenExchangeParams: public TOauth2TokenExchangeParams {
    TPrivateOauth2TokenExchangeParams(const TOauth2TokenExchangeParams& params)
        : TOauth2TokenExchangeParams(params)
    {
        if (TokenEndpoint_.empty()) {
            throw std::invalid_argument(INV_ARG "no token endpoint");
        }

        for (const std::string& aud : Audience_) {
            if (aud.empty()) {
                throw std::invalid_argument(INV_ARG "empty audience");
            }
        }

        for (const std::string& scope : Scope_) {
            if (scope.empty()) {
                throw std::invalid_argument(INV_ARG "empty scope");
            }
        }

        ParseTokenEndpoint();
    }

    TPrivateOauth2TokenExchangeParams(const TPrivateOauth2TokenExchangeParams&) = default;

public:
    std::string TokenHost_; // https://host
    ui16 TokenPort_;
    std::string TokenRequest_; // Path without host:port

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
            TokenPort_ = FromString<ui16>(url.GetField(NUri::TUri::FieldPort));
        }

        TStringBuilder req;
        req << url.GetField(NUri::TUri::FieldPath);
        if (!url.IsNull(NUri::TUri::FieldQuery)) {
            req << '?' << url.GetField(NUri::TUri::FieldQuery);
        }
        TokenRequest_ = std::move(req);
    }
};

class TTokenExchangeState final : public std::enable_shared_from_this<TTokenExchangeState> {
    struct TTokenExchangeResult {
        std::string Token;
        TInstant TokenRefreshTime;
    };

public:
    TTokenExchangeState(const TPrivateOauth2TokenExchangeParams& params,
                        std::weak_ptr<ICoreFacility> responseFacility)
        : Params(params)
        , ResponseFacility(std::move(responseFacility))
        , AuthInfo(NThreading::NewPromise<std::string>())
    {
    }

    void Start() {
        if (ResponseFacility.expired()) {
            Stop();
        } else {
            Schedule(TDuration::Zero());
        }
    }

    void Stop(std::exception_ptr error = std::make_exception_ptr(yexception() << PROV_ERR "stopped")) {
        NThreading::TPromise<std::string> promise;
        with_lock (Lock) {
            Stopping = true;
            promise = AuthInfo;
        }
        promise.TrySetException(std::move(error));
    }

    NThreading::TFuture<std::string> GetAuthInfoAsync() const {
        with_lock (Lock) {
            return AuthInfo.GetFuture();
        }
    }

private:
    std::string GetScopeParam() const {
        TStringBuilder scope;
        for (const std::string& s : Params.Scope_) {
            if (!scope.empty()) {
                scope << ' ';
            }
            scope << s;
        }
        return std::move(scope);
    }

    std::string GetRequestParams() const {
        TCgiParameters params;
        params.emplace("grant_type", Params.GrantType_);
        params.emplace("requested_token_type", Params.RequestedTokenType_);

        auto addIfNotEmpty = [&](std::string_view name, const std::string& value) {
            if (!value.empty()) {
                params.emplace(name, value);
            }
        };

        for (const std::string& res : Params.Resource_) {
            params.emplace("resource", res);
        }
        for (const std::string& aud : Params.Audience_) {
            params.emplace("audience", aud);
        }
        addIfNotEmpty("scope", GetScopeParam());

        auto addTokenSource = [&](std::string_view tokenParamName, std::string_view tokenTypeParamName, const std::shared_ptr<ITokenSource>& tokenSource) {
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

            auto optionalStringParam = [&](std::string_view name) -> std::string_view {
                auto iter = resp.find(std::string{name});
                if (iter == resp.end()) {
                    return {};
                }
                return iter->second.GetString();
            };

            std::string_view error = optionalStringParam("error");
            if (!error.empty()) {
                err << ", error: " << error;
            }

            std::string_view description = optionalStringParam("error_description");
            if (!description.empty()) {
                err << ", description: " << description;
            }

            std::string_view uri = optionalStringParam("error_uri");
            if (!uri.empty()) {
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

        auto requiredParam = [&](std::string_view name) -> const NJson::TJsonValue& {
            auto iter = resp.find(std::string{name});
            if (iter == resp.end()) {
                throw std::runtime_error(TStringBuilder() << EXCH_ERR "no field \"" << name << "\" in response");
            }
            return iter->second;
        };

        const std::string& tokenType = requiredParam("token_type").GetString();
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

        const std::string& token = requiredParam("access_token").GetString();
        if (token.empty()) {
            throw std::runtime_error(EXCH_ERR "got empty token");
        }

        const TDuration expireDelta = TDuration::Seconds(expireTime);
        TTokenExchangeResult result;
        result.TokenRefreshTime = now + expireDelta / 2;
        result.Token = TStringBuilder() << "Bearer " << token;
        return result;
    }

    // May be run without lock
    // Can throw exceptions
    TTokenExchangeResult ExchangeToken(TInstant now) const {
        TKeepAliveHttpClient client(TString(Params.TokenHost_), Params.TokenPort_, Params.SocketTimeout_, Params.ConnectTimeout_);
        TStringStream responseStream;
        TKeepAliveHttpClient::THeaders headers;
        headers["Content-Type"] = "application/x-www-form-urlencoded";
        const TKeepAliveHttpClient::THttpCode statusCode = client.DoPost(Params.TokenRequest_, GetRequestParams(), &responseStream, headers);
        return ProcessExchangeTokenResponse(now, statusCode, responseStream);
    }

    void Schedule(TDuration delay, bool refresh = false) {
        if (IsStopping()) {
            return;
        }
        try {
            GetRuntime().Schedule(delay, [weak = weak_from_this(), refresh] {
                if (auto self = weak.lock()) {
                    if (refresh) {
                        with_lock (self->Lock) {
                            if (self->Stopping) {
                                return;
                            }
                            self->AuthInfo = NThreading::NewPromise<std::string>();
                        }
                    }
                    self->Run();
                }
            });
        } catch (...) {
            Fail(std::current_exception());
        }
    }

    void Run() {
        if (IsStopping()) {
            return;
        }
        TTokenExchangeResult result;
        try {
            result = ExchangeToken(TInstant::Now());
        } catch (const std::exception& ex) {
            if (!RetryState) {
                RetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                    RetryPolicyClass,
                    TDuration::MilliSeconds(10),
                    TDuration::MilliSeconds(200),
                    TDuration::Seconds(30))->CreateRetryState();
            }
            if (auto delay = RetryState->GetNextRetryDelay(&ex)) {
                Schedule(*delay);
            } else {
                Fail(std::current_exception());
            }
            return;
        } catch (...) {
            Fail(std::current_exception());
            return;
        }

        RetryState.reset();
        NThreading::TPromise<std::string> promise;
        with_lock (Lock) {
            if (Stopping) {
                return;
            }
            promise = AuthInfo;
        }
        Complete([promise, token = std::move(result.Token)]() mutable {
            promise.TrySetValue(std::move(token));
        });
        Schedule(result.TokenRefreshTime - TInstant::Now(), true);
    }

    bool IsStopping() const {
        with_lock (Lock) {
            return Stopping;
        }
    }

    void Fail(std::exception_ptr error) {
        NThreading::TPromise<std::string> promise;
        with_lock (Lock) {
            promise = AuthInfo;
        }
        Complete([promise, error = std::move(error)]() mutable {
            promise.TrySetException(std::move(error));
        });
    }

    void Complete(TPostTaskCb&& callback) {
        try {
            if (auto facility = ResponseFacility.lock()) {
                facility->PostToResponseQueue(std::move(callback));
                return;
            }
        } catch (...) {
            if (!GetAuthInfoAsync().IsReady()) {
                Stop(std::current_exception());
            }
            return;
        }
        if (!GetAuthInfoAsync().IsReady()) {
            Stop();
        }
    }

private:
    const TPrivateOauth2TokenExchangeParams Params;
    const std::weak_ptr<ICoreFacility> ResponseFacility;
    mutable TAdaptiveLock Lock;
    NThreading::TPromise<std::string> AuthInfo;
    TRetryPolicy::IRetryState::TPtr RetryState;
    bool Stopping = false;
};

class TOauth2TokenExchangeProviderImpl final : public ICredentialsProvider {
public:
    TOauth2TokenExchangeProviderImpl(const TPrivateOauth2TokenExchangeParams& params,
                                    std::weak_ptr<ICoreFacility> facility)
        : State(std::make_shared<TTokenExchangeState>(params, std::move(facility)))
    {
        State->Start();
    }

    ~TOauth2TokenExchangeProviderImpl() override {
        // Active HTTP calls retain State, but never the provider itself.
        try {
            State->Stop();
        } catch (...) {
            // A user future continuation must not throw out of destruction.
        }
    }

    std::string GetAuthInfo() const override {
        return GetAuthInfoAsync().GetValueSync();
    }

    NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
        return State->GetAuthInfoAsync();
    }

    bool IsValid() const override {
        return true;
    }

private:
    const std::shared_ptr<TTokenExchangeState> State;
};

class TOauth2TokenExchangeFactory: public ICredentialsProviderFactory {
public:
    explicit TOauth2TokenExchangeFactory(const TOauth2TokenExchangeParams& params)
        : Params(params)
    {
    }

    TCredentialsProviderPtr CreateProvider() const override {
        std::lock_guard lock(Lock);
        if (!Provider) {
            Provider = std::make_shared<TOauth2TokenExchangeProviderImpl>(Params, CreateSimpleCoreFacility());
        }
        return Provider;
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return std::make_shared<TOauth2TokenExchangeProviderImpl>(Params, std::move(facility));
    }

private:
    TPrivateOauth2TokenExchangeParams Params;
    mutable std::mutex Lock;
    mutable TCredentialsProviderPtr Provider;
};

} // namespace

std::shared_ptr<ICredentialsProviderFactory> CreateOauth2TokenExchangeCredentialsProviderFactory(const TOauth2TokenExchangeParams& params) {
    return std::make_shared<TOauth2TokenExchangeFactory>(params);
}

std::shared_ptr<ITokenSource> CreateFixedTokenSource(const std::string& token, const std::string& tokenType) {
    return std::make_shared<TFixedTokenSource>(token, tokenType);
}

} // namespace NYdb
