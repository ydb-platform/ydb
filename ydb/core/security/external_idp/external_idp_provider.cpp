#include "external_idp_provider.h"

#include <ydb/core/security/util/counters.h>
#include <ydb/core/security/util/jwk.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/security/util.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>

#include <chrono>
#include <exception>
#include <functional>
#include <unordered_map>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::EXTERNAL_IDP_PROVIDER

namespace NKikimr {

namespace {

constexpr TDuration REFRESH_PERIOD = TDuration::Seconds(1);
constexpr TDuration DEFAULT_EXPIRATION_PERIOD = TDuration::Minutes(10);

constexpr TStringBuf JWKS_URI = "jwks_uri";
constexpr TStringBuf ISSUER = "issuer";
constexpr TStringBuf WELL_KNOWN_DISCOVERY_URI = ".well-known/openid-configuration";

// Asymmetric algorithms supported for external IdP JWT verification.
// HMAC (HS*) algorithms are deliberately excluded: they require a shared secret,
// not a public key. Including them would enable the "algorithm confusion" attack
// where an attacker signs a token with HS256 using the (public) RSA key.
template <typename TVerifier>
const std::unordered_map<NSecurity::EJwkAlg, std::function<void(TVerifier&, const TString&)>> SUPPORTED_ALGOS = {
    {NSecurity::EJwkAlg::ES256,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::es256(pubkey.c_str())); }},
    {NSecurity::EJwkAlg::ES384,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::es384(pubkey.c_str())); }},
    {NSecurity::EJwkAlg::ES512,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::es512(pubkey.c_str())); }},
    {NSecurity::EJwkAlg::PS256,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::ps256(pubkey.c_str())); }},
    {NSecurity::EJwkAlg::PS384,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::ps384(pubkey.c_str())); }},
    {NSecurity::EJwkAlg::PS512,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::ps512(pubkey.c_str())); }},
    {NSecurity::EJwkAlg::RS256,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::rs256(pubkey.c_str())); }},
    {NSecurity::EJwkAlg::RS384,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::rs384(pubkey.c_str())); }},
    {NSecurity::EJwkAlg::RS512,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::rs512(pubkey.c_str())); }},
};

TString BuildKey(const TString& kty, const TString& kid) {
    return TStringBuilder() << kty << '-' << kid;
}

TString BuildDiscoveryUrl(TString issuer) {
    return TStringBuilder() << issuer << '/' << WELL_KNOWN_DISCOVERY_URI;
}

bool IsHttpsUrl(TStringBuf url) {
    return url.starts_with("https://");
}

// returns a value in [50%, 100%] of its input
TDuration ApplyDownwardJitter(const TDuration& jitter) {
    const ui64 half = jitter.MilliSeconds() / 2;
    const ui64 range = jitter.MilliSeconds() - half + 1;
    const ui64 randomMs = half + RandomNumber<ui64>(range);
    return TDuration::MilliSeconds(randomMs);
}

TDuration CalculateRetry(const TDuration& minPeriod, const TDuration& maxPeriod, ui64 attempt) {
    const auto backoff = minPeriod + TDuration::Seconds(1ul << Min(attempt, 10ul));
    const auto clamped = Min(backoff, maxPeriod);
    return ApplyDownwardJitter(clamped);
}

NMonitoring::TBucketBounds ResponseTimeBucketsMs() {
    static NMonitoring::TBucketBounds buckets = {0, 5, 10, 50, 100, 500, 1000, 5000, 10000, 30000};
    return buckets;
}

NMonitoring::TBucketBounds AuthProcessingTimeBucketsMs() {
    static NMonitoring::TBucketBounds buckets = {0, 1, 5, 10, 50, 100, 500, 1000, 2000, 5000};
    return buckets;
}

class TRecurringPeriod {
public:
    TRecurringPeriod() = default;
    TRecurringPeriod(
        const NKikimrProto::TExternalIdpConfig::TPeriodicSettings& settings,
        TInstant scheduled,
        NMonitoring::TDynamicCounterPtr counters
    );

    bool CanSendRequest(TInstant now) const;
    bool IsSameRequest(NHttp::THttpOutgoingRequestPtr request) const;
    void ResetBackoff();

    TDuration OnRequestSending(TInstant now, NHttp::THttpOutgoingRequestPtr request);
    void OnSuccessReceived(TInstant now);
    void OnErrorReceived(TInstant now);

    TString GetHtml() const;

private:
    void OnReceived(TInstant now);

private:
    TDuration SuccessRefreshPeriod{TDuration::Max()};
    TDuration MinErrorRefreshPeriod{TDuration::Max()};
    TDuration MaxErrorRefreshPeriod{TDuration::Max()};
    TDuration RequestTimeout{TDuration::Zero()};

    TInstant Scheduled{TInstant::Max()};
    TInstant Started{TInstant::Zero()};
    TInstant Ended{TInstant::Zero()};
    ui64 RetryCount{0};

    NHttp::THttpOutgoingRequestPtr InFlight{nullptr};

    NMonitoring::THistogramPtr ResponseTime{nullptr};
    NMonitoring::TDynamicCounters::TCounterPtr Successes{nullptr};
    NMonitoring::TDynamicCounters::TCounterPtr Errors{nullptr};
};

class TJwksCache {
public:
    TJwksCache() = default;
    TJwksCache(
        const NKikimrProto::TExternalIdpConfig::TJwksCacheSettings& settings,
        NMonitoring::TDynamicCounterPtr counters
    );

    bool IsStale(const TInstant& now) const;

    size_t Count() const;
    TMaybe<TString> Get(const TString& key) const;
    void Update(const TInstant& now, THashMap<TString, TString> keys);

    void Clear();

    TString GetHtml(const TInstant& now) const;

private:
    THashMap<TString, TString> Keys;

    TDuration Timeout{TDuration::Max()};
    TInstant LastUpdate{TInstant::Zero()};

    NMonitoring::TDynamicCounters::TCounterPtr KeyCount{nullptr};
};

class TExternalIdpProvider : public NActors::TActorBootstrapped<TExternalIdpProvider> {
public:
    TExternalIdpProvider(const NKikimrProto::TExternalIdpConfig& config, const NActors::TActorId& httpProxyId);

    void Bootstrap(const TActorContext& ctx);

private:
    using TThis = TExternalIdpProvider;

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev);

    // Bootstrap
    void RegisterPages(const TActorContext& ctx);
    void RegisterCounters(const TActorContext& ctx);
    void RegisterFields(const TActorContext& ctx);

    // Authenticate request flow
    void Handle(TEvExternalIdpProvider::TEvAuthenticateRequest::TPtr& ev, const TActorContext& ctx);
    // Discovery / JWKS HTTP fetches
    void StartDiscoveryFetch(const TInstant& now);
    void StartJwksFetch(const TInstant& now);

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev, const TActorContext& ctx);

    void HandleDiscoveryResponse(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& resp, const TInstant& now);
    void HandleJwksResponse(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& resp, const TInstant& now);

    // Mon page
    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx);

    // Periodic refresh
    void HandleWakeup(const TActorContext& ctx);

    void ReplyError(
        const TActorId& sender, const TString& key,
        TEvExternalIdpProvider::EStatus status,
        const TString& message, bool retryable = false
    );

private:
    NKikimrProto::TExternalIdpConfig Config;
    NActors::TActorId HttpProxyId;

    TDuration AllowedClockSkew{TDuration::Zero()};
    TRecurringPeriod DiscoveryRefresh;
    TRecurringPeriod JwksRefresh;

    TString JwksUrl;
    TJwksCache JwksCache;

    NMonitoring::TDynamicCounterPtr Counters;

    NMonitoring::THistogramPtr CounterAuthProcessingTime;
    NMonitoring::TDynamicCounters::TCounterPtr CounterAuthSuccess;
    NMonitoring::TDynamicCounters::TCounterPtr CounterAuthFailed;
};

TRecurringPeriod::TRecurringPeriod(
    const NKikimrProto::TExternalIdpConfig::TPeriodicSettings& settings,
    TInstant scheduled,
    NMonitoring::TDynamicCounterPtr counters)
    : SuccessRefreshPeriod{TDuration::Parse(settings.GetSuccessRefreshPeriod())}
    , MinErrorRefreshPeriod{TDuration::Parse(settings.GetMinErrorRefreshPeriod())}
    , MaxErrorRefreshPeriod{TDuration::Parse(settings.GetMaxErrorRefreshPeriod())}
    , RequestTimeout{TDuration::Parse(settings.GetRequestTimeout())}
    , Scheduled{scheduled}
    , ResponseTime(counters->GetHistogram("ResponseTimeMs", NMonitoring::ExplicitHistogram(ResponseTimeBucketsMs())))
    , Successes(counters->GetCounter("Successes", true))
    , Errors(counters->GetCounter("Errors", true))
{}

bool TRecurringPeriod::CanSendRequest(TInstant now) const {
    return InFlight == nullptr && Scheduled < now;
}

bool TRecurringPeriod::IsSameRequest(NHttp::THttpOutgoingRequestPtr request) const {
    return InFlight != nullptr && InFlight == request;
}

void TRecurringPeriod::ResetBackoff() {
    if (InFlight != nullptr) {
        return;
    }

    RetryCount = 0;
    Scheduled = TInstant::Zero();
}

TDuration TRecurringPeriod::OnRequestSending(TInstant now, NHttp::THttpOutgoingRequestPtr request) {
    Started = now;
    Ended = TInstant::Zero();

    InFlight = request;
    return RequestTimeout;
}

void TRecurringPeriod::OnSuccessReceived(TInstant now) {
    OnReceived(now);

    RetryCount = 0;
    Scheduled = now + ApplyDownwardJitter(SuccessRefreshPeriod);

    Successes->Inc();
}

void TRecurringPeriod::OnErrorReceived(TInstant now) {
    OnReceived(now);

    ++RetryCount;
    Scheduled = now + CalculateRetry(MinErrorRefreshPeriod, MaxErrorRefreshPeriod, RetryCount);

    Errors->Inc();
}

TString TRecurringPeriod::GetHtml() const {
    TStringBuilder html;
    html << "<div>";

    html << "<table class='table table-hover simple-table1'>";
    html << "<caption>Refresh info</caption>";

    html << "<tr><td scope=\"row\">Success refresh period</td><td>" << SuccessRefreshPeriod << "</td></tr>";
    html << "<tr><td scope=\"row\">Min error refresh period</td><td>" << MinErrorRefreshPeriod << "</td></tr>";
    html << "<tr><td scope=\"row\">Max error refresh period</td><td>" << MaxErrorRefreshPeriod << "</td></tr>";
    html << "<tr><td scope=\"row\">Request timeout</td><td>" << RequestTimeout << "</td></tr>";

    if (InFlight == nullptr) {
        html << "<tr><td scope=\"row\">Request in progress</td><td>FALSE</td></tr>";
        html << "<tr><td scope=\"row\">Previous request sent</td><td>" << Started << "</td></tr>";
        html << "<tr><td scope=\"row\">Previous request received</td><td>" << Ended << "</td></tr>";
        html << "<tr><td scope=\"row\">Next request scheduled</td><td>" << Scheduled << "</td></tr>";
    } else {
        html << "<tr><td scope=\"row\">Request in progress</td><td>TRUE</td></tr>";
        html << "<tr><td scope=\"row\">Current request sent</td><td>" << Started << "</td></tr>";
        html << "<tr><td scope=\"row\">Current request scheduled</td><td>" << Scheduled << "</td></tr>";
    }
    html << "<tr><td scope=\"row\">Retry count</td><td>" << RetryCount << "</td></tr>";

    html << "</table>";

    html << "</div>";
    return html;
}

void TRecurringPeriod::OnReceived(TInstant now) {
    Ended = now;
    InFlight = nullptr;

    ResponseTime->Collect((Ended - Started).MilliSeconds());
}

TJwksCache::TJwksCache(
    const NKikimrProto::TExternalIdpConfig::TJwksCacheSettings& settings,
    NMonitoring::TDynamicCounterPtr counters)
    : Timeout{TDuration::Parse(settings.GetTimeout())}
    , KeyCount(counters->GetCounter("JwksKeys", false))
{}

bool TJwksCache::IsStale(const TInstant& now) const {
    return LastUpdate + Timeout < now;
}

size_t TJwksCache::Count() const {
    return Keys.size();
}

TMaybe<TString> TJwksCache::Get(const TString& key) const {
    const auto it = Keys.find(key);
    return (it == Keys.end()) ? Nothing() : MakeMaybe(it->second);
}

void TJwksCache::Update(const TInstant& now, THashMap<TString, TString> keys) {
    Keys = std::move(keys);
    LastUpdate = now;
    KeyCount->Set(static_cast<i64>(Keys.size()));
}

void TJwksCache::Clear() {
    if (Count() == 0) {
        return;
    }

    Keys.clear();
    KeyCount->Set(0);
}

TString TJwksCache::GetHtml(const TInstant& now) const {
    TStringBuilder html;
    html << "<div>";

    html << "<table class='table table-hover simple-table1'>";
    html << "<caption>Refresh info</caption>";

    html << "<tr><td scope=\"row\">Timeout</td><td>" << Timeout << "</td></tr>";
    html << "<tr><td scope=\"row\">Last update</td><td>" << LastUpdate << "</td></tr>";
    html << "<tr><td scope=\"row\">Freshness deadline</td><td>" << (LastUpdate + Timeout) << "</td></tr>";
    html << "<tr><td scope=\"row\">Key count</td><td>" << Count() << "</td></tr>";
    html << "<tr><td scope=\"row\">Is stale</td><td>" << (IsStale(now) ? "TRUE" : "FALSE") << "</td></tr>";

    html << "</table>";

    if (Count() > 0) {
        html << "<table class='table table-hover simple-table1'>";
        html << "<caption>Keys</caption>";

        html << "<tr><th scope=\"col\">Kid</th><th scope=\"col\">Public key</th></tr>";
        for (const auto& [kid, pem] : Keys) {
            html << "<tr>";
            html << "<td scope=\"row\">" << EncodeHtmlPcdata(kid) << "</td>";
            html << "<td>" << EncodeHtmlPcdata(pem) << "</td>";
            html << "</tr>";
        }

        html << "</table>";
    }

    html << "</div>";
    return html;
}

TExternalIdpProvider::TExternalIdpProvider(
    const NKikimrProto::TExternalIdpConfig& config,
    const NActors::TActorId& httpProxyId)
    : Config(config)
    , HttpProxyId(httpProxyId)
{}

void TExternalIdpProvider::Bootstrap(const TActorContext& ctx) {
    if (!HttpProxyId) {
        HttpProxyId = Register(NHttp::CreateHttpProxy());
    }

    RegisterCounters(ctx);
    RegisterPages(ctx);
    RegisterFields(ctx);

    YDB_LOG_DEBUG("Initializing ExternalIdp",
        {"issuer", Config.GetIssuer()},
        {"audience", Config.GetAudience()},
        {"subjectClaimName", Config.GetSubjectClaimName()},
        {"groupsClaimName", Config.GetGroupsClaimName()});

    Become(&TThis::StateWork, REFRESH_PERIOD, new NActors::TEvents::TEvWakeup());
}

void TExternalIdpProvider::StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvExternalIdpProvider::TEvAuthenticateRequest, Handle);
        HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        HFunc(NMon::TEvHttpInfo, Handle);
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
    }
}

void TExternalIdpProvider::RegisterPages(const TActorContext& ctx) {
    auto* const mon = AppData(ctx)->Mon;
    if (mon != nullptr) {
        auto* actorsPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(
           actorsPage,
           "external_idp_provider",
           "", // Set title empty, because front should not show this page in /actors list
           false,
           TActivationContext::ActorSystem(), this->SelfId()
        );
    }
}

void TExternalIdpProvider::RegisterCounters(const TActorContext& ctx) {
    Counters = NSecurity::GetCountersForExternalIdpProvider(AppData(ctx)->Counters);

    const auto authReq = Counters->GetSubgroup("component", "AuthRequest");
    CounterAuthProcessingTime = authReq->GetHistogram(
        "ProcessingTimeMs", NMonitoring::ExplicitHistogram(AuthProcessingTimeBucketsMs())
    );
    CounterAuthSuccess = authReq->GetCounter("Success", true);
    CounterAuthFailed = authReq->GetCounter("Failed", true);
}

void TExternalIdpProvider::RegisterFields(const TActorContext& ctx) {
    const auto now = ctx.Now();

    if (Config.HasIssuer()) {
        Config.SetIssuer(StripStringRight(Config.GetIssuer(), EqualsStripAdapter('/')));
    }

    if (Config.HasIssuer() && !Config.GetIssuer().empty() && !IsHttpsUrl(Config.GetIssuer())) {
        YDB_LOG_ERROR("Issuer must use https scheme",
            {"issuer", Config.GetIssuer()});
        Config.ClearIssuer();
    }

    if (const auto skewStr = Config.GetAllowedClockSkew(); !skewStr.empty()) {
        AllowedClockSkew = TDuration::Parse(skewStr);
    }

    DiscoveryRefresh = TRecurringPeriod(
        Config.GetDiscoveryPeriodicSettings(), now,
        Counters->GetSubgroup("component", "Discovery")
    );
    JwksRefresh = TRecurringPeriod(
        Config.GetJwksPeriodicSettings(), now,
        Counters->GetSubgroup("component", "Jwks")
    );

    JwksCache = TJwksCache(
        Config.GetJwksCacheSettings(),
        Counters->GetSubgroup("component", "Jwks")
    );
}

void TExternalIdpProvider::Handle(TEvExternalIdpProvider::TEvAuthenticateRequest::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    const auto now = ctx.Now();
    Y_DEFER {
        CounterAuthProcessingTime->Collect((ctx.Now() - now).MilliSeconds());
    };

    std::optional<jwt::decoded_jwt> decoded;
    try {
        decoded.emplace(jwt::decode(msg->Token));
    } catch (const std::invalid_argument&) {
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::BAD_REQUEST, "Token is not in correct format"
        );
    } catch (const std::exception& e) {
        YDB_LOG_ERROR("Failed to decode",
            {"token", e.what()});
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::UNAUTHORIZED, "Failed to decode token"
        );
    }

    if (!decoded->has_key_id()) {
        return ReplyError(
            ev->Sender, msg->Key, TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "No kid was found for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
            );
    }
    const TString kid = TString{decoded->get_key_id()};

    if (!decoded->has_algorithm()) {
        return ReplyError(
            ev->Sender, msg->Key, TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "No algorithm was found for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid
        );
    }

    NSecurity::EJwkAlg alg;
    if (!TryFromString(decoded->get_algorithm(), alg)) {
        return ReplyError(
            ev->Sender, msg->Key, TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "Unsupported JWT algorithm for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid
                << " algorithm=" << decoded->get_algorithm()
        );
    }

    const auto kty = NSecurity::GetKeyType(alg);
    if (!kty.has_value()) {
        return ReplyError(
            ev->Sender, msg->Key, TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "Unsupported JWT algorithm for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid
                << " algorithm=" << alg
        );
    }

    const auto pem = JwksCache.Get(BuildKey(ToString(kty.value()), kid));
    if (!pem.Defined()) {
        return ReplyError(
            ev->Sender, msg->Key, TEvExternalIdpProvider::EStatus::UNAVAILABLE,
            TStringBuilder() << "No matching key was found for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid
                << " kty=" << *kty
                << " algorithm=" << alg,
            true
        );
    }

    auto verifier = jwt::verify();

    if (AllowedClockSkew != TDuration::Zero()) {
        verifier.leeway(AllowedClockSkew.Seconds());
    }

    if (const auto it = SUPPORTED_ALGOS<decltype(verifier)>.find(alg); it != SUPPORTED_ALGOS<decltype(verifier)>.end()) {
        it->second(verifier, *pem);
    } else {
        return ReplyError(
            ev->Sender, msg->Key, TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "Unsupported JWT algorithm for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid
                << " algorithm=" << alg
        );
    }
    verifier.with_issuer(Config.GetIssuer());

    {
        std::set<std::string> audiences;
        if (Config.HasAudience() && !Config.GetAudience().empty()) {
            audiences.insert(Config.GetAudience());
        }
        if (!audiences.empty()) {
            verifier.with_audience(std::move(audiences));
        }
    }

    try {
        verifier.verify(*decoded);
    } catch (const std::exception& e) {
        YDB_LOG_ERROR("Failed to verify",
            {"token", e.what()});
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::UNAUTHORIZED, "Failed to verify token"
        );
    }

    auto resp = MakeHolder<TEvExternalIdpProvider::TEvAuthenticateResponse>(msg->Key);
    resp->Status = TEvExternalIdpProvider::EStatus::SUCCESS;

    const TString subject = std::invoke([&]() {
        const auto sub = decoded->has_subject() ? TString{decoded->get_subject()} : "";
        const auto subClaim = Config.GetSubjectClaimName();
        if (subClaim.empty()) {
            return sub;
        }
        if (!decoded->has_payload_claim(subClaim)) {
            YDB_LOG_WARN("Unknown claim for subject is used for token, fallback to 'sub'",
                {"issuer", Config.GetIssuer()},
                {"subjectClaimName", subClaim});
            return sub;
        }
        const auto& claim = decoded->get_payload_claim(subClaim);
        if (claim.get_type() != jwt::claim::type::string) {
            YDB_LOG_WARN("Value in subject claim is not a string, fallback to 'sub'",
                {"issuer", Config.GetIssuer()},
                {"subjectClaimName", subClaim});
            return sub;
        }
        return TString{claim.as_string()};
    });
    if (subject.empty()) {
        return ReplyError(
            ev->Sender, msg->Key, TEvExternalIdpProvider::EStatus::UNAUTHORIZED,
            TStringBuilder() << "Token subject is empty (checked claim: '" << Config.GetSubjectClaimName() << "'"
                << " fallback: 'sub')"
        );
    }
    resp->User = subject;

    if (const auto groupsClaim = Config.GetGroupsClaimName(); !groupsClaim.empty()
        && decoded->has_payload_claim(groupsClaim))
    {
        const auto& claim = decoded->get_payload_claim(groupsClaim);
        if (claim.get_type() == jwt::claim::type::array) {
            size_t skipped = 0;
            for (const auto& v : claim.as_array()) {
                if (v.is<std::string>()) {
                    resp->Groups.push_back(TString{v.get<std::string>()});
                } else {
                    ++skipped;
                }
            }
            if (skipped > 0) {
                YDB_LOG_WARN("Ignored non-string element(s) in groups claim",
                    {"issuer", Config.GetIssuer()},
                    {"groupsClaimName", groupsClaim},
                    {"ignored", skipped});
            }
        } else {
            YDB_LOG_WARN("Groups claim is not an array, no groups extracted",
                {"issuer", Config.GetIssuer()},
                {"groupsClaimName", groupsClaim});
        }
    }

    if (decoded->has_expires_at()) {
        const auto exp = decoded->get_expires_at();
        resp->ExpiresAt = TInstant::Seconds(
            std::chrono::duration_cast<std::chrono::seconds>(exp.time_since_epoch()).count()
        );
    } else {
        YDB_LOG_WARN("`exp` claim is not set, use default expiration period",
            {"issuer", Config.GetIssuer()},
            {"defaultExpirationPeriod", DEFAULT_EXPIRATION_PERIOD.Seconds()});
        resp->ExpiresAt = now + DEFAULT_EXPIRATION_PERIOD;
    }

    CounterAuthSuccess->Inc();

    YDB_LOG_DEBUG("Authentication succeeded in ExternalIdp groups=[",
        {"issuer", Config.GetIssuer()},
        {"key", MaskTicket(msg->Key)},
        {"status", resp->Status},
        {"user", resp->User},
        {"groups", JoinSeq(", ", resp->Groups)},
        {"expiresAt", resp->ExpiresAt});

    Send(ev->Sender, resp.Release());
}

void TExternalIdpProvider::StartDiscoveryFetch(const TInstant& now) {
    if (!Config.HasIssuer() || Config.GetIssuer().empty() || !DiscoveryRefresh.CanSendRequest(now)) {
        return;
    }
    const auto discoveryUrl = BuildDiscoveryUrl(Config.GetIssuer());

    YDB_LOG_DEBUG("Discovery fetch IdP",
        {"issuer", Config.GetIssuer()},
        {"url", discoveryUrl});

    const auto request = NHttp::THttpOutgoingRequest::CreateRequestGet(discoveryUrl);
    const auto timeout = DiscoveryRefresh.OnRequestSending(now, request);
    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(request, timeout));
}

void TExternalIdpProvider::StartJwksFetch(const TInstant& now) {
    if (JwksUrl.empty() || !JwksRefresh.CanSendRequest(now)) {
        return;
    }

    YDB_LOG_DEBUG("JWKS fetch IdP",
        {"issuer", Config.GetIssuer()},
        {"url", JwksUrl});

    const auto request = NHttp::THttpOutgoingRequest::CreateRequestGet(JwksUrl);
    const auto timeout = JwksRefresh.OnRequestSending(now, request);
    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(request, timeout));
}

void TExternalIdpProvider::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev, const TActorContext& ctx) {
    const auto now = ctx.Now();
    const auto* msg = ev->Get();

    if (DiscoveryRefresh.IsSameRequest(msg->Request)) {
        return HandleDiscoveryResponse(*msg, now);
    }

    if (JwksRefresh.IsSameRequest(msg->Request)) {
        return HandleJwksResponse(*msg, now);
    }

    YDB_LOG_ERROR("Got unexpected HTTP response",
        {"issuer", Config.GetIssuer()},
        {"request", msg->Request->AsString()});
}

void TExternalIdpProvider::HandleDiscoveryResponse(
    const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& resp, const TInstant& now)
{
    bool isSuccess = false;
    Y_DEFER {
        if (isSuccess) {
            DiscoveryRefresh.OnSuccessReceived(now);
        } else {
            DiscoveryRefresh.OnErrorReceived(now);
        }
    };

    if (resp.Response == nullptr) {
        YDB_LOG_ERROR("IdP message=Discovery",
            {"issuer", Config.GetIssuer()},
            {"failed", resp.Error});
        return;
    }

    if (resp.Response->Status != "200") {
        YDB_LOG_ERROR("IdP message=Discovery fetch network error",
            {"issuer", Config.GetIssuer()},
            {"responseStatus", resp.Response->Status},
            {"responseMessage", resp.Response->Message});
        return;
    }

    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(TString{resp.Response->Body}, &json)) {
        YDB_LOG_ERROR("IdP message=Invalid JSON in discovery response",
            {"issuer", Config.GetIssuer()});
        return;
    }
    if (!Config.HasIssuer() || Config.GetIssuer().empty()) {
        YDB_LOG_ERROR("IdP message=Discovery failed with empty issuer in config",
            {"issuer", Config.GetIssuer()});
        return;
    }
    if (!json.Has(ISSUER) || !json[ISSUER].IsString() || Config.GetIssuer() != json[ISSUER].GetString()) {
        YDB_LOG_ERROR("IdP message=Discovery document mismatch",
            {"issuer", Config.GetIssuer()},
            {"ISSUER", ISSUER});
        return;
    }
    if (!json.Has(JWKS_URI) || !json[JWKS_URI].IsString() || json[JWKS_URI].GetString().empty()) {
        YDB_LOG_ERROR("IdP message=Discovery document missing",
            {"issuer", Config.GetIssuer()},
            {"JWKSURI", JWKS_URI});
        return;
    }

    const auto jwksUri = json[JWKS_URI].GetString();
    if (!IsHttpsUrl(jwksUri)) {
        YDB_LOG_ERROR("IdP message=Discovery document must use https:// scheme",
            {"issuer", Config.GetIssuer()},
            {"JWKSURI", JWKS_URI},
            {"jwksUri", jwksUri});
        return;
    }
    JwksUrl = jwksUri;

    YDB_LOG_DEBUG("Discovery ok IdP",
        {"issuer", Config.GetIssuer()},
        {"jwksUri", JwksUrl});
    isSuccess = true;
}

void TExternalIdpProvider::HandleJwksResponse(
    const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& resp, const TInstant& now)
{
    bool isSuccess = false;
    Y_DEFER {
        if (isSuccess) {
            JwksRefresh.OnSuccessReceived(now);
        } else {
            JwksRefresh.OnErrorReceived(now);
        }
    };

    if (resp.Response == nullptr) {
        YDB_LOG_ERROR("IdP message=JWKS response",
            {"issuer", Config.GetIssuer()},
            {"failed", resp.Error});
        return;
    }

    if (resp.Response->Status != "200") {
        YDB_LOG_ERROR("IdP message=JWKS fetch network error",
            {"issuer", Config.GetIssuer()},
            {"responseStatus", resp.Response->Status},
            {"responseMessage", resp.Response->Message});
        return;
    }

    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(TString{resp.Response->Body}, &json)) {
        YDB_LOG_ERROR("IdP message=Invalid JSON in JWKS response",
            {"issuer", Config.GetIssuer()});
        return;
    }

    const auto arr = NSecurity::ParseJwkSet(json);
    if (!arr.has_value()) {
        YDB_LOG_ERROR("IdP message=Failed to parse JWKS",
            {"issuer", Config.GetIssuer()});
        return;
    }

    THashMap<TString, TString> newKeys;
    for (const auto& jwk : arr->Keys) {
        auto pubkey = jwk.CalculatePublicKey();
        if (!pubkey.has_value()) {
            YDB_LOG_WARN("Skipping JWKS key with kid=' unsupported key format (no x5c)",
                {"keyId", jwk.KeyId});
            continue;
        }
        newKeys[BuildKey(ToString(jwk.Type), TString{jwk.KeyId})] = std::move(pubkey.value());
    }
    if (newKeys.empty()) {
        YDB_LOG_ERROR("IdP message=No supported keys in JWKS response",
            {"issuer", Config.GetIssuer()});
        return;
    }

    const auto oldKeyCount = JwksCache.Count();
    JwksCache.Update(now, std::move(newKeys));

    YDB_LOG_DEBUG("JWKS refreshed IdP",
        {"issuer", Config.GetIssuer()},
        {"oldKeys", oldKeyCount},
        {"newKeys", JwksCache.Count()});
    isSuccess = true;
}

void TExternalIdpProvider::Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
    const auto now = ctx.Now();

    TStringBuilder html;

    html << "<head>";
    html << "<style>";
    html << "table.simple-table1 th { margin: 0px 3px; text-align: center; } ";
    html << "table.simple-table1 > tbody > tr > td { padding: 1px 3px; } ";
    html << "table.table-hover tbody tr:hover > td { background-color: #9dddf2; }";
    html << "</style>";
    html << "</head>";

    html << "<h3>Config</h3>";
    html << "<div>";
    html << "<table class='table table-hover simple-table1'>";
    html << "<tr><td scope=\"row\">Now</td><td>" << now << "</td></tr>";
    html << "<tr><td scope=\"row\">Issuer</td><td>" << EncodeHtmlPcdata(Config.GetIssuer()) << "</td></tr>";
    html << "<tr><td scope=\"row\">Audience</td><td>" << EncodeHtmlPcdata(Config.GetAudience()) << "</td></tr>";
    html << "<tr><td scope=\"row\">Subject claim name</td><td>" << EncodeHtmlPcdata(Config.GetSubjectClaimName()) << "</td></tr>";
    html << "<tr><td scope=\"row\">Groups claim name</td><td>" << EncodeHtmlPcdata(Config.GetGroupsClaimName()) << "</td></tr>";
    html << "</table>";
    html << "</div>";

    html << "<h3>Discovery</h3>";
    html << "<div>";
    html << "<table class='table table-hover simple-table1'>";
    html << "<caption>Common info</caption>";
    html << "<tr><td scope=\"row\">Discovery URL</td><td>" << EncodeHtmlPcdata(BuildDiscoveryUrl(Config.GetIssuer())) << "</td></tr>";
    html << "</table>";
    html << DiscoveryRefresh.GetHtml();
    html << "</div>";

    html << "<h3>JWKS</h3>";
    html << "<div>";
    html << "<table class='table table-hover simple-table1'>";
    html << "<caption>Common info</caption>";
    html << "<tr><td scope=\"row\">URL</td><td>" << EncodeHtmlPcdata(JwksUrl) << "</td></tr>";
    html << "</table>";
    html << JwksRefresh.GetHtml();

    html << "<h4>Cache</h4>";
    html << JwksCache.GetHtml(now);
    html << "</div>";

    Send(ev->Sender, new NMon::TEvHttpInfoRes(html));
}

void TExternalIdpProvider::HandleWakeup(const TActorContext& ctx) {
    const auto now = ctx.Now();

    StartDiscoveryFetch(now);
    StartJwksFetch(now);

    if (JwksCache.IsStale(now)) {
        JwksCache.Clear();
        DiscoveryRefresh.ResetBackoff();
        JwksRefresh.ResetBackoff();
    }

    Schedule(REFRESH_PERIOD, new NActors::TEvents::TEvWakeup());
}

void TExternalIdpProvider::ReplyError(
    const TActorId& sender, const TString& key, TEvExternalIdpProvider::EStatus status,
    const TString& message, bool retryable)
{
    CounterAuthFailed->Inc();

    YDB_LOG_WARN("Authentication failed in ExternalIdp",
        {"issuer", Config.GetIssuer()},
        {"key", MaskTicket(key)},
        {"status", status},
        {"retryable", retryable},
        {"message", message});

    auto resp = MakeHolder<TEvExternalIdpProvider::TEvAuthenticateResponse>(key);
    resp->Status = status;
    resp->Error.Message = message;
    resp->Error.Retryable = retryable;
    Send(sender, resp.Release());
}

} // namespace

NActors::IActor* CreateExternalIdpProvider(
    const NKikimrProto::TExternalIdpConfig& config,
    const NActors::TActorId& httpProxyId)
{
    return new TExternalIdpProvider(config, httpProxyId);
}

} // namespace NKikimr
