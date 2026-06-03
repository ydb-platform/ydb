#include "external_idp_provider.h"

#include <ydb/core/security/external_idp/external_idp_log.h>
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
#include <util/string/split.h>
#include <util/string/strip.h>

#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>

#include <chrono>
#include <exception>
#include <functional>
#include <unordered_map>

namespace NKikimr {

namespace {

static constexpr TDuration REFRESH_PERIOD = TDuration::Seconds(1);
static constexpr TDuration DEFAULT_EXPIRATION_PERIOD = TDuration::Minutes(10);

static constexpr TStringBuf JWKS_URI = "jwks_uri";
static constexpr TStringBuf ISSUER = "issuer";

// Asymmetric algorithms supported for external IdP JWT verification.
// HMAC (HS*) algorithms are deliberately excluded: they require a shared secret,
// not a public key. Including them would enable the "algorithm confusion" attack
// where an attacker signs a token with HS256 using the (public) RSA key.
template <typename TVerifier>
static const std::unordered_map<NSecurity::EJWKAlg, std::function<void(TVerifier&, const TString&)>> SUPPORTED_ALGORITHMS = {
    {NSecurity::EJWKAlg::ES256,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::es256(pubkey.c_str())); }},
    {NSecurity::EJWKAlg::ES384,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::es384(pubkey.c_str())); }},
    {NSecurity::EJWKAlg::ES512,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::es512(pubkey.c_str())); }},
    {NSecurity::EJWKAlg::PS256,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::ps256(pubkey.c_str())); }},
    {NSecurity::EJWKAlg::PS384,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::ps384(pubkey.c_str())); }},
    {NSecurity::EJWKAlg::PS512,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::ps512(pubkey.c_str())); }},
    {NSecurity::EJWKAlg::RS256,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::rs256(pubkey.c_str())); }},
    {NSecurity::EJWKAlg::RS384,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::rs384(pubkey.c_str())); }},
    {NSecurity::EJWKAlg::RS512,
        [](TVerifier& v, const TString& pubkey) { v.allow_algorithm(jwt::algorithm::rs512(pubkey.c_str())); }},
};

TString BuildKey(const TString& kty, const TString& kid) {
    return kty + "-" + kid;
}

TString BuildDiscoveryUrl(TString issuer) {
    return TStringBuilder() << issuer << "/.well-known/openid-configuration";
}

// returns a value in [50%, 100%] of its input
TDuration ApplyDownwardJitter(const TDuration& jitter) {
    const ui64 half = jitter.MilliSeconds() / 2;
    const ui64 range = jitter.MilliSeconds() - half + 1;
    const ui64 randomMs = half + RandomNumber<ui64>(range);
    return TDuration::MilliSeconds(randomMs);
}

TDuration CalculateRetry(const TDuration& minPeriod, const TDuration& maxPeriod, ui64 attempt) {
    const auto backoff = minPeriod + TDuration::Seconds(1ul << Min(attempt, 20ul));
    const auto clamped = Min(backoff, maxPeriod);
    return ApplyDownwardJitter(clamped);
}

NMonitoring::TBucketBounds ResponseTimeBuckets() {
    static NMonitoring::TBucketBounds buckets = {0, 5, 10, 50, 100, 500, 1000, 5000, 10000, 30000};
    return buckets;
}

NMonitoring::TBucketBounds AuthProcessingTimeBuckets() {
    static NMonitoring::TBucketBounds buckets = {0, 1, 5, 10, 50, 100, 500, 1000, 2000, 5000};
    return buckets;
}

class TExternalIdpProvider : public NActors::TActorBootstrapped<TExternalIdpProvider> {
    class TRecurringPeriod {
    public:
        TRecurringPeriod() = default;
        TRecurringPeriod(
            const NKikimrProto::TExternalIdpConfig::TPeriodicSettings& settings,
            TInstant scheduled,
            NMonitoring::TDynamicCounterPtr counters);

        bool CanSendRequest(TInstant now) const;
        bool IsSameRequest(NHttp::THttpOutgoingRequestPtr request) const;

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

    class TJWKsCache {
    public:
        TJWKsCache() = default;
        TJWKsCache(
            const NKikimrProto::TExternalIdpConfig::TJWKsCacheSettings& settings,
            NMonitoring::TDynamicCounterPtr counters);

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

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType();

    TExternalIdpProvider(
        const NKikimrProto::TExternalIdpConfig& config,
        const NActors::TActorId& httpProxyId);

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
        const TString& message, bool retryable = false);

private:
    NKikimrProto::TExternalIdpConfig Config;
    NActors::TActorId HttpProxyId;

    TRecurringPeriod DiscoveryRefresh;
    TRecurringPeriod JwksRefresh;

    TString JwksUrl;
    TJWKsCache JwksCache;

    NMonitoring::TDynamicCounterPtr Counters;

    NMonitoring::THistogramPtr CounterAuthProcessingTime;
    NMonitoring::TDynamicCounters::TCounterPtr CounterAuthSuccess;
    NMonitoring::TDynamicCounters::TCounterPtr CounterAuthFailed;
};

TExternalIdpProvider::TRecurringPeriod::TRecurringPeriod(
    const NKikimrProto::TExternalIdpConfig::TPeriodicSettings& settings,
    TInstant scheduled,
    NMonitoring::TDynamicCounterPtr counters)
    : SuccessRefreshPeriod{TDuration::Parse(settings.GetSuccessRefreshPeriod())}
    , MinErrorRefreshPeriod{TDuration::Parse(settings.GetMinErrorRefreshPeriod())}
    , MaxErrorRefreshPeriod{TDuration::Parse(settings.GetMaxErrorRefreshPeriod())}
    , RequestTimeout{TDuration::Parse(settings.GetRequestTimeout())}
    , Scheduled{scheduled}
    , ResponseTime(counters->GetHistogram("ResponseTimeMs", NMonitoring::ExplicitHistogram(ResponseTimeBuckets())))
    , Successes(counters->GetCounter("Successes", true))
    , Errors(counters->GetCounter("Errors", true))
{}

bool TExternalIdpProvider::TRecurringPeriod::CanSendRequest(TInstant now) const {
    return InFlight == nullptr && Scheduled < now;
}

bool TExternalIdpProvider::TRecurringPeriod::IsSameRequest(NHttp::THttpOutgoingRequestPtr request) const {
    return InFlight != nullptr && InFlight == request;
}

TDuration TExternalIdpProvider::TRecurringPeriod::OnRequestSending(
    TInstant now, NHttp::THttpOutgoingRequestPtr request)
{
    Started = now;
    Ended = TInstant::Zero();

    InFlight = request;
    return RequestTimeout;
}

void TExternalIdpProvider::TRecurringPeriod::OnSuccessReceived(TInstant now) {
    OnReceived(now);

    RetryCount = 0;
    Scheduled = now + ApplyDownwardJitter(SuccessRefreshPeriod);

    Successes->Inc();
}

void TExternalIdpProvider::TRecurringPeriod::OnErrorReceived(TInstant now) {
    OnReceived(now);

    ++RetryCount;
    Scheduled = now + CalculateRetry(MinErrorRefreshPeriod, MaxErrorRefreshPeriod, RetryCount);

    Errors->Inc();
}

TString TExternalIdpProvider::TRecurringPeriod::GetHtml() const {
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

void TExternalIdpProvider::TRecurringPeriod::OnReceived(TInstant now) {
    Ended = now;
    InFlight = nullptr;

    ResponseTime->Collect((Ended - Started).MilliSeconds());
}

TExternalIdpProvider::TJWKsCache::TJWKsCache(
    const NKikimrProto::TExternalIdpConfig::TJWKsCacheSettings& settings,
    NMonitoring::TDynamicCounterPtr counters)
    : Timeout{TDuration::Parse(settings.GetTimeout())}
    , KeyCount(counters->GetCounter("JwksKeys", false))
{}

bool TExternalIdpProvider::TJWKsCache::IsStale(const TInstant& now) const {
    return LastUpdate + Timeout < now;
}

size_t TExternalIdpProvider::TJWKsCache::Count() const {
    return Keys.size();
}

TMaybe<TString> TExternalIdpProvider::TJWKsCache::Get(const TString& key) const {
    const auto it = Keys.find(key);
    return (it == Keys.end()) ? Nothing() : MakeMaybe(it->second);
}

void TExternalIdpProvider::TJWKsCache::Update(const TInstant& now, THashMap<TString, TString> keys) {
    Keys = std::move(keys);
    LastUpdate = now;
    KeyCount->Set(static_cast<i64>(Keys.size()));
}

void TExternalIdpProvider::TJWKsCache::Clear() {
    if (Count() == 0) {
        return;
    }

    Keys.clear();
    KeyCount->Set(0);
}

TString TExternalIdpProvider::TJWKsCache::GetHtml(const TInstant& now) const {
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

constexpr NKikimrServices::TActivity::EType TExternalIdpProvider::ActorActivityType() {
    return NKikimrServices::TActivity::EXTERNAL_IDP_PROVIDER_ACTOR;
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

    BLOG_D("Initializing ExternalIdp"
        << " issuer=" << Config.GetIssuer()
        << " audience=" << Config.GetAudience()
        << " subject_claim_name=" << Config.GetSubjectClaimName()
        << " groups_claim_name=" << Config.GetGroupsClaimName());

    Schedule(REFRESH_PERIOD, new NActors::TEvents::TEvWakeup());

    Become(&TThis::StateWork);
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
           TActivationContext::ActorSystem(), this->SelfId());
    }
}

void TExternalIdpProvider::RegisterCounters(const TActorContext& ctx) {
    Counters = NSecurity::GetCountersForExternalIdpProvider(AppData(ctx)->Counters);

    const auto authReq = Counters->GetSubgroup("component", "AuthRequest");
    CounterAuthProcessingTime = authReq->GetHistogram(
        "ProcessingTimeMs", NMonitoring::ExplicitHistogram(AuthProcessingTimeBuckets()));
    CounterAuthSuccess = authReq->GetCounter("Success", true);
    CounterAuthFailed = authReq->GetCounter("Failed", true);
}

void TExternalIdpProvider::RegisterFields(const TActorContext& ctx) {
    const auto now = ctx.Now();

    if (Config.HasIssuer()) {
        Config.SetIssuer(StripStringRight(Config.GetIssuer(), EqualsStripAdapter('/')));
    }

    DiscoveryRefresh = TRecurringPeriod(Config.GetDiscoveryPeriodicSettings(), now,
        Counters->GetSubgroup("component", "Discovery"));
    JwksRefresh = TRecurringPeriod(Config.GetJwksPeriodicSettings(), now,
        Counters->GetSubgroup("component", "Jwks"));

    JwksCache = TJWKsCache(Config.GetJwksCacheSettings(),
        Counters->GetSubgroup("component", "Jwks"));
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
            TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            "Token is not in correct format");
    } catch (const std::exception& e) {
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::UNAUTHORIZED, e.what());
    }

    if (!decoded->has_key_id()) {
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "No kid was found for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'");
    }
    const TString kid = TString{decoded->get_key_id()};

    if (!decoded->has_algorithm()) {
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "No algorithm was found for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid);
    }

    NSecurity::EJWKAlg alg;
    if (!TryFromString(decoded->get_algorithm(), alg)) {
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "Unsupported JWT algorithm for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid
                << " algorithm=" << decoded->get_algorithm());
    }

    const auto pem = JwksCache.Get(BuildKey(ToString(GetKeyType(alg)), kid));
    if (!pem.Defined()) {
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::UNAVAILABLE,
            TStringBuilder() << "No matching key was found for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid
                << " kty=" << GetKeyType(alg)
                << " algorithm=" << alg,
            true);
    }

    auto verifier = jwt::verify();

    if (const auto skewStr = Config.GetAllowedClockSkew(); !skewStr.empty()) {
        verifier.leeway(TDuration::Parse(skewStr).Seconds());
    }

    if (const auto it = SUPPORTED_ALGORITHMS<decltype(verifier)>.find(alg);
        it != SUPPORTED_ALGORITHMS<decltype(verifier)>.end()) {
        it->second(verifier, *pem);
    } else {
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::BAD_REQUEST,
            TStringBuilder() << "Unsupported JWT algorithm for token in"
                << " issuer=" << Config.GetIssuer()
                << " token='" << MaskTicket(msg->Token) << "'"
                << " kid=" << kid
                << " algorithm=" << alg);
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
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::UNAUTHORIZED, e.what());
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
            BLOG_W("Unknown claim for subject is used for token, fallback to 'sub'"
                << " issuer=" << Config.GetIssuer()
                << " subject_claim_name=" << subClaim);
            return sub;
        }
        const auto& claim = decoded->get_payload_claim(subClaim);
        if (claim.get_type() != jwt::claim::type::string) {
            BLOG_W("Value in subject claim is not a string, fallback to 'sub'"
                << " issuer=" << Config.GetIssuer()
                << " subject_claim_name=" << subClaim);
            return sub;
        }
        return TString{claim.as_string()};
    });
    if (subject.empty()) {
        return ReplyError(
            ev->Sender, msg->Key,
            TEvExternalIdpProvider::EStatus::UNAUTHORIZED,
            TStringBuilder() << "Token subject is empty (checked claim: '" << Config.GetSubjectClaimName() << "'"
                << " fallback: 'sub')");
    }
    resp->User = subject;

    if (const auto groupsClaim = Config.GetGroupsClaimName(); !groupsClaim.empty()
        && decoded->has_payload_claim(groupsClaim)) {
        const auto& claim = decoded->get_payload_claim(groupsClaim);
        if (claim.get_type() == jwt::claim::type::array) {
            for (const auto& v : claim.as_array()) {
                if (v.is<std::string>()) {
                    resp->Groups.push_back(TString{v.get<std::string>()});
                }
            }
        }
    }

    if (decoded->has_expires_at()) {
        const auto exp = decoded->get_expires_at();
        resp->ExpiresAt = TInstant::Seconds(
            std::chrono::duration_cast<std::chrono::seconds>(exp.time_since_epoch()).count());
    } else {
        BLOG_W("`exp` claim is not set, use default expiration period"
            << " issuer=" << Config.GetIssuer()
            << " default_expiration_period=" << DEFAULT_EXPIRATION_PERIOD.Seconds());
        resp->ExpiresAt = now + DEFAULT_EXPIRATION_PERIOD;
    }

    CounterAuthSuccess->Inc();

    Send(ev->Sender, resp.Release());
}

void TExternalIdpProvider::StartDiscoveryFetch(const TInstant& now) {
    if (!Config.HasIssuer() || Config.GetIssuer().empty() || !DiscoveryRefresh.CanSendRequest(now)) {
        return;
    }
    const auto discoveryUrl = BuildDiscoveryUrl(Config.GetIssuer());

    BLOG_D("Discovery fetch IdP issuer=" << Config.GetIssuer() << " url=" << discoveryUrl);

    const auto request = NHttp::THttpOutgoingRequest::CreateRequestGet(discoveryUrl);
    const auto timeout = DiscoveryRefresh.OnRequestSending(now, request);
    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(request, timeout));
}

void TExternalIdpProvider::StartJwksFetch(const TInstant& now) {
    if (JwksUrl.empty() || !JwksRefresh.CanSendRequest(now)) {
        return;
    }

    BLOG_D("JWKS fetch IdP issuer=" << Config.GetIssuer() << " url=" << JwksUrl);

    const auto request = NHttp::THttpOutgoingRequest::CreateRequestGet(JwksUrl);
    const auto timeout = JwksRefresh.OnRequestSending(now, request);
    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(request, timeout));
}

void TExternalIdpProvider::Handle(
    NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev, const TActorContext& ctx)
{
    const auto now = ctx.Now();
    const auto* msg = ev->Get();

    if (DiscoveryRefresh.IsSameRequest(msg->Request)) {
        return HandleDiscoveryResponse(*msg, now);
    }

    if (JwksRefresh.IsSameRequest(msg->Request)) {
        return HandleJwksResponse(*msg, now);
    }

    BLOG_E("Got unexpected HTTP response for issuer=" << Config.GetIssuer()
        << " request=" << msg->Request->AsString());
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
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=Discovery failed: " << resp.Error);
        return;
    }

    if (resp.Response->Status != "200") {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=Discovery fetch network error"
            << " (" << resp.Response->Status << ") " << resp.Response->Message);
        return;
    }

    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(TString{resp.Response->Body}, &json)) {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=Invalid JSON in discovery response");
        return;
    }
    if (!Config.HasIssuer() || Config.GetIssuer().empty()) {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=Discovery failed with empty issuer in config");
        return;
    }
    if (!json.Has(ISSUER) || !json[ISSUER].IsString() || Config.GetIssuer() != json[ISSUER].GetString()) {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=Discovery document mismatch '" << ISSUER << "'");
        return;
    }
    if (!json.Has(JWKS_URI) || !json[JWKS_URI].IsString() || json[JWKS_URI].GetString().empty()) {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=Discovery document missing '" << JWKS_URI << "'");
        return;
    }

    JwksUrl = json[JWKS_URI].GetString();

    BLOG_D("Discovery ok IdP issuer=" << Config.GetIssuer() << " jwks_uri=" << JwksUrl);
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
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=JWKs response failed: " << resp.Error);
        return;
    }

    if (resp.Response->Status != "200") {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=JWKs fetch network error"
            << " (" << resp.Response->Status << ")" << " " << resp.Response->Message);
        return;
    }

    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(TString{resp.Response->Body}, &json)) {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=Invalid JSON in JWKS response");
        return;
    }

    const auto arr = NSecurity::ParseJWKSet(json);
    if (!arr.has_value()) {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=Failed to parse JWKSet");
        return;
    }

    THashMap<TString, TString> newKeys;
    for (const auto& jwk : arr->Keys) {
        auto pubkey = jwk.CalculatePublicKey();
        if (!pubkey.has_value()) {
            BLOG_W("Skipping JWKS key with kid='" << jwk.KeyId << "': unsupported key format (no x5c)");
            continue;
        }
        newKeys[BuildKey(ToString(jwk.Type), TString{jwk.KeyId})] = std::move(pubkey.value());
    }
    if (newKeys.empty()) {
        BLOG_E("IdP issuer=" << Config.GetIssuer() << " message=No supported keys in JWKS response");
        return;
    }

    const auto oldKeyCount = JwksCache.Count();
    JwksCache.Update(now, std::move(newKeys));

    BLOG_D("JWKS refreshed IdP issuer=" << Config.GetIssuer()
        << " old_keys=" << oldKeyCount
        << " new_keys=" << JwksCache.Count());
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

    html << "<h3>JWKs</h3>";
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
    }

    Schedule(REFRESH_PERIOD, new NActors::TEvents::TEvWakeup());
}

void TExternalIdpProvider::ReplyError(
    const TActorId& sender, const TString& key, TEvExternalIdpProvider::EStatus status,
    const TString& message, bool retryable)
{
    CounterAuthFailed->Inc();

    BLOG_W("Authentication failed in ExternalIdp"
        << " issuer=" << Config.GetIssuer()
        << " key=" << MaskTicket(key)
        << " status=" << status
        << " retryable=" << retryable
        << " message=" << message);

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
