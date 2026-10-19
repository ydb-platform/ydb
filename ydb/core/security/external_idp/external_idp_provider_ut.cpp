#include <ydb/core/security/external_idp/external_idp_provider.h>
#include <ydb/core/security/external_idp/test_utils/rsa_key_pair.h>

#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/security/util.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>

#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>

#include <chrono>

namespace NKikimr {

namespace {

using NExternalIdpTestUtils::GenerateRsaKeyPair;
using TRsaKeyPair = NExternalIdpTestUtils::TRsaKeyPair;

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    const auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

void ReplyHttp(
    TTestActorSystem* rt, ui32 node, const TActorId& proxy,
    std::unique_ptr<TEventHandle<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>>& req,
    const TString& code, const TString& msg, const TString& body)
{
    NHttp::THttpIncomingResponsePtr resp = new NHttp::THttpIncomingResponse(req->Get()->Request);
    EatWholeString(
        resp,
        TStringBuilder() << "HTTP/1.1 " << code << " " << msg << "\r\n"
            << "Connection: close\r\n"
            << "Content-Type: application/json\r\n"
            << "Content-Length: " << ToString(body.length()) << "\r\n\r\n"
            << body);
    rt->Send(
        new IEventHandle(
            req->Sender, proxy,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(req->Get()->Request, resp)),
        node);
}

void ReplyHttpError(
    TTestActorSystem* rt, ui32 node, const TActorId& proxy,
    std::unique_ptr<TEventHandle<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>>& req,
    const TString& err)
{
    rt->Send(
        new IEventHandle(
            req->Sender, proxy,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(req->Get()->Request, nullptr, err)),
        node);
}

TString BuildDiscoveryJson(const TString& issuer, const TString& jwksUri) {
    NJson::TJsonValue json;
    json["issuer"] = issuer;
    json["jwks_uri"] = jwksUri;
    return NJson::WriteJson(json, false);
}

TString BuildJwksJson(const TString& kid, const TString& x5c) {
    NJson::TJsonValue json;
    NJson::TJsonValue keys(NJson::JSON_ARRAY);
    NJson::TJsonValue key;
    key["kid"] = kid;
    key["kty"] = "RSA";
    key["alg"] = "RS256";

    NJson::TJsonValue x5cArr(NJson::JSON_ARRAY);
    x5cArr.AppendValue(x5c);
    key["x5c"] = x5cArr;
    keys.AppendValue(key);
    json["keys"] = keys;
    return NJson::WriteJson(json, false);
}

TString BuildMultiKeyJwksJson(const TVector<std::pair<TString, TString>>& kidX5cPairs) {
    NJson::TJsonValue json;
    NJson::TJsonValue keys(NJson::JSON_ARRAY);
    for (const auto& [kid, x5c] : kidX5cPairs) {
        NJson::TJsonValue key;
        key["kid"] = kid;
        key["kty"] = "RSA";
        key["alg"] = "RS256";
        NJson::TJsonValue x5cArr(NJson::JSON_ARRAY);
        x5cArr.AppendValue(x5c);
        key["x5c"] = x5cArr;
        keys.AppendValue(key);
    }
    json["keys"] = keys;
    return NJson::WriteJson(json, false);
}

TString BuildUnsupportedJwksJsonWithoutX5c(const TString& kid) {
    NJson::TJsonValue json;
    NJson::TJsonValue keys(NJson::JSON_ARRAY);
    NJson::TJsonValue key;
    key["kid"] = kid;
    key["kty"] = "oct";
    key["alg"] = "HS256";
    key["k"] = "some-shared-secret";
    keys.AppendValue(key);
    json["keys"] = keys;
    return NJson::WriteJson(json, false);
}

enum class EJwtSigningAlgorithm {
    RS256,
    PS256,
    HS256,
};

struct TJwtSettings {
    TMaybe<TString> Kid;
    TString Issuer;
    TString Subject;
    std::set<std::string> Audience;
    std::chrono::seconds IssuedAtOffset = std::chrono::seconds(0);
    std::chrono::seconds ExpiresAtOffset = std::chrono::seconds(3600);
};

TJwtSettings MakeJwtSettings(
    TMaybe<TString> kid,
    const TString& issuer,
    const TString& subject,
    const std::set<std::string>& audience,
    std::chrono::seconds issuedAtOffset = std::chrono::seconds(0),
    std::chrono::seconds expiresAtOffset = std::chrono::seconds(3600))
{
    return {std::move(kid), issuer, subject, audience, issuedAtOffset, expiresAtOffset};
}

auto MakeJwtBuilder(const TJwtSettings& settings) {
    const auto now = std::chrono::system_clock::now();
    auto token = jwt::create()
        .set_issuer(std::string(settings.Issuer))
        .set_subject(std::string(settings.Subject))
        .set_issued_at(now + settings.IssuedAtOffset)
        .set_expires_at(now + settings.ExpiresAtOffset)
        .set_audience(settings.Audience);

    if (settings.Kid) {
        token.set_key_id(std::string(*settings.Kid));
    }

    return token;
}

struct TNoopJwtCustomizer {
    template <typename TBuilder>
    void operator()(TBuilder&) const {}
};

template <typename TCustomize>
TString CreateSignedJwt(
    const TRsaKeyPair& keys,
    const TJwtSettings& settings,
    EJwtSigningAlgorithm algorithm,
    TCustomize customize)
{
    auto token = MakeJwtBuilder(settings);
    customize(token);

    switch (algorithm) {
        case EJwtSigningAlgorithm::RS256:
            return TString(token.sign(jwt::algorithm::rs256("", std::string(keys.PrivateKeyPem), "", "")));
        case EJwtSigningAlgorithm::PS256:
            return TString(token.sign(jwt::algorithm::ps256("", std::string(keys.PrivateKeyPem), "", "")));
        case EJwtSigningAlgorithm::HS256:
            return TString(token.sign(jwt::algorithm::hs256("some-secret")));
    }
}

TString CreateSignedJwt(
    const TRsaKeyPair& keys,
    const TJwtSettings& settings,
    EJwtSigningAlgorithm algorithm = EJwtSigningAlgorithm::RS256)
{
    return CreateSignedJwt(keys, settings, algorithm, TNoopJwtCustomizer{});
}

TString CreateJwt(
    const TRsaKeyPair& keys, const TString& kid, const TString& issuer,
    const TString& subject, const std::set<std::string>& aud,
    const std::vector<std::string>& groups = {},
    const TString& subClaimName = "", const TString& grpClaimName = "",
    std::chrono::seconds exp = std::chrono::seconds(3600))
{
    return CreateSignedJwt(
        keys,
        MakeJwtSettings(MakeMaybe(kid), issuer, subject, aud, std::chrono::seconds(0), exp),
        EJwtSigningAlgorithm::RS256,
        [&](auto& token) {
            if (!subClaimName.empty()) {
                token.set_payload_claim(
                    std::string(subClaimName),
                    jwt::claim(std::string(subject + "_custom-claim")));
            }

            if (!grpClaimName.empty() && !groups.empty()) {
                std::vector<picojson::value> groupValues;
                for (const auto& group : groups) {
                    groupValues.emplace_back(group);
                }
                token.set_payload_claim(std::string(grpClaimName), jwt::claim(picojson::value(groupValues)));
            }
        });
}

TString CreatePs256Jwt(
    const TRsaKeyPair& keys, const TString& kid, const TString& issuer,
    const TString& subject, const std::set<std::string>& aud)
{
    return CreateSignedJwt(keys, MakeJwtSettings(MakeMaybe(kid), issuer, subject, aud), EJwtSigningAlgorithm::PS256);
}

TString CreateExpiredJwt(
    const TRsaKeyPair& keys, const TString& kid,
    const TString& issuer, const TString& subject,
    const std::set<std::string>& aud)
{
    return CreateSignedJwt(
        keys,
        MakeJwtSettings(MakeMaybe(kid), issuer, subject, aud,
            -std::chrono::seconds(7200), -std::chrono::seconds(3600)));
}

TString CreateJwtWithoutKid(
    const TRsaKeyPair& keys, const TString& issuer,
    const TString& subject, const std::set<std::string>& aud)
{
    return CreateSignedJwt(keys, MakeJwtSettings(Nothing(), issuer, subject, aud));
}

TString CreateJwtWithEmptySubject(
    const TRsaKeyPair& keys, const TString& kid,
    const TString& issuer, const std::set<std::string>& aud)
{
    return CreateJwt(keys, kid, issuer, "", aud);
}

TString CreateJwtWithIntSubjectClaim(
    const TRsaKeyPair& keys, const TString& kid,
    const TString& issuer, const TString& subject,
    const std::set<std::string>& aud,
    const TString& subClaimName)
{
    return CreateSignedJwt(
        keys,
        MakeJwtSettings(MakeMaybe(kid), issuer, subject, aud),
        EJwtSigningAlgorithm::RS256,
        [&](auto& token) {
            token.set_payload_claim(std::string(subClaimName), jwt::claim(picojson::value(static_cast<double>(42))));
        });
}

TString CreateJwtWithMixedGroups(
    const TRsaKeyPair& keys, const TString& kid,
    const TString& issuer, const TString& subject,
    const std::set<std::string>& aud,
    const TString& grpClaimName)
{
    return CreateSignedJwt(
        keys,
        MakeJwtSettings(MakeMaybe(kid), issuer, subject, aud),
        EJwtSigningAlgorithm::RS256,
        [&](auto& token) {
            std::vector<picojson::value> groupValues;
            groupValues.emplace_back("admin");
            groupValues.emplace_back(static_cast<double>(42));
            groupValues.emplace_back("dev");
            groupValues.emplace_back(true);
            token.set_payload_claim(std::string(grpClaimName), jwt::claim(picojson::value(groupValues)));
        });
}

NKikimrProto::TExternalIdpConfig MakeConfig(
    const TString& issuer,
    const TString& audience = "",
    const TString& subClaim = "sub", const TString& grpClaim = "groups")
{
    NKikimrProto::TExternalIdpConfig c;
    c.SetIssuer(issuer);
    if (!audience.empty()) {
        c.SetAudience(audience);
    }
    c.SetSubjectClaimName(subClaim);
    c.SetGroupsClaimName(grpClaim);
    c.MutableDiscoveryPeriodicSettings()->SetSuccessRefreshPeriod("1h");
    c.MutableJwksPeriodicSettings()->SetSuccessRefreshPeriod("30m");
    c.SetAllowedClockSkew("30s");
    return c;
}

struct TSetup : public NUnitTest::TBaseFixture {
    std::unique_ptr<TTestActorSystem> Rt;
    TActorId Proxy;
    TActorId Edge;
    ui32 Node = 1;

    THashSet<TActorId> RegisteredActors;

    void SetUp(NUnitTest::TTestContext& /* ctx */) override {
        Rt = std::make_unique<TTestActorSystem>(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        Rt->Start();
        Rt->GetNode(1)->AppData->DomainsInfo->AddDomain(
            TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());
        Rt->SetLogPriority(NKikimrServices::EXTERNAL_IDP_PROVIDER, NLog::PRI_TRACE);
        Proxy = Rt->AllocateEdgeActor(Node);
        Edge = Rt->AllocateEdgeActor(Node);
    }

    void TearDown(NUnitTest::TTestContext& /* ctx */) override {
        for (const TActorId& actorId : RegisteredActors) {
            Rt->Send(new IEventHandle(actorId, Edge, new TEvents::TEvPoisonPill()), Node);
        }
    }

    TActorId RegProvider(const NKikimrProto::TExternalIdpConfig& c) {
        auto instance = Rt->Register(CreateExternalIdpProvider(c, Proxy), Node);
        RegisteredActors.insert(instance);
        return instance;
    }

    auto WaitHttp() {
        return Rt->WaitForEdgeActorEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(Proxy, false);
    }

    void DoDiscoveryAndJwks(const TString& iss, const TString& jwksUri, const TString& kid, const TString& x5c) {
        auto d = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", BuildDiscoveryJson(iss, jwksUri));
        auto j = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, j, "200", "OK", BuildJwksJson(kid, x5c));
    }

    void SendAuth(const TActorId& to, const TString& key, const TString& token) {
        Rt->Send(new IEventHandle(to, Edge, new TEvExternalIdpProvider::TEvAuthenticateRequest(key, token)), Node);
    }

    auto WaitAuth() {
        return Rt->WaitForEdgeActorEvent<TEvExternalIdpProvider::TEvAuthenticateResponse>(Edge, false);
    }
};

const TString ISS = "https://idp.example.com";
const TString JWKS = "https://idp.example.com/jwks.json";
const TString KID = "test-key-1";

} // namespace

Y_UNIT_TEST_SUITE(TExternalIdpProviderTest) {

    Y_UNIT_TEST_F(DiscoveryErrorBackoffSurvivesStaleCache, TSetup) {
        auto config = MakeConfig(ISS);
        config.MutableJwksCacheSettings()->SetTimeout("0s");
        config.MutableDiscoveryPeriodicSettings()->SetMinErrorRefreshPeriod("1h");
        config.MutableDiscoveryPeriodicSettings()->SetMaxErrorRefreshPeriod("1h");
        std::ignore = RegProvider(config);

        auto first = WaitHttp();
        const auto failedAt = Rt->GetClock();
        ReplyHttpError(Rt.get(), Node, Proxy, first, "Connection refused");
        const auto retry = WaitHttp();
        const auto elapsed = Rt->GetClock() - failedAt;

        // Downward jitter permits [50%, 100%] of the configured hour.
        UNIT_ASSERT_C(elapsed >= TDuration::Minutes(30),
            TStringBuilder() << "Cache maintenance overwrote discovery backoff; retry after " << elapsed);
    }

    Y_UNIT_TEST_F(JwksErrorBackoffSurvivesStaleCache, TSetup) {
        auto config = MakeConfig(ISS);
        config.MutableJwksCacheSettings()->SetTimeout("0s");
        config.MutableJwksPeriodicSettings()->SetMinErrorRefreshPeriod("1h");
        config.MutableJwksPeriodicSettings()->SetMaxErrorRefreshPeriod("1h");
        std::ignore = RegProvider(config);

        auto discovery = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, discovery, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto first = WaitHttp();
        UNIT_ASSERT_STRING_CONTAINS(TString{first->Get()->Request->URL}, "jwks.json");
        const auto failedAt = Rt->GetClock();
        ReplyHttpError(Rt.get(), Node, Proxy, first, "Connection refused");

        auto retry = WaitHttp();
        while (!TString{retry->Get()->Request->URL}.Contains("jwks.json")) {
            ReplyHttp(Rt.get(), Node, Proxy, retry, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
            retry = WaitHttp();
        }
        const auto elapsed = Rt->GetClock() - failedAt;
        UNIT_ASSERT_C(elapsed >= TDuration::Minutes(30),
            TStringBuilder() << "Cache maintenance overwrote JWKS backoff; retry after " << elapsed);
    }

    Y_UNIT_TEST_F(DiscoverySuccessPeriodSurvivesStaleCache, TSetup) {
        auto config = MakeConfig(ISS);
        config.MutableJwksCacheSettings()->SetTimeout("0s");
        std::ignore = RegProvider(config);
        auto discovery = WaitHttp();
        const auto succeededAt = Rt->GetClock();
        ReplyHttp(Rt.get(), Node, Proxy, discovery, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto jwks = WaitHttp();
        UNIT_ASSERT_STRING_CONTAINS(TString{jwks->Get()->Request->URL}, "jwks.json");
        ReplyHttpError(Rt.get(), Node, Proxy, jwks, "Connection refused");
        auto retry = WaitHttp();
        while (!TString{retry->Get()->Request->URL}.Contains(".well-known")) {
            ReplyHttpError(Rt.get(), Node, Proxy, retry, "Connection refused");
            retry = WaitHttp();
        }
        const auto elapsed = Rt->GetClock() - succeededAt;
        UNIT_ASSERT_C(elapsed >= TDuration::Minutes(30),
            TStringBuilder() << "Cache maintenance overwrote successful discovery interval; refresh after " << elapsed);
    }

    Y_UNIT_TEST_F(JwksFailureBacksOffDiscoveryForTheSameSource, TSetup) {
        auto config = MakeConfig(ISS);
        config.MutableJwksCacheSettings()->SetTimeout("0s");
        config.MutableDiscoveryPeriodicSettings()->SetSuccessRefreshPeriod("10s");
        config.MutableJwksPeriodicSettings()->SetMinErrorRefreshPeriod("1h");
        config.MutableJwksPeriodicSettings()->SetMaxErrorRefreshPeriod("1h");
        std::ignore = RegProvider(config);

        auto discovery = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, discovery, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto jwks = WaitHttp();
        UNIT_ASSERT_STRING_CONTAINS(TString{jwks->Get()->Request->URL}, "jwks.json");
        const auto failedAt = Rt->GetClock();
        ReplyHttpError(Rt.get(), Node, Proxy, jwks, "Connection refused");

        const auto next = WaitHttp();
        const auto elapsed = Rt->GetClock() - failedAt;
        UNIT_ASSERT_C(elapsed >= TDuration::Minutes(30),
            TStringBuilder() << "A JWKS failure must back off the entire IdP source; next request after " << elapsed);
    }

    Y_UNIT_TEST_F(SourceBackoffGrowsUntilJwksRecovery, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        auto config = MakeConfig(ISS);
        config.MutableDiscoveryPeriodicSettings()->SetSuccessRefreshPeriod("0s");
        config.MutableJwksPeriodicSettings()->SetSuccessRefreshPeriod("0s");
        config.MutableJwksPeriodicSettings()->SetMinErrorRefreshPeriod("0s");
        config.MutableJwksPeriodicSettings()->SetMaxErrorRefreshPeriod("1m");
        std::ignore = RegProvider(config);

        auto nextJwks = [&] {
            auto request = WaitHttp();
            while (TString{request->Get()->Request->URL}.Contains(".well-known")) {
                ReplyHttp(Rt.get(), Node, Proxy, request, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
                request = WaitHttp();
            }
            UNIT_ASSERT_STRING_CONTAINS(TString{request->Get()->Request->URL}, "jwks.json");
            return request;
        };

        auto request = nextJwks();
        for (size_t attempt = 0; attempt < 3; ++attempt) {
            const auto failedAt = Rt->GetClock();
            ReplyHttpError(Rt.get(), Node, Proxy, request, "Connection refused");
            request = nextJwks();
            UNIT_ASSERT_C(Rt->GetClock() - failedAt >= TDuration::Seconds(1u << attempt),
                "Successful discovery must not reset the retry sequence of a still-failing source");
        }

        ReplyHttp(Rt.get(), Node, Proxy, request, "200", "OK", BuildJwksJson(KID, keys.X5cBase64));
        request = nextJwks();
        const auto failedAt = Rt->GetClock();
        ReplyHttpError(Rt.get(), Node, Proxy, request, "Connection refused");
        request = nextJwks();
        UNIT_ASSERT_C(Rt->GetClock() - failedAt <= TDuration::Seconds(3),
            "A recovered source must start a new retry sequence after its next error");
    }

    Y_UNIT_TEST_F(CacheExpiryRecoveryRetainsSourceBackoff, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        auto config = MakeConfig(ISS);
        config.MutableJwksCacheSettings()->SetTimeout("2s");
        config.MutableDiscoveryPeriodicSettings()->SetSuccessRefreshPeriod("2h");
        config.MutableDiscoveryPeriodicSettings()->SetMinErrorRefreshPeriod("1h");
        config.MutableDiscoveryPeriodicSettings()->SetMaxErrorRefreshPeriod("1h");
        config.MutableJwksPeriodicSettings()->SetSuccessRefreshPeriod("1s");
        config.MutableJwksPeriodicSettings()->SetMinErrorRefreshPeriod("1h");
        config.MutableJwksPeriodicSettings()->SetMaxErrorRefreshPeriod("1h");
        std::ignore = RegProvider(config);
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        auto renewal = WaitHttp();
        ReplyHttpError(Rt.get(), Node, Proxy, renewal, "Connection refused");
        auto recovery = WaitHttp();
        const auto failedAt = Rt->GetClock();
        ReplyHttpError(Rt.get(), Node, Proxy, recovery, "Connection refused");
        const auto retry = WaitHttp();
        const auto elapsed = Rt->GetClock() - failedAt;
        UNIT_ASSERT_C(elapsed >= TDuration::Minutes(30),
            TStringBuilder() << "Cache expiry permits one recovery attempt; its failure must back off, got " << elapsed);
    }

    Y_UNIT_TEST_F(DiscoveryDeadlineRejectsLateResponseAndRecovers, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        auto config = MakeConfig(ISS);
        config.MutableDiscoveryPeriodicSettings()->SetRequestTimeout("1s");
        config.MutableDiscoveryPeriodicSettings()->SetMinErrorRefreshPeriod("0s");
        config.MutableDiscoveryPeriodicSettings()->SetMaxErrorRefreshPeriod("0s");
        const auto id = RegProvider(config);
        auto first = WaitHttp();
        const auto startedAt = Rt->GetClock();

        // A proxy can keep an incomplete response alive with incoming bytes.
        // Withhold its completion to exercise the provider's total deadline.
        using TOutgoing = NHttp::TEvHttpProxy::TEvHttpOutgoingRequest;
        std::unique_ptr<TEventHandle<TOutgoing>> retry;
        Rt->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TOutgoing::EventType) {
                retry.reset(reinterpret_cast<TEventHandle<TOutgoing>*>(event.release()));
                return false;
            }
            return true;
        };
        Rt->Sim([&] { return !retry && Rt->GetClock() < startedAt + TDuration::Seconds(3); });
        Rt->FilterFunction = {};
        UNIT_ASSERT_C(retry, "An incomplete HTTP response must not stall source refresh indefinitely");

        ReplyHttp(Rt.get(), Node, Proxy, first, "200", "OK", BuildDiscoveryJson(ISS, ISS + "/obsolete-jwks"));
        ReplyHttp(Rt.get(), Node, Proxy, retry, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto jwks = WaitHttp();
        UNIT_ASSERT_STRING_CONTAINS(TString{jwks->Get()->Request->URL}, "jwks.json");
        ReplyHttp(Rt.get(), Node, Proxy, jwks, "200", "OK", BuildJwksJson(KID, keys.X5cBase64));

        SendAuth(id, "after-deadline", CreateJwt(keys, KID, ISS, "u1", {}));
        const auto auth = WaitAuth();
        UNIT_ASSERT_EQUAL(auth->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
    }

    Y_UNIT_TEST_F(PoisonDuringBackoffStopsProvider, TSetup) {
        const auto id = RegProvider(MakeConfig(ISS));
        auto first = WaitHttp();
        ReplyHttpError(Rt.get(), Node, Proxy, first, "Connection refused");
        Rt->Send(new IEventHandle(id, Edge, new TEvents::TEvPoisonPill()), Node);
        Rt->Send(new IEventHandle(Edge, Edge, new TEvents::TEvWakeup()), Node);
        std::ignore = Rt->WaitForEdgeActorEvent<TEvents::TEvWakeup>(Edge, false);

        UNIT_ASSERT_C(Rt->GetActor(id) == nullptr, "Source backoff must be cancelled when the provider stops");
        UNIT_ASSERT_C(Rt->GetActor(Proxy) != nullptr, "An injected HTTP proxy belongs to the caller");
    }

    Y_UNIT_TEST_F(PoisonStopsProvider, TSetup) {
        const auto id = RegProvider(MakeConfig(ISS));
        std::ignore = WaitHttp();

        Rt->Send(new IEventHandle(id, Edge, new TEvents::TEvPoisonPill()), Node);
        Rt->Send(new IEventHandle(Edge, Edge, new TEvents::TEvWakeup()), Node);
        std::ignore = Rt->WaitForEdgeActorEvent<TEvents::TEvWakeup>(Edge, false);

        UNIT_ASSERT_C(Rt->GetActor(id) == nullptr,
            "External IdP provider must stop when its owner sends PoisonPill");
        UNIT_ASSERT_C(Rt->GetActor(Proxy) != nullptr, "An injected HTTP proxy belongs to the caller");
    }

    Y_UNIT_TEST_F(DeadlineReplacesOwnedHttpProxy, TSetup) {
        auto config = MakeConfig(ISS);
        config.MutableDiscoveryPeriodicSettings()->SetRequestTimeout("1s");
        config.MutableDiscoveryPeriodicSettings()->SetMinErrorRefreshPeriod("0s");
        config.MutableDiscoveryPeriodicSettings()->SetMaxErrorRefreshPeriod("0s");
        TVector<TActorId> proxies;
        Rt->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::EventType) {
                proxies.push_back(event->GetRecipientRewrite());
                return false;
            }
            return true;
        };
        const auto id = Rt->Register(CreateExternalIdpProvider(config, {}), Node);
        RegisteredActors.insert(id);
        Rt->Sim([&] { return proxies.size() < 2 && Rt->GetClock() < TInstant::Seconds(4); });

        UNIT_ASSERT_VALUES_EQUAL(proxies.size(), 2);
        UNIT_ASSERT_C(proxies[0] != proxies[1], "A deadline must replace the proxy owning the timed-out request");
        UNIT_ASSERT_C(Rt->GetActor(proxies[0]) == nullptr, "The old proxy must stop its outstanding transports");
        UNIT_ASSERT_C(Rt->GetActor(proxies[1]) != nullptr, "The source needs a live replacement proxy");
    }

    Y_UNIT_TEST_F(PoisonStopsOwnedHttpProxy, TSetup) {
        TActorId ownedProxy;
        Rt->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::EventType) {
                ownedProxy = event->GetRecipientRewrite();
                return false;
            }
            return true;
        };
        const auto id = Rt->Register(CreateExternalIdpProvider(MakeConfig(ISS), {}), Node);
        RegisteredActors.insert(id);
        Rt->Sim([&] { return !ownedProxy; });

        Rt->Send(new IEventHandle(id, Edge, new TEvents::TEvPoisonPill()), Node);
        for (size_t i = 0; i < 2; ++i) {
            Rt->Send(new IEventHandle(Edge, Edge, new TEvents::TEvWakeup()), Node);
            std::ignore = Rt->WaitForEdgeActorEvent<TEvents::TEvWakeup>(Edge, false);
        }
        UNIT_ASSERT_C(Rt->GetActor(id) == nullptr, "Provider must stop with an outstanding HTTP request");
        UNIT_ASSERT_C(Rt->GetActor(ownedProxy) == nullptr, "Provider must stop its owned HTTP proxy");
    }

    Y_UNIT_TEST_F(DiscoveryFetchOnBootstrap, TSetup) {
        std::ignore = RegProvider(MakeConfig(ISS));
        const auto req = WaitHttp();
        UNIT_ASSERT(req);
        UNIT_ASSERT_STRING_CONTAINS(TString{req->Get()->Request->URL}, ".well-known/openid-configuration");
    }

    Y_UNIT_TEST_F(DiscoveryInvalidResponse, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        auto d = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", "not-json{{{");

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);
        const auto r1 = WaitAuth();
        UNIT_ASSERT_EQUAL(r1->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);

        auto d2 = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d2, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto j = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, j, "200", "OK", BuildJwksJson(KID, keys.X5cBase64));

        const auto token2 = CreateJwt(keys, KID, ISS, "u2", {});
        SendAuth(id, "k2", token2);
        const auto r2 = WaitAuth();
        UNIT_ASSERT_EQUAL(r2->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
    }

    Y_UNIT_TEST_F(DiscoveryRepeatsIfNoJwksUri, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        auto d = WaitHttp();
        NJson::TJsonValue noJwks;
        noJwks["issuer"] = ISS;
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", NJson::WriteJson(noJwks, false));

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);
        const auto r1 = WaitAuth();
        UNIT_ASSERT_EQUAL(r1->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);

        auto d2 = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d2, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto j = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, j, "200", "OK", BuildJwksJson(KID, keys.X5cBase64));

        const auto token2 = CreateJwt(keys, KID, ISS, "u2", {});
        SendAuth(id, "k2", token2);
        const auto r2 = WaitAuth();
        UNIT_ASSERT_EQUAL(r2->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
    }

    Y_UNIT_TEST_F(JwksFetchAfterDiscovery, TSetup) {
        std::ignore = RegProvider(MakeConfig(ISS));
        auto d = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        const auto j = WaitHttp();
        UNIT_ASSERT(j);
        UNIT_ASSERT_STRING_CONTAINS(TString{j->Get()->Request->URL}, "jwks.json");
    }

    Y_UNIT_TEST_F(JwksKeysPopulated, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);
        const auto token = CreateJwt(keys, KID, ISS, "user1", {});
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "user1");
    }

    Y_UNIT_TEST_F(JwksInvalidResponse, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        auto d = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto j = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, j, "200", "OK", "bad{{{");

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);
        const auto r1 = WaitAuth();
        UNIT_ASSERT_EQUAL(r1->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);

        auto j2 = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, j2, "200", "OK", BuildJwksJson(KID, keys.X5cBase64));

        const auto token2 = CreateJwt(keys, KID, ISS, "u2", {});
        SendAuth(id, "k2", token2);
        const auto r2 = WaitAuth();
        UNIT_ASSERT_EQUAL(r2->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
    }

    Y_UNIT_TEST_F(ValidTokenOk, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "", "sub", "groups"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);
        const auto token = CreateJwt(keys, KID, ISS, "testuser", {},
            {"g1", "g2"}, "", "groups");
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "testuser");
        UNIT_ASSERT(r->Get()->ExpiresAt > TInstant::Now());
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups[0], "g1");
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups[1], "g2");
    }

    Y_UNIT_TEST_F(NoMatchingJwk, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, "other-key", keys.X5cBase64);
        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);
    }

    Y_UNIT_TEST_F(InvalidTokenErrors, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto expired = CreateExpiredJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k-exp", expired);
        const auto r1 = WaitAuth();
        UNIT_ASSERT_EQUAL(r1->Get()->Status, TEvExternalIdpProvider::EStatus::UNAUTHORIZED);

        const auto other = GenerateRsaKeyPair();
        const auto wrongSig = CreateJwt(other, KID, ISS, "u1", {});
        SendAuth(id, "k-sig", wrongSig);
        const auto r2 = WaitAuth();
        UNIT_ASSERT_EQUAL(r2->Get()->Status, TEvExternalIdpProvider::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST_F(PreferredSubjectClaim, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "", "preferred_username", "groups"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);
        const auto token = CreateJwt(keys, KID, ISS, "custom-user", {},
                                     {}, "preferred_username", "");
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "custom-user_custom-claim");
    }

    Y_UNIT_TEST_F(PreferredGroupsClaim, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "", "sub", "custom_groups"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);
        const auto token = CreateJwt(keys, KID, ISS, "u1", {},
                                     {"admins", "devs"}, "", "custom_groups");
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups[0], "admins");
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups[1], "devs");
    }

    Y_UNIT_TEST_F(InvalidSubjectClaimFallsBackToSub, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "", "nonexistent_claim", "groups"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);
        const auto token = CreateJwt(keys, KID, ISS, "real-user", {});
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "real-user");
    }

    Y_UNIT_TEST_F(InvalidGroupsClaimResultsInEmptyGroups, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "", "sub", "nonexistent_groups"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);
        const auto token = CreateJwt(keys, KID, ISS, "u1", {},
                                     {"g1"}, "", "actual_groups");
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups.size(), 0);
    }

    Y_UNIT_TEST_F(AuthBeforeJwksReturnsError, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));

        auto d = WaitHttp();

        const auto t1 = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", t1);

        const auto r1 = WaitAuth();
        UNIT_ASSERT_EQUAL(r1->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);

        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto j = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, j, "200", "OK", BuildJwksJson(KID, keys.X5cBase64));

        const auto t2 = CreateJwt(keys, KID, ISS, "u2", {});
        SendAuth(id, "k2", t2);
        const auto r2 = WaitAuth();
        UNIT_ASSERT_EQUAL(r2->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
    }

    Y_UNIT_TEST_F(AuthBeforeDiscoveryReturnsError, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));

        auto d = WaitHttp();

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);

        ReplyHttpError(Rt.get(), Node, Proxy, d, "Network unreachable");
    }

    Y_UNIT_TEST_F(WrongIssuerRejected, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateJwt(keys, KID, "https://wrong-issuer.com", "u1", {});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST_F(NonHttpsIssuerDisablesFetches, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const TString httpIss = "http://idp.example.com";
        const auto id = RegProvider(MakeConfig(httpIss));

        const auto token = CreateJwt(keys, KID, httpIss, "u1", {});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);
    }

    Y_UNIT_TEST_F(NonHttpsJwksUriRejected, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));

        const TString httpJwks = "http://idp.example.com/jwks.json";
        auto d = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", BuildDiscoveryJson(ISS, httpJwks));

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);
    }

    Y_UNIT_TEST_F(ConfiguredAudienceAccepted, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "my-client-id"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateJwt(keys, KID, ISS, "u1", {"my-client-id"});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "u1");
    }

    Y_UNIT_TEST_F(AudienceMismatchRejected, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "my-client-id"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateJwt(keys, KID, ISS, "u1", {"wrong-audience"});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST_F(NoConfiguredAudienceDoesNotVerifyTokenAudience, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateJwt(keys, KID, ISS, "u1", {"some-random-audience"});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "u1");
    }

    Y_UNIT_TEST_F(NoKidInTokenRejected, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateJwtWithoutKid(keys, ISS, "u1", {});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST_F(JwksFetchNetworkError, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));

        auto d = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", BuildDiscoveryJson(ISS, JWKS));

        auto j = WaitHttp();
        ReplyHttpError(Rt.get(), Node, Proxy, j, "Connection refused");

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);
    }

    Y_UNIT_TEST_F(DiscoveryIssuerMismatch, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));

        auto d = WaitHttp();
        NJson::TJsonValue discoveryJson;
        discoveryJson["issuer"] = "https://wrong-issuer.com";
        discoveryJson["jwks_uri"] = JWKS;
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", NJson::WriteJson(discoveryJson, false));

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);
    }

    Y_UNIT_TEST_F(DiscoveryHttp500Error, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));

        auto d = WaitHttp();
        ReplyHttpError(Rt.get(), Node, Proxy, d, "HTTP 500 Internal Server Error");

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);

        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);
    }

    Y_UNIT_TEST_F(PeriodicJwksRefresh, TSetup) {
        const auto keys1 = GenerateRsaKeyPair();
        const auto keys2 = GenerateRsaKeyPair();

        auto config = MakeConfig(ISS);
        config.MutableJwksPeriodicSettings()->SetSuccessRefreshPeriod("1s");
        config.MutableDiscoveryPeriodicSettings()->SetSuccessRefreshPeriod("2h");
        const auto id = RegProvider(config);

        DoDiscoveryAndJwks(ISS, JWKS, KID, keys1.X5cBase64);

        auto token1 = CreateJwt(keys1, KID, ISS, "u1", {});
        SendAuth(id, "k1", token1);
        auto r1 = WaitAuth();
        UNIT_ASSERT_EQUAL(r1->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);

        auto jwksReq = WaitHttp();
        UNIT_ASSERT_STRING_CONTAINS(TString{jwksReq->Get()->Request->URL}, "jwks");
        ReplyHttp(Rt.get(), Node, Proxy, jwksReq, "200", "OK", BuildJwksJson("key-2", keys2.X5cBase64));

        auto token2 = CreateJwt(keys2, "key-2", ISS, "u2", {});
        SendAuth(id, "k2", token2);
        auto r2 = WaitAuth();
        UNIT_ASSERT_EQUAL(r2->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r2->Get()->User, "u2");
    }

    Y_UNIT_TEST_F(PeriodicDiscoveryRefresh, TSetup) {
        const auto initialKeys = GenerateRsaKeyPair();
        const auto fallbackKeys = GenerateRsaKeyPair();

        auto config = MakeConfig(ISS);
        config.MutableDiscoveryPeriodicSettings()->SetSuccessRefreshPeriod("1s");
        config.MutableJwksPeriodicSettings()->SetSuccessRefreshPeriod("2h");
        std::ignore = RegProvider(config);

        DoDiscoveryAndJwks(ISS, JWKS, KID, initialKeys.X5cBase64);

        bool sawDiscovery = false;
        for (int i = 0; i < 3; ++i) {
            auto req = WaitHttp();
            TString url(req->Get()->Request->URL);
            if (url.Contains("openid-configuration")) {
                sawDiscovery = true;
                ReplyHttp(Rt.get(), Node, Proxy, req, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
                break;
            }
            ReplyHttp(Rt.get(), Node, Proxy, req, "200", "OK", BuildJwksJson(KID, fallbackKeys.X5cBase64));
        }
        UNIT_ASSERT_C(sawDiscovery, "Expected at least one periodic discovery request");
    }

    Y_UNIT_TEST_F(MalformedTokenReturnsBadRequest, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        SendAuth(id, "k1", "this-is-not-a-jwt-token");
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST_F(UnsupportedAlgorithmReturnsBadRequest, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateSignedJwt(
            keys,
            MakeJwtSettings(MakeMaybe(KID), ISS, "u1", {}),
            EJwtSigningAlgorithm::HS256);

        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST_F(Ps256TokenAuthenticates, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreatePs256Jwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "u1");
    }

    Y_UNIT_TEST_F(UnsupportedJwksWithoutX5cLeavesEmptyCache, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));

        auto d = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", BuildDiscoveryJson(ISS, JWKS));

        auto j = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, j, "200", "OK",
            BuildUnsupportedJwksJsonWithoutX5c(KID));

        const auto token = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);
    }

    Y_UNIT_TEST_F(AllowedClockSkewAcceptsRecentlyExpired, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        auto config = MakeConfig(ISS);
        config.SetAllowedClockSkew("60s");
        const auto id = RegProvider(config);
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        // Create a token that expired 30 seconds ago (within 60s skew)
        const auto token = CreateSignedJwt(
            keys,
            MakeJwtSettings(MakeMaybe(KID), ISS, "u1", {},
                -std::chrono::seconds(3600), -std::chrono::seconds(30)));

        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "u1");
    }

    Y_UNIT_TEST_F(MultipleJwksKeysBothUsable, TSetup) {
        const auto keys1 = GenerateRsaKeyPair();
        const auto keys2 = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));

        auto d = WaitHttp();
        ReplyHttp(Rt.get(), Node, Proxy, d, "200", "OK", BuildDiscoveryJson(ISS, JWKS));
        auto j = WaitHttp();
        TVector<std::pair<TString, TString>> kidX5cPairs = {
            {"key-1", keys1.X5cBase64},
            {"key-2", keys2.X5cBase64}
        };
        ReplyHttp(Rt.get(), Node, Proxy, j, "200", "OK", BuildMultiKeyJwksJson(kidX5cPairs));

        // Token signed with key-1
        const auto token1 = CreateJwt(keys1, "key-1", ISS, "u1", {});
        SendAuth(id, "k1", token1);
        const auto r1 = WaitAuth();
        UNIT_ASSERT_EQUAL(r1->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r1->Get()->User, "u1");

        // Token signed with key-2
        const auto token2 = CreateJwt(keys2, "key-2", ISS, "u2", {});
        SendAuth(id, "k2", token2);
        const auto r2 = WaitAuth();
        UNIT_ASSERT_EQUAL(r2->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r2->Get()->User, "u2");
    }

    Y_UNIT_TEST_F(EmptySubjectReturnsUnauthorized, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateJwtWithEmptySubject(keys, KID, ISS, {});
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST_F(NonStringSubjectClaimFallsBackToSub, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "", "int_claim", "groups"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateJwtWithIntSubjectClaim(keys, KID, ISS, "fallback-sub",
            {}, "int_claim");
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->User, "fallback-sub");
    }

    Y_UNIT_TEST_F(GroupsClaimMixedTypesExtractsStringsOnly, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        const auto id = RegProvider(MakeConfig(ISS, "", "sub", "mixed_groups"));
        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        const auto token = CreateJwtWithMixedGroups(keys, KID, ISS, "u1",
                                                    {}, "mixed_groups");
        SendAuth(id, "k1", token);
        const auto r = WaitAuth();
        UNIT_ASSERT_EQUAL(r->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);
        // Only string values should be extracted from the mixed array
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups[0], "admin");
        UNIT_ASSERT_VALUES_EQUAL(r->Get()->Groups[1], "dev");
    }

    Y_UNIT_TEST_F(JwksCacheStalenessClears, TSetup) {
        const auto keys = GenerateRsaKeyPair();
        auto config = MakeConfig(ISS);
        config.MutableJwksCacheSettings()->SetTimeout("2s");
        config.MutableJwksPeriodicSettings()->SetSuccessRefreshPeriod("1s");
        config.MutableDiscoveryPeriodicSettings()->SetSuccessRefreshPeriod("2h");
        const auto id = RegProvider(config);

        DoDiscoveryAndJwks(ISS, JWKS, KID, keys.X5cBase64);

        // Auth should succeed initially
        auto token1 = CreateJwt(keys, KID, ISS, "u1", {});
        SendAuth(id, "k1", token1);
        auto r1 = WaitAuth();
        UNIT_ASSERT_EQUAL(r1->Get()->Status, TEvExternalIdpProvider::EStatus::SUCCESS);

        // JWKS refresh fails -- reply with error
        auto jwksReq = WaitHttp();
        ReplyHttpError(Rt.get(), Node, Proxy, jwksReq, "Connection refused");

        // Expiry clears the cache and starts one source recovery attempt.
        // A failed recovery is retried using the shared source backoff.
        auto req2 = WaitHttp();
        ReplyHttpError(Rt.get(), Node, Proxy, req2, "Connection refused");
        auto req3 = WaitHttp();
        ReplyHttpError(Rt.get(), Node, Proxy, req3, "Connection refused");

        // Auth should now fail because cache was cleared due to staleness
        auto token2 = CreateJwt(keys, KID, ISS, "u2", {});
        SendAuth(id, "k2", token2);
        auto r2 = WaitAuth();
        UNIT_ASSERT_EQUAL(r2->Get()->Status, TEvExternalIdpProvider::EStatus::UNAVAILABLE);
    }

}

} // namespace NKikimr
