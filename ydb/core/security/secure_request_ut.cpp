#include <ydb/core/base/backtrace.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/security/secure_request.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

using namespace NActors;

constexpr char VALID_TOKEN[] = "Bearer token";
constexpr char ROOT_TOKEN[] = "root@builtin";

struct TEvSecureRequestTest {
    enum EEv {
        EvResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    struct TEvResponse : TEventLocal<TEvResponse, EvResponse> {
        bool AccessDenied;
        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        bool IsUserAdmin;
        TString ErrorMessage;
    };
};

class TSecureRequestTestActor : public TActorBootstrappedSecureRequest<TSecureRequestTestActor> {
public:
    TSecureRequestTestActor(TActorId replyTo, const TString& securityToken, bool requireAdminAccess)
        : ReplyTo(replyTo)
    {
        SetSecurityToken(securityToken);
        SetRequireAdminAccess(requireAdminAccess);
    }

    TSecureRequestTestActor(TActorId replyTo, TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool requireAdminAccess)
        : ReplyTo(replyTo)
    {
        SetInternalToken(std::move(userToken));
        SetRequireAdminAccess(requireAdminAccess);
    }

    void Bootstrap(const TActorContext& ctx) {
        auto response = MakeHolder<TEvSecureRequestTest::TEvResponse>();
        response->AccessDenied = false;
        response->UserToken = GetParsedToken();
        response->IsUserAdmin = IsUserAdmin();
        ctx.Send(ReplyTo, response.Release());
        Die(ctx);
    }

    void OnAccessDenied(const TEvTicketParser::TError& error, const TActorContext& ctx) {
        auto response = MakeHolder<TEvSecureRequestTest::TEvResponse>();
        response->AccessDenied = true;
        response->UserToken = GetParsedToken();
        response->IsUserAdmin = IsUserAdmin();
        response->ErrorMessage = error.Message;
        ctx.Send(ReplyTo, response.Release());
        Die(ctx);
    }

private:
    TActorId ReplyTo;
};

IActor* CreateSecureRequestTestActor(TActorId replyTo, const TString& securityToken, bool requireAdminAccess) {
    return new TSecureRequestTestActor(replyTo, securityToken, requireAdminAccess);
}

IActor* CreateSecureRequestTestActor(TActorId replyTo, TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    bool requireAdminAccess)
{
    return new TSecureRequestTestActor(replyTo, std::move(userToken), requireAdminAccess);
}

struct TFakeTicketParserActor : public TActor<TFakeTicketParserActor> {
    TFakeTicketParserActor()
        : TActor<TFakeTicketParserActor>(&TFakeTicketParserActor::StateFunc)
    {}

    void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        if (ev->Get()->Ticket == ROOT_TOKEN || ev->Get()->Ticket == VALID_TOKEN) {
            return Success(ev);
        } else {
            Fail(ev);
        }
    }
    void Fail(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        TEvTicketParser::TError err;
        err.Retryable = false;
        err.Message = "Parsing error";
        Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, err));
    }

    void Success(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        NACLib::TUserToken::TUserTokenInitFields args;
        if (ev->Get()->Ticket == ROOT_TOKEN) {
            args.UserSID = ROOT_TOKEN;
        } else {
            args.UserSID = "user";
        }
        TIntrusivePtr<NACLib::TUserToken> userToken = MakeIntrusive<NACLib::TUserToken>(args);
        userToken->SaveSerializationInfo();
        Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, userToken));
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTicketParser::TEvAuthorizeTicket, Handle);
            default:
                break;
        }
    }
};

IActor* CreateFakeTicketParser(const TTicketParserSettings&) {
    return new TFakeTicketParserActor();
}

struct TTestEnvSettings {
    bool EnforceUserTokenRequirement = false;
    bool EnforceUserTokenCheckRequirement = false;
    TVector<TString> AdministrationAllowedSIDs = {};
    TVector<TString> DefaultUserSIDs = {};
};

struct TSecureRequestTestEnv {
private:
    TPortManager PortManager;

    Tests::TServerSettings::TPtr Settings;
    Tests::TServer::TPtr Server;

public:
    TSecureRequestTestEnv(const TTestEnvSettings& settings) {
        EnableYDBBacktraceFormat();

        const ui16 mbusPort = PortManager.GetPort();
        Settings = new Tests::TServerSettings(mbusPort);
        Settings->SetDomainName("Root");
        Settings->SetNodeCount(1);
        Settings->CreateTicketParser = CreateFakeTicketParser;

        auto& securityConfig = *Settings->AppConfig->MutableDomainsConfig()->MutableSecurityConfig();
        securityConfig.SetEnforceUserTokenRequirement(settings.EnforceUserTokenRequirement);
        securityConfig.SetEnforceUserTokenCheckRequirement(settings.EnforceUserTokenCheckRequirement);

        for (const auto& sid : settings.AdministrationAllowedSIDs) {
            securityConfig.AddAdministrationAllowedSIDs(sid);
        }

        for (const auto& sid : settings.DefaultUserSIDs) {
            securityConfig.AddDefaultUserSIDs(sid);
        }

        Server = new Tests::TServer(*Settings);
    }

    TTestActorRuntime* GetRuntime() const {
        return Server->GetRuntime();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TSecureRequestActorTest) {

// Anonymous requests: allowed
// Invalid tokens: allowed
// Admins: all sids
// No default sids
Y_UNIT_TEST(SecurityConfig1) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = false,
        .AdministrationAllowedSIDs = {},
        .DefaultUserSIDs = {},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: allowed
// Invalid tokens: allowed
// Admins: all sids
// Default sids: defaultUser
Y_UNIT_TEST(SecurityConfig2) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = false,
        .AdministrationAllowedSIDs = {},
        .DefaultUserSIDs = {"defaultUser"},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: allowed
// Invalid tokens: allowed
// Admins: root@builtin
// No default sids
Y_UNIT_TEST(SecurityConfig3) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = false,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Access denied without user token");

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Root builtin token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Internal root token request, admin access isn't required
    userToken = new NACLib::TUserToken(ROOT_TOKEN, {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal root token request, admin access is required
    userToken = new NACLib::TUserToken(ROOT_TOKEN, {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: allowed
// Invalid tokens: allowed
// Admins: root@builtin
// Default sids: defaultUser
Y_UNIT_TEST(SecurityConfig4) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = false,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {"defaultUser"},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");
}

// Anonymous requests: allowed
// Invalid tokens: allowed
// Admins: root@builtin
// Default sids: root@builtin
Y_UNIT_TEST(SecurityConfig5) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = false,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {ROOT_TOKEN},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");
}

// Anonymous requests: allowed
// Invalid tokens: forbidden
// Admins: all sids
// No default sids
Y_UNIT_TEST(SecurityConfig6) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = true,
        .AdministrationAllowedSIDs = {},
        .DefaultUserSIDs = {},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: allowed
// Invalid tokens: forbidden
// Admins: all sids
// Default sids: defaultUser
Y_UNIT_TEST(SecurityConfig7) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = true,
        .AdministrationAllowedSIDs = {},
        .DefaultUserSIDs = {"defaultUser"},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: allowed
// Invalid tokens: forbidden
// Admins: root@builtin
// No default sids
Y_UNIT_TEST(SecurityConfig8) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = true,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Access denied without user token");

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Root builtin token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Internal root token request, admin access isn't required
    userToken = new NACLib::TUserToken(ROOT_TOKEN, {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal root token request, admin access is required
    userToken = new NACLib::TUserToken(ROOT_TOKEN, {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: allowed
// Invalid tokens: forbidden
// Admins: root@builtin
// Default sids: defaultUser
Y_UNIT_TEST(SecurityConfig9) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = true,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {"defaultUser"},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");
}

// Anonymous requests: allowed
// Invalid tokens: forbidden
// Admins: root@builtin
// Default sids: root@builtin
Y_UNIT_TEST(SecurityConfig10) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = false,
        .EnforceUserTokenCheckRequirement = true,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {ROOT_TOKEN},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");
}

// Anonymous requests: forbidden
// Admins: all sids
// No default sids
Y_UNIT_TEST(SecurityConfig11) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = true,
        .AdministrationAllowedSIDs = {},
        .DefaultUserSIDs = {},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Access denied without user token");

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Access denied without user token");

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: forbidden
// Admins: root@builtin
// No default sids
Y_UNIT_TEST(SecurityConfig12) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = true,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Access denied without user token");

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Access denied without user token");

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Root builtin token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Internal root token request, admin access isn't required
    userToken = new NACLib::TUserToken(ROOT_TOKEN, {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal root token request, admin access is required
    userToken = new NACLib::TUserToken(ROOT_TOKEN, {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: forbidden
// Admins: all sids
// Default sids: defaultUser
Y_UNIT_TEST(SecurityConfig13) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = true,
        .AdministrationAllowedSIDs = {},
        .DefaultUserSIDs = {"defaultUser"},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Valid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, VALID_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Root builtin token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, ROOT_TOKEN, true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Internal user token request, admin access isn't required
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Internal user token request, admin access is required
    userToken = new NACLib::TUserToken("user@builtin", {});
    runtime->Register(CreateSecureRequestTestActor(sender, std::move(userToken), true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "user@builtin");
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());
}

// Anonymous requests: forbidden
// Admins: root@builtin
// Default sids: defaultUser
Y_UNIT_TEST(SecurityConfig14) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = true,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {"defaultUser"},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), "defaultUser");
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Administrative access denied");

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");
}

// Anonymous requests: forbidden
// Admins: root@builtin
// Default sids: root@builtin
Y_UNIT_TEST(SecurityConfig15) {
    TTestEnvSettings settings {
        .EnforceUserTokenRequirement = true,
        .AdministrationAllowedSIDs = {ROOT_TOKEN},
        .DefaultUserSIDs = {ROOT_TOKEN},
    };
    TSecureRequestTestEnv env(settings);
    auto* runtime = env.GetRuntime();

    const TActorId sender = runtime->AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    // Anonymous request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "", false));
    auto response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Anonymous request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(!response->Get()->AccessDenied);
    UNIT_ASSERT(response->Get()->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->UserToken->GetUserSID(), ROOT_TOKEN);
    UNIT_ASSERT(response->Get()->IsUserAdmin);
    UNIT_ASSERT(response->Get()->ErrorMessage.empty());

    // Invalid token request, admin access isn't required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", false));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");

    // Invalid token request, admin access is required
    runtime->Register(CreateSecureRequestTestActor(sender, "invalid token", true));
    response = runtime->GrabEdgeEvent<TEvSecureRequestTest::TEvResponse>(sender);
    UNIT_ASSERT(response->Get()->AccessDenied);
    UNIT_ASSERT(!response->Get()->UserToken);
    UNIT_ASSERT(!response->Get()->IsUserAdmin);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->ErrorMessage, "Parsing error");
}

}

} // namespace NKikimr
