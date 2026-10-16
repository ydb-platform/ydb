#include <ydb/core/protos/grpc.grpc.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/grpc/server/actors/logger.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

#include <chrono>

using namespace NKikimr;
using namespace Tests;

namespace {

constexpr TDuration CallTimeout = TDuration::Seconds(10);
constexpr TDuration RetryDelay = TDuration::MilliSeconds(50);
const TString BuiltinToken = "root@builtin";
const TString InvalidToken = "invalid-token";

struct TCallAuth {
    TString ProtoToken;
    TString MetadataToken;
};

struct TLegacyCallResult {
    grpc::Status Status;
    NKikimrClient::TResponse Response;
};

std::unique_ptr<NKikimrClient::TGRpcServer::Stub> MakeStub(ui16 grpcPort) {
    auto channel = grpc::CreateChannel(
        TStringBuilder() << "localhost:" << grpcPort,
        grpc::InsecureChannelCredentials());
    return NKikimrClient::TGRpcServer::NewStub(channel);
}

template <typename TRequest>
void ApplyAuth(grpc::ClientContext& context, TRequest& request, const TCallAuth& auth) {
    if (!auth.ProtoToken.empty()) {
        request.SetSecurityToken(auth.ProtoToken);
    }
    if (!auth.MetadataToken.empty()) {
        context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, auth.MetadataToken);
    }
    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::milliseconds(CallTimeout.MilliSeconds()));
}

TLegacyCallResult CallChooseProxy(ui16 grpcPort, const TCallAuth& auth = {}) {
    auto stub = MakeStub(grpcPort);
    NKikimrClient::TChooseProxyRequest request;

    TLegacyCallResult result;
    grpc::ClientContext context;
    ApplyAuth(context, request, auth);
    result.Status = stub->ChooseProxy(&context, request, &result.Response);
    return result;
}

TLegacyCallResult CallSchemeDescribe(ui16 grpcPort, const TCallAuth& auth = {}) {
    auto stub = MakeStub(grpcPort);
    NKikimrClient::TSchemeDescribe request;
    request.SetPath("/Root");

    TLegacyCallResult result;
    grpc::ClientContext context;
    ApplyAuth(context, request, auth);
    result.Status = stub->SchemeDescribe(&context, request, &result.Response);
    return result;
}

bool IsTemporarilyUnavailable(const TLegacyCallResult& result) {
    if (result.Status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        return true;
    }
    if (!result.Status.ok()) {
        return false;
    }
    const auto status = static_cast<NMsgBusProxy::EResponseStatus>(result.Response.GetStatus());
    return status == NMsgBusProxy::MSTATUS_ERROR || status == NMsgBusProxy::MSTATUS_NOTREADY;
}

TLegacyCallResult WaitChooseProxy(ui16 grpcPort, const TCallAuth& auth = {}) {
    TLegacyCallResult result;
    const TInstant deadline = TInstant::Now() + CallTimeout;
    do {
        result = CallChooseProxy(grpcPort, auth);
        if (!IsTemporarilyUnavailable(result)) {
            return result;
        }
        Sleep(RetryDelay);
    } while (TInstant::Now() < deadline);
    return result;
}

TLegacyCallResult WaitSchemeDescribe(ui16 grpcPort, const TCallAuth& auth = {}) {
    TLegacyCallResult result;
    const TInstant deadline = TInstant::Now() + CallTimeout;
    do {
        result = CallSchemeDescribe(grpcPort, auth);
        if (result.Status.error_code() != grpc::StatusCode::UNAVAILABLE) {
            return result;
        }
        Sleep(RetryDelay);
    } while (TInstant::Now() < deadline);
    return result;
}

void AssertChooseProxyOk(const TLegacyCallResult& result) {
    UNIT_ASSERT_C(result.Status.ok(), result.Status.error_message());
    UNIT_ASSERT_VALUES_EQUAL_C(
        static_cast<NMsgBusProxy::EResponseStatus>(result.Response.GetStatus()),
        NMsgBusProxy::MSTATUS_OK,
        result.Response.ShortDebugString());
    UNIT_ASSERT_C(result.Response.HasProxyName(), result.Response.ShortDebugString());
    UNIT_ASSERT_C(result.Response.HasProxyCookie(), result.Response.ShortDebugString());
}

void AssertUnauthenticated(const TLegacyCallResult& result) {
    UNIT_ASSERT_VALUES_EQUAL_C(
        result.Status.error_code(),
        grpc::StatusCode::UNAUTHENTICATED,
        result.Status.error_message());
}

struct TChooseProxyServer {
    TPortManager PortManager;
    const ui16 GrpcPort;
    TServerSettings Settings;
    TServer Server;

    explicit TChooseProxyServer(bool enforceUserToken)
        : GrpcPort(PortManager.GetPort(2135))
        , Settings(MakeSettings(enforceUserToken))
        , Server(Settings)
    {
        Server.EnableGRpc(NYdbGrpc::TServerOptions()
            .SetHost("localhost")
            .SetPort(GrpcPort)
            .SetUseAuth(enforceUserToken)
            .SetLogger(NYdbGrpc::CreateActorSystemLogger(
                *Server.GetRuntime()->GetActorSystem(0),
                NKikimrServices::GRPC_SERVER)));

        TClient client(Settings);
        if (enforceUserToken) {
            client.SetSecurityToken(BuiltinToken);
        }
        client.InitRootScheme("Root");
    }

private:
    TServerSettings MakeSettings(bool enforceUserToken) {
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBuiltinDomain(true);

        TServerSettings settings(PortManager.GetPort(2134), authConfig);
        settings.SetDomainName("Root");
        settings.AppConfig->MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(enforceUserToken);
        return settings;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TChooseProxyGrpc) {
    Y_UNIT_TEST(WithoutTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertChooseProxyOk(WaitChooseProxy(env.GrpcPort));
    }

    Y_UNIT_TEST(WithProtoTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertChooseProxyOk(WaitChooseProxy(env.GrpcPort, {.ProtoToken = BuiltinToken}));
    }

    Y_UNIT_TEST(WithMetadataTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertChooseProxyOk(WaitChooseProxy(env.GrpcPort, {.MetadataToken = BuiltinToken}));
    }

    Y_UNIT_TEST(WithInvalidProtoTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertUnauthenticated(WaitChooseProxy(env.GrpcPort, {.ProtoToken = InvalidToken}));
    }

    Y_UNIT_TEST(WithInvalidMetadataTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertUnauthenticated(WaitChooseProxy(env.GrpcPort, {.MetadataToken = InvalidToken}));
    }

    Y_UNIT_TEST(WithoutTokenWhenTokenNotRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/false);
        AssertChooseProxyOk(WaitChooseProxy(env.GrpcPort));
    }

    Y_UNIT_TEST(SchemeDescribeWithoutTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertUnauthenticated(WaitSchemeDescribe(env.GrpcPort));
    }

    Y_UNIT_TEST(SchemeDescribeWithMetadataTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        const auto result = WaitSchemeDescribe(env.GrpcPort, {.MetadataToken = BuiltinToken});
        UNIT_ASSERT_C(result.Status.ok(), result.Status.error_message());
    }

    Y_UNIT_TEST(SchemeDescribeWithInvalidProtoTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertUnauthenticated(WaitSchemeDescribe(env.GrpcPort, {.ProtoToken = InvalidToken}));
    }
}
