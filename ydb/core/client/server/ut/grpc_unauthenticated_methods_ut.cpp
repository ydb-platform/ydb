#include <ydb/core/protos/grpc.grpc.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/grpc/server/actors/logger.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
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
const TString ChooseProxyMethod = "NKikimrClient.TGRpcServer/ChooseProxy";
const TString SchemeOperationMethod = "NKikimrClient.TGRpcServer/SchemeOperation";
const TString ListEndpointsMethod = "Ydb.Discovery.V1.DiscoveryService/ListEndpoints";
const TString Database = "/Root";

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

TLegacyCallResult CallSchemeOperation(ui16 grpcPort, const TCallAuth& auth = {}) {
    auto stub = MakeStub(grpcPort);
    NKikimrClient::TSchemeOperation request;

    TLegacyCallResult result;
    grpc::ClientContext context;
    ApplyAuth(context, request, auth);
    result.Status = stub->SchemeOperation(&context, request, &result.Response);
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

TLegacyCallResult WaitSchemeOperation(ui16 grpcPort, const TCallAuth& auth = {}) {
    TLegacyCallResult result;
    const TInstant deadline = TInstant::Now() + CallTimeout;
    do {
        result = CallSchemeOperation(grpcPort, auth);
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

void AssertUnauthenticated(const grpc::Status& status) {
    UNIT_ASSERT_VALUES_EQUAL_C(
        status.error_code(),
        grpc::StatusCode::UNAUTHENTICATED,
        status.error_message());
}

void AssertUnauthenticated(const TLegacyCallResult& result) {
    AssertUnauthenticated(result.Status);
}

struct TListEndpointsCallResult {
    grpc::Status Status;
    Ydb::Discovery::ListEndpointsResponse Response;
};

TListEndpointsCallResult CallListEndpoints(ui16 grpcPort, const TCallAuth& auth = {}) {
    auto channel = grpc::CreateChannel(
        TStringBuilder() << "localhost:" << grpcPort,
        grpc::InsecureChannelCredentials());
    auto stub = Ydb::Discovery::V1::DiscoveryService::NewStub(channel);
    Ydb::Discovery::ListEndpointsRequest request;
    request.set_database(Database);

    TListEndpointsCallResult result;
    grpc::ClientContext context;
    context.AddMetadata(NYdb::YDB_DATABASE_HEADER, Database);
    if (!auth.MetadataToken.empty()) {
        context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, auth.MetadataToken);
    }
    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::milliseconds(CallTimeout.MilliSeconds()));
    result.Status = stub->ListEndpoints(&context, request, &result.Response);
    return result;
}

bool ShouldRetryListEndpoints(const TListEndpointsCallResult& result) {
    if (result.Status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        return true;
    }
    if (!result.Status.ok()) {
        return false;
    }
    const auto status = result.Response.operation().status();
    if (status == Ydb::StatusIds::UNAVAILABLE || status == Ydb::StatusIds::OVERLOADED) {
        return true;
    }
    if (status == Ydb::StatusIds::SUCCESS && result.Response.operation().result().value().empty()) {
        return true;
    }
    return false;
}

TListEndpointsCallResult WaitListEndpoints(ui16 grpcPort, const TCallAuth& auth = {}) {
    TListEndpointsCallResult result;
    const TInstant deadline = TInstant::Now() + CallTimeout;
    do {
        result = CallListEndpoints(grpcPort, auth);
        if (!ShouldRetryListEndpoints(result)) {
            return result;
        }
        Sleep(RetryDelay);
    } while (TInstant::Now() < deadline);
    return result;
}

void AssertListEndpointsOk(const TListEndpointsCallResult& result) {
    UNIT_ASSERT_C(result.Status.ok(), result.Status.error_message());
    UNIT_ASSERT_VALUES_EQUAL_C(
        result.Response.operation().status(),
        Ydb::StatusIds::SUCCESS,
        result.Response.ShortDebugString());
    Ydb::Discovery::ListEndpointsResult endpoints;
    UNIT_ASSERT_C(
        result.Response.operation().result().UnpackTo(&endpoints),
        result.Response.ShortDebugString());
    UNIT_ASSERT_C(endpoints.endpoints_size() > 0, endpoints.ShortDebugString());
}

struct TChooseProxyServer {
    TPortManager PortManager;
    const ui16 GrpcPort;
    TServerSettings Settings;
    TServer Server;

    explicit TChooseProxyServer(bool enforceUserToken, std::initializer_list<TString> unauthenticatedMethods = {})
        : GrpcPort(PortManager.GetPort(2135))
        , Settings(MakeSettings(enforceUserToken, unauthenticatedMethods))
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
    TServerSettings MakeSettings(bool enforceUserToken, std::initializer_list<TString> unauthenticatedMethods) {
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBuiltinDomain(true);

        TServerSettings settings(PortManager.GetPort(2134), authConfig);
        settings.SetDomainName("Root");
        auto* securityConfig = settings.AppConfig->MutableDomainsConfig()->MutableSecurityConfig();
        securityConfig->SetEnforceUserTokenRequirement(enforceUserToken);
        for (const auto& method : unauthenticatedMethods) {
            securityConfig->AddUnauthenticatedGrpcMethods(method);
        }
        return settings;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TChooseProxyGrpc) {
    Y_UNIT_TEST(WithoutTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertUnauthenticated(WaitChooseProxy(env.GrpcPort));
    }

    Y_UNIT_TEST(WithoutTokenWhenMethodListed) {
        TChooseProxyServer env(/*enforceUserToken=*/true, {ChooseProxyMethod});
        AssertChooseProxyOk(WaitChooseProxy(env.GrpcPort));
    }

    Y_UNIT_TEST(WithInvalidProtoTokenWhenMethodListed) {
        TChooseProxyServer env(/*enforceUserToken=*/true, {ChooseProxyMethod});
        AssertUnauthenticated(WaitChooseProxy(env.GrpcPort, {.ProtoToken = InvalidToken}));
    }

    Y_UNIT_TEST(SchemeOperationWithoutTokenWhenListed) {
        TChooseProxyServer env(/*enforceUserToken=*/true, {SchemeOperationMethod});
        AssertUnauthenticated(WaitSchemeOperation(env.GrpcPort));
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
}

Y_UNIT_TEST_SUITE(TListEndpointsGrpc) {
    Y_UNIT_TEST(WithoutTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertUnauthenticated(WaitListEndpoints(env.GrpcPort).Status);
    }

    Y_UNIT_TEST(WithoutTokenWhenMethodListed) {
        TChooseProxyServer env(/*enforceUserToken=*/true, {ListEndpointsMethod});
        AssertListEndpointsOk(WaitListEndpoints(env.GrpcPort));
    }

    Y_UNIT_TEST(WithInvalidMetadataTokenWhenMethodListed) {
        TChooseProxyServer env(/*enforceUserToken=*/true, {ListEndpointsMethod});
        AssertUnauthenticated(WaitListEndpoints(env.GrpcPort, {.MetadataToken = InvalidToken}).Status);
    }

    Y_UNIT_TEST(WithMetadataTokenWhenTokenRequired) {
        TChooseProxyServer env(/*enforceUserToken=*/true);
        AssertListEndpointsOk(WaitListEndpoints(env.GrpcPort, {.MetadataToken = BuiltinToken}));
    }

    Y_UNIT_TEST(WithoutTokenWhenOtherMethodListed) {
        TChooseProxyServer env(/*enforceUserToken=*/true, {ChooseProxyMethod});
        AssertUnauthenticated(WaitListEndpoints(env.GrpcPort).Status);
    }
}
