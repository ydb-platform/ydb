#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/token_manager/token_manager.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <vector>

namespace NKikimr {

namespace {

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

struct TProviderSettings {
    TString SuccessRefreshPeriod = "1h";
    TString MinErrorRefreshPeriod = "1s";
    TString MaxErrorRefreshPeriod = "1m";
    TString RequestTimeout = "1m";
};

struct TVmMetadataInfoSettings {
    TString Id;
    TString Endpoint;
};

struct TVmMetadataProviderInitializer {
    std::vector<TVmMetadataInfoSettings> VmMetadataInfo;
    TProviderSettings Settings;
};

NKikimrProto::TTokenManager::TVmMetadataProvider ConfigureVmMetadataProvider(const TVmMetadataProviderInitializer& initializer) {
    NKikimrProto::TTokenManager::TVmMetadataProvider vmMetadataTokenProviderConfig;
    auto vmMetadataInfoList = vmMetadataTokenProviderConfig.MutableProvidersInfo();
    for (const auto& infoSettings : initializer.VmMetadataInfo) {
        auto vmMetadataInfo = vmMetadataInfoList->Add();
        vmMetadataInfo->SetId(infoSettings.Id);
        vmMetadataInfo->SetEndpoint(infoSettings.Endpoint);
    }
    auto vmMetadataProviderSettings = vmMetadataTokenProviderConfig.MutableSettings();
    vmMetadataProviderSettings->SetSuccessRefreshPeriod(initializer.Settings.SuccessRefreshPeriod);
    vmMetadataProviderSettings->SetMinErrorRefreshPeriod(initializer.Settings.MinErrorRefreshPeriod);
    vmMetadataProviderSettings->SetMaxErrorRefreshPeriod(initializer.Settings.MaxErrorRefreshPeriod);
    vmMetadataProviderSettings->SetRequestTimeout(initializer.Settings.RequestTimeout);

    return vmMetadataTokenProviderConfig;
}

struct TTokenManagerInitializer {
    TVmMetadataProviderInitializer VmMetadataProviderInitializer;
};

NKikimrProto::TTokenManager CreateTokenManagerConfig(const TTokenManagerInitializer& initializer) {
    NKikimrProto::TTokenManager tokenManagerConfig;
    *(tokenManagerConfig.MutableVmMetadataProvider()) = ConfigureVmMetadataProvider(initializer.VmMetadataProviderInitializer);
    return tokenManagerConfig;
}

struct TTokenManagerRegisterSettings {
    NActors::TActorId HttpProxyId;
    ui32 NodeId;
    NKikimrProto::TTokenManager TokenManagerConfig;
};

void RegisterTokenManager(TTestActorSystem* runtime, const TTokenManagerRegisterSettings& settings) {
    IActor* tokenManager = NKikimr::CreateTokenManager({.Config = settings.TokenManagerConfig, .HttpProxyId = settings.HttpProxyId});
    TActorId tokenManagerId = runtime->Register(tokenManager, settings.NodeId);
    runtime->RegisterService(MakeTokenManagerID(), tokenManagerId);
}

struct TVmMetadataHandlerSettings {
    struct TResponse {
        TString Code = "200";
        TString Message = "OK";
    };

    NActors::TActorId HttpProxyId;
    ui32 NodeId;
    TString AccessTokenField = "\"access_token\":\"ydb.service.token\"";
    TString ExpiresInField = "\"expires_in\":86400";
    bool IsNetworkUnreachable = false;
    TResponse Response;
};

void HandleVmMetadataRequest(TTestActorSystem* runtime, const TVmMetadataHandlerSettings& settings) {
    auto outgoingRequestEv = runtime->WaitForEdgeActorEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(settings.HttpProxyId, false);
    if (settings.IsNetworkUnreachable) {
        runtime->Send(new IEventHandle(outgoingRequestEv->Sender, settings.HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Get()->Request, nullptr, "Network is unreachable")), settings.NodeId);
        return;
    }
    const TString contentResponse = "{"
                                        + settings.AccessTokenField + (settings.AccessTokenField.empty() ? "" : ",")
                                        + settings.ExpiresInField + (settings.ExpiresInField.empty() ? "" : ",") +
                                        "\"token_type\":\"Bearer\""
                                    "}";
    NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Get()->Request);
    EatWholeString(incomingResponse, "HTTP/1.1 " + settings.Response.Code + " " + settings.Response.Message + "\r\n"
                                                "Connection: close\r\n"
                                                "Content-Type: application/json; charset=utf-8\r\n"
                                                "Content-Length: " + ToString(contentResponse.length()) + "\r\n\r\n" + contentResponse);
    runtime->Send(new IEventHandle(outgoingRequestEv->Sender, settings.HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Get()->Request, incomingResponse)), settings.NodeId);
}

}

Y_UNIT_TEST_SUITE(VmMetadataTokenProvider) {

Y_UNIT_TEST(CanGetTokenFromVmMetadata) {
    TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
    runtime.Start();
    ui32 nodeId = 1;
    auto &appData = runtime.GetNode(1)->AppData;
    appData->DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());

    runtime.SetLogPriority(NKikimrServices::TOKEN_MANAGER, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::HTTP_PROXY, NLog::PRI_TRACE);

    const TString TOKEN_PROVIDER_NAME = "ydb-service-token-name";
    NKikimrProto::TTokenManager tokenManagerConfig = CreateTokenManagerConfig({
        .VmMetadataProviderInitializer = {
            .VmMetadataInfo = {
                {
                    .Id = TOKEN_PROVIDER_NAME,
                    .Endpoint = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
                }
            },
            .Settings = {}
        }
    });

    NActors::TActorId fakeHttpProxy = runtime.AllocateEdgeActor(nodeId);
    RegisterTokenManager(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .TokenManagerConfig = tokenManagerConfig
    });

    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "\"access_token\":\"ydb.service.token\"",
        .Response = {}
    });

    NActors::TActorId fakeTicketParser = runtime.AllocateEdgeActor(nodeId);
    runtime.Send(new IEventHandle(MakeTokenManagerID(), fakeTicketParser, new TEvTokenManager::TEvSubscribeUpdateToken(TOKEN_PROVIDER_NAME)), nodeId);
    auto updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    auto updateTokenEv = updateTokenEvPtr->Get();
    if (updateTokenEv->Status.Code == TEvTokenManager::TStatus::ECode::NOT_READY) {
        updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser);
        updateTokenEv = updateTokenEvPtr->Get();
    }
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::SUCCESS);
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token");
}

Y_UNIT_TEST(CanRefreshTokenFromVmMetadata) {
    TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
    runtime.Start();
    ui32 nodeId = 1;
    auto &appData = runtime.GetNode(1)->AppData;
    appData->DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());

    runtime.SetLogPriority(NKikimrServices::TOKEN_MANAGER, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::HTTP_PROXY, NLog::PRI_TRACE);

    const TString TOKEN_PROVIDER_NAME = "ydb-service-token-name";
    NKikimrProto::TTokenManager tokenManagerConfig = CreateTokenManagerConfig({
        .VmMetadataProviderInitializer = {
            .VmMetadataInfo = {
                {
                    .Id = TOKEN_PROVIDER_NAME,
                    .Endpoint = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
                }
            },
            .Settings = {
                .SuccessRefreshPeriod = "1s"
            }
        }
    });

    NActors::TActorId fakeHttpProxy = runtime.AllocateEdgeActor(nodeId);
    RegisterTokenManager(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .TokenManagerConfig = tokenManagerConfig
    });

    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "\"access_token\":\"ydb.service.token\"",
        .ExpiresInField = "\"expires_in\":86400",
        .Response = {} // Default
    });

    NActors::TActorId fakeTicketParser = runtime.AllocateEdgeActor(nodeId);
    runtime.Send(new IEventHandle(MakeTokenManagerID(), fakeTicketParser, new TEvTokenManager::TEvSubscribeUpdateToken(TOKEN_PROVIDER_NAME)), nodeId);
    auto updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    auto updateTokenEv = updateTokenEvPtr->Get();
    if (updateTokenEv->Status.Code == TEvTokenManager::TStatus::ECode::NOT_READY) {
        updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
        updateTokenEv = updateTokenEvPtr->Get();
    }
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::SUCCESS);
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token");

    // Waiting for new request to metadata service to refresh token
    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "\"access_token\":\"ydb.service.token-refreshed\"",
        .ExpiresInField = "\"expires_in\":86400",
        .Response = {} // Default
    });

    // Waiting for refreshed token
    updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    updateTokenEv = updateTokenEvPtr->Get();
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::SUCCESS);
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token-refreshed");
}

Y_UNIT_TEST(CanHandleErrorFormVmMetadataservice) {
    TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
    runtime.Start();
    ui32 nodeId = 1;
    auto &appData = runtime.GetNode(1)->AppData;
    appData->DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());

    runtime.SetLogPriority(NKikimrServices::TOKEN_MANAGER, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::HTTP_PROXY, NLog::PRI_TRACE);

    const TString TOKEN_PROVIDER_NAME = "ydb-service-token-name";
    const NKikimrProto::TTokenManager tokenManagerConfig = CreateTokenManagerConfig({
        .VmMetadataProviderInitializer = {
            .VmMetadataInfo = {
                {
                    .Id = TOKEN_PROVIDER_NAME,
                    .Endpoint = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
                }
            },
            .Settings = {
                .SuccessRefreshPeriod = "10s",
                .MaxErrorRefreshPeriod = "5s"
            }
        }
    });

    const NActors::TActorId fakeHttpProxy = runtime.AllocateEdgeActor(nodeId);
    RegisterTokenManager(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .TokenManagerConfig = tokenManagerConfig
    });

    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "\"access_token\":\"ydb.service.token\"",
        .ExpiresInField = "\"expires_in\":86400",
        .IsNetworkUnreachable = true,
        .Response = {} // Default
    });


    NActors::TActorId fakeTicketParser = runtime.AllocateEdgeActor(nodeId);
    runtime.Send(new IEventHandle(MakeTokenManagerID(), fakeTicketParser, new TEvTokenManager::TEvSubscribeUpdateToken(TOKEN_PROVIDER_NAME)), nodeId);
    auto updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    auto updateTokenEv = updateTokenEvPtr->Get();
    if (updateTokenEv->Status.Code == TEvTokenManager::TStatus::ECode::NOT_READY) {
        updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
        updateTokenEv = updateTokenEvPtr->Get();
    }
    // Get Error
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::ERROR);
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Status.Message, "Network is unreachable", updateTokenEv->Status.Message);
    UNIT_ASSERT(updateTokenEv->Token.empty());

    // Try refresh token
    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "\"access_token\":\"ydb.service.token\"",
        .ExpiresInField = "\"expires_in\":86400",
        .IsNetworkUnreachable = false,
        .Response = {} // Default
    });

    // Get successful token
    updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    updateTokenEv = updateTokenEvPtr->Get();
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::SUCCESS);
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token");

    // Try refresh token
    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "\"access_token\":\"ydb.service.token\"",
        .ExpiresInField = "\"expires_in\":86400",
        .IsNetworkUnreachable = false,
        .Response = {
            .Code = "403",
            .Message = "Forbidden"
        }
    });

    // Get Error
    updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    updateTokenEv = updateTokenEvPtr->Get();
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::ERROR);
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Status.Message, "Forbidden", updateTokenEv->Status.Message);
    // Last successful token
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token");

    // Try refresh token
    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = ",",
        .ExpiresInField = "\"expires_in\":86400",
        .IsNetworkUnreachable = false,
        .Response = {}
    });

    // Get Error
    updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    updateTokenEv = updateTokenEvPtr->Get();
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::ERROR);
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Status.Message, "Can not read json", updateTokenEv->Status.Message);
    // Last successful token
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token");

    // Try refresh token
    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "",
        .ExpiresInField = "\"expires_in\":86400",
        .IsNetworkUnreachable = false,
        .Response = {}
    });

    // Get Error
    updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    updateTokenEv = updateTokenEvPtr->Get();
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::ERROR);
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Status.Message, "Result doesn't contain access_token", updateTokenEv->Status.Message);
    // Last successful token
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token");

    // Try refresh token
    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "\"access_token\":\"\"",
        .ExpiresInField = "\"expires_in\":86400",
        .IsNetworkUnreachable = false,
        .Response = {}
    });

    // Get Error about empty token
    updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    updateTokenEv = updateTokenEvPtr->Get();
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::ERROR);
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Status.Message, "Got empty token", updateTokenEv->Status.Message);
    // Last successful token
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token");

    // Try refresh token
    HandleVmMetadataRequest(&runtime, {
        .HttpProxyId = fakeHttpProxy,
        .NodeId = nodeId,
        .AccessTokenField = "\"access_token\":\"ydb.service.token-refreshed\"",
        .ExpiresInField = "\"expires_in\":86400",
        .IsNetworkUnreachable = false,
        .Response = {} // Default
    });

    // Get successful token
    updateTokenEvPtr = runtime.WaitForEdgeActorEvent<TEvTokenManager::TEvUpdateToken>(fakeTicketParser, false);
    updateTokenEv = updateTokenEvPtr->Get();
    UNIT_ASSERT_STRINGS_EQUAL_C(updateTokenEv->Id, TOKEN_PROVIDER_NAME, updateTokenEv->Id);
    UNIT_ASSERT_EQUAL(updateTokenEv->Status.Code, TEvTokenManager::TStatus::ECode::SUCCESS);
    UNIT_ASSERT(!updateTokenEv->Token.empty());
    UNIT_ASSERT_STRINGS_EQUAL(updateTokenEv->Token, "ydb.service.token-refreshed");
}

}

} // NKikimr
