#include "server.h"

#include "server_test.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/client/client.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/critical_events.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/critical_events_init.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/volume_stats_test.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service_test.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

using NYdb::NBS::E_ARGUMENT;
using NYdb::NBS::E_GRPC_CANCELLED;
using NYdb::NBS::E_GRPC_UNIMPLEMENTED;
using NYdb::NBS::E_REJECTED;
using NYdb::NBS::EDiagnosticsErrorKind;
using NYdb::NBS::EErrorKind;
using NYdb::NBS::GetDiagnosticsErrorKind;
using NYdb::NBS::GetErrorKind;
using NYdb::NBS::HasError;
using NYdb::NBS::S_OK;
using NYdb::NBS::TGuardedSgList;
using NYdb::NBS::TSgList;

namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckDescribe(auto endpoint, TString cellId, ui32 errorCode)
{
    auto request = std::make_shared<NProto::TDescribeVolumeRequest>();
    if (cellId) {
        request->MutableHeaders()->SetCellId(cellId);
    }

    auto future = endpoint->DescribeVolume(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    const auto& response = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(errorCode, response.GetError().GetCode());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerTest)
{
    Y_UNIT_TEST(ShouldPrepareRequestHeaders)
    {
        NProto::THeaders headers;

        NImpl::PrepareRequestHeaders(
            NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL,
            "ipv6:%5Bfe80::1%2542%5D:12345",
            "test-auth-token",
            headers);

        const auto& internal = headers.GetInternal();
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL),
            static_cast<ui32>(internal.GetRequestSource()));
        UNIT_ASSERT_VALUES_EQUAL(
            "ipv6:[fe80::1%42]:12345",
            internal.GetPeer());
        UNIT_ASSERT_VALUES_EQUAL("test-auth-token", internal.GetAuthToken());
    }

    Y_UNIT_TEST(ShouldHandleRequests)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TPingResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldHandleAuthRequests)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);
        ui16 secureEndpointPort = portManager.GetPort(9003);

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    "test",
                    request->GetHeaders().GetInternal().GetAuthToken()
                );
                return MakeFuture<NProto::TPingResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetSecureEndpoint(
                secureEndpointPort,
                "certs/server.crt",
                "certs/server.crt",
                "certs/server.key")
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetSecureEndpoint(
                secureEndpointPort,
                "certs/server.crt",
                "test")
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    // NBS-3132
    // Y_UNIT_TEST(ShouldHandleAuthRequestsWithCustomClientCertificate)
    // {
    //     TPortManager portManager;
    //     ui16 port = portManager.GetPort(9001);

    //     auto service = std::make_shared<TTestService>();
    //     service->PingHandler =
    //         [&] (std::shared_ptr<NProto::TPingRequest> request) {
    //             UNIT_ASSERT_VALUES_EQUAL(
    //                 "test",
    //                 request->GetHeaders().GetInternal().GetAuthToken()
    //             );
    //             return MakeFuture<NProto::TPingResponse>();
    //         };

    //     TTestFactory testFactory;

    //     auto server = testFactory.CreateServerBuilder()
    //         .SetSecureEndpoint(
    //             port,
    //             "certs/server.crt",
    //             "certs/server.crt",
    //             "certs/server.key")
    //         .BuildServer(service);

    //     auto client = testFactory.CreateClientBuilder()
    //         .SetSecureEndpoint(
    //             port,
    //             "certs/server.crt",
    //             "test")
    //         .SetCertificate(
    //             "certs/server.crt",
    //             "certs/server.key")
    //         .BuildClient();

    //     server->Start();
    //     client->Start();
    //     Y_DEFER {
    //         client->Stop();
    //         server->Stop();
    //     };

    //     auto endpoint = client->CreateEndpoint();
    //     endpoint = testFactory.CreateDurableClient(std::move(endpoint));

    //     endpoint->Start();
    //     Y_DEFER {
    //         endpoint->Stop();
    //     };

    //     auto future = endpoint->Ping(
    //         MakeIntrusive<TCallContext>(),
    //         std::make_shared<NProto::TPingRequest>()
    //     );

    //     const auto& response = future.GetValue(TDuration::Seconds(5));
    //     UNIT_ASSERT_C(!HasError(response), response.GetError());
    // }

    Y_UNIT_TEST(ShouldHandleAuthRequestsWithMultipleCertificates)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);
        ui16 secureEndpointPort = portManager.GetPort(9003);

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    "test",
                    request->GetHeaders().GetInternal().GetAuthToken()
                );
                return MakeFuture<NProto::TPingResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetSecureEndpoint(
                secureEndpointPort,
                "certs/server.crt",
                {},
                {})
            .AddCert(
                "certs/server_fallback.crt",
                "certs/server.key")
            .AddCert(
                "certs/server.crt",
                "certs/server.key")
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetSecureEndpoint(
                secureEndpointPort,
                "certs/server.crt",
                "test")
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldHandleSecureAndInsecureClientsSimultaneously)
    {
        TPortManager portManager;
        ui16 securePort = portManager.GetPort(9001);
        ui16 insecurePort = portManager.GetPort(9002);
        ui16 dataPort = portManager.GetPort(9003);

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TPingResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(insecurePort)
            .SetSecureEndpoint(
                securePort,
                "certs/server.crt",
                "certs/server.crt",
                "certs/server.key")
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto secureClient = testFactory.CreateClientBuilder()
            .SetSecureEndpoint(
                securePort,
                "certs/server.crt",
                "test")
            .SetDataPort(dataPort)
            .BuildClient();

        auto insecureClient = testFactory.CreateClientBuilder()
            .SetPort(insecurePort)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        secureClient->Start();
        insecureClient->Start();
        Y_DEFER {
            insecureClient->Stop();
            secureClient->Stop();
            server->Stop();
        };

        auto secureEndpoint = secureClient->CreateEndpoint();
        auto insecureEndpoint = insecureClient->CreateEndpoint();

        secureEndpoint->Start();
        insecureEndpoint->Start();
        Y_DEFER {
            insecureEndpoint->Stop();
            secureEndpoint->Stop();
        };

        auto futureFromSecure = secureEndpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        auto futureFromInsecure = insecureEndpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        const auto& responseFromSecure =
            futureFromSecure.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(responseFromSecure), responseFromSecure.GetError());

        const auto& responseFromInsecure =
            futureFromInsecure.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(responseFromInsecure), responseFromInsecure.GetError());
    }

    Y_UNIT_TEST(ShouldFailRequestWithNonEmptyInternalHeaders)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request->GetHeaders().GetInternal().GetAuthToken()
                );
                return MakeFuture<NProto::TPingResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto request = std::make_shared<NProto::TPingRequest>();
        request->MutableHeaders()->MutableInternal()->SetAuthToken("test");
        auto future = endpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::move(request)
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldPropagateClientId)
    {
        TString clientId = "testClientId";
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                UNIT_ASSERT(request->HasHeaders());
                UNIT_ASSERT_VALUES_EQUAL(clientId, request->GetHeaders().GetClientId());
                return MakeFuture<NProto::TPingResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetClientId(clientId)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldFailRequestsToWrongServiceFromControlClient)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TWriteBlocksResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        // mismatched Client/DataClient
        auto client = testFactory.CreateClientBuilder()
            .SetPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TWriteBlocksRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            E_GRPC_UNIMPLEMENTED,
            response.GetError().GetCode()
        );
    }

    Y_UNIT_TEST(ShouldFailRequestsToWrongServiceFromDataClient)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TWriteBlocksResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        // mismatched Client/DataClient
        auto client = testFactory.CreateClientBuilder()
            .SetDataPort(port)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateDataEndpoint();
        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TWriteBlocksRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            E_GRPC_UNIMPLEMENTED,
            response.GetError().GetCode()
        );
    }

    Y_UNIT_TEST(ShouldCancelRequestsOnServerShutdown)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto writePromise = NewPromise<NProto::TWriteBlocksResponse>();
        auto writeRequestReceivedPromise = NewPromise<void>();

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TPingResponse>();
            };
        service->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                Y_UNUSED(request);
                writeRequestReceivedPromise.SetValue();
                return writePromise;   // will hang until value is set
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TWriteBlocksRequest>()
        );

        // wait until server receive write request
        writeRequestReceivedPromise.GetFuture().GetValue(TDuration::Seconds(5));

        server->Stop();

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL_C(
            EDiagnosticsErrorKind::ErrorRetriable,
            GetDiagnosticsErrorKind(response.GetError()),
            response.GetError());
        UNIT_ASSERT_VALUES_EQUAL_C(
            EErrorKind::ErrorRetriable,
            GetErrorKind(response.GetError()),
            response.GetError()
        );

        writePromise.SetValue(NProto::TWriteBlocksResponse());
    }

    Y_UNIT_TEST(ShouldCancelRequestsOnClientShutdown)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto writePromise = NewPromise<NProto::TWriteBlocksResponse>();

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TPingResponse>();
            };
        service->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                Y_UNUSED(request);
                return writePromise;   // will hang until value is set
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TWriteBlocksRequest>()
        );

        // control request to ensure client and server completely started
        {
            auto future = endpoint->Ping(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TPingRequest>()
            );
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        endpoint->Stop();
        client->Stop();

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            E_GRPC_CANCELLED,
            response.GetError().GetCode()
        );

        writePromise.SetValue(NProto::TWriteBlocksResponse());
    }

    Y_UNIT_TEST(ShouldIdentifyInsecureControlChannelSource)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    int(NProto::SOURCE_INSECURE_CONTROL_CHANNEL),
                    int(request->GetHeaders().GetInternal().GetRequestSource())
                );
                return MakeFuture<NProto::TReadBlocksResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldIdentifySecureControlChannelSource)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);
        ui16 securePort = portManager.GetPort(9003);

        auto service = std::make_shared<TTestService>();
        service->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    int(NProto::SOURCE_SECURE_CONTROL_CHANNEL),
                    int(request->GetHeaders().GetInternal().GetRequestSource())
                );
                return MakeFuture<NProto::TReadBlocksResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetSecureEndpoint(
                securePort,
                "certs/server.crt",
                "certs/server.crt",
                "certs/server.key")
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetSecureEndpoint(
                securePort,
                "certs/server.crt",
                "test")
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldIdentifyTcpDataChannelSource)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->UploadClientMetricsHandler =
            [&] (std::shared_ptr<NProto::TUploadClientMetricsRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    int(NProto::SOURCE_TCP_DATA_CHANNEL),
                    int(request->GetHeaders().GetInternal().GetRequestSource())
                );
                return MakeFuture<NProto::TUploadClientMetricsResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateDataEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->UploadClientMetrics(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TUploadClientMetricsRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldIdentifyFdControlChannelSource)
    {
        TFsPath unixSocket(CreateGuidAsString() + ".sock");
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    int(NProto::SOURCE_FD_CONTROL_CHANNEL),
                    int(request->GetHeaders().GetInternal().GetRequestSource())
                );
                return MakeFuture<NProto::TReadBlocksResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetUnixSocketPath(unixSocket.GetPath())
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(nullptr, service);

        auto client = testFactory.CreateClientBuilder()
            .SetUnixSocketPath(unixSocket.GetPath())
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldHandleRequestsWithUnixSocket)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);
        TFsPath unixSocket(CreateGuidAsString() + ".sock");

        auto service = std::make_shared<TTestService>();
        service->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    int(NProto::SOURCE_FD_CONTROL_CHANNEL),
                    int(request->GetHeaders().GetInternal().GetRequestSource())
                );
                return MakeFuture<NProto::TReadBlocksResponse>();
            };
        service->ListEndpointsHandler =
            [&] (std::shared_ptr<NProto::TListEndpointsRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    int(NProto::SOURCE_FD_CONTROL_CHANNEL),
                    int(request->GetHeaders().GetInternal().GetRequestSource())
                );
                UNIT_ASSERT(!request->GetHeaders().GetInternal().GetPeer().empty());
                return MakeFuture<NProto::TListEndpointsResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetUnixSocketPath(unixSocket.GetPath())
            .BuildServer(nullptr, service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetUnixSocketPath(unixSocket.GetPath())
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        {
            auto future = endpoint->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TReadBlocksRequest>()
            );

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        {
            auto future = endpoint->ListEndpoints(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListEndpointsRequest>()
            );

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }
    }

    Y_UNIT_TEST(ShouldThrowCriticalEventIfFailedToStartUnixSocketEndpoint)
    {
        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto errorCounter =
            counters->GetCounter("AppCriticalEvents/EndpointStartingError", true);

        TFsPath unixSocket("./invalid/path/test_socket");

        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TReadBlocksResponse>();
            };

        TTestFactory testFactory;
        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetUnixSocketPath(unixSocket.GetPath())
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*errorCounter));
        server->Start();
        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<int>(*errorCounter));

        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldCleanVolumeStatsWhileUnmountVolume)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);
        TString diskId = "testDiskId";

        auto service = std::make_shared<TTestService>();
        service->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetDiskId(request->GetDiskId());
                return MakeFuture(response);
            };
        service->UnmountVolumeHandler =
            [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto serverVolumeStats = std::make_shared<TTestVolumeStats<>>();
        auto clientVolumeStats = std::make_shared<TTestVolumeStats<>>();

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetVolumeStats(serverVolumeStats)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetVolumeStats(clientVolumeStats)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        {
            auto request = std::make_shared<NProto::TMountVolumeRequest>();
            request->SetDiskId(diskId);

            auto future = endpoint->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TSet<TString>{diskId},
            serverVolumeStats->DiskIds
        );
        UNIT_ASSERT_VALUES_EQUAL(
            TSet<TString>{diskId},
            clientVolumeStats->DiskIds
        );

        {
            auto request = std::make_shared<NProto::TUnmountVolumeRequest>();
            request->SetDiskId(diskId);

            auto future = endpoint->UnmountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TSet<TString>(),
            serverVolumeStats->DiskIds
        );
        UNIT_ASSERT_VALUES_EQUAL(
            TSet<TString>(),
            clientVolumeStats->DiskIds
        );
    }

    Y_UNIT_TEST(ShouldHandleRequestsWithUnixSocketAfterRestart)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);
        TFsPath unixSocket(CreateGuidAsString() + ".sock");

        auto service = std::make_shared<TTestService>();
        service->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TReadBlocksResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetUnixSocketPath(unixSocket.GetPath())
            .BuildServer(nullptr, service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetUnixSocketPath(unixSocket.GetPath())
            .BuildClient();

        server->Start();

        client->Start();

        auto endpoint = client->CreateEndpoint();
        endpoint->Start();

        {
            server->Stop();
            server = testFactory.CreateServerBuilder()
                .SetPort(port)
                .SetDataPort(dataPort)
                .SetUnixSocketPath(unixSocket.GetPath())
                .BuildServer(nullptr, service);
            server->Start();
        }

        bool success = false;
        for (int i = 0; i < 3; ++i) {
            auto future = endpoint->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TReadBlocksRequest>()
            );

            const auto& response = future.GetValue(TDuration::Seconds(5));
            if (!HasError(response)) {
                success = true;
                break;
            }
        }

        UNIT_ASSERT(success);

        endpoint->Stop();
        client->Stop();
        server->Stop();
    }

    Y_UNIT_TEST(ShouldHandleLocalIORequestsWithOverheadBuffer)
    {
        ui32 blocksCount = 42;
        ui32 blockSize = 512;
        ui32 overheadSize = 1234;
        char content = 'x';

        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(blocksCount, request->GetBlocksCount());

                NProto::TReadBlocksResponse response;
                auto& buffers = *response.MutableBlocks()->MutableBuffers();
                for (size_t i = 0; i < blocksCount; ++i) {
                    auto* buf = buffers.Add();
                    buf->ReserveAndResize(blockSize);
                    memset(
                        const_cast<char*>(buf->data()),
                        content,
                        buf->size());
                }

                return MakeFuture(std::move(response));
            };
        service->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    blocksCount,
                    request->GetBlocks().GetBuffers().size());

                for (const auto& block: request->GetBlocks().GetBuffers()) {
                    UNIT_ASSERT_VALUES_EQUAL(block.size(), blockSize);
                    UNIT_ASSERT_VALUES_EQUAL(
                        TString(blockSize, content),
                        block.data());
                }

                return MakeFuture<NProto::TWriteBlocksResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        {
            TString buffer(blocksCount * blockSize + overheadSize, 0);
            auto sglist = TSgList{{buffer.data(), buffer.size()}};

            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetBlocksCount(blocksCount);
            request->SetBlockSize(blockSize);
            request->Sglist = TGuardedSgList(sglist);

            auto future = endpoint->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));

            for (size_t i = 0; i < buffer.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    i < blocksCount * blockSize ? content : 0,
                    buffer[i]);
            }
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        {
            TString buffer(blocksCount * blockSize + overheadSize, 0);
            memset(const_cast<char*>(buffer.data()), content, blocksCount * blockSize);

            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetBlockSize(blockSize);
            request->BlocksCount = blocksCount;
            request->Sglist = TGuardedSgList({{buffer.data(), buffer.size()}});

            auto future = endpoint->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }
    }

    Y_UNIT_TEST(ShouldNotHandleIORequestsFromTcpDataChannelSource)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TReadBlocksResponse>();
            };
        service->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TWriteBlocksResponse>();
            };
        service->ZeroBlocksHandler =
            [&] (std::shared_ptr<NProto::TZeroBlocksRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TZeroBlocksResponse>();
            };
        service->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TMountVolumeResponse>();
            };
        service->UnmountVolumeHandler =
            [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateDataEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto checkError = []<typename T>(T future) {
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            auto error = response.GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        };

        checkError(endpoint->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksRequest>()));
        checkError(endpoint->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TWriteBlocksRequest>()));
        checkError(endpoint->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TZeroBlocksRequest>()));
        checkError(endpoint->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>()));
        checkError(endpoint->UnmountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TUnmountVolumeRequest>()));
    }

    Y_UNIT_TEST(ShouldCheckCellIdInDescribeVolumeRequestIfCellsAreOn)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->DescribeVolumeHandler =
            [&] (std::shared_ptr<NProto::TDescribeVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TDescribeVolumeResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .SetCellId("abc")  // turn on cell id check in DescribeVolume
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        CheckDescribe(endpoint, "xyz", E_REJECTED);
        CheckDescribe(endpoint, "", S_OK);
    }

    Y_UNIT_TEST(ShouldCheckCellIdInDescribeVolumeRequestIfCellsAreOff)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->DescribeVolumeHandler =
            [&] (std::shared_ptr<NProto::TDescribeVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TDescribeVolumeResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        CheckDescribe(endpoint, "xyz", E_REJECTED);
        CheckDescribe(endpoint, "", S_OK);
    }
}

}   // namespace NCloud::NBlockStore::NServer
