#include "helpers.h"

using namespace NFq::NWasmServices::NTestHelpers;

Y_UNIT_TEST_SUITE(TFqWasmModuleTest) {
    Y_UNIT_TEST(ProfileJsonStackHandlesLongStringsWithinResponseLimit) {
        using namespace NYdb::NWasm::NServices;
        THttpMock http;
        TGrpcMock grpc;
        // Exercise the response-size/allocator boundary independently of the
        // CPU quota: parsing 32 KiB in the instrumented guest exceeds 100 ms.
        TLimits runtimeLimits;
        runtimeLimits.CpuBudget = TDuration::Seconds(5);
        TEnv env(Bindings(http, grpc), {}, "/fq_transport_coroutine.wasm", TestExecutor(), runtimeLimits);
        const TString prefix = "[{\"id\":42,\"name\":\"";
        const TString suffix = "\",\"score\":97,\"version\":1}]";
        TVector<TString> responses;
        for (const size_t length :
            { size_t(257), size_t(4095), size_t(4096), size_t(5000), MaxProfileBatchBytes - prefix.size() - suffix.size() }) {
            responses.push_back(prefix + TString(length, 'x') + suffix);
        }
        responses.push_back("[{\"" + TString(5000, 'k') + "\":0}]");
        TString escaped;
        for (size_t i = 0; i < 1000; ++i) {
            escaped += "\\u0061";
        }
        responses.push_back(prefix + escaped + suffix);
        size_t exchange = 0;
        for (const auto& response : responses) {
            char arguments[64];
            TRowWriter writer(arguments, sizeof(arguments));
            UNIT_ASSERT(writer.Put(TServiceRequest{
                ServiceMagic, ServiceVersion, NGenerated::NModuleWASM_PROFILE::MethodProfile, 2, 0, 1, MaxProfileBatchBytes, 1 }));
            UNIT_ASSERT(writer.Put(ui64(42)));
            const auto call = env.Runtime->Start(TStringBuf(arguments, writer.Size()), TInstant::Now() + TDuration::Seconds(10));
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            http.State->Wait(exchange);
            http.State->Reply(exchange++, response, 200);
            TCallResult result;
            try {
                result = env.AwaitTerminal(call);
            } catch (const std::exception& error) {
                UNIT_FAIL("JSON response " << exchange << " (" << response.size() << " bytes): " << error.what());
            }
            UNIT_ASSERT(result.Status == ECallStatus::Completed);
            UNIT_ASSERT(!env.Runtime->IsPoisoned());
            TRowReader reader(View(result.Data));
            TServiceResult reply;
            UNIT_ASSERT(reader.Get(reply));
            UNIT_ASSERT(
                reply.Error == static_cast<ui32>(EServiceError::InvalidProfile) || reply.Error == static_cast<ui32>(EServiceError::Decode));
            env.Clean(call);
        }
        const auto good = env.StartProfile(EProfileMode::Http);
        UNIT_ASSERT(env.Runtime->Poll(good).Status == ECallStatus::Waiting);
        http.State->Wait(exchange);
        http.State->Reply(exchange, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        const auto result = env.AwaitTerminal(good);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT(ProfileResult(result).Items[0].Error == EServiceError::None);
        UNIT_ASSERT(!env.Runtime->IsPoisoned());
        env.Clean(good);
    }

    Y_UNIT_TEST(EchoModuleLinksAndRejectsUnknownMethod) {
        TEnv env({}, {}, "/echo_service.wasm");
        const NYdb::NWasm::NServices::TServiceRequest request{ NYdb::NWasm::NServices::ServiceMagic, NYdb::NWasm::NServices::ServiceVersion, 999,
            0, 0, 1, 32768, 0 };
        const auto call =
            env.Runtime->Start(TStringBuf(reinterpret_cast<const char*>(&request), sizeof(request)), TInstant::Now() + TDuration::Seconds(30));
        const auto result = env.AwaitTerminal(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        NYdb::NWasm::NServices::TRowReader reader(View(result.Data));
        NYdb::NWasm::NServices::TServiceResult reply;
        UNIT_ASSERT(reader.Get(reply));
        UNIT_ASSERT_VALUES_EQUAL(reply.Error, 1);
        UNIT_ASSERT(reader.Remaining().empty());
        env.Clean(call);
    }

    Y_UNIT_TEST(EchoModuleHttpBatchDecodesDifferentSchemas) {
        using namespace NYdb::NWasm::NServices;
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc), {}, "/echo_service.wasm");
        const std::string_view messages[] = { std::string_view("a\0b", 3), std::string_view() };
        size_t exchange = 0;
        using namespace NYdb::NWasm::NServices::NGenerated::NModuleEcho;
        for (const ui32 method : { MethodEcho, MethodLength }) {
            char arguments[256];
            TRowWriter writer(arguments, sizeof(arguments));
            UNIT_ASSERT(writer.Put(TServiceRequest{ ServiceMagic, ServiceVersion, method, 0, 0, 2, 32768, 1 }));
            for (const auto message : messages) {
                UNIT_ASSERT(writer.String(message));
            }
            const auto call = env.Runtime->Start(TStringBuf(arguments, writer.Size()), TInstant::Now() + TDuration::Seconds(30));
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            const auto request = http.State->Wait(exchange);
            http.State->Reply(exchange++, request->Payload, 200);
            const auto result = env.AwaitTerminal(call);
            UNIT_ASSERT(result.Status == ECallStatus::Completed);
            TRowReader reader(View(result.Data));
            TServiceResult reply;
            UNIT_ASSERT(reader.Get(reply));
            UNIT_ASSERT_VALUES_EQUAL(reply.Version, ServiceVersion);
            UNIT_ASSERT_VALUES_EQUAL(reply.Count, 2);
            UNIT_ASSERT_VALUES_EQUAL(reply.Error, 0);
            for (const auto expected : messages) {
                if (method == MethodEcho) {
                    std::string_view value;
                    ui64 length;
                    ui8 empty;
                    i64 delta;
                    UNIT_ASSERT(reader.String(value, 1024) && reader.Get(length) && reader.Get(empty) && reader.Get(delta));
                    UNIT_ASSERT(value == expected);
                    UNIT_ASSERT_VALUES_EQUAL(length, expected.size());
                    UNIT_ASSERT_VALUES_EQUAL(empty, expected.empty());
                    UNIT_ASSERT_VALUES_EQUAL(delta, -static_cast<i64>(expected.size()));
                } else {
                    ui32 length;
                    UNIT_ASSERT(reader.Get(length));
                    UNIT_ASSERT_VALUES_EQUAL(length, expected.size());
                }
            }
            UNIT_ASSERT(reader.Remaining().empty());
            env.Clean(call);
        }
    }

    Y_UNIT_TEST(TypedGrpcTlsTrustAndUntrustedCa) {
        using namespace NKikimr::NCertTestUtils;
        const auto ca = GenerateCA(TProps::AsCA().WithValid(TDuration::Days(1)));
        const auto serverCert = GenerateSignedCert(ca, TProps::AsServer().WithValid(TDuration::Days(1)));
        grpc::SslServerCredentialsOptions serverOptions;
        grpc::SslServerCredentialsOptions::PemKeyCertPair keyCertPair;
        keyCertPair.private_key = serverCert.PrivateKey;
        keyCertPair.cert_chain = serverCert.Certificate;
        serverOptions.pem_key_cert_pairs.push_back(std::move(keyCertPair));
        TGrpcMock grpc(grpc::SslServerCredentials(serverOptions));
        THttpMock http;

        grpc::SslCredentialsOptions trustedOptions;
        trustedOptions.pem_root_certs = ca.Certificate;
        TEnv env(Bindings(http, grpc, grpc::SslCredentials(trustedOptions)));
        const auto call = env.StartProfile(EProfileMode::Grpc, 42, 3, 3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, GrpcProfilePayload(), 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        auto decoded = ProfileResult(result);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::None);
        env.Clean(call);

        TGrpcMock untrustedGrpc(grpc::SslServerCredentials(serverOptions));
        THttpMock untrustedHttp;
        TEnv untrustedEnv(Bindings(untrustedHttp, untrustedGrpc, grpc::SslCredentials(grpc::SslCredentialsOptions{})));
        const auto failedCall = untrustedEnv.StartProfile(EProfileMode::Grpc, 42, 3, 3);
        const auto failed = untrustedEnv.AwaitTerminal(failedCall);
        UNIT_ASSERT(failed.Status == ECallStatus::Completed);
        decoded = ProfileResult(failed);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::Transport);
        UNIT_ASSERT(decoded.Items[0].ClientError == EClientError::Connection);
        UNIT_ASSERT_VALUES_EQUAL(untrustedGrpc.State->Count(), 0);
        untrustedEnv.Clean(failedCall);
    }

    Y_UNIT_TEST(TypedHttpProfileDecoding) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.StartProfile(EProfileMode::Http);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        auto request = http.State->Wait();
        UNIT_ASSERT(request->Method.StartsWith("POST /profile "));
        UNIT_ASSERT_VALUES_EQUAL(request->Payload, "{\"id\":42}");
        UNIT_ASSERT(HasHeader(*request, "Content-Type", "application/json"));
        http.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        const auto decoded = ProfileResult(result);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Count, 1);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::None);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Items[0].Profile.Id, 42);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Items[0].Profile.Score, 97);
        UNIT_ASSERT_VALUES_EQUAL(TString(decoded.Items[0].Profile.Name, decoded.Items[0].Profile.NameBytes), "Ada");
        env.Clean(call);
    }

    Y_UNIT_TEST(TypedGrpcProfileDecodingAndMalformedPayload) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.StartProfile(EProfileMode::Grpc, 42, 3, 3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        auto request = grpc.State->Wait();
        NFq::NWasmServices::NTest::ProfileRequest parsedRequest;
        UNIT_ASSERT(parsedRequest.ParseFromString(request->Payload));
        UNIT_ASSERT_VALUES_EQUAL(parsedRequest.id(), 42);
        grpc.State->Reply(0, GrpcProfilePayload(), 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        auto decoded = ProfileResult(result);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::None);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Items[0].Profile.Id, 42);
        env.Clean(call);

        THttpMock badHttp;
        TGrpcMock badGrpc;
        TEnv badEnv(Bindings(badHttp, badGrpc));
        const auto badCall = badEnv.StartProfile(EProfileMode::Http);
        UNIT_ASSERT(badEnv.Runtime->Poll(badCall).Status == ECallStatus::Waiting);
        badHttp.State->Wait();
        badHttp.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":101,\"version\":1}", 200);
        const auto badResult = badEnv.Resume(badCall);
        UNIT_ASSERT(badResult.Status == ECallStatus::Completed);
        decoded = ProfileResult(badResult);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::InvalidProfile);
        UNIT_ASSERT(!badEnv.Runtime->IsPoisoned());
        badEnv.Clean(badCall);

        THttpMock malformedHttp;
        TGrpcMock malformedGrpc;
        TEnv malformedEnv(Bindings(malformedHttp, malformedGrpc));
        const auto malformedCall = malformedEnv.StartProfile(EProfileMode::Grpc, 42, 3, 3);
        UNIT_ASSERT(malformedEnv.Runtime->Poll(malformedCall).Status == ECallStatus::Waiting);
        malformedGrpc.State->Wait();
        malformedGrpc.State->Reply(0, "not-protobuf", 0);
        const auto malformedResult = malformedEnv.Resume(malformedCall);
        UNIT_ASSERT(malformedResult.Status == ECallStatus::Completed);
        decoded = ProfileResult(malformedResult);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::Decode);
        malformedEnv.Clean(malformedCall);
    }

    Y_UNIT_TEST(TypedProfileSequentialAndParallelModes) {
        THttpMock sequentialHttp;
        TGrpcMock sequentialGrpc;
        TEnv sequentialEnv(Bindings(sequentialHttp, sequentialGrpc));
        const auto sequentialCall = sequentialEnv.StartProfile(EProfileMode::HttpThenGrpc);
        UNIT_ASSERT(sequentialEnv.Runtime->Poll(sequentialCall).Status == ECallStatus::Waiting);
        sequentialHttp.State->Wait();
        sequentialHttp.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        UNIT_ASSERT(sequentialEnv.Resume(sequentialCall).Status == ECallStatus::Waiting);
        auto grpcRequest = sequentialGrpc.State->Wait();
        NFq::NWasmServices::NTest::ProfileRequest request;
        UNIT_ASSERT(request.ParseFromString(grpcRequest->Payload));
        UNIT_ASSERT_VALUES_EQUAL(request.id(), 42);
        sequentialGrpc.State->Reply(0, GrpcProfilePayload(), 0);
        auto sequentialResult = sequentialEnv.Resume(sequentialCall);
        UNIT_ASSERT(sequentialResult.Status == ECallStatus::Completed);
        auto sequentialProfiles = ProfileResult(sequentialResult);
        UNIT_ASSERT_VALUES_EQUAL(sequentialProfiles.Count, 2);
        UNIT_ASSERT(sequentialProfiles.Items[0].Error == EServiceError::None);
        UNIT_ASSERT(sequentialProfiles.Items[1].Error == EServiceError::None);
        sequentialEnv.Clean(sequentialCall);

        THttpMock parallelHttp;
        TGrpcMock parallelGrpc;
        TEnv parallelEnv(Bindings(parallelHttp, parallelGrpc));
        const auto parallelCall = parallelEnv.StartProfile(EProfileMode::Parallel);
        UNIT_ASSERT(parallelEnv.Runtime->Poll(parallelCall).Status == ECallStatus::Waiting);
        parallelHttp.State->Wait();
        parallelGrpc.State->Wait();
        parallelGrpc.State->Reply(0, GrpcProfilePayload(), 0);
        parallelHttp.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        auto parallelResult = parallelEnv.AwaitTerminal(parallelCall);
        UNIT_ASSERT(parallelResult.Status == ECallStatus::Completed);
        auto parallelProfiles = ProfileResult(parallelResult);
        UNIT_ASSERT_VALUES_EQUAL(parallelProfiles.Count, 2);
        UNIT_ASSERT(parallelProfiles.Items[0].Error == EServiceError::None);
        UNIT_ASSERT(parallelProfiles.Items[1].Error == EServiceError::None);
        parallelEnv.Clean(parallelCall);
    }
}
