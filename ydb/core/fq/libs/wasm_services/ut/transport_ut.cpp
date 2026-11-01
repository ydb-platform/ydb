#include "helpers.h"

#include <ydb/core/fq/libs/wasm_services/testing.h>

using namespace NFq::NWasmServices::NTestHelpers;

Y_UNIT_TEST_SUITE(TFqWasmTransportTest) {
    Y_UNIT_TEST(CompletionBeforeWaitCanNotifyOnOwnerThread) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        const auto operation = env.Runtime->StartOperation(call, MakeRequest(0, "body"));
        http.State->Wait();
        http.State->Reply(0, "reply", 200);
        Eventually([&] {
            return env.Runtime->PollOperation(call, operation) == EOperationStatus::Ready;
        });
        {
            // Model a completion between the guest's poll and wait imports.
            // Wait must queue readiness without clearing the owner's TLS.
            TCurrentQueryCompartmentGuard queryGuard(env.Query);
            TCurrentCompartmentGuard compartmentGuard(env.Query->Compartment.get());
            env.Runtime->Wait(call, operation);
            UNIT_ASSERT(GetCurrentQueryCompartment() == env.Query);
            UNIT_ASSERT(GetCurrentCompartment() == env.Query->Compartment.get());
        }
        UNIT_ASSERT(!GetCurrentAsyncInvocation() && !GetCurrentQueryCompartment() && !GetCurrentCompartment());
        UNIT_ASSERT(env.Wakeup->CleanTls);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{ call });
        env.Clean(call);
    }

    Y_UNIT_TEST(HttpHeadCompletesWithoutBodyOrConnectionClose) {
        for (const bool keepAlive : { false, true }) {
            THttpMock http(keepAlive);
            TGrpcMock grpc;
            TTransport transport(Bindings(http, grpc), TestExecutor());
            auto done = std::make_shared<std::promise<std::pair<EOperationStatus, TString>>>();
            auto future = done->get_future();
            transport.Start(1, EOperationKind::Request, MakeRequest(0, {}, "HEAD", "/call"), TInstant::Now() + TDuration::Seconds(10),
                [done](EOperationStatus status, TString bytes) {
                    done->set_value({ status, std::move(bytes) });
                    return true;
                });
            UNIT_ASSERT(http.State->Wait()->Method.StartsWith("HEAD /call "));
            // Advertise the GET representation length, but send no body for HEAD.
            http.State->Reply(0, "hello", 200);
            UNIT_ASSERT(future.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
            const auto result = future.get();
            UNIT_ASSERT(result.first == EOperationStatus::Ready);
            TResponseHeader header;
            TStringBuf payload;
            UNIT_ASSERT(ParseResponse(result.second, header, payload));
            UNIT_ASSERT_VALUES_EQUAL(header.Code, 200);
            UNIT_ASSERT(header.Error == EClientError::None);
            UNIT_ASSERT(payload.empty());
        }
    }

    Y_UNIT_TEST(HttpPostThroughWasmAndNativeHeaders) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        auto request = http.State->Wait();
        UNIT_ASSERT(request->Method.StartsWith("POST /call "));
        UNIT_ASSERT_VALUES_EQUAL(Value(request->Payload), "input");
        UNIT_ASSERT(HasHeader(*request, "Authorization", "Bearer host-secret"));
        http.State->Reply(0, Proto("answer"), 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 200)), "answer");
        env.Clean(call);
    }

    Y_UNIT_TEST(GenericGrpcThroughWasmAndNativeCredentials) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        auto request = grpc.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(Value(request->Payload), "input");
        UNIT_ASSERT(HasHeader(*request, "authorization", "Bearer host-secret"));
        grpc.State->Reply(0, "answer", 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 0)), "answer");
        env.Clean(call);
    }

    Y_UNIT_TEST(SequentialHttpThenGrpcUsesFirstResponse) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(1, 0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        http.State->Reply(0, Proto("intermediate"), 200);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT_VALUES_EQUAL(Value(grpc.State->Wait()->Payload), "intermediate");
        grpc.State->Reply(0, "final", 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 200)), "intermediate");
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 1, 0)), "final");
        env.Clean(call);
    }

    Y_UNIT_TEST(SequentialGrpcThenHttpUsesFirstResponse) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(1, 1, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, "intermediate", 0);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT_VALUES_EQUAL(Value(http.State->Wait()->Payload), "intermediate");
        http.State->Reply(0, Proto("final"), 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 0)), "intermediate");
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 1, 200)), "final");
        env.Clean(call);
    }

    Y_UNIT_TEST(ParallelHttpAndGrpcCompleteOutOfOrder) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(2, 0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        grpc.State->Wait();
        grpc.State->Reply(0, "second", 0);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Waiting);
        http.State->Reply(0, Proto("first"), 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 200)), "first");
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 1, 0)), "second");
        env.Clean(call);
    }

    Y_UNIT_TEST(PendingHttpDoesNotBlockAnotherCall) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto slow = env.Start(0, 0);
        const auto fast = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(slow).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Runtime->Poll(fast).Status == ECallStatus::Waiting);
        http.State->Wait();
        grpc.State->Wait();
        grpc.State->Reply(0, "fast", 0);
        UNIT_ASSERT(env.Resume(fast).Status == ECallStatus::Completed);
        UNIT_ASSERT(env.Runtime->Poll(slow).Status == ECallStatus::Waiting);
        env.Runtime->Drop(fast);
        env.Runtime->Cancel(slow);
        env.Clean(slow);
    }

    Y_UNIT_TEST(HttpErrorsDoNotRetryOrPoisonGuest) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        size_t index = 0;
        for (const auto code : { 429, 503 }) {
            const auto call = env.Start(0, 0);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            http.State->Wait(index);
            http.State->Reply(index++, "controlled error", code);
            auto result = env.Resume(call);
            UNIT_ASSERT(result.Status == ECallStatus::Failed);
            UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::HttpStatus);
            UNIT_ASSERT_VALUES_EQUAL(ReplyBody(result, 0, code), "controlled error");
            env.Clean(call);
            UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), index);
        }
    }

    Y_UNIT_TEST(GrpcUnavailableDoesNotRetry) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, {}, grpc::StatusCode::UNAVAILABLE);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::Connection);
        ReplyBody(result, 0, grpc::StatusCode::UNAVAILABLE);
        env.Clean(call);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 1);
    }

    Y_UNIT_TEST(HttpDisconnectIsAnOperationError) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, {}, 0, true);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
    }

    Y_UNIT_TEST(GrpcServerStopsBeforeReply) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.Stop();
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
    }

    Y_UNIT_TEST(MalformedApplicationPayloadIsNotReinterpretedByTransport) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, "not protobuf", 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        auto bytes = ReplyBody(result, 0, 200);
        google::protobuf::BytesValue message;
        UNIT_ASSERT(!message.ParseFromArray(bytes.data(), bytes.size()));
        env.Clean(call);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
    }

    Y_UNIT_TEST(GrpcBytesResultCanCarryMalformedApplicationPayload) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, "not protobuf", 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        const auto payload = Value(ReplyBody(result, 0, 0));
        UNIT_ASSERT_VALUES_EQUAL(payload, "not protobuf");
        google::protobuf::BytesValue application;
        UNIT_ASSERT(!application.ParseFromArray(payload.data(), payload.size()));
        env.Clean(call);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 1);
    }

    Y_UNIT_TEST(CancelHttpReleasesPhysicalBuffersBeforeLateServerReply) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(1, 0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        UNIT_ASSERT(env.Transport->Stats().ReservedBytes > 0);
        env.Runtime->Cancel(call);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Cancelled);
        env.Clean(call);
        http.State->Reply(0, Proto("late"), 200);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
    }

    Y_UNIT_TEST(CancelGrpcPropagatesToServer) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        env.Runtime->Cancel(call);
        grpc.State->WaitCancelled(0);
        env.Clean(call);
    }

    Y_UNIT_TEST(DeadlineCancelsBothProtocols) {
        for (const auto binding : { 0u, 1u }) {
            THttpMock http;
            TGrpcMock grpc;
            TEnv env(Bindings(http, grpc));
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            const auto call = env.Start(0, binding, 1, Proto("input"), deadline);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            (binding ? grpc.State : http.State)->Wait();
            UNIT_ASSERT_VALUES_EQUAL(env.Runtime->NextDeadline(), deadline);
            env.Runtime->Expire(deadline);
            UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Cancelled);
            env.Clean(call);
        }
    }

    Y_UNIT_TEST(OversizedHttpResponseIsBounded) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxPayloadBytes = 128;
        TEnv env(Bindings(http, grpc), limits);
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, TString(8192, 'x'), 200);
        const auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::ResourceLimit);
        env.Clean(call);
    }

    Y_UNIT_TEST(OversizedGrpcResponseIsBounded) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxPayloadBytes = 128;
        TEnv env(Bindings(http, grpc), limits);
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, TString(8192, 'x'), 0);
        const auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::ResourceLimit);
        env.Clean(call);
    }

    Y_UNIT_TEST(CompressedHttpResponseIsBoundedAfterDecompression) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxPayloadBytes = 128;
        TEnv env(Bindings(http, grpc), limits);
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        TString compressed;
        TStringOutput output(compressed);
        TZLibCompress gzip(&output, ZLib::GZip);
        gzip.Write(TString(8192, 'x'));
        gzip.Finish();
        UNIT_ASSERT(compressed.size() < limits.MaxPayloadBytes);
        http.State->Reply(0, std::move(compressed), 200, false, {}, "gzip");
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
    }

    Y_UNIT_TEST(SequentialCallsKeepTheOriginalDeadline) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        const auto call = env.Start(1, 0, 1, Proto("input"), deadline);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, Proto("intermediate"), 200);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->NextDeadline(), deadline);
        env.Runtime->Expire(deadline);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Cancelled);
        grpc.State->WaitCancelled(0);
        env.Clean(call);
    }

    Y_UNIT_TEST(OperationQuotaRejectsBeforeSendingAnotherRpc) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxOperations = 1;
        TEnv env(Bindings(http, grpc), limits);
        const auto first = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(first).Status == ECallStatus::Waiting);
        http.State->Wait();
        const auto second = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(second).Status == ECallStatus::Failed);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        env.Runtime->Drop(second);
        env.Runtime->Cancel(first);
        env.Clean(first);
    }

    Y_UNIT_TEST(BufferQuotaRejectsBeforeNetworkDispatch) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxReservedBytes = 1;
        TEnv env(Bindings(http, grpc), limits);
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Failed);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 0);
        env.Clean(call);
    }

    Y_UNIT_TEST(GuestCannotSelectAnUnboundEndpoint) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 99);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Failed);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        env.Clean(call);
    }

    Y_UNIT_TEST(HttpRedirectIsNotFollowed) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, {}, 302, false, http.Url());
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
    }

    Y_UNIT_TEST(ForcedOwnerTeardownCancelsPendingFanout) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(2, 0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        grpc.State->Wait();
        env.Runtime.reset();
        grpc.State->WaitCancelled(0);
        Eventually([&] {
            return env.Transport->Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Transport->Stats().ReservedBytes, 0);
        http.State->Reply(0, Proto("late"), 200);
    }

    Y_UNIT_TEST(RepeatedNetworkCallsReturnToBaseline) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        for (size_t i = 0; i < 10; ++i) {
            const auto call = env.Start(0, 1);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            grpc.State->Wait(i);
            grpc.State->Reply(i, "reply", 0);
            UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Completed);
            env.Clean(call);
        }
    }

    Y_UNIT_TEST(NativeTimerUsesTheSharedExecutor) {
        TTransport transport({}, TestExecutor());
        const ui64 delay = 1000;
        auto completions = std::make_shared<TCompletions>();
        auto cancel = transport.Start(1, EOperationKind::Timer, TString(reinterpret_cast<const char*>(&delay), sizeof(delay)),
            TInstant::Now() + TDuration::Seconds(10), [completions](EOperationStatus status, TString) {
                completions->Notify(status);
                return true;
            });
        completions->Wait(1);
        UNIT_ASSERT(completions->Statuses[0] == EOperationStatus::Ready);
        cancel();
        Eventually([&] {
            return transport.Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(transport.Stats().ReservedBytes, 0);
    }

    Y_UNIT_TEST(MaximumNativeGrpcDeadlineDoesNotOverflow) {
        THttpMock http;
        TGrpcMock grpc;
        TTransport transport(Bindings(http, grpc), TestExecutor());
        auto completions = std::make_shared<TCompletions>();
        transport.Start(1, EOperationKind::Request, MakeRequest(1, Proto("input"), "/NFq.NWasmServices.NTest.MockService/Call", ""),
            TInstant::Max(), [completions](EOperationStatus status, TString) {
                completions->Notify(status);
                return true;
            });
        grpc.State->Wait();
        grpc.State->Reply(0, "answer", 0);
        completions->Wait(1);
        UNIT_ASSERT(completions->Statuses[0] == EOperationStatus::Ready);
        Eventually([&] {
            return transport.Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(transport.Stats().ReservedBytes, 0);
    }

    Y_UNIT_TEST(NativeDeadlineStopsBothClientsWithoutOwnerPolling) {
        THttpMock http;
        TGrpcMock grpc;
        TTransport transport(Bindings(http, grpc), TestExecutor());
        auto completions = std::make_shared<TCompletions>();
        const auto deadline = TInstant::Now() + TDuration::Seconds(1);
        for (ui32 binding = 0; binding < 2; ++binding) {
            transport.Start(binding + 1, EOperationKind::Request,
                MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call"),
                deadline, [completions](EOperationStatus status, TString) {
                    completions->Notify(status);
                    return true;
                });
        }
        completions->Wait(2);
        for (const auto status : completions->Statuses) {
            UNIT_ASSERT(status == EOperationStatus::Failed);
        }
        Eventually([&] {
            return transport.Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(transport.Stats().ReservedBytes, 0);
        UNIT_ASSERT(http.State->Count() <= 1 && grpc.State->Count() <= 1);
    }

    Y_UNIT_TEST(ActorSystemShutdownReleasesActiveRequestsAndCredits) {
        THttpMock http;
        TGrpcMock grpc;
        TExecutorFixture fixture;
        TTransport transport(Bindings(http, grpc), fixture.Executor);
        auto completions = std::make_shared<std::atomic<ui32>>(0);
        for (ui32 binding = 0; binding < 2; ++binding) {
            transport.Start(binding + 1, EOperationKind::Request,
                MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call"),
                TInstant::Now() + TDuration::Seconds(30), [completions](EOperationStatus, TString) {
                    ++*completions;
                    return true;
                });
        }
        http.State->Wait();
        grpc.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().Operations, 2);
        fixture.Runtime.GetActorSystem(0)->Cleanup();
        // Hard teardown cannot notify owners whose actors have already stopped.
        UNIT_ASSERT_VALUES_EQUAL(completions->load(), 0);
        Eventually([&] {
            return transport.Stats().Operations == 0;
        });
        Eventually([&] {
            return fixture.Executor->Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
    }

    Y_UNIT_TEST(GrpcCompletionCanReleaseLastTransportOwner) {
        for (const bool shutdownExecutor : { false, true }) {
            TGrpcMock server;
            TExecutorFixture fixture;
            auto published = std::make_shared<std::promise<std::weak_ptr<void>>>();
            auto publication = published->get_future();
            std::promise<void> release;
            const auto released = release.get_future().share();
            TTransportTestAccess::SetCompletionPublishedHook(*fixture.Executor, [published, released](std::weak_ptr<void> owner) {
                published->set_value(std::move(owner));
                // Bounded even if an assertion on the test thread fails.
                released.wait_for(std::chrono::seconds(10));
            });
            TBinding binding;
            binding.Protocol = EProtocol::Grpc;
            binding.Endpoint = server.Endpoint();
            binding.GrpcCredentials = grpc::InsecureChannelCredentials();
            auto transport = std::make_unique<TTransport>(TVector<TBinding>{ binding }, fixture.Executor);
            auto completed = std::make_shared<std::promise<EOperationStatus>>();
            auto completion = completed->get_future();
            transport->Start(1, EOperationKind::Request, MakeRequest(0, Proto("input"), "/NFq.NWasmServices.NTest.MockService/Call", ""),
                TInstant::Now() + TDuration::Seconds(30), [completed](EOperationStatus status, TString) {
                    completed->set_value(status);
                    return true;
                });
            server.State->Wait();
            transport.reset();
            UNIT_ASSERT(publication.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            const auto owner = publication.get();
            UNIT_ASSERT(completion.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            UNIT_ASSERT(completion.get() == EOperationStatus::Cancelled);
            // The user-pool actor has drained the transport; only native completion
            // still owns it. Releasing that reference used to join its own CQ thread.
            Eventually([&] {
                return owner.use_count() == 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().Operations, 1);
            UNIT_ASSERT(fixture.Executor->Stats().ReservedBytes > 0);
            std::future<void> shutdown;
            if (shutdownExecutor) {
                shutdown = std::async(std::launch::async, [&fixture] {
                    fixture.Runtime.GetActorSystem(0)->Cleanup();
                });
                Eventually([&] {
                    return TTransportTestAccess::IsClosed(*fixture.Executor);
                });
                // Hard shutdown must retain the CQ and drain the paused tag.
                UNIT_ASSERT(shutdown.wait_for(std::chrono::seconds(0)) != std::future_status::ready);
            }
            release.set_value();
            if (shutdown.valid()) {
                UNIT_ASSERT(shutdown.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
                shutdown.get();
            }
            Eventually([&] {
                return owner.expired();
            });
            Eventually([&] {
                return fixture.Executor->Stats().Operations == 0;
            });
            UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
        }
    }

    Y_UNIT_TEST(TransportShutdownCancelsAndDrainsBothClients) {
        THttpMock http;
        TGrpcMock grpc;
        auto transport = std::make_unique<TTransport>(Bindings(http, grpc), TestExecutor());
        auto completions = std::make_shared<TCompletions>();
        for (ui32 binding = 0; binding < 2; ++binding) {
            transport->Start(binding + 1, EOperationKind::Request,
                MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call"),
                TInstant::Now() + TDuration::Seconds(30), [completions](EOperationStatus status, TString) {
                    completions->Notify(status);
                    return true;
                });
        }
        http.State->Wait();
        grpc.State->Wait();
        transport.reset();
        completions->Wait(2);
        UNIT_ASSERT_VALUES_EQUAL(completions->Statuses.size(), 2);
        for (const auto status : completions->Statuses) {
            UNIT_ASSERT(status == EOperationStatus::Cancelled);
        }
    }
    Y_UNIT_TEST(HttpTlsTrustedUntrustedAndHostnameVerification) {
        auto perform = [](THttpTlsMock& server, bool trust, bool serve) {
            TBinding binding;
            binding.Endpoint = server.Url();
            binding.Headers = { { "Content-Type", "application/json" } };
            if (trust) {
                binding.CaFile = server.CaFile.Name();
            }
            TTransport transport({ binding }, TestExecutor());
            auto reply = StartDirectHttp(transport, std::make_shared<TDirectReply>());
            if (serve) {
                server.ReplyOnce("{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}");
            }
            std::unique_lock lock(reply->Mutex);
            UNIT_ASSERT(reply->Changed.wait_for(lock, std::chrono::seconds(10), [&] {
                return reply->Done;
            }));
            TResponseHeader header;
            TStringBuf payload;
            UNIT_ASSERT(ParseResponse(reply->Bytes, header, payload));
            return std::make_pair(reply->Status, header);
        };

        THttpTlsMock trustedServer;
        auto [trustedStatus, trustedHeader] = perform(trustedServer, true, true);
        UNIT_ASSERT(trustedStatus == EOperationStatus::Ready);
        UNIT_ASSERT(trustedHeader.Error == EClientError::None);

        THttpTlsMock untrustedServer;
        auto [untrustedStatus, untrustedHeader] = perform(untrustedServer, false, false);
        UNIT_ASSERT(untrustedStatus == EOperationStatus::Failed);
        UNIT_ASSERT(untrustedHeader.Error == EClientError::Tls);

        THttpTlsMock wrongHostnameServer(false);
        auto [hostnameStatus, hostnameHeader] = perform(wrongHostnameServer, true, false);
        UNIT_ASSERT(hostnameStatus == EOperationStatus::Failed);
        UNIT_ASSERT(hostnameHeader.Error == EClientError::Tls);
    }

    Y_UNIT_TEST(SharedMemoryQuotaAppliesAcrossTransportsAndIsReleased) {
        THttpMock http;
        TGrpcMock grpc;
        TExecutorFixture fixture({ .MaxOperations = 128, .MaxReservedBytes = 45000 });
        TTransportLimits limits;
        limits.MaxPayloadBytes = 4096;
        TEnv first(Bindings(http, grpc), limits, "/fq_transport_coroutine.wasm", fixture.Executor);
        TEnv second(Bindings(http, grpc), limits, "/fq_transport_coroutine.wasm", fixture.Executor);
        const auto waiting = first.Start(0, 0);
        UNIT_ASSERT(first.Runtime->Poll(waiting).Status == ECallStatus::Waiting);
        http.State->Wait();
        const auto bytes = fixture.Executor->Stats().ReservedBytes;
        UNIT_ASSERT(bytes > 22500 && bytes <= 45000);
        const auto rejected = second.Start(0, 1);
        const auto failure = second.Runtime->Poll(rejected);
        UNIT_ASSERT(failure.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(failure, 0).Error == EClientError::ResourceLimit);
        second.Clean(rejected);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, bytes);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        first.Runtime->Cancel(waiting);
        first.Clean(waiting);
        Eventually([&] {
            return fixture.Executor->Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
        const auto accepted = second.Start(0, 1);
        UNIT_ASSERT(second.Runtime->Poll(accepted).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, "answer", 0);
        UNIT_ASSERT(second.Resume(accepted).Status == ECallStatus::Completed);
        second.Clean(accepted);
    }

    Y_UNIT_TEST(SharedOperationQuotaAppliesAcrossTransportsAndIsReleased) {
        THttpMock http;
        TGrpcMock grpc;
        TExecutorFixture fixture({ .MaxOperations = 1 });
        TTransportLimits limits;
        limits.MaxPayloadBytes = 4096;
        TEnv first(Bindings(http, grpc), limits, "/fq_transport_coroutine.wasm", fixture.Executor);
        TEnv second(Bindings(http, grpc), limits, "/fq_transport_coroutine.wasm", fixture.Executor);
        const auto waiting = first.Start(0, 0);
        UNIT_ASSERT(first.Runtime->Poll(waiting).Status == ECallStatus::Waiting);
        http.State->Wait();
        const auto bytes = fixture.Executor->Stats().ReservedBytes;
        UNIT_ASSERT(bytes > 0);
        const auto rejected = second.Start(0, 1);
        const auto failure = second.Runtime->Poll(rejected);
        UNIT_ASSERT(failure.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(failure, 0).Error == EClientError::ResourceLimit);
        second.Clean(rejected);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, bytes);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        first.Runtime->Cancel(waiting);
        first.Clean(waiting);
        Eventually([&] {
            return fixture.Executor->Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
        const auto accepted = second.Start(0, 1);
        UNIT_ASSERT(second.Runtime->Poll(accepted).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, "answer", 0);
        UNIT_ASSERT(second.Resume(accepted).Status == ECallStatus::Completed);
        second.Clean(accepted);
    }

    Y_UNIT_TEST(SharedExecutorMakesNetworkProgressOnOneUserThread) {
        THttpMock http;
        TGrpcMock grpc;
        TExecutorFixture fixture;
        TEnv slow(Bindings(http, grpc), {}, "/fq_transport_coroutine.wasm", fixture.Executor);
        TEnv fast(Bindings(http, grpc), {}, "/fq_transport_coroutine.wasm", fixture.Executor);
        const auto waiting = slow.Start(0, 0);
        UNIT_ASSERT(slow.Runtime->Poll(waiting).Status == ECallStatus::Waiting);
        http.State->Wait();
        const auto ready = fast.Start(0, 1);
        UNIT_ASSERT(fast.Runtime->Poll(ready).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, "answer", 0);
        UNIT_ASSERT(fast.Resume(ready).Status == ECallStatus::Completed);
        fast.Clean(ready);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().Operations, 1);
        http.State->Reply(0, Proto("answer"), 200);
        UNIT_ASSERT(slow.Resume(waiting).Status == ECallStatus::Completed);
        slow.Clean(waiting);
        Eventually([&] {
            return fixture.Executor->Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
    }
    Y_UNIT_TEST(GrpcTlsTrustAndUntrustedCa) {
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
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, "answer", 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 0)), "answer");
        env.Clean(call);

        TGrpcMock untrustedGrpc(grpc::SslServerCredentials(serverOptions));
        THttpMock untrustedHttp;
        TEnv untrustedEnv(Bindings(untrustedHttp, untrustedGrpc, grpc::SslCredentials(grpc::SslCredentialsOptions{})));
        const auto failedCall = untrustedEnv.Start(0, 1);
        const auto failed = untrustedEnv.AwaitTerminal(failedCall);
        UNIT_ASSERT(failed.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(failed, 0).Error == EClientError::Connection);
        UNIT_ASSERT_VALUES_EQUAL(untrustedGrpc.State->Count(), 0);
        untrustedEnv.Clean(failedCall);
    }

    Y_UNIT_TEST(RequestSelectsHttpTargetAndGrpcMethodPerCall) {
        THttpMock http;
        TGrpcMock grpc;
        TTransport transport(Bindings(http, grpc), TestExecutor());
        ui64 handle = 100;
        for (const ui32 binding : { 0u, 1u }) {
            for (size_t i = 0; i < 2; ++i) {
                const TString method = binding ? (i ? "/NFq.NWasmServices.NTest.MockService/Other" : "/NFq.NWasmServices.NTest.MockService/Call")
                                               : (i ? "PATCH" : "GET");
                const TString path = binding ? "" : (i ? "/second?x=1" : "/first");
                std::promise<EOperationStatus> done;
                auto future = done.get_future();
                transport.Start(++handle, EOperationKind::Request, MakeRequest(binding, Proto("input"), method, path),
                    TInstant::Now() + TDuration::Seconds(10), [&](EOperationStatus status, TString) {
                        done.set_value(status);
                        return true;
                    });
                auto state = binding ? grpc.State : http.State;
                auto request = state->Wait(i);
                if (!binding) {
                    UNIT_ASSERT(request->Method.StartsWith(method + " " + path + " "));
                }
                state->Reply(i, binding ? TString("answer") : Proto("answer"), binding ? 0 : 200);
                UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
                UNIT_ASSERT(future.get() == EOperationStatus::Ready);
            }
        }
    }

    Y_UNIT_TEST(RequestRejectsAuthorityOverrideAndOldWireVersion) {
        THttpMock http;
        TGrpcMock grpc;
        TTransport transport(Bindings(http, grpc), TestExecutor());
        ui64 handle = 100;
        for (const TString path : { "http://elsewhere/call", "//elsewhere/call", "/call#fragment", "/bad\\path", "/bad\r\n" }) {
            bool failed = false;
            transport.Start(++handle, EOperationKind::Request, MakeRequest(0, {}, "POST", path), TInstant::Now() + TDuration::Seconds(10),
                [&](EOperationStatus status, TString) {
                    failed = status == EOperationStatus::Failed;
                    return true;
                });
            UNIT_ASSERT(failed);
        }
        auto bytes = MakeRequest(0, {});
        const ui32 oldVersion = 2;
        std::memcpy(bytes.Detach(), &oldVersion, sizeof(oldVersion));
        bool failed = false;
        transport.Start(
            ++handle, EOperationKind::Request, bytes, TInstant::Now() + TDuration::Seconds(10), [&](EOperationStatus status, TString) {
                failed = status == EOperationStatus::Failed;
                return true;
            });
        UNIT_ASSERT(failed);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count() + grpc.State->Count(), 0);
    }
    Y_UNIT_TEST(SocketAndGrpcReadinessDoNotPollIdleRequests) {
        for (const auto binding : { 0u, 1u }) {
            THttpMock http;
            TGrpcMock grpc;
            TExecutorFixture fixture;
            TTransport transport(Bindings(http, grpc), fixture.Executor);
            std::promise<bool> completion;
            auto future = completion.get_future();
            transport.Start(1, EOperationKind::Request,
                MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call"),
                TInstant::Now() + TDuration::Seconds(10), [&](EOperationStatus status, TString) {
                    completion.set_value(
                        status == EOperationStatus::Ready && NActors::TlsThreadContext && NActors::TlsThreadContext->PoolId() == 1);
                    return true;
                });
            auto state = binding ? grpc.State : http.State;
            state->Wait();
            Sleep(TDuration::MilliSeconds(30));
            const auto dispatches = fixture.Executor->Stats().Dispatches;
            Sleep(TDuration::MilliSeconds(100));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().Dispatches, dispatches);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().Operations, 1);
            state->Reply(0, binding ? TString("answer") : Proto("answer"), binding ? 0 : 200);
            UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            UNIT_ASSERT(future.get());
            Eventually([&] {
                return fixture.Executor->Stats().Operations == 0;
            });
            UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
        }
    }
}
