#include "tvm_helpers.h"

using namespace NFq::NWasmServices::NTestHelpers;

namespace {
void CheckRuntimeAuthCancellation(bool timeout, bool cleanupAfterDeadline = false) {
    for (const ui32 binding : { 0u, 1u }) {
        auto metrics = std::make_shared<TRequestMetrics>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TExecutorFixture fixture({}, metrics);
        THttpMock http;
        TGrpcMock grpc;
        auto provider = std::make_shared<TTvmTestProvider>();
        provider->State->Pending = true;
        auto bindings = Bindings(http, grpc);
        SetTvm(bindings[binding], provider);
        TEnv env(std::move(bindings), {}, "/fq_transport_coroutine.wasm", fixture.Executor);
        const auto deadline = TInstant::Now() + TDuration::Seconds(timeout || cleanupAfterDeadline ? 2 : 30);
        const auto call = env.Start(0, binding, 1, Proto("input"), deadline);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        Eventually([&] {
            return provider->Count() == 1;
        });
        const auto parent = metrics->Get("unlabeled", binding ? EMetricKind::Grpc : EMetricKind::Http);
        const auto auth = metrics->Get("auth.tvm", EMetricKind::Internal);
        const size_t outcome = timeout ? 2 : 3;
        {
            TUserPoolGate gate(fixture);
            UNIT_ASSERT(TInstant::Now() < deadline);
            if (!timeout) {
                env.Runtime->Cancel(call);
            }
            if (timeout || cleanupAfterDeadline) {
                const auto now = TInstant::Now();
                if (now < deadline) {
                    Sleep(deadline - now + TDuration::MilliSeconds(20));
                }
            }
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Cancelled);
            for (const auto& series : { parent, auth }) {
                UNIT_ASSERT_VALUES_EQUAL(series->InFlight->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(series->Completed[outcome]->Val(), 1);
                UNIT_ASSERT_VALUES_EQUAL(series->CleanupPending->Val(), 1);
            }
            UNIT_ASSERT(parent->Bytes->Val() > 0);
            UNIT_ASSERT_VALUES_EQUAL(auth->Bytes->Val(), 0);
        }
        env.Clean(call);
        Eventually([&] {
            return parent->Bytes->Val() == 0 && parent->CleanupPending->Val() == 0;
        });
        // The late callback holds the auth metric, not the parent's native lease.
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(auth->CleanupPending->Val(), 1);
        {
            std::lock_guard lock(provider->State->Mutex);
            provider->LastCompletion({ .Status = ETvmTicketStatus::Ready, .Ticket = "late-ticket" });
            provider->LastCompletion = {};
        }
        Eventually([&] {
            return auth->CleanupPending->Val() == 0;
        });
        for (const auto& series : { parent, auth }) {
            UNIT_ASSERT_VALUES_EQUAL(series->Completed[outcome]->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(series->Completed[timeout ? 3 : 2]->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(series->Completed[0]->Val(), 0);
        }
    }
}
}   // namespace

Y_UNIT_TEST_SUITE(TFqWasmTvmTest) {
    Y_UNIT_TEST(RuntimeAuthCancellationCompletesBeforePoolCleanup) {
        CheckRuntimeAuthCancellation(false);
    }

    Y_UNIT_TEST(RuntimeAuthDeadlineIsTimeoutBeforePoolCleanup) {
        CheckRuntimeAuthCancellation(true);
        CheckRuntimeAuthCancellation(false, true);
    }

    Y_UNIT_TEST(UniversalAuthMetricsAreIndependentAndFinishExactlyOnce) {
        for (const auto outcome : { EMetricOutcome::Success, EMetricOutcome::Error, EMetricOutcome::Timeout, EMetricOutcome::Cancelled }) {
            auto metrics = std::make_shared<TRequestMetrics>(MakeIntrusive<NMonitoring::TDynamicCounters>());
            TExecutorFixture fixture({}, metrics);
            THttpMock http;
            TGrpcMock grpc;
            auto provider = std::make_shared<TTvmTestProvider>();
            provider->State->Pending = true;
            provider->State->Fail = outcome == EMetricOutcome::Error;
            auto bindings = Bindings(http, grpc);
            SetTvm(bindings[0], provider);
            TTransport transport(std::move(bindings), fixture.Executor);
            std::promise<EOperationStatus> result;
            auto future = result.get_future();
            auto cancel = transport.Start(1, EOperationKind::Request, MakeRequest(0, Proto("input"), "POST", "/call", "lookup"),
                TInstant::Now() + (outcome == EMetricOutcome::Timeout ? TDuration::Seconds(1) : TDuration::Seconds(10)),
                [&](EOperationStatus status, TString) {
                    result.set_value(status);
                    return true;
                });
            Eventually([&] {
                return provider->Count() == 1;
            });
            auto auth = metrics->Get("auth.tvm", EMetricKind::Internal);
            auto parent = metrics->Get("lookup", EMetricKind::Http);
            UNIT_ASSERT_VALUES_EQUAL(auth->Started->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(auth->InFlight->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(auth->Bytes->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parent->Bytes->Val(), fixture.Executor->Stats().ReservedBytes);
            if (outcome == EMetricOutcome::Cancelled) {
                cancel();
            } else if (outcome != EMetricOutcome::Timeout) {
                provider->Ready();
            }
            if (outcome == EMetricOutcome::Success) {
                http.State->Wait();
                UNIT_ASSERT_VALUES_EQUAL(auth->Completed[0]->Val(), 1);
                UNIT_ASSERT_VALUES_EQUAL(parent->InFlight->Val(), 1);
                http.State->Reply(0, Proto("answer"), 200);
            }
            UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            UNIT_ASSERT_VALUES_EQUAL(auth->Completed[static_cast<size_t>(outcome)]->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parent->Completed[static_cast<size_t>(outcome)]->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(auth->InFlight->Val(), 0);
            // The provider intentionally keeps a late callback: logical completion
            // must not report physical cleanup, nor count its late reply twice.
            UNIT_ASSERT_VALUES_EQUAL(auth->CleanupPending->Val(), 1);
            {
                std::lock_guard lock(provider->State->Mutex);
                provider->LastCompletion({ .Status = ETvmTicketStatus::Ready, .Ticket = "late-ticket" });
                provider->LastCompletion = {};
            }
            Eventually([&] {
                return auth->CleanupPending->Val() == 0 && parent->Bytes->Val() == 0;
            });
            UNIT_ASSERT_VALUES_EQUAL(auth->Completed[static_cast<size_t>(outcome)]->Val(), 1);
        }
    }

    Y_UNIT_TEST(AuthMetricCleanupAndHardShutdownRespectPhysicalOwners) {
        for (const bool hardShutdown : { false, true }) {
            for (const ui32 binding : { 0u, 1u }) {
                auto metrics = std::make_shared<TRequestMetrics>(MakeIntrusive<NMonitoring::TDynamicCounters>());
                TExecutorFixture fixture({}, metrics);
                THttpMock http;
                TGrpcMock grpc;
                auto provider = std::make_shared<TTvmTestProvider>();
                provider->State->Pending = true;
                auto bindings = Bindings(http, grpc);
                SetTvm(bindings[binding], provider);
                TTransport transport(std::move(bindings), fixture.Executor);
                auto cancel = transport.Start(1, EOperationKind::Request,
                    MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call",
                        "lookup"), TInstant::Now() + TDuration::Seconds(30), [](EOperationStatus, TString) {
                        return true;
                    });
                Eventually([&] {
                    return provider->Count() == 1;
                });
                auto auth = metrics->Get("auth.tvm", EMetricKind::Internal);
                auto parent = metrics->Get("lookup", binding ? EMetricKind::Grpc : EMetricKind::Http);
                if (hardShutdown) {
                    fixture.Runtime.GetActorSystem(0)->Cleanup();
                } else {
                    provider->Ready();
                    (binding ? grpc.State : http.State)->Wait();
                }
                UNIT_ASSERT_VALUES_EQUAL(auth->InFlight->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(auth->Completed[hardShutdown ? 3 : 0]->Val(), 1);
                UNIT_ASSERT_VALUES_EQUAL(auth->CleanupPending->Val(), 1);
                {
                    std::lock_guard lock(provider->State->Mutex);
                    provider->LastCompletion({ .Status = ETvmTicketStatus::Ready, .Ticket = "late-ticket" });
                    provider->LastCompletion = {};
                }
                Eventually([&] {
                    return auth->CleanupPending->Val() == 0;
                });
                UNIT_ASSERT_VALUES_EQUAL(parent->InFlight->Val(), hardShutdown ? 0 : 1);
                // Successful auth cleanup must not wait for the parent's HTTP response.
                if (!hardShutdown) {
                    cancel();
                }
                Eventually([&] {
                    return parent->Bytes->Val() == 0 && parent->CleanupPending->Val() == 0;
                });
                UNIT_ASSERT_VALUES_EQUAL(parent->Completed[3]->Val(), 1);
            }
        }
    }

    Y_UNIT_TEST(MockTvmActorTimerDeliversTickets) {
        THttpMock http;
        TGrpcMock grpc;
        auto bindings = Bindings(http, grpc);
        for (auto& binding : bindings) {
            SetTvm(binding, CreateMockTvmTicketProvider(TDuration::MilliSeconds(20)));
        }
        TEnv env(std::move(bindings));
        for (auto binding : { 0u, 1u }) {
            const auto call = env.Start(0, binding);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            auto state = binding ? grpc.State : http.State;
            UNIT_ASSERT(HasHeader(*state->Wait(), binding ? "x-ya-service-ticket" : "X-Ya-Service-Ticket", "fqrun-mock-tvm-service-ticket"));
            state->Reply(0, binding ? TString("answer") : Proto("answer"), binding ? 0 : 200);
            UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Completed);
            env.Clean(call);
        }
    }

    Y_UNIT_TEST(TvmTicketsAreAppliedPerCallForHttpAndGrpc) {
        THttpMock http;
        TGrpcMock grpc;
        auto provider = std::make_shared<TTvmTestProvider>();
        auto bindings = Bindings(http, grpc);
        for (auto& binding : bindings) {
            SetTvm(binding, provider);
        }
        TEnv env(std::move(bindings), { .MaxPayloadBytes = 4096 });
        size_t index = 0;
        for (auto binding : { 0u, 1u, 0u, 1u }) {
            const auto call = env.Start(0, binding);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            auto state = binding ? grpc.State : http.State;
            auto request = state->Wait(index / 2);
            UNIT_ASSERT(
                HasHeader(*request, binding ? "x-ya-service-ticket" : "X-Ya-Service-Ticket", "test-service-ticket-" + ToString(index + 1)));
            UNIT_ASSERT(!HasHeader(*request, "Authorization", "Bearer host-secret"));
            state->Reply(index / 2, binding ? TString("answer") : Proto("answer"), binding ? 0 : 200);
            auto result = env.Resume(call);
            UNIT_ASSERT(result.Status == ECallStatus::Completed);
            UNIT_ASSERT(!result.Data.Contains("test-service-ticket"));
            UNIT_ASSERT(!result.Data.Contains("test-tvm-source-secret"));
            env.Clean(call);
            ++index;
        }
        UNIT_ASSERT_VALUES_EQUAL(provider->Count(), 4);
        std::lock_guard lock(provider->State->Mutex);
        for (const auto& credentials : provider->State->Calls) {
            UNIT_ASSERT_VALUES_EQUAL(credentials.SourceId, 2020001);
            UNIT_ASSERT_VALUES_EQUAL(credentials.DestinationId, 2020002);
            UNIT_ASSERT_VALUES_EQUAL(credentials.Secret, "test-tvm-source-secret");
        }
    }

    Y_UNIT_TEST(TvmPendingAcquisitionDoesNotBlockUnauthenticatedOperations) {
        THttpMock http;
        TGrpcMock grpc;
        auto provider = std::make_shared<TTvmTestProvider>();
        provider->State->Pending = true;
        auto bindings = Bindings(http, grpc);
        SetTvm(bindings[1], provider);
        TEnv env(std::move(bindings));
        const auto waiting = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(waiting).Status == ECallStatus::Waiting);
        Eventually([&] {
            return provider->Count() == 1;
        });
        const auto ready = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(ready).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, Proto("answer"), 200);
        UNIT_ASSERT(env.Resume(ready).Status == ECallStatus::Completed);
        env.Runtime->Drop(ready);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        provider->Ready();
        UNIT_ASSERT(HasHeader(*grpc.State->Wait(), "x-ya-service-ticket", "test-service-ticket-1"));
        grpc.State->Reply(0, "answer", 0);
        UNIT_ASSERT(env.Resume(waiting).Status == ECallStatus::Completed);
        env.Clean(waiting);
    }

    Y_UNIT_TEST(DelayedMockTvmAcquisitionDoesNotBlockOtherRequests) {
        for (auto slowBinding : { 0u, 1u }) {
            for (bool otherUsesTvm : { false, true }) {
                THttpMock http;
                TGrpcMock grpc;
                TControlledTvmScheduler timers;
                auto provider = CreateMockTvmTicketProvider(TDuration::Seconds(30), timers.Scheduler());
                auto bindings = Bindings(http, grpc);
                const auto fastBinding = 1u - slowBinding;
                SetTvm(bindings[slowBinding], provider);
                if (otherUsesTvm) {
                    SetTvm(bindings[fastBinding], CreateMockTvmTicketProvider(TDuration::Zero()));
                }
                TEnv env(std::move(bindings));
                const auto waiting = env.Start(0, slowBinding);
                UNIT_ASSERT(env.Runtime->Poll(waiting).Status == ECallStatus::Waiting);
                // Hold the actual mock completion until other work finishes.
                Eventually([&] {
                    return timers.Count() == 1;
                });

                const auto ready = env.Start(0, fastBinding);
                UNIT_ASSERT(env.Runtime->Poll(ready).Status == ECallStatus::Waiting);
                auto fastState = fastBinding ? grpc.State : http.State;
                const auto request = fastState->Wait();
                if (otherUsesTvm) {
                    UNIT_ASSERT(
                        HasHeader(*request, fastBinding ? "x-ya-service-ticket" : "X-Ya-Service-Ticket", "fqrun-mock-tvm-service-ticket"));
                }
                fastState->Reply(0, fastBinding ? TString("answer") : Proto("answer"), fastBinding ? 0 : 200);
                UNIT_ASSERT(env.Resume(ready).Status == ECallStatus::Completed);
                env.Runtime->Drop(ready);
                auto slowState = slowBinding ? grpc.State : http.State;
                UNIT_ASSERT_VALUES_EQUAL(slowState->Count(), 0);
                UNIT_ASSERT(env.Runtime->Poll(waiting).Status == ECallStatus::Waiting);

                timers.Fire();
                UNIT_ASSERT(
                    HasHeader(*slowState->Wait(), slowBinding ? "x-ya-service-ticket" : "X-Ya-Service-Ticket", "fqrun-mock-tvm-service-ticket"));
                slowState->Reply(0, slowBinding ? TString("answer") : Proto("answer"), slowBinding ? 0 : 200);
                UNIT_ASSERT(env.Resume(waiting).Status == ECallStatus::Completed);
                UNIT_ASSERT(timers.OnUserPool);
                env.Clean(waiting);
            }
        }
    }

    Y_UNIT_TEST(SharedExecutorMakesProgressAcrossTransportsOnOneUserThread) {
        THttpMock http;
        TGrpcMock grpc;
        TExecutorFixture fixture;
        TControlledTvmScheduler timers;
        auto provider = CreateMockTvmTicketProvider(TDuration::Seconds(30), timers.Scheduler());
        auto bindings = Bindings(http, grpc);
        SetTvm(bindings[1], provider);
        TEnv slow(bindings, {}, "/fq_transport_coroutine.wasm", fixture.Executor);
        TEnv fast(Bindings(http, grpc), {}, "/fq_transport_coroutine.wasm", fixture.Executor);
        const auto waiting = slow.Start(0, 1);
        UNIT_ASSERT(slow.Runtime->Poll(waiting).Status == ECallStatus::Waiting);
        Eventually([&] {
            return timers.Count() == 1;
        });
        const auto ready = fast.Start(0, 0);
        UNIT_ASSERT(fast.Runtime->Poll(ready).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, Proto("answer"), 200);
        UNIT_ASSERT(fast.Resume(ready).Status == ECallStatus::Completed);
        fast.Clean(ready);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().Operations, 1);
        Sleep(TDuration::MilliSeconds(20));   // allow already queued notifications to drain
        const auto dispatches = fixture.Executor->Stats().Dispatches;
        Sleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().Dispatches, dispatches);
        UNIT_ASSERT_VALUES_EQUAL(timers.Count(), 1);
        timers.Fire();
        grpc.State->Wait();
        grpc.State->Reply(0, "answer", 0);
        UNIT_ASSERT(slow.Resume(waiting).Status == ECallStatus::Completed);
        slow.Clean(waiting);
        Eventually([&] {
            return fixture.Executor->Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
    }

    Y_UNIT_TEST(SharedOperationQuotaIncludesTicketWaitAndRecoversAfterCancellation) {
        THttpMock http;
        TGrpcMock grpc;
        TExecutorFixture fixture({ .MaxOperations = 1 });
        auto provider = std::make_shared<TTvmTestProvider>();
        provider->State->Pending = true;
        auto bindings = Bindings(http, grpc);
        SetTvm(bindings[1], provider);
        TEnv first(bindings, {}, "/fq_transport_coroutine.wasm", fixture.Executor);
        TEnv second(Bindings(http, grpc), {}, "/fq_transport_coroutine.wasm", fixture.Executor);
        const auto waiting = first.Start(0, 1);
        UNIT_ASSERT(first.Runtime->Poll(waiting).Status == ECallStatus::Waiting);
        Eventually([&] {
            return provider->Count() == 1;
        });
        const auto rejected = second.Start(0, 0);
        const auto failure = second.Runtime->Poll(rejected);
        UNIT_ASSERT(failure.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(failure, 0).Error == EClientError::ResourceLimit);
        second.Clean(rejected);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count() + grpc.State->Count(), 0);
        first.Runtime->Cancel(waiting);
        first.Clean(waiting);
        Eventually([&] {
            return fixture.Executor->Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
        const auto accepted = second.Start(0, 0);
        UNIT_ASSERT(second.Runtime->Poll(accepted).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, Proto("answer"), 200);
        UNIT_ASSERT(second.Resume(accepted).Status == ECallStatus::Completed);
        second.Clean(accepted);
    }

    Y_UNIT_TEST(TvmAcquisitionFailureAndInvalidTicketsNeverReachService) {
        for (auto binding : { 0u, 1u }) {
            for (const TString& prefix : { TString("bad\r\nheader"), TString(MaxTvmTicketBytes, 'x') }) {
                THttpMock http;
                TGrpcMock grpc;
                auto provider = std::make_shared<TTvmTestProvider>();
                provider->Prefix = prefix;
                provider->State->Pending = true;
                auto bindings = Bindings(http, grpc);
                SetTvm(bindings[binding], provider);
                TEnv env(std::move(bindings));
                const auto call = env.Start(0, binding);
                UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
                Eventually([&] {
                    return provider->Count() == 1;
                });
                provider->Ready();
                auto result = env.Resume(call);
                UNIT_ASSERT(result.Status == ECallStatus::Failed);
                UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::Authentication);
                UNIT_ASSERT_VALUES_EQUAL(http.State->Count() + grpc.State->Count(), 0);
                env.Clean(call);
            }
            THttpMock http;
            TGrpcMock grpc;
            auto bindings = Bindings(http, grpc);
            SetTvm(bindings[binding], CreateTvmTicketProvider());
            std::promise<TResponseHeader> completion;
            auto future = completion.get_future();
            TTransport transport(std::move(bindings), TestExecutor());
            transport.Start(1, EOperationKind::Request,
                MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call"),
                TInstant::Now() + TDuration::Seconds(10), [&](EOperationStatus status, TString response) {
                    TResponseHeader header;
                    TStringBuf body;
                    UNIT_ASSERT(status == EOperationStatus::Failed);
                    UNIT_ASSERT(ParseResponse(response, header, body));
                    completion.set_value(header);
                    return true;
                });
            UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            UNIT_ASSERT(future.get().Error == EClientError::Authentication);
            UNIT_ASSERT_VALUES_EQUAL(http.State->Count() + grpc.State->Count(), 0);
        }
    }

    Y_UNIT_TEST(TvmPendingAcquisitionCanBeCancelledOrExpire) {
        for (auto binding : { 0u, 1u }) {
            for (bool expire : { false, true }) {
                THttpMock http;
                TGrpcMock grpc;
                auto provider = std::make_shared<TTvmTestProvider>();
                provider->State->Pending = true;
                auto bindings = Bindings(http, grpc);
                SetTvm(bindings[binding], provider);
                TEnv env(std::move(bindings));
                const auto deadline = TInstant::Now() + TDuration::Seconds(30);
                const auto call = env.Start(0, binding, 1, Proto("input"), deadline);
                UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
                Eventually([&] {
                    return provider->Count() == 1;
                });
                if (expire) {
                    env.Runtime->Expire(deadline);
                } else {
                    env.Runtime->Cancel(call);
                }
                UNIT_ASSERT(env.AwaitTerminal(call).Status == ECallStatus::Cancelled);
                Eventually([&] {
                    return provider->State->Cancelled.load() == 1;
                });
                UNIT_ASSERT_VALUES_EQUAL(http.State->Count() + grpc.State->Count(), 0);
                env.Clean(call);
            }
        }
    }

    Y_UNIT_TEST(TvmConnectionCredentialsReachDqTransport) {
        for (auto binding : { 2u, 3u }) {
            THttpMock http;
            TGrpcMock grpc;
            auto provider = std::make_shared<TTvmTestProvider>();
            auto config = Bindings(http, grpc)[binding];
            SetTvm(config, provider);
            TQueryTransformEnv env(config);
            env.Start();
            auto state = binding == 2 ? http.State : grpc.State;
            UNIT_ASSERT(HasHeader(*state->Wait(), binding == 2 ? "X-Ya-Service-Ticket" : "x-ya-service-ticket", "test-service-ticket-1"));
            state->Reply(
                0, binding == 2 ? TString(R"({"id":42,"name":"Ada","score":97,"version":1})") : GrpcProfilePayload(), binding == 2 ? 200 : 0);
            Eventually([&] {
                return env.Output->Finished.load();
            });
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
        }
    }

    Y_UNIT_TEST(TvmCredentialsRequireProviderAndCannotOverrideHeaders) {
        TBinding binding;
        binding.Endpoint = "http://127.0.0.1:1";
        SetTvm(binding, {});
        UNIT_ASSERT_EXCEPTION(TTransport({ binding }, TestExecutor()), yexception);
        binding.TvmProvider = CreateTvmTicketProvider();
        binding.Headers = { { "X-Ya-Service-Ticket", "override" } };
        UNIT_ASSERT_EXCEPTION(TTransport({ binding }, TestExecutor()), yexception);
    }

    Y_UNIT_TEST(TvmConnectionValidationAndSecretMasking) {
        auto connection = TestServiceConnection();
        auto& service = *connection.mutable_content()->mutable_setting()->mutable_external_service();
        auto& tvm = *service.mutable_tvm_auth();
        tvm.set_source_id(2020001);
        tvm.set_destination_id(2020002);
        tvm.set_secret("source-secret-for-validation");
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.clear_auth();
        UNIT_ASSERT(NFq::GetMutableAuth(*connection.mutable_content()->mutable_setting()) == nullptr);
        UNIT_ASSERT(!service.has_auth());
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        UNIT_ASSERT(!NKikimr::SecureDebugString(connection).Contains("source-secret-for-validation"));
        FederatedQuery::ExternalService resolved;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(PrepareServiceConnection(connection, {}), &resolved).ok());
        UNIT_ASSERT_VALUES_EQUAL(resolved.tvm_auth().secret(), tvm.secret());
        for (auto header : { "X-Ya-Service-Ticket", "x-ya-service-ticket", "X-Ya-User-Ticket" }) {
            (*service.mutable_headers())[header] = "override";
            UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
            service.clear_headers();
        }
        tvm.set_source_id(0);
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        tvm.set_source_id(2020001);
        tvm.clear_secret();
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
    }

    Y_UNIT_TEST(LateTvmCompletionAfterCancellationAndShutdownIsIgnored) {
        THttpMock http;
        TGrpcMock grpc;
        TExecutorFixture fixture;
        auto provider = std::make_shared<TTvmTestProvider>();
        provider->State->Pending = true;
        auto bindings = Bindings(http, grpc);
        SetTvm(bindings[0], provider);
        TTransport transport(std::move(bindings), fixture.Executor);
        std::promise<EOperationStatus> completion;
        auto future = completion.get_future();
        const auto cancel = transport.Start(1, EOperationKind::Request, MakeRequest(0, "input"), TInstant::Now() + TDuration::Seconds(30),
            [&](EOperationStatus status, TString) {
                completion.set_value(status);
                return true;
            });
        Eventually([&] {
            return provider->Count() == 1;
        });
        ITvmTicketProvider::TCompletion late;
        {
            std::lock_guard lock(provider->State->Mutex);
            late = provider->LastCompletion;
        }
        cancel();
        UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        UNIT_ASSERT(future.get() == EOperationStatus::Cancelled);
        Eventually([&] {
            return fixture.Executor->Stats().Operations == 0;
        });
        late({ .Status = ETvmTicketStatus::Ready, .Ticket = "too-late" });
        fixture.Runtime.GetActorSystem(0)->Cleanup();
        late({ .Status = ETvmTicketStatus::Ready, .Ticket = "after-shutdown" });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
    }
}
