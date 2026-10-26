#include "helpers.h"

using namespace NFq::NWasmServices::NTestHelpers;

Y_UNIT_TEST_SUITE(TWasmServiceMetrics) {
    Y_UNIT_TEST(RuntimeCancellationCompletesBeforeNativeCleanup) {
        for (const ui32 binding : { 0u, 1u }) {
            auto metrics = std::make_shared<TRequestMetrics>(MakeIntrusive<NMonitoring::TDynamicCounters>());
            TExecutorFixture fixture({}, metrics);
            THttpMock http;
            TGrpcMock grpc;
            TEnv env(Bindings(http, grpc), {}, "/fq_transport_coroutine.wasm", fixture.Executor);
            const auto call = env.Start(0, binding);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            (binding ? grpc.State : http.State)->Wait();
            auto series = metrics->Get("unlabeled", binding ? EMetricKind::Grpc : EMetricKind::Http);
            {
                TUserPoolGate gate(fixture);
                env.Runtime->Cancel(call);
                UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Cancelled);
                UNIT_ASSERT_VALUES_EQUAL(series->InFlight->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(series->Completed[3]->Val(), 1);
                UNIT_ASSERT_VALUES_EQUAL(series->CleanupPending->Val(), 1);
                UNIT_ASSERT(series->Bytes->Val() > 0);
            }
            env.Clean(call);
            Eventually([&] {
                return series->Bytes->Val() == 0 && series->CleanupPending->Val() == 0;
            });
            UNIT_ASSERT_VALUES_EQUAL(series->Completed[3]->Val(), 1);
        }
    }

    Y_UNIT_TEST(RuntimeDeadlineOutcomeDoesNotDependOnExecutorOrder) {
        for (const bool cancelBeforeDeadline : { false, true }) {
            for (const ui32 binding : { 0u, 1u }) {
                auto metrics = std::make_shared<TRequestMetrics>(MakeIntrusive<NMonitoring::TDynamicCounters>());
                TExecutorFixture fixture({}, metrics);
                THttpMock http;
                TGrpcMock grpc;
                TEnv env(Bindings(http, grpc), {}, "/fq_transport_coroutine.wasm", fixture.Executor);
                const auto deadline = TInstant::Now() + TDuration::Seconds(2);
                const auto call = env.Start(0, binding, 1, Proto("input"), deadline);
                UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
                (binding ? grpc.State : http.State)->Wait();
                auto series = metrics->Get("unlabeled", binding ? EMetricKind::Grpc : EMetricKind::Http);
                const size_t outcome = cancelBeforeDeadline ? 3 : 2;
                {
                    TUserPoolGate gate(fixture);
                    UNIT_ASSERT(TInstant::Now() < deadline);
                    if (cancelBeforeDeadline) {
                        env.Runtime->Cancel(call);
                    }
                    const auto now = TInstant::Now();
                    if (now < deadline) {
                        Sleep(deadline - now + TDuration::MilliSeconds(20));
                    }
                    UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Cancelled);
                    UNIT_ASSERT_VALUES_EQUAL(series->InFlight->Val(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(series->Completed[outcome]->Val(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(series->CleanupPending->Val(), 1);
                    UNIT_ASSERT(series->Bytes->Val() > 0);
                }
                env.Clean(call);
                Eventually([&] {
                    return series->Bytes->Val() == 0 && series->CleanupPending->Val() == 0;
                });
                UNIT_ASSERT_VALUES_EQUAL(series->Completed[outcome]->Val(), 1);
                UNIT_ASSERT_VALUES_EQUAL(series->Completed[cancelBeforeDeadline ? 2 : 3]->Val(), 0);
            }
        }
    }

    Y_UNIT_TEST(HardShutdownCompletesHttpAndGrpcMetricsExactlyOnce) {
        for (const ui32 binding : { 0u, 1u }) {
            auto metrics = std::make_shared<TRequestMetrics>(MakeIntrusive<NMonitoring::TDynamicCounters>());
            TExecutorFixture fixture({}, metrics);
            THttpMock http;
            TGrpcMock grpc;
            TTransport transport(Bindings(http, grpc), fixture.Executor);
            transport.Start(1, EOperationKind::Request,
                MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call",
                    "lookup"), TInstant::Now() + TDuration::Seconds(30), [](EOperationStatus, TString) {
                    return true;
                });
            (binding ? grpc.State : http.State)->Wait();
            fixture.Runtime.GetActorSystem(0)->Cleanup();
            auto series = metrics->Get("lookup", binding ? EMetricKind::Grpc : EMetricKind::Http);
            UNIT_ASSERT_VALUES_EQUAL(series->InFlight->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(series->Completed[3]->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(series->Bytes->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(series->CleanupPending->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
        }
    }

    Y_UNIT_TEST(AliasWireCompatibilityAndBounds) {
        for (const auto& alias : { TString("lookup"), TString(), TString("/users/123?secret=x"), TString(100, 'x') }) {
            auto bytes = MakeRequest(0, "body", "POST", "/users/123", alias);
            TRequestHeader header;
            std::string_view payload, method, path, decoded;
            UNIT_ASSERT(ReadRequest(View(bytes), header, payload, method, path, decoded));
            UNIT_ASSERT_VALUES_EQUAL(TString(decoded), TString(MonitoringAlias(View(alias))));
            UNIT_ASSERT_VALUES_EQUAL(TString(payload), "body");
            UNIT_ASSERT_VALUES_EQUAL(TString(path), "/users/123");
        }
        auto bytes = MakeRequest(0, "body", "POST", "/call");
        // Real v3 layout: no alias/reserved fields, not merely a different version.
        TString legacy = bytes.substr(0, 24) + bytes.substr(sizeof(TRequestHeader));
        const ui32 version = 3;
        std::memcpy(legacy.Detach(), &version, sizeof(version));
        TRequestHeader header;
        std::string_view payload, method, path, alias;
        UNIT_ASSERT(ReadRequest(View(legacy), header, payload, method, path, alias));
        UNIT_ASSERT(alias.empty());
        UNIT_ASSERT_VALUES_EQUAL(TString(payload), "body");
        auto* malformed = reinterpret_cast<TRequestHeader*>(bytes.Detach());
        malformed->AliasBytes = 100;
        UNIT_ASSERT(!ReadRequest(View(bytes), header, payload, method, path, alias));
    }

    Y_UNIT_TEST(BoundedAliasesAndPhysicalLifetime) {
        auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TRequestMetrics metrics(root, 2);
        const auto first = metrics.Get("lookup", EMetricKind::Http);
        metrics.Get("update", EMetricKind::Http);
        const auto other = metrics.Get("other", EMetricKind::Http);
        for (ui32 i = 0; i < 1000; ++i) {
            UNIT_ASSERT(metrics.Get(ToString(i), EMetricKind::Http) == other);
        }
        UNIT_ASSERT(metrics.Get("/secret/token", EMetricKind::Http) == other);
        auto request = metrics.Start("lookup", EMetricKind::Http, 123, 42);
        request->Queued();
        request->Dispatched();
        auto nativeOwner = request;
        request->Complete(EMetricOutcome::Success, 7);
        request->Complete(EMetricOutcome::Error);
        UNIT_ASSERT_VALUES_EQUAL(first->Started->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(first->InFlight->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(first->Completed[0]->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(first->Completed[1]->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(first->Bytes->Val(), 123);
        request.reset();
        UNIT_ASSERT_VALUES_EQUAL(first->CleanupPending->Val(), 1);
        nativeOwner.reset();
        UNIT_ASSERT_VALUES_EQUAL(first->Bytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(first->CleanupPending->Val(), 0);
        { auto shutdown = metrics.Start("lookup", EMetricKind::Http, 456); }
        UNIT_ASSERT_VALUES_EQUAL(first->Completed[3]->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(first->Bytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(first->InFlight->Val(), 0);
    }

    Y_UNIT_TEST(HttpAndGrpcAliasesShareUniversalCounters) {
        for (ui32 binding : { 0u, 1u }) {
            auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto metrics = std::make_shared<TRequestMetrics>(root);
            TExecutorFixture fixture({}, metrics);
            THttpMock http;
            TGrpcMock grpc;
            TTransport transport(Bindings(http, grpc), fixture.Executor);
            auto result = std::make_shared<std::promise<EOperationStatus>>();
            auto completed = result->get_future();
            transport.Start(1, EOperationKind::Request,
                MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call",
                    "lookup"), TInstant::Now() + TDuration::Seconds(10), [result](EOperationStatus status, TString) {
                    result->set_value(status);
                    return true;
                });
            auto state = binding ? grpc.State : http.State;
            state->Wait();
            auto series = metrics->Get("lookup", binding ? EMetricKind::Grpc : EMetricKind::Http);
            UNIT_ASSERT_VALUES_EQUAL(series->Started->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(series->InFlight->Val(), 1);
            UNIT_ASSERT(series->Bytes->Val() > 0);
            state->Reply(0, binding ? TString("answer") : Proto("answer"), binding ? 0 : 200);
            UNIT_ASSERT(completed.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            UNIT_ASSERT(completed.get() == EOperationStatus::Ready);
            Eventually([&] {
                return series->Bytes->Val() == 0 && series->CleanupPending->Val() == 0;
            });
            UNIT_ASSERT_VALUES_EQUAL(series->Completed[0]->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(series->InFlight->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(series->CleanupPending->Val(), 0);
        }
    }

    Y_UNIT_TEST(RejectedCallsAreNotCountedAsStarted) {
        auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto metrics = std::make_shared<TRequestMetrics>(root);
        TExecutorFixture fixture({ .MaxOperations = 1 }, metrics);
        THttpMock http;
        TGrpcMock grpc;
        TTransport first(Bindings(http, grpc), fixture.Executor);
        TTransport second(Bindings(http, grpc), fixture.Executor);
        auto cancelled = std::make_shared<std::promise<EOperationStatus>>();
        auto result = cancelled->get_future();
        auto cancel = first.Start(1, EOperationKind::Request, MakeRequest(0, "input", "POST", "/call", "lookup"),
            TInstant::Now() + TDuration::Seconds(10), [cancelled](EOperationStatus s, TString) {
                cancelled->set_value(s);
                return true;
            });
        http.State->Wait();
        bool rejected = false;
        second.Start(1, EOperationKind::Request, MakeRequest(0, "input", "POST", "/call", "lookup"), TInstant::Now() + TDuration::Seconds(10),
            [&rejected](EOperationStatus s, TString) {
                rejected = s == EOperationStatus::Failed;
                return true;
            });
        UNIT_ASSERT(rejected);
        auto series = metrics->Get("lookup", EMetricKind::Http);
        UNIT_ASSERT_VALUES_EQUAL(series->Started->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(series->Rejected[static_cast<size_t>(EAdmissionReason::SharedOperations)]->Val(), 1);
        cancel();
        UNIT_ASSERT(result.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        UNIT_ASSERT(result.get() == EOperationStatus::Cancelled);
        Eventually([&] {
            return series->Bytes->Val() == 0 && series->CleanupPending->Val() == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(series->Completed[3]->Val(), 1);
    }
}
