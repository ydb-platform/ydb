#include "tvm_helpers.h"

#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

using namespace NFq::NWasmServices::NTestHelpers;

namespace {
using TSpan = NWilson::TTraceSnapshot::TOtelSpan;

TString Attribute(const TSpan& span, TStringBuf key) {
    for (const auto& attr : span.attributes()) {
        if (attr.key() == key) {
            return attr.value().string_value();
        }
    }
    return {};
}

struct TCollector {
    TExecutorFixture& Fixture;
    NActors::TActorId Id;

    explicit TCollector(TExecutorFixture& fixture)
        : Fixture(fixture)
    {
        Id = fixture.Runtime.Register(new NWilson::TFakeWilsonUploader);
        fixture.Runtime.RegisterService(NWilson::MakeWilsonUploaderId(), Id);
    }

    std::vector<TSpan> Snapshot() {
        auto promise = NThreading::NewPromise<std::vector<TSpan>>();
        Fixture.Runtime.GetActorSystem(0)->Send(Id, new NWilson::TFakeWilsonUploader::TEvGetSnapshot(promise));
        UNIT_ASSERT(promise.GetFuture().Wait(TDuration::Seconds(10)));
        return promise.GetFuture().GetValueSync();
    }

    TTraceContext Context(ui32 budget = 4096) {
        return TTraceContext::Create(NWilson::TTraceId::NewTraceId(15, 64), Fixture.Runtime.GetActorSystem(0), budget);
    }
};
}   // namespace

Y_UNIT_TEST_SUITE(TWasmServiceTracing) {
    Y_UNIT_TEST(BoundedDisabledAndExactlyOnce) {
        TExecutorFixture fixture;
        TCollector collector(fixture);
        UNIT_ASSERT(!TCallTrace::Start({}, "disabled"));
        auto context = collector.Context(2);
        auto parent = TCallTrace::Start(context, "WASM invocation");
        auto child = TCallTrace::Start(parent->Context(), "External call");
        UNIT_ASSERT(!TCallTrace::Start(context, "overflow"));
        UNIT_ASSERT_VALUES_EQUAL(context.Budget->Dropped.load(), 1);
        child->Complete(EMetricOutcome::Timeout);
        child->Complete(EMetricOutcome::Success);
        child.reset();
        parent->Complete(EMetricOutcome::Success);
        auto spans = collector.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(spans[0].parent_span_id(), spans[1].span_id());
        UNIT_ASSERT_VALUES_EQUAL(Attribute(spans[0], "outcome"), "timeout");
    }

    Y_UNIT_TEST(ConcurrentQueriesKeepParentsAndMetricAlias) {
        auto metrics = std::make_shared<TRequestMetrics>(MakeIntrusive<NMonitoring::TDynamicCounters>(), 0);
        TExecutorFixture fixture({}, metrics);
        TCollector collector(fixture);
        THttpMock http;
        TGrpcMock grpc;
        TTransport transport(Bindings(http, grpc), fixture.Executor);
        auto first = TCallTrace::Start(collector.Context(), "SQL execution");
        auto second = TCallTrace::Start(collector.Context(), "SQL execution");
        transport.SetTraceContext(first->Context());
        auto cancel1 = transport.Start(1, EOperationKind::Request, MakeRequest(0, Proto("secret-payload"), "POST", "/call", "lookup"),
            TInstant::Now() + TDuration::Seconds(30), [](auto, auto) {
                return true;
            });
        transport.SetTraceContext(second->Context());
        auto cancel2 = transport.Start(2, EOperationKind::Request,
            MakeRequest(1, Proto("secret-payload"), "/NFq.NWasmServices.NTest.MockService/Call", "", "lookup"),
            TInstant::Now() + TDuration::Seconds(30), [](auto, auto) {
                return true;
            });
        http.State->Wait();
        grpc.State->Wait();
        cancel1();
        cancel2();
        first->Complete(EMetricOutcome::Success);
        second->Complete(EMetricOutcome::Success);
        auto spans = collector.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(spans.size(), 4);
        UNIT_ASSERT(spans[0].trace_id() != spans[1].trace_id());
        for (size_t i = 0; i < 2; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(spans[i].parent_span_id(), spans[i + 2].span_id());
            UNIT_ASSERT_VALUES_EQUAL(Attribute(spans[i], "call_alias"), "other");
            UNIT_ASSERT(!spans[i].DebugString().Contains("secret-payload"));
        }
    }

    Y_UNIT_TEST(TracingMemoryCannotRejectAnOtherwiseAdmissibleCall) {
        THttpMock http;
        TGrpcMock grpc;
        const auto request = MakeRequest(0, Proto("input"), "POST", "/call", "lookup");
        ui64 baseBytes;
        {
            TExecutorFixture fixture;
            TTransport transport(Bindings(http, grpc), fixture.Executor);
            auto cancel = transport.Start(1, EOperationKind::Request, request, TInstant::Now() + TDuration::Seconds(30), [](auto, auto) {
                return true;
            });
            http.State->Wait();
            baseBytes = transport.Stats().ReservedBytes;
            cancel();
            Eventually([&] {
                return transport.Stats().ReservedBytes == 0;
            });
        }
        size_t index = 1;
        for (bool shared : { false, true }) {
            TTransportExecutorLimits executorLimits;
            TTransportLimits localLimits;
            if (shared) {
                executorLimits.MaxReservedBytes = baseBytes;
            } else {
                localLimits.MaxReservedBytes = baseBytes;
            }
            TExecutorFixture fixture(executorLimits);
            TCollector collector(fixture);
            TTransport transport(Bindings(http, grpc), fixture.Executor, localLimits);
            auto context = collector.Context();
            transport.SetTraceContext(context);
            std::promise<EOperationStatus> result;
            auto future = result.get_future();
            transport.Start(
                1, EOperationKind::Request, request, TInstant::Now() + TDuration::Seconds(30), [&](EOperationStatus status, TString) {
                    result.set_value(status);
                    return true;
                });
            http.State->Wait(index);
            UNIT_ASSERT_VALUES_EQUAL(transport.Stats().ReservedBytes, baseBytes);
            http.State->Reply(index++, Proto("response"), 200);
            UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            UNIT_ASSERT(future.get() == EOperationStatus::Ready);
            Eventually([&] {
                return transport.Stats().ReservedBytes == 0;
            });
            UNIT_ASSERT_VALUES_EQUAL(context.Budget->Dropped.load(), 1);
            UNIT_ASSERT(collector.Snapshot().empty());
        }
    }

    Y_UNIT_TEST(HardShutdownWithTracingAndLateAuthCallback) {
        for (bool auth : { false, true }) {
            for (ui32 binding : { 0u, 1u }) {
                auto metrics = std::make_shared<TRequestMetrics>(MakeIntrusive<NMonitoring::TDynamicCounters>());
                TExecutorFixture fixture({}, metrics);
                THttpMock http;
                TGrpcMock grpc;
                auto bindings = Bindings(http, grpc);
                auto provider = std::make_shared<TTvmTestProvider>();
                provider->State->Pending = true;
                if (auth) {
                    SetTvm(bindings[binding], provider);
                }
                TTransport transport(std::move(bindings), fixture.Executor);
                // No uploader: export is best effort and cannot impede execution or
                // teardown. Shutdown must finalize spans before their system dies.
                transport.SetTraceContext(TTraceContext::Create(NWilson::TTraceId::NewTraceId(15, 64), fixture.Runtime.GetActorSystem(0)));
                transport.Start(1, EOperationKind::Request,
                    MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST", binding ? "" : "/call",
                        "lookup"), TInstant::Now() + TDuration::Seconds(30), [](auto, auto) {
                        return true;
                    });
                if (auth) {
                    Eventually([&] {
                        return provider->Count() == 1;
                    });
                } else {
                    (binding ? grpc.State : http.State)->Wait();
                }
                fixture.Runtime.GetActorSystem(0)->Cleanup();
                if (auth) {
                    provider->LastCompletion({ .Status = ETvmTicketStatus::Ready, .Ticket = "late-secret" });
                }
                UNIT_ASSERT_VALUES_EQUAL(fixture.Executor->Stats().ReservedBytes, 0);
                auto series = metrics->Get("lookup", binding ? EMetricKind::Grpc : EMetricKind::Http);
                UNIT_ASSERT_VALUES_EQUAL(series->InFlight->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(series->Completed[3]->Val(), 1);
            }
        }
    }

    Y_UNIT_TEST(SuccessfulAuthKeepsOutcomeWhenParentIsCancelled) {
        for (ui32 binding : { 0u, 1u }) {
            TExecutorFixture fixture;
            TCollector collector(fixture);
            THttpMock http;
            TGrpcMock grpc;
            auto bindings = Bindings(http, grpc);
            auto provider = std::make_shared<TTvmTestProvider>();
            SetTvm(bindings[binding], provider);
            TTransport transport(std::move(bindings), fixture.Executor);
            transport.SetTraceContext(collector.Context());
            auto cancel = transport.Start(
                1, EOperationKind::Request, MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST",
                                                binding ? "" : "/call", "lookup"), TInstant::Now() + TDuration::Seconds(30), [](auto, auto) {
                    return true;
                });
            (binding ? grpc.State : http.State)->Wait();
            cancel();
            const auto spans = collector.Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(Attribute(spans[0], "call_alias"), "auth.tvm");
            UNIT_ASSERT_VALUES_EQUAL(Attribute(spans[0], "outcome"), "success");
            UNIT_ASSERT_VALUES_EQUAL(Attribute(spans[1], "outcome"), "cancelled");
            UNIT_ASSERT_VALUES_EQUAL(spans[0].parent_span_id(), spans[1].span_id());
        }
    }

    Y_UNIT_TEST(OwnerShutdownCompletesAuthAndTransportOnce) {
        for (ui32 binding : { 0u, 1u }) {
            TExecutorFixture fixture;
            TCollector collector(fixture);
            THttpMock http;
            TGrpcMock grpc;
            auto bindings = Bindings(http, grpc);
            auto provider = std::make_shared<TTvmTestProvider>();
            provider->State->Pending = true;
            SetTvm(bindings[binding], provider);
            auto transport = std::make_unique<TTransport>(std::move(bindings), fixture.Executor);
            transport->SetTraceContext(collector.Context());
            transport->Start(
                1, EOperationKind::Request, MakeRequest(binding, Proto("input"), binding ? "/NFq.NWasmServices.NTest.MockService/Call" : "POST",
                                                binding ? "" : "/call", "lookup"), TInstant::Now() + TDuration::Seconds(30), [](auto, auto) {
                    return true;
                });
            Eventually([&] {
                return provider->Count() == 1;
            });
            {
                TUserPoolGate gate(fixture);
                transport.reset();
                auto spans = collector.Snapshot();
                UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
                for (const auto& span : spans) {
                    UNIT_ASSERT_VALUES_EQUAL(Attribute(span, "outcome"), "cancelled");
                }
            }
            provider->LastCompletion({ .Status = ETvmTicketStatus::Ready, .Ticket = "late-secret" });
            Eventually([&] {
                return fixture.Executor->Stats().ReservedBytes == 0;
            });
            UNIT_ASSERT_VALUES_EQUAL(collector.Snapshot().size(), 2);
        }
    }

    Y_UNIT_TEST(CancellationAndDeadlineFinishBeforePoolCleanupIncludingAuth) {
        for (bool auth : { false, true }) {
            for (ui32 binding : { 0u, 1u }) {
                for (bool timeout : { false, true }) {
                    TExecutorFixture fixture;
                    TCollector collector(fixture);
                    THttpMock http;
                    TGrpcMock grpc;
                    auto bindings = Bindings(http, grpc);
                    auto provider = std::make_shared<TTvmTestProvider>();
                    provider->State->Pending = true;
                    if (auth) {
                        SetTvm(bindings[binding], provider);
                    }
                    TEnv env(std::move(bindings), {}, "/fq_transport_coroutine.wasm", fixture.Executor);
                    env.Transport->SetTraceContext(collector.Context());
                    const auto deadline = TInstant::Now() + TDuration::Seconds(2);
                    const auto call = env.Start(0, binding, 1, Proto("input"), deadline);
                    UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
                    if (auth) {
                        Eventually([&] {
                            return provider->Count() == 1;
                        });
                    } else {
                        (binding ? grpc.State : http.State)->Wait();
                    }
                    {
                        TUserPoolGate gate(fixture);
                        if (timeout) {
                            const auto now = TInstant::Now();
                            if (now < deadline) {
                                Sleep(deadline - now + TDuration::MilliSeconds(10));
                            }
                            env.Runtime->Poll(call);
                        } else {
                            env.Runtime->Cancel(call);
                        }
                        auto spans = collector.Snapshot();
                        UNIT_ASSERT_VALUES_EQUAL(spans.size(), auth ? 2 : 1);
                        for (const auto& span : spans) {
                            UNIT_ASSERT_VALUES_EQUAL(Attribute(span, "outcome"), timeout ? "timeout" : "cancelled");
                        }
                        if (auth) {
                            UNIT_ASSERT_VALUES_EQUAL(spans[1].parent_span_id(), spans[0].span_id());
                        }
                        UNIT_ASSERT(env.Transport->Stats().ReservedBytes > 0);
                    }
                    if (auth) {
                        provider->LastCompletion({ .Status = ETvmTicketStatus::Ready, .Ticket = "late-secret" });
                    }
                    env.Clean(call);
                    UNIT_ASSERT_VALUES_EQUAL(collector.Snapshot().size(), auth ? 2 : 1);
                }
            }
        }
    }
}
