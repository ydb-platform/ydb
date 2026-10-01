#include "actor_benchmark_helper.h"
#include "subsystems/inmemory_metrics.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>
#include <thread>
#include <array>

using namespace NActors;
using namespace NActors::NTests;

Y_UNIT_TEST_SUITE(InMemoryMetricsSubsystem) {
    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<>;

    void Configure(TTestActorRuntimeBase* runtime, TInMemoryMetricsConfig config) {
        runtime->SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
        runtime->SetupNodeSubSystems = [config = std::move(config)](ui32, TActorSystemSetup* setup) {
            setup->RegisterSubSystem(MakeInMemoryMetricsRegistry(config));
        };
        runtime->Initialize();
    }

    TInMemorySnapshot Fetch(TTestActorRuntimeBase* runtime, TInMemoryMetricsStats* stats = nullptr) {
        auto* registry = GetInMemoryMetrics(*runtime->GetActorSystem(0));
        const auto edge = runtime->AllocateEdgeActor();
        UNIT_ASSERT(registry->RequestSnapshot(edge, 123));
        auto reply = runtime->GrabEdgeEventRethrow<TEvInMemoryMetricsSnapshot>(edge, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, 123);
        if (stats) {
            *stats = reply->Get()->Stats;
        }
        return reply->Get()->Snapshot;
    }

    Y_UNIT_TEST(SubsystemAccessor) {
        auto setup = TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, false, false);
        setup->RegisterSubSystem(MakeInMemoryMetricsRegistry({.MemoryBytes = 4096, .ChunkSizeBytes = 256, .MaxLines = 4}));
        TActorSystem actorSystem(setup);
        UNIT_ASSERT_VALUES_EQUAL(GetInMemoryMetrics(actorSystem)->GetConfig().MemoryBytes, 4096);
        UNIT_ASSERT_EQUAL(GetInMemoryMetrics(), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(GetMetricSystem(actorSystem), GetInMemoryMetrics(actorSystem));
    }

    Y_UNIT_TEST(AsyncRegistrationRefillAndClose) {
        TTestActorRuntimeBase runtime;
        Configure(&runtime, {.MemoryBytes = 64 * 16, .ChunkSizeBytes = 64, .MaxLines = 1, .ReserveChunks = 3, .AllowedMetricPrefixes = {"user."}});
        auto* registry = GetInMemoryMetrics(*runtime.GetActorSystem(0));
        auto line = registry->CreateLine("user.line", {});
        UNIT_ASSERT(line);
        UNIT_ASSERT_VALUES_EQUAL(line.GetLineId(), 0);
        UNIT_ASSERT(!line.Append(0));
        Fetch(&runtime);
        UNIT_ASSERT(line.GetLineId());
        for (ui64 i = 1; i <= 9; ++i) {
            UNIT_ASSERT(line.Append(i));
        }
        UNIT_ASSERT(!line.Append(10));
        Fetch(&runtime);
        UNIT_ASSERT(line.Append(10));
        line.Close();
        auto snapshot = Fetch(&runtime);
        snapshot.Read([](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 1);
            const auto& item = view.GetLine(0);
            UNIT_ASSERT(item.Closed);
            const auto values = item.ReadValuesAs<ui64>();
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 10);
            for (ui32 i = 0; i < values.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(values[i], i + 1);
            }
        });
    }

    Y_UNIT_TEST(CloseBeforeRegistrationAndDuplicateRejection) {
        TTestActorRuntimeBase runtime;
        Configure(&runtime, {.MemoryBytes = 4096, .ChunkSizeBytes = 64, .MaxLines = 1, .AllowedMetricPrefixes = {"user."}});
        auto* registry = GetInMemoryMetrics(*runtime.GetActorSystem(0));
        auto cancelled = registry->CreateLine("user.cancelled", {});
        cancelled.Close();
        const TVector<TLabel> labels = {{"b", "2"}, {"a", "1"}};
        const TVector<TLabel> reverse = {{"a", "1"}, {"b", "2"}};
        auto first = registry->CreateLine("user.same", labels);
        auto duplicate = registry->CreateLine("user.same", reverse);
        UNIT_ASSERT(duplicate); // Outcome is not known until actor processing.
        TInMemoryMetricsStats stats;
        Fetch(&runtime, &stats);
        UNIT_ASSERT_VALUES_EQUAL(stats.Lines, 1);
        UNIT_ASSERT(first);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(!duplicate.Append(42));
        UNIT_ASSERT(first.Append(42));
    }

    Y_UNIT_TEST(BoundedCommandQueueAndLabelsOrder) {
        TTestActorRuntimeBase runtime;
        Configure(&runtime, {.MemoryBytes = 4096, .ChunkSizeBytes = 64, .MaxLines = 4, .MaxPendingRequests = 2, .AllowedMetricPrefixes = {"user."}});
        auto* registry = GetInMemoryMetrics(*runtime.GetActorSystem(0));
        auto first = registry->CreateLine("user.first", {});
        const TVector<TLabel> labels = {{"service", "before"}};
        UNIT_ASSERT(registry->SetCommonLabels(labels));
        auto rejected = registry->CreateLine("user.overflow", {});
        UNIT_ASSERT(!rejected);
        UNIT_ASSERT(!registry->RequestSnapshot(runtime.AllocateEdgeActor()));
        TDispatchOptions dispatch;
        dispatch.CustomFinalCondition = [&] { return first.GetLineId() != 0; };
        runtime.DispatchEvents(dispatch, TDuration::Seconds(5));
        auto old = Fetch(&runtime);
        const TVector<TLabel> changed = {{"service", "after"}};
        UNIT_ASSERT(registry->SetCommonLabels(changed));
        auto current = Fetch(&runtime);
        old.Read([](const TSnapshotView& view) { UNIT_ASSERT_VALUES_EQUAL(view.GetCommonLabel(0).Value, "before"); });
        current.Read([](const TSnapshotView& view) { UNIT_ASSERT_VALUES_EQUAL(view.GetCommonLabel(0).Value, "after"); });
    }

    Y_UNIT_TEST(ShutdownCancelsUndeliveredRegistration) {
        TTestActorRuntimeBase runtime;
        Configure(&runtime, {.MemoryBytes = 4096, .ChunkSizeBytes = 64, .MaxLines = 4});
        auto* registry = GetInMemoryMetrics(*runtime.GetActorSystem(0));
        auto pending = registry->CreateLine("pending", {});
        UNIT_ASSERT(pending);
        runtime.GetActorSystem(0)->Stop();
        UNIT_ASSERT(!pending);
        UNIT_ASSERT(!pending.Append(1));
        UNIT_ASSERT(!registry->CreateLine("after-stop", {}));
        UNIT_ASSERT(!registry->RequestSnapshot(runtime.AllocateEdgeActor()));
    }

    Y_UNIT_TEST(SnapshotSurvivesRuntimeDestruction) {
        TInMemorySnapshot snapshot;
        {
            TTestActorRuntimeBase runtime;
            Configure(&runtime, {.MemoryBytes = 1024, .ChunkSizeBytes = 64, .MaxLines = 1, .AllowedMetricPrefixes = {"user."}});
            auto* registry = GetInMemoryMetrics(*runtime.GetActorSystem(0));
            auto line = registry->CreateLine("user.survivor", {});
            Fetch(&runtime);
            UNIT_ASSERT(line.Append(42));
            snapshot = Fetch(&runtime);
            line.Close();
            // No dispatch of Close: the shutdown hook performs final cleanup.
            runtime.GetActorSystem(0)->Stop();
        }
        snapshot.Read([](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.GetLine(0).ReadValuesAs<ui64>().front(), 42);
        });
        snapshot = {};
    }

    Y_UNIT_TEST(ConcurrentProducersRegisterThroughActor) {
        TTestActorRuntimeBase runtime;
        Configure(&runtime, {.MemoryBytes = 64 * 1024, .ChunkSizeBytes = 64, .MaxLines = 256, .AllowedMetricPrefixes = {"user."}});
        auto* registry = GetInMemoryMetrics(*runtime.GetActorSystem(0));
        std::array<TVector<TLine<TRawLineFrontend<>>>, 4> lines;
        std::array<std::thread, 4> producers;
        for (ui32 id = 0; id < producers.size(); ++id) {
            producers[id] = std::thread([&, id] {
                for (ui32 i = 0; i < 32; ++i) {
                    lines[id].push_back(registry->CreateLine(TStringBuilder() << "user." << id << "." << i, {}));
                }
            });
        }
        for (auto& producer : producers) {
            producer.join();
        }
        TInMemoryMetricsStats stats;
        Fetch(&runtime, &stats);
        UNIT_ASSERT_VALUES_EQUAL(stats.Lines, 128);
        for (auto& group : lines) {
            for (auto& line : group) {
                UNIT_ASSERT(line.GetLineId());
                UNIT_ASSERT(line.Append(42));
                line.Close();
            }
        }
        auto snapshot = Fetch(&runtime, &stats);
        UNIT_ASSERT_VALUES_EQUAL(stats.ClosedLines, 128);
        snapshot.Read([](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 128);
            view.ForEachLine([](const TLineSnapshot& line) {
                UNIT_ASSERT_VALUES_EQUAL(line.ReadValuesAs<ui64>().front(), 42);
            });
        });
    }

    Y_UNIT_TEST(ActorAccessorAndReply) {
        class TClient final : public TActorBootstrapped<TClient> {
        public:
            explicit TClient(const TActorId& edge) : Edge(edge) {}
            void Bootstrap() {
                UNIT_ASSERT(GetInMemoryMetrics());
                Line = GetInMemoryMetrics()->CreateLine("user.actor", {});
                UNIT_ASSERT(!Line.Append(42));
                Become(&TThis::StateWork);
                UNIT_ASSERT(GetInMemoryMetrics()->RequestSnapshot(SelfId()));
            }
            void Handle(TEvInMemoryMetricsSnapshot::TPtr&) {
                if (!Written) {
                    if (Line.Append(42)) {
                        Written = true;
                        Line.Close();
                    }
                    UNIT_ASSERT(GetInMemoryMetrics()->RequestSnapshot(SelfId()));
                } else {
                    Send(Edge, new TEvents::TEvWakeup());
                    PassAway();
                }
            }
            STRICT_STFUNC(StateWork,
                hFunc(TEvInMemoryMetricsSnapshot, Handle);
            )
        private:
            TActorId Edge;
            TLine<TRawLineFrontend<>> Line;
            bool Written = false;
        };
        TTestActorRuntimeBase runtime;
        Configure(&runtime, {.MemoryBytes = 4096, .ChunkSizeBytes = 64, .MaxLines = 16});
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(new TClient(edge));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge, TDuration::Seconds(5));
        auto snapshot = Fetch(&runtime);
        snapshot.Read([](const TSnapshotView& view) {
            bool found = false;
            view.ForEachLine([&](const TLineSnapshot& line) {
                if (line.Name == "user.actor") {
                    found = line.Closed && line.ReadValuesAs<ui64>().front() == 42;
                }
            });
            UNIT_ASSERT(found);
        });
    }
    Y_UNIT_TEST(TargetedSnapshotByIdAndCanonicalKey) {
        TTestActorRuntimeBase runtime;
        Configure(&runtime, {.MemoryBytes = 64 * 16, .ChunkSizeBytes = 64,
            .MaxLines = 2, .AllowedMetricPrefixes = {"user."}});
        auto* registry = GetInMemoryMetrics(*runtime.GetActorSystem(0));
        const TVector<TLabel> labels = {{"b", "2"}, {"a", "1"}};
        const TVector<TLabel> reversed = {{"a", "1"}, {"b", "2"}};
        auto first = registry->CreateLine("user.same", labels);
        auto second = registry->CreateLine("user.same", {});
        Fetch(&runtime);
        UNIT_ASSERT(first.Append(42));
        UNIT_ASSERT(second.Append(99));
        const auto edge = runtime.AllocateEdgeActor();
        UNIT_ASSERT(registry->RequestLineSnapshot(edge, first.GetLineId(), 101));
        UNIT_ASSERT(registry->RequestLineSnapshot(edge, "user.same", reversed, 102));
        UNIT_ASSERT(registry->RequestLineSnapshot(edge, 0, 103));
        UNIT_ASSERT(registry->RequestLineSnapshot(edge, "user.missing", {}, 104));
        for (ui64 cookie = 101; cookie <= 104; ++cookie) {
            auto reply = runtime.GrabEdgeEventRethrow<TEvInMemoryMetricsSnapshot>(edge, TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, cookie);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Stats.Lines, 2);
            reply->Get()->Snapshot.Read([&](const TSnapshotView& view) {
                UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), cookie < 103 ? 1 : 0);
                if (cookie < 103) {
                    UNIT_ASSERT_VALUES_EQUAL(view.GetLine(0).LineId, first.GetLineId());
                    UNIT_ASSERT_VALUES_EQUAL(view.GetLine(0).ReadValuesAs<ui64>().front(), 42);
                }
            });
        }
    }

}
