#include <ydb/core/tx/datashard/hnsw_index/hnsw_index_build_actor.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>

namespace NKikimr::NDataShard {
    namespace {

        using namespace NActors;
        using TRows = std::vector<std::pair<TString, TString>>;

        TString Vector(std::initializer_list<float> values) {
            TString result;
            TStringOutput output(result);
            NKnnVectorSerialization::TSerializer<float> serializer(&output);
            for (float value : values) {
                serializer.HandleElement(value);
            }
            serializer.Finish();
            return result;
        }

        TRows Rows() {
            return {{"near", Vector({0, 0})}, {"far", Vector({10, 0})}};
        }

        struct TCompletion {
            size_t Calls = 0;
            TActorId Actor;
            std::thread::id Thread;
            std::optional<THnswIndexBuildResult> Result;
        };

        struct TJob {
            TActorId Actor;
            std::shared_ptr<TCompletion> Completion;
            std::shared_ptr<std::atomic<bool>> Cancelled;
        };

        struct TFixture {
            TTestBasicRuntime Runtime;
            Ydb::Table::VectorIndexSettings Settings;

            TFixture() {
                Runtime.Initialize(TAppPrepare().Unwrap());
                Settings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
                Settings.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN);
                Settings.set_vector_dimension(2);
            }

            TJob Start(TRows rows = Rows(), ui64 budget = 0, bool allowEmpty = false,
                       std::shared_ptr<void> reservation = {}, std::function<void(ui64)> reduce = {},
                       std::shared_ptr<std::atomic<bool>> cancelled = {}, bool throwInCallback = false) {
                auto completion = std::make_shared<TCompletion>();
                if (!cancelled) {
                    cancelled = std::make_shared<std::atomic<bool>>(false);
                }
                const auto actor = Runtime.Register(CreateHnswIndexBuildWorker(Settings,
                                                                               std::move(rows), std::move(reservation), budget,
                                                                               [completion, throwInCallback](THnswIndexBuildResult&& result, const TActorContext& ctx) {
                                                                                   ++completion->Calls;
                                                                                   completion->Actor = ctx.SelfID;
                                                                                   completion->Thread = std::this_thread::get_id();
                                                                                   completion->Result.emplace(std::move(result));
                                                                                   if (throwInCallback) {
                                                                                       throw std::runtime_error("injected completion exception");
                                                                                   }
                                                                               }, allowEmpty, cancelled, std::move(reduce)));
                return {actor, std::move(completion), std::move(cancelled)};
            }

            template <typename TCondition>
            void WaitFor(TStringBuf description, const TCondition& condition) {
                // An empty minimal runtime has no timer/mailbox keeping DispatchEvents
                // alive while a native build thread is running. Pump one message at a
                // time and give external work real wall-clock time to deliver its reply.
                TDispatchOptions options;
                options.Quiet = true;
                options.CustomFinalCondition = [] { return true; };
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
                while (!condition()) {
                    Runtime.DispatchEvents(options);
                    if (condition()) {
                        return;
                    }
                    UNIT_ASSERT_C(std::chrono::steady_clock::now() < deadline, "Timeout waiting for " << description);
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }

            THnswIndexBuildResult& Wait(const TJob& job) {
                WaitFor("HNSW worker completion", [&] { return job.Completion->Result.has_value(); });
                UNIT_ASSERT_VALUES_EQUAL(job.Completion->Calls, 1);
                UNIT_ASSERT(job.Completion->Actor == job.Actor);
                UNIT_ASSERT(job.Completion->Thread == std::this_thread::get_id());
                WaitFor("HNSW worker termination", [&] { return Runtime.FindActor(job.Actor) == nullptr; });
                return *job.Completion->Result;
            }
        };

        struct TReservation {
            struct TState {
                const std::thread::id Caller = std::this_thread::get_id();
                std::atomic<bool> ReleasedOffCaller = false;
                std::promise<void> Promise;
                std::shared_future<void> Released = Promise.get_future().share();
            };

            std::shared_ptr<TState> State = std::make_shared<TState>();
            std::shared_ptr<void> Memory = std::shared_ptr<void>(new int(0), [state = State](void* value) {
                delete static_cast<int*>(value);
                state->ReleasedOffCaller.store(std::this_thread::get_id() != state->Caller);
                state->Promise.set_value();
            });

            bool Released() const {
                return State->Released.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
            }

            void Wait() const {
                UNIT_ASSERT_C(State->Released.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
                              "Timeout waiting for HNSW reservation release");
            }
        };

        // Hold real CPU jobs without blocking actor dispatch. Every test opens the gate
        // on scope exit too, so a failed assertion cannot strand a build thread.
        struct TBuildGate {
            std::atomic<size_t> Entered = 0;
            std::atomic<size_t> Active = 0;
            std::atomic<size_t> MaxActive = 0;
            std::mutex Mutex;
            std::condition_variable Condition;
            bool Open = false;

            void Wait() {
                const auto active = Active.fetch_add(1) + 1;
                auto maximum = MaxActive.load();
                while (maximum < active && !MaxActive.compare_exchange_weak(maximum, active)) {
                }
                ++Entered;
                std::unique_lock lock(Mutex);
                Condition.wait(lock, [&] { return Open; });
                --Active;
            }

            void Release() {
                std::lock_guard lock(Mutex);
                Open = true;
                Condition.notify_all();
            }
        };

        struct TReleaseGate {
            std::shared_ptr<TBuildGate> Gate;
            ~TReleaseGate() {
                Gate->Release();
            }
        };

    } // namespace

    Y_UNIT_TEST_SUITE(HnswIndexBuildActor) {
        Y_UNIT_TEST(BuildsSearchableGraphAndRetainsReducedReservationUntilDestruction) {
            TFixture fixture;
            TReservation reservation;
            const auto caller = std::this_thread::get_id();
            std::atomic<size_t> reductions = 0;
            std::atomic<ui64> reducedBytes = 0;
            std::atomic<bool> reducedOffCaller = false;
            const auto job = fixture.Start(Rows(), 0, false, std::move(reservation.Memory), [&](ui64 bytes) {
                reducedBytes.store(bytes);
                reducedOffCaller.store(std::this_thread::get_id() != caller);
                ++reductions;
            });
            auto& result = fixture.Wait(job);
            UNIT_ASSERT_C(result.Index, result.Error);
            UNIT_ASSERT(result.Error.empty());
            UNIT_ASSERT(result.MemoryReservation);
            UNIT_ASSERT_VALUES_EQUAL(result.Index->Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(result.Index->Search(Vector({0, 0}), 1).Results.front().first, "near");
            UNIT_ASSERT_VALUES_EQUAL(reductions.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(reducedBytes.load(), result.Index->EstimatedMemoryBytes());
            UNIT_ASSERT(reducedOffCaller.load());
            result.MemoryReservation.reset();
            UNIT_ASSERT(!reservation.Released());
            result.Index.reset();
            reservation.Wait();
            UNIT_ASSERT(reservation.State->ReleasedOffCaller.load());
        }

        Y_UNIT_TEST(ReportsBudgetFailureAndReleasesReservation) {
            TFixture fixture;
            TReservation reservation;
            const auto job = fixture.Start(Rows(), 1, false, std::move(reservation.Memory));
            const auto& result = fixture.Wait(job);
            UNIT_ASSERT(!result.Index);
            UNIT_ASSERT(!result.MemoryReservation);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "exceeds budget");
            reservation.Wait();
        }

        Y_UNIT_TEST(ReportsRejectedSettingsWithoutCrashingActorSystem) {
            TFixture fixture;
            fixture.Settings.set_m(1);
            const auto failed = fixture.Start();
            const auto& failure = fixture.Wait(failed);
            UNIT_ASSERT(!failure.Index);
            UNIT_ASSERT_STRING_CONTAINS(failure.Error, "Invalid HNSW settings");
            fixture.Settings.set_m(2);
            const auto healthy = fixture.Start();
            const auto& result = fixture.Wait(healthy);
            UNIT_ASSERT_C(result.Index, result.Error);
        }

        Y_UNIT_TEST(EmptyGenerationRequiresExplicitPermission) {
            TFixture fixture;
            const auto rejected = fixture.Start({}, 0, false);
            const auto& failure = fixture.Wait(rejected);
            UNIT_ASSERT(!failure.Index);
            UNIT_ASSERT_STRING_CONTAINS(failure.Error, "No vectors");
            const auto allowed = fixture.Start({}, 0, true);
            const auto& result = fixture.Wait(allowed);
            UNIT_ASSERT_C(result.Index, result.Error);
            UNIT_ASSERT_VALUES_EQUAL(result.Index->Size(), 0);
            UNIT_ASSERT(result.Index->Search(Vector({0, 0}), 1).Results.empty());
        }

        Y_UNIT_TEST(PreCancelledBuildReturnsErrorAndReleasesReservation) {
            TFixture fixture;
            TReservation reservation;
            auto cancelled = std::make_shared<std::atomic<bool>>(true);
            const auto job = fixture.Start(Rows(), 0, false, std::move(reservation.Memory), {}, cancelled);
            const auto& result = fixture.Wait(job);
            UNIT_ASSERT(!result.Index);
            UNIT_ASSERT(!result.MemoryReservation);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "cancelled");
            reservation.Wait();
        }

        Y_UNIT_TEST(ReductionExceptionBecomesFailedBuildAndReleasesReservation) {
            TFixture fixture;
            TReservation reservation;
            const auto job = fixture.Start(Rows(), 0, false, std::move(reservation.Memory), [](ui64) {
                throw std::runtime_error("injected reduction exception");
            });
            const auto& result = fixture.Wait(job);
            UNIT_ASSERT(!result.Index);
            UNIT_ASSERT(!result.MemoryReservation);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "injected reduction exception");
            reservation.Wait();
        }

        Y_UNIT_TEST(PoisonRepliesOnceWhileRunningJobStillOwnsReservation) {
            TFixture fixture;
            auto gate = std::make_shared<TBuildGate>();
            const TReleaseGate release{gate};
            TReservation reservation;
            const auto job = fixture.Start(Rows(), 0, false, std::move(reservation.Memory), [gate](ui64) { gate->Wait(); });
            fixture.WaitFor("running HNSW CPU job", [&] { return gate->Entered.load() == 1; });
            fixture.Runtime.Send(new IEventHandle(job.Actor, {}, new TEvents::TEvPoison()));
            const auto& result = fixture.Wait(job);
            UNIT_ASSERT(!result.Index);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "cancelled");
            UNIT_ASSERT(job.Cancelled->load());
            UNIT_ASSERT(!reservation.Released());
            gate->Release();
            reservation.Wait();
            UNIT_ASSERT_VALUES_EQUAL(job.Completion->Calls, 1);
        }

        Y_UNIT_TEST(RuntimeShutdownCancelsOutstandingJobWithoutLateCallback) {
            auto fixture = std::make_unique<TFixture>();
            auto gate = std::make_shared<TBuildGate>();
            const TReleaseGate release{gate};
            TReservation reservation;
            const auto job = fixture->Start(Rows(), 0, false, std::move(reservation.Memory), [gate](ui64) { gate->Wait(); });
            fixture->WaitFor("running HNSW CPU job", [&] { return gate->Entered.load() == 1; });
            fixture.reset();
            UNIT_ASSERT(job.Cancelled->load());
            UNIT_ASSERT(!reservation.Released());
            gate->Release();
            UNIT_ASSERT(reservation.State->Released.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            UNIT_ASSERT_VALUES_EQUAL(job.Completion->Calls, 0);
        }

        Y_UNIT_TEST(CompletionExceptionDoesNotInvokeCallbackTwice) {
            TFixture fixture;
            const auto job = fixture.Start(Rows(), 0, false, {}, {}, {}, true);
            UNIT_ASSERT(fixture.Wait(job).Index);
            UNIT_ASSERT_VALUES_EQUAL(job.Completion->Calls, 1);
            UNIT_ASSERT(job.Cancelled->load());
        }

        Y_UNIT_TEST(BoundsCpuConcurrencyAndRejectsOverflowingQueue) {
            TFixture fixture;
            auto gate = std::make_shared<TBuildGate>();
            const TReleaseGate release{gate};
            std::vector<TJob> jobs;
            std::vector<TReservation> reservations(19);
            for (size_t i = 0; i < 2; ++i) {
                jobs.push_back(fixture.Start(Rows(), 0, false, std::move(reservations[i].Memory),
                                             [gate](ui64) { gate->Wait(); }));
            }
            fixture.WaitFor("two active HNSW CPU jobs", [&] { return gate->Entered.load() == 2; });
            for (size_t i = 2; i < reservations.size(); ++i) {
                jobs.push_back(fixture.Start(Rows(), 0, false, std::move(reservations[i].Memory),
                                             [gate](ui64) { gate->Wait(); }));
            }
            auto completed = [&] {
                size_t count = 0;
                for (const auto& job : jobs) {
                    count += job.Completion->Calls;
                }
                return count;
            };
            fixture.WaitFor("HNSW build queue rejection", [&] { return completed() != 0; });
            UNIT_ASSERT_VALUES_EQUAL(completed(), 1);
            UNIT_ASSERT_VALUES_EQUAL(gate->Entered.load(), 2);
            gate->Release();
            size_t successes = 0;
            size_t failures = 0;
            for (const auto& job : jobs) {
                auto& result = fixture.Wait(job);
                if (result.Index) {
                    ++successes;
                    result.Index.reset();
                    result.MemoryReservation.reset();
                } else {
                    ++failures;
                    UNIT_ASSERT_STRING_CONTAINS(result.Error, "queue is full");
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(successes, 18);
            UNIT_ASSERT_VALUES_EQUAL(failures, 1);
            UNIT_ASSERT_VALUES_EQUAL(gate->MaxActive.load(), 2);
            for (const auto& reservation : reservations) {
                reservation.Wait();
            }
        }
    } // Y_UNIT_TEST_SUITE(HnswIndexBuildActor)

} // namespace NKikimr::NDataShard
