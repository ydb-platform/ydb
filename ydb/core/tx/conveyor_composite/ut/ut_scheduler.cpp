#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/tree/snapshot.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/conveyor_composite/service/manager.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>
#include <barrier>
#include <deque>
#include <thread>

namespace NKikimr::NConveyorComposite {

    namespace {

        using TLinkConfig = std::pair<ESpecialTaskCategory, double>;

        // Worker requests and timers may have no sender. TBlockEvents logs actor names and
        // cannot hold those events; this blocker only captures and replays their envelopes.
        template <class TEvent>
        class TWorkerEventBlocker: public std::deque<typename TEvent::TPtr> {
            NActors::TTestActorRuntime& Runtime;
            THashSet<NActors::IEventHandle*> Replayed;
            NActors::TTestActorRuntime::TEventObserverHolder Holder;
            bool Stopped = false;

        public:
            explicit TWorkerEventBlocker(NActors::TTestActorRuntime& runtime,
                                         std::function<bool(const typename TEvent::TPtr&)> predicate = {})
                : Runtime(runtime)
                , Holder(Runtime.AddObserver<TEvent>([this, predicate = std::move(predicate)](auto& ev) {
                    if (!Replayed.erase(ev.Get()) && (!predicate || predicate(ev))) {
                        this->emplace_back(std::move(ev));
                    }
                }))
            {
            }

            TWorkerEventBlocker& Stop() {
                Holder.Remove();
                Stopped = true;
                Replayed.clear();
                return *this;
            }

            TWorkerEventBlocker& Unblock(size_t count = Max<size_t>()) {
                while (count-- && !this->empty()) {
                    auto& ev = this->front();
                    if (!Stopped) {
                        Replayed.insert(ev.Get());
                    }
                    Runtime.Send(ev.Release(), 0, true);
                    this->pop_front();
                }
                return *this;
            }
        };

        TSchedulerQueryIdentity MakeIdentity(const ui64 queryId) {
            return {queryId};
        }

        NKqp::NScheduler::NHdrf::TFullPoolId MakeSchedulerPool() {
            return {"database", "pool"};
        }

        void SetCapacity(TQueryRegistry& registry, const TSchedulerQueryIdentity& identity, ui64 count) {
            registry.PrepareWorkCapacity(identity, count);
            registry.ApplyWorkCapacity(identity);
        }

        NKikimrConfig::TCompositeConveyorConfig BuildConfig(
            const std::vector<double>& workersCounts, const std::vector<std::vector<TLinkConfig>>& links) {
            NKikimrConfig::TCompositeConveyorConfig result;
            result.SetEnabled(true);
            for (ui64 poolIdx = 0; poolIdx < workersCounts.size(); ++poolIdx) {
                auto* pool = result.AddWorkerPools();
                pool->SetName("pool-" + ::ToString(poolIdx));
                pool->SetWorkersCount(workersCounts[poolIdx]);
                pool->SetSchedulingMode(NConfig::TProtoWorkerPool::All);
                for (const auto& [category, weight] : links[poolIdx]) {
                    auto* link = pool->AddLinks();
                    link->SetCategory(::ToString(category));
                    link->SetWeight(weight);
                }
            }
            return result;
        }

        NConfig::TConfig ParseConfig(const NKikimrConfig::TCompositeConveyorConfig& proto) {
            auto result = NConfig::TConfig::BuildFromProto(proto);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetErrorMessage());
            return result.DetachResult();
        }

        // Deterministic drain tests need an actor context, but no Distributor event ordering.
        struct TManagerFixture {
            NActors::TTestActorRuntime Runtime;
            NActors::TActorId Sink;
            TCounters Counters{"MANAGER_TEST", MakeIntrusive<NMonitoring::TDynamicCounters>()};
            std::vector<TInstant> Deadlines;

            TManagerFixture() {
                Runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
                Runtime.UpdateCurrentTime(TInstant::FromValue(TMonotonic::Now().GetValue()), true);
                Sink = Runtime.AllocateEdgeActor();
                Runtime.SetScheduledEventFilter([this](auto&, auto& ev, TDuration, TInstant& deadline) {
                    if (ev->Recipient != Sink || ev->GetTypeRewrite() != NActors::TEvents::TEvWakeup::EventType) {
                        return false;
                    }
                    Deadlines.push_back(deadline);
                    return true;
                });
            }

            template <class TCallback>
            void Run(const NKikimrConfig::TCompositeConveyorConfig& proto, TCallback&& callback) {
                Runtime.RunCall([&] {
                    TTasksManager manager("MANAGER_TEST", ParseConfig(proto), Sink, Counters);
                    callback(manager);
                    return true;
                });
            }
        };

        struct TTestSchedulerQuery {
            std::shared_ptr<NKqp::NScheduler::TDelayParams> DelayParams;
            NKqp::NScheduler::NHdrf::NDynamic::TRootPtr Root;
            NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr Query;
            NKqp::NScheduler::NHdrf::NSnapshot::TQueryPtr Snapshot;

            void SetFairShare(const ui64 fairShare) {
                Snapshot->FairShare = fairShare;
            }
        };

        TTestSchedulerQuery MakeSchedulerQuery(
            const TSchedulerQueryIdentity& identity, const TDuration delay = TDuration::MicroSeconds(10), const ui64 fairShare = 1,
            const std::optional<TDuration> minDelay = std::nullopt) {
            using namespace NKqp::NScheduler;
            auto delayParams = std::make_shared<TDelayParams>(TDelayParams{
                .MaxDelay = delay,
                .MinDelay = minDelay.value_or(delay),
                .AttemptBonus = TDuration::Zero(),
                .MaxRandomDelay = TDuration::MicroSeconds(1),
            });
            auto counters = MakeIntrusive<NKqp::TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
            auto root = std::make_shared<NHdrf::NDynamic::TRoot>(counters);
            auto database = std::make_shared<NHdrf::NDynamic::TDatabase>("database");
            auto pool = std::make_shared<NHdrf::NDynamic::TPool>("pool", counters);
            auto query = std::make_shared<NHdrf::NDynamic::TQuery>(identity.QueryId, delayParams.get(), false);
            root->AddDatabase(database);
            database->AddPool(pool);
            pool->AddQuery(query);
            auto snapshot = std::make_shared<NHdrf::NSnapshot::TQuery>(identity.QueryId, query);
            snapshot->FairShare = fairShare;
            query->SetSnapshot(snapshot);
            return {
                .DelayParams = std::move(delayParams),
                .Root = std::move(root),
                .Query = std::move(query),
                .Snapshot = std::move(snapshot),
            };
        }

        class TCounterTask final: public ITask {
        private:
            TAtomicCounter& Counter;
            bool Executed = false;

            void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
                UNIT_ASSERT(!std::exchange(Executed, true));
                Counter.Inc();
            }

        public:
            explicit TCounterTask(TAtomicCounter& counter)
                : Counter(counter)
            {
            }

            TString GetTaskClassIdentifier() const override {
                return "SCHEDULER_TEST";
            }
        };

        class TSchedulerRuntimeFixture {
        public:
            THashMap<ui64, ui64> Adds;
            THashMap<ui64, ui64> Removes;
            ui64 HdrfEvents = 0;
            NActors::TTestActorRuntime Runtime;
            NActors::TActorId Sink;
            NActors::TActorId Distributor;
            NActors::TActorId Scheduler;
            TIntrusivePtr<NMonitoring::TDynamicCounters> Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();

            explicit TSchedulerRuntimeFixture(const NKikimrConfig::TCompositeConveyorConfig& proto,
                                              std::optional<bool> enableScheduling = true) {
                Runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
                if (enableScheduling) {
                    Runtime.GetAppData().FeatureFlags.SetEnableCompositeConveyorScheduling(*enableScheduling);
                }
                Runtime.SetEventFilter([this](NActors::TTestActorRuntimeBase&, TAutoPtr<NActors::IEventHandle>& ev) {
                    using namespace NKqp::NScheduler;
                    const auto type = ev->GetTypeRewrite();
                    HdrfEvents += type == TEvAddDatabase::EventType || type == TEvAddPool::EventType || type == TEvAddQuery::EventType || type == TEvRemoveQuery::EventType;
                    if (type == TEvAddQuery::EventType) {
                        ++Adds[ev->Get<TEvAddQuery>()->QueryId];
                    } else if (type == TEvRemoveQuery::EventType) {
                        UNIT_ASSERT(!ev->Get<TEvRemoveQuery>()->IsForceRemove);
                        ++Removes[ev->Get<TEvRemoveQuery>()->QueryId];
                    }
                    return false;
                });
                Sink = Runtime.AllocateEdgeActor();
                Scheduler = Runtime.AllocateEdgeActor();
                Runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(Runtime.GetNodeId(0)), Scheduler);
                auto config = ParseConfig(proto);
                Distributor = Runtime.Register(CreateService(config, Counters));
                Runtime.EnableScheduleForActor(Distributor, true);
                Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }

            void RegisterProcess(const ui64 processId, const TSchedulerQueryIdentity& identity,
                                 const ESpecialTaskCategory category = ESpecialTaskCategory::Scan, const TString& scopeId = "scope", double scopeLimit = 1000) {
                const auto schedulerPool = identity.IsServiceQuery ? std::nullopt : std::make_optional(MakeSchedulerPool());
                Runtime.Send(Distributor, Sink, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(scopeLimit), category, scopeId, processId, identity.QueryId, schedulerPool));
            }

            void SendQueryResponse(const NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr& query) {
                auto database = Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddDatabase>(Scheduler);
                auto pool = Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddPool>(Scheduler);
                auto request = Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddQuery>(Scheduler);
                UNIT_ASSERT_VALUES_EQUAL(database->Get()->DatabaseId, "database");
                UNIT_ASSERT_VALUES_EQUAL(pool->Get()->PoolId, "pool");
                UNIT_ASSERT_VALUES_EQUAL(request->Cookie, request->Get()->QueryId);
                auto response = std::make_unique<NKqp::NScheduler::TEvQueryResponse>();
                response->Query = query;
                Runtime.Send(new NActors::IEventHandle(request->Sender, Scheduler, response.release(), 0, request->Cookie));
            }

            void UnregisterProcess(ui64 processId, ESpecialTaskCategory category = ESpecialTaskCategory::Scan) {
                Runtime.Send(Distributor, Sink, new TEvExecution::TEvUnregisterProcess(category, processId));
            }

            void PrimeBatching() {
                auto scheduling = Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
                    Runtime.EnableScheduleForActor(ev->Recipient, true);
                });
                NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(Runtime);
                TAtomicCounter executed;
                Submit(executed, 0);
                WaitFor([&] { return !results.empty(); });
                auto& result = *results.front()->Get();
                // Feed a deterministic delivery estimate through the normal accounting event.
                auto delayed = MakeHolder<TEvInternal::TEvTaskProcessedResult>(result.DetachResults(),
                                                                               TDuration::Seconds(1), result.GetWorkerIdx(), result.GetWorkersPoolId(), result.GetQueryIdentity());
                Runtime.Send(Distributor, results.front()->Sender, delayed.Release());
                results.Stop().clear();
            }

            void SetLocalSchedulerEnabled(bool enabled) {
                auto& scheduler = Runtime.GetAppData().KqpComputeScheduler;
                if (!scheduler) {
                    scheduler = std::make_shared<NKqp::NScheduler::TComputeScheduler>(
                        MakeIntrusive<NKqp::TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()), NKqp::NScheduler::TOptions{});
                }
                scheduler->ToggleEnabled(enabled);
            }

            void UpdateSchedulingFlag(bool enabled) {
                NKikimrConfig::TFeatureFlags flags = Runtime.GetAppData().FeatureFlags;
                flags.SetEnableCompositeConveyorScheduling(enabled);
                Runtime.GetAppData().UpdateRuntimeFlags(flags);
                UNIT_ASSERT_VALUES_EQUAL(Runtime.GetAppData().FeatureFlags.GetEnableCompositeConveyorScheduling(), enabled);
            }

            auto FindNoTasks(const TString& pool, ESpecialTaskCategory category) const {
                const auto module = Counters->FindSubgroup("module_id", "COMPOSITE_CONVEYOR");
                UNIT_ASSERT(module);
                const auto poolCounters = module->FindSubgroup("pool_name", pool);
                UNIT_ASSERT(poolCounters);
                const auto categoryCounters = poolCounters->FindSubgroup("wp_category", ::ToString(category));
                UNIT_ASSERT(categoryCounters);
                const auto counter = categoryCounters->FindCounter("Deriviative/NoTasks");
                UNIT_ASSERT(counter);
                return counter;
            }

            void UpdateConfig(const NKikimrConfig::TCompositeConveyorConfig& config) {
                auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
                update->Record.MutableConfig()->MutableCompositeConveyorConfig()->CopyFrom(config);
                Runtime.Send(Distributor, Sink, update.Release());
                Runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(Sink);
            }

            void Submit(TAtomicCounter& counter, const ui64 processId, const ESpecialTaskCategory category = ESpecialTaskCategory::Scan) {
                Runtime.Send(Distributor, Sink, new TEvExecution::TEvNewTask(std::make_shared<TCounterTask>(counter), category, processId));
            }

            void WaitFor(const std::function<bool()>& predicate) {
                for (ui32 attempt = 0; attempt < 1000 && !predicate(); ++attempt) {
                    Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                }
                UNIT_ASSERT(predicate());
            }
        };

    } // namespace

    Y_UNIT_TEST_SUITE(CompositeConveyorScheduler) {
        /* Scenario:
            Register and migrate through TasksManager; index all processes, including idle ones, by full identity.
            Preserve usage, queues and scopes through rehash, migration and late accounting.
         */
        Y_UNIT_TEST(ProcessIndexPreservesEligibilityAndLifecycle) {
            TManagerFixture fixture;
            TAtomicCounter executed;
            fixture.Run(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}), [&](TTasksManager& manager) {
                auto& category = manager.MutableCategoryVerified(ESpecialTaskCategory::Scan);
                const auto signals = fixture.Counters.GetWorkersPoolSignals("test")->GetCategorySignals(ESpecialTaskCategory::Scan);
                UNIT_ASSERT(category.HasProcesses(kServiceQueryIdentity));
                UNIT_ASSERT(!category.HasProcesses(MakeIdentity(0)));
                UNIT_ASSERT(!category.HasTasks(kServiceQueryIdentity));

                for (ui64 id = 1; id <= 128; ++id) {
                    manager.RegisterProcess(ESpecialTaskCategory::Scan, ::ToString(id), id, TCPULimitsConfig(1), MakeIdentity(id));
                    UNIT_ASSERT(category.HasProcesses(MakeIdentity(id)));
                    UNIT_ASSERT(!category.HasTasks(MakeIdentity(id)));
                    category.RegisterTask(id, std::make_shared<TCounterTask>(executed));
                    category.RegisterTask(id, std::make_shared<TCounterTask>(executed));
                }
                // Each process keeps one queued task while its first task acquires known usage.
                for (ui64 id = 1; id <= 128; ++id) {
                    THashSet<TString> scopes;
                    auto task = category.ExtractTaskWithPrediction(signals, scopes, MakeIdentity(id), 0, {});
                    UNIT_ASSERT(task && task->GetProcessId() == id);
                    scopes.clear();
                    category.PutTaskResult(task->GetResult(TMonotonic::MicroSeconds(1000), TMonotonic::MicroSeconds(1000 + id)), scopes);
                    UNIT_ASSERT_VALUES_EQUAL(*category.GetMinProcessUsage(MakeIdentity(id)), TDuration::MicroSeconds(id));
                }
                for (ui64 id = 128; id > 0; --id) {
                    manager.MovePendingQueryToService(MakeIdentity(id));
                    UNIT_ASSERT_VALUES_EQUAL(*category.GetMinProcessUsage(kServiceQueryIdentity), TDuration::MicroSeconds(id));
                    UNIT_ASSERT(!category.HasProcesses(MakeIdentity(id)));
                    UNIT_ASSERT(!category.HasTasks(MakeIdentity(id)));
                }
                UNIT_ASSERT_VALUES_EQUAL(*category.GetMinProcessUsage(kServiceQueryIdentity), TDuration::MicroSeconds(1));
                auto firstScope = category.GetProcessScopePtrVerified("1");
                firstScope->IncInFlight();
                UNIT_ASSERT_VALUES_EQUAL(*category.GetMinProcessUsage(kServiceQueryIdentity), TDuration::MicroSeconds(2));
                NKikimrConfig::TCompositeConveyorConfig::THeavyLimit protoLimit;
                protoLimit.SetCpuLimitUs(2);
                protoLimit.SetThreadLimit(1);
                NConfig::THeavyLimit limit;
                UNIT_ASSERT(limit.DeserializeFromProto(protoLimit).IsSuccess());
                UNIT_ASSERT(!category.GetMinProcessUsage(kServiceQueryIdentity, 1, {limit}));
                firstScope->DecInFlight();
                UNIT_ASSERT_VALUES_EQUAL(*category.GetMinProcessUsage(kServiceQueryIdentity, 1, {limit}), TDuration::MicroSeconds(1));

                manager.RegisterProcess(ESpecialTaskCategory::Scan, "late", 200, TCPULimitsConfig(1), MakeIdentity(0));
                auto oldScope = category.GetProcessScopePtrVerified("late");
                category.RegisterTask(200, std::make_shared<TCounterTask>(executed));
                THashSet<TString> scopes;
                auto task = category.ExtractTaskWithPrediction(signals, scopes, MakeIdentity(0), 0, {});
                UNIT_ASSERT(task);
                UNIT_ASSERT(manager.UnregisterProcess(ESpecialTaskCategory::Scan, 200) == MakeIdentity(0));
                UNIT_ASSERT(!category.HasProcesses(MakeIdentity(0)));
                manager.RegisterProcess(ESpecialTaskCategory::Scan, "late", 201, TCPULimitsConfig(1), MakeIdentity(0));
                UNIT_ASSERT(category.GetProcessScopePtrVerified("late") != oldScope);
                category.RegisterTask(201, std::make_shared<TCounterTask>(executed));
                scopes.clear();
                category.PutTaskResult(task->GetResult(TMonotonic::MicroSeconds(2000), TMonotonic::MicroSeconds(2001)), scopes);
                UNIT_ASSERT_VALUES_EQUAL(oldScope->GetCountInFlight(), 0);
                UNIT_ASSERT(category.HasTasks(MakeIdentity(0)));
                UNIT_ASSERT(manager.UnregisterProcess(ESpecialTaskCategory::Scan, 201) == MakeIdentity(0));
                for (ui64 id = 1; id <= 128; ++id) {
                    UNIT_ASSERT(manager.UnregisterProcess(ESpecialTaskCategory::Scan, id) == kServiceQueryIdentity);
                }
                UNIT_ASSERT(category.HasProcesses(kServiceQueryIdentity));
                UNIT_ASSERT(!category.HasTasks());
                UNIT_ASSERT(!category.GetMinProcessUsage(kServiceQueryIdentity));
                // Index membership is independent of runnable state, including shared scopes.
                for (ui64 id : {202, 203}) {
                    manager.RegisterProcess(ESpecialTaskCategory::Scan, "shared", id, TCPULimitsConfig(1), MakeIdentity(id));
                    category.RegisterTask(id, std::make_shared<TCounterTask>(executed));
                    UNIT_ASSERT_VALUES_EQUAL(*category.GetMinProcessUsage(MakeIdentity(id)), TDuration::Zero());
                }
                auto scope = category.GetProcessScopePtrVerified("shared");
                scope->IncInFlight();
                for (ui64 id : {202, 203}) {
                    UNIT_ASSERT(category.HasProcesses(MakeIdentity(id)));
                    UNIT_ASSERT(!category.HasTasks(MakeIdentity(id)));
                    UNIT_ASSERT(!category.GetMinProcessUsage(MakeIdentity(id)));
                }
                scope->DecInFlight();
                for (ui64 id : {202, 203}) {
                    UNIT_ASSERT(category.HasTasks(MakeIdentity(id)));
                    manager.UnregisterProcess(ESpecialTaskCategory::Scan, id);
                    UNIT_ASSERT(!category.HasTasks(MakeIdentity(id)));
                    UNIT_ASSERT(!category.GetMinProcessUsage(MakeIdentity(id)));
                    UNIT_ASSERT(!category.HasProcesses(MakeIdentity(id)));
                }
            });
        }

        /* Scenario:
            A query can retry in a second pool after a first-pool refusal.
            The one manager timer uses the final deadline, or disappears after success.
         */
        Y_UNIT_TEST(RetryUsesFinalOutcomeAcrossPools) {
            for (ui32 outcome = 0; outcome < 3; ++outcome) {
                TManagerFixture fixture;
                auto& runtime = fixture.Runtime;
                const auto& deadlines = fixture.Deadlines;
                const auto identity = MakeIdentity(0);
                auto query = MakeSchedulerQuery(identity, TDuration::Seconds(1), 1, TDuration::MicroSeconds(1));
                TQueryRegistry external;
                external.RegisterProcess(identity);
                external.SetQuery(identity, query.Query);
                SetCapacity(external, identity, 1);
                std::optional<TSchedulerLease> occupied(std::get<TSchedulerLease>(external.GetStateVerified(identity).TryStart(TMonotonic::Now())));
                if (outcome == 2) {
                    query.SetFairShare(0);
                }
                ui32 serviceBatches = 0;
                ui32 managedBatches = 0;
                const auto previousFilter = runtime.SetEventFilter([&](auto&, auto& ev) {
                    if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                        if (ev->template Get<TEvInternal::TEvNewTask>()->GetQueryIdentity().IsServiceQuery) {
                            ++serviceBatches;
                            if (outcome == 0) {
                                occupied.reset();
                            } else {
                                query.SetFairShare(outcome == 1 ? 0 : 1);
                            }
                        } else {
                            ++managedBatches;
                        }
                    }
                    return false;
                });
                TWorkerEventBlocker<TEvInternal::TEvNewTask> batches(runtime);
                auto config = BuildConfig({1, 1}, {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}},
                                                   {{ESpecialTaskCategory::Scan, 1}}});
                TAtomicCounter executed;
                fixture.Run(config, [&](TTasksManager& manager) {
                    manager.RegisterProcess(ESpecialTaskCategory::Scan, "query", 1, TCPULimitsConfig(1), identity);
                    manager.SetQuery(identity, query.Query);
                    manager.MutableCategoryVerified(ESpecialTaskCategory::Scan).RegisterTask(1, std::make_shared<TCounterTask>(executed));
                    manager.MutableCategoryVerified(ESpecialTaskCategory::Insert).RegisterTask(0, std::make_shared<TCounterTask>(executed));
                    const auto before = TMonotonic::Now();
                    UNIT_ASSERT(manager.DrainTasks());
                    const auto after = TMonotonic::Now();
                    UNIT_ASSERT_VALUES_EQUAL(serviceBatches, 1);
                    UNIT_ASSERT_VALUES_EQUAL(managedBatches, outcome == 0 ? 1 : 0);
                    if (outcome == 0) {
                        UNIT_ASSERT(deadlines.empty());
                    } else {
                        const auto delay = outcome == 1 ? TDuration::Seconds(1) : TDuration::MicroSeconds(200);
                        UNIT_ASSERT_VALUES_EQUAL(deadlines.size(), 1);
                        UNIT_ASSERT(deadlines[0].GetValue() >= (before + delay).GetValue());
                        UNIT_ASSERT(deadlines[0].GetValue() <= (after + delay).GetValue());
                    }
                });
                runtime.SetEventFilter(previousFilter);
            }
        }

        /* Scenario:
            Deliver old wakeups after workers become busy or the query loses its queued tasks.
            Neither case creates a new timer; ordinary accounting/task events restore progress.
         */
        Y_UNIT_TEST_TWIN(IdleRetryDoesNotReschedule, NoQueuedTasks) {
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity, TDuration::MicroSeconds(1), 0);
            TSchedulerRuntimeFixture fixture(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}}));
            ui64 timers = 0;
            fixture.Runtime.SetScheduledEventFilter([&](auto&, auto& ev, TDuration, TInstant&) {
                if (ev->Recipient == fixture.Distributor && ev->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType) {
                    ++timers;
                    return true;
                }
                return false;
            });
            fixture.RegisterProcess(1, identity);
            fixture.SendQueryResponse(query.Query);
            fixture.RegisterProcess(2, identity);
            TAtomicCounter executed, service;
            fixture.Submit(executed, 1);
            fixture.WaitFor([&] { return timers == 1; });
            NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
            if (NoQueuedTasks) {
                fixture.UnregisterProcess(1);
            } else {
                fixture.Submit(service, 0, ESpecialTaskCategory::Insert);
                fixture.WaitFor([&] { return results.size() == 1; });
            }
            const auto previousTimers = timers;
            for (ui32 i = 0; i < 3; ++i) {
                fixture.Runtime.Send(fixture.Distributor, fixture.Sink, new NActors::TEvents::TEvWakeup());
                fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(timers, previousTimers);
            }
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
            results.Stop().Unblock();
            if (!NoQueuedTasks) {
                fixture.WaitFor([&] { return timers == previousTimers + 1; });
            }
            if (NoQueuedTasks) {
                fixture.UnregisterProcess(2);
                fixture.WaitFor([&] { return fixture.Removes[identity.QueryId] == 1; });
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 0);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);
                fixture.RegisterProcess(2, identity);
                fixture.SendQueryResponse(query.Query);
            }
            query.SetFairShare(1);
            if (NoQueuedTasks) {
                fixture.Submit(executed, 2);
            } else {
                fixture.Runtime.Send(fixture.Distributor, fixture.Sink, new NActors::TEvents::TEvWakeup());
            }
            fixture.WaitFor([&] { return executed.Val() == 1; });
            if (!NoQueuedTasks) {
                fixture.UnregisterProcess(1);
            }
            fixture.UnregisterProcess(2);
            fixture.WaitFor([&] { return fixture.Removes[identity.QueryId] == (NoQueuedTasks ? 2 : 1); });
        }

        /* Scenario:
            Worker-thread lease releases overlap actor-side shrink/grow, state moves or forced cleanup.
            Counts remain balanced; native work is never stopped twice.
         */
        Y_UNIT_TEST_TWIN(ConcurrentLeaseReleaseAndStateMaintenance, ForceClose) {
            for (ui32 round = 0; round < 50; ++round) {
                const auto identity = MakeIdentity(0);
                auto query = MakeSchedulerQuery(identity, TDuration::MicroSeconds(10), 8);
                auto registry = std::make_unique<TQueryRegistry>();
                registry->RegisterProcess(identity);
                registry->SetQuery(identity, query.Query);
                SetCapacity(*registry, identity, 8);
                std::vector<TSchedulerLease> leases;
                for (ui32 i = 0; i < 8; ++i) {
                    leases.emplace_back(std::get<TSchedulerLease>(registry->GetStateVerified(identity).TryStart(TMonotonic::Now())));
                }
                std::barrier rendezvous(2);
                std::jthread worker([&, leases = std::move(leases)]() mutable {
                    rendezvous.arrive_and_wait();
                    while (!leases.empty()) {
                        leases.pop_back();
                        std::this_thread::yield();
                    }
                });
                rendezvous.arrive_and_wait();
                if (ForceClose) {
                    registry.reset();
                } else {
                    auto& source = registry->GetStateVerified(identity);
                    TSchedulerQueryState moved(std::move(source));
                    for (ui32 i = 0; i < 50; ++i) {
                        moved.PrepareWorkCapacity(i % 2 ? 4 : 16);
                        moved.ApplyWorkCapacity();
                        Y_UNUSED(moved.HasWorksCapacity());
                    }
                    source = std::move(moved);
                }
                worker.join();
                UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
                if (registry) {
                    UNIT_ASSERT(registry->GetStateVerified(identity).HasWorksCapacity());
                    registry->UnregisterProcess(identity);
                    UNIT_ASSERT(registry->TryReleaseQuery(identity));
                }
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);
            }
        }

        /* Scenario:
            All six mode transitions preserve a held batch and its lease.
            Subsequent service and managed batches use only eligible pools.
         */
        Y_UNIT_TEST(ModeTransitionsWithLiveBatch) {
            for (const auto from : {NConfig::TProtoWorkerPool::NonSchedulable, NConfig::TProtoWorkerPool::Schedulable, NConfig::TProtoWorkerPool::All}) {
                for (const auto to : {NConfig::TProtoWorkerPool::NonSchedulable, NConfig::TProtoWorkerPool::Schedulable, NConfig::TProtoWorkerPool::All}) {
                    if (from == to) {
                        continue;
                    }
                    const TString transition = TStringBuilder() << "from=" << static_cast<int>(from)
                                                                << ", to=" << static_cast<int>(to);
                    auto proto = BuildConfig({1, 1}, {{{ESpecialTaskCategory::Scan, 1}},
                                                      {{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}});
                    proto.MutableWorkerPools(0)->SetSchedulingMode(from);
                    auto query = MakeSchedulerQuery(MakeIdentity(0), TDuration::MicroSeconds(10), 2);
                    TSchedulerRuntimeFixture fixture(proto);
                    NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
                    TAtomicCounter blocker, service, managed;
                    fixture.Submit(blocker, 0, ESpecialTaskCategory::Insert);
                    fixture.WaitFor([&] { return results.size() == 1; });
                    auto insert = std::move(results.front());
                    results.pop_front();
                    fixture.RegisterProcess(1, kServiceQueryIdentity, ESpecialTaskCategory::Scan, "service");
                    fixture.RegisterProcess(2, MakeIdentity(0), ESpecialTaskCategory::Scan, "managed");
                    fixture.SendQueryResponse(query.Query);
                    const bool managedFirst = from != NConfig::TProtoWorkerPool::NonSchedulable;
                    TWorkerEventBlocker<TEvInternal::TEvNewTask> batches(fixture.Runtime);
                    fixture.Submit(managedFirst ? managed : service, managedFirst ? 2 : 1);
                    fixture.WaitFor([&] { return batches.size() == 1; });
                    fixture.Submit(service, 1);
                    fixture.Submit(managed, 2); // These tasks were queued under the old mode.
                    proto.MutableWorkerPools(0)->SetSchedulingMode(to);
                    fixture.UpdateConfig(proto);
                    UNIT_ASSERT_C(query.Query->GetParent()->CpuUsage.load() == (managedFirst ? 1 : 0), transition);
                    batches.Stop().Unblock();
                    fixture.WaitFor([&] { return results.size() == 1; });
                    UNIT_ASSERT_C(results.front()->Get()->GetWorkersPoolId() == 2, transition);
                    UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
                    const auto oldScope = results.front()->Get()->GetResults().front().GetScope();
                    results.Unblock();
                    const ui32 expectedBatches = to == NConfig::TProtoWorkerPool::All ? 2 : 1;
                    for (ui32 i = 0; i < expectedBatches; ++i) {
                        fixture.WaitFor([&] { return !results.empty(); });
                        UNIT_ASSERT_C(results.size() == 1, transition);
                        const auto& result = results.front()->Get();
                        UNIT_ASSERT_C(result->GetWorkersPoolId() == 2, transition);
                        UNIT_ASSERT_C(to == NConfig::TProtoWorkerPool::All || result->GetQueryIdentity().IsServiceQuery == (to == NConfig::TProtoWorkerPool::NonSchedulable), transition);
                        const auto scope = result->GetResults().front().GetScope();
                        results.Unblock(1);
                        fixture.WaitFor([&] { return scope->GetCountInFlight() == 0; });
                    }
                    fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                    UNIT_ASSERT_C(results.empty(), transition);
                    UNIT_ASSERT_VALUES_EQUAL(oldScope->GetCountInFlight(), 0);
                    UNIT_ASSERT_C(service.Val() == (!managedFirst ? 1 : 0) + (to != NConfig::TProtoWorkerPool::Schedulable ? 1 : 0), transition);
                    UNIT_ASSERT_C(managed.Val() == (managedFirst ? 1 : 0) + (to != NConfig::TProtoWorkerPool::NonSchedulable ? 1 : 0), transition);
                    if (to != NConfig::TProtoWorkerPool::All) {
                        // A second applied update must release the excluded queue in this same pool.
                        proto.MutableWorkerPools(0)->SetSchedulingMode(NConfig::TProtoWorkerPool::All);
                        fixture.UpdateConfig(proto);
                        fixture.WaitFor([&] { return results.size() == 1; });
                        UNIT_ASSERT_VALUES_EQUAL(results.front()->Get()->GetWorkersPoolId(), 2);
                        const auto scope = results.front()->Get()->GetResults().front().GetScope();
                        results.Unblock();
                        fixture.WaitFor([&] { return scope->GetCountInFlight() == 0; });
                    }
                    results.Stop().Unblock();
                    fixture.Runtime.Send(insert.Release(), 0, true);
                    fixture.WaitFor([&] { return service.Val() + managed.Val() == 3 && query.Query->GetParent()->CpuUsage.load() == 0; });
                    fixture.UnregisterProcess(1);
                    fixture.UnregisterProcess(2);
                    fixture.WaitFor([&] { return fixture.Removes[0] == 1; });
                }
            }
        }

        /* Scenario:
            A changes from All to NonSchedulable with live managed leases; B remains All.
            STARTED == or > target capacity prevents new batches despite idle cells/workers.
            Another query can run; releasing old leases restores admission without a throttle.
         */
        Y_UNIT_TEST_TWIN(ModeChangeHonorsTargetCapacity, AboveTarget) {
            const ui64 started = AboveTarget ? 2 : 1;
            auto proto = BuildConfig({3, 1}, {{{ESpecialTaskCategory::Scan, 1}},
                                              {{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}});
            proto.MutableWorkerPools(0)->SetMaxBatchSize(1);
            proto.MutableWorkerPools(1)->SetMaxBatchSize(1);
            auto query = MakeSchedulerQuery(MakeIdentity(0), TDuration::Seconds(1), 4);
            auto other = MakeSchedulerQuery(MakeIdentity(1));
            TSchedulerRuntimeFixture fixture(proto);
            NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
            TAtomicCounter blocker, executed, independent;
            fixture.Submit(blocker, 0, ESpecialTaskCategory::Insert);
            fixture.WaitFor([&] { return results.size() == 1; });
            auto insert = std::move(results.front());
            results.pop_front();
            TWorkerEventBlocker<TEvInternal::TEvNewTask> batches(fixture.Runtime,
                                                                 [&](const auto& ev) { return ev->Get()->GetQueryIdentity() == MakeIdentity(0); });
            fixture.RegisterProcess(1, MakeIdentity(0));
            for (ui64 i = 0; i < started; ++i) {
                fixture.Submit(executed, 1);
            }
            fixture.SendQueryResponse(query.Query);
            fixture.WaitFor([&] { return batches.size() == started; });
            proto.MutableWorkerPools(0)->SetSchedulingMode(NConfig::TProtoWorkerPool::NonSchedulable);
            fixture.UpdateConfig(proto);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), AboveTarget ? 4 : 1);
            fixture.Submit(executed, 1);
            fixture.Submit(executed, 1);
            fixture.RegisterProcess(2, MakeIdentity(1), ESpecialTaskCategory::Scan, "independent");
            fixture.SendQueryResponse(other.Query);
            fixture.Submit(independent, 2);
            results.push_front(std::move(insert));
            results.Unblock(1);
            fixture.WaitFor([&] { return independent.Val() == 1 && results.size() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 0);
            results.Stop().Unblock();
            batches.Stop().Unblock();
            fixture.WaitFor([&] { return executed.Val() == started + 2 && query.Query->GetParent()->CpuUsage.load() == 0; });
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 1);
            fixture.UnregisterProcess(1);
            fixture.UnregisterProcess(2);
            fixture.WaitFor([&] { return fixture.Removes[0] == 1 && fixture.Removes[1] == 1; });
        }

        /* Scenario:
            Registry prepare/apply is per-query, deferred below live leases and idempotent.
            State/lease moves preserve counters and clean a throttled destination; invalid migration is rejected.
            Native usage/demand and service references are balanced without inspecting private cells.
         */
        Y_UNIT_TEST(RegistryCapacityAndWorkOwnership) {
            auto query = MakeSchedulerQuery(MakeIdentity(0), TDuration::Seconds(1), 3);
            auto other = MakeSchedulerQuery(MakeIdentity(1), TDuration::Seconds(1), 0);
            TQueryRegistry registry;
            registry.RegisterProcess(MakeIdentity(1));
            registry.SetQuery(MakeIdentity(1), other.Query);
            SetCapacity(registry, MakeIdentity(1), 2);
            Y_UNUSED(registry.GetStateVerified(MakeIdentity(1)).TryStart(TMonotonic::Now()));
            UNIT_ASSERT_VALUES_EQUAL(other.Query->CpuThrottle.load(), 1);
            registry.UnregisterProcess(MakeIdentity(1));
            UNIT_ASSERT(registry.TryReleaseQuery(MakeIdentity(1)));
            UNIT_ASSERT_VALUES_EQUAL(other.Query->CpuThrottle.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(other.Query->CpuMaxDemand.load(), 0);
            for (ui64 id : {0, 1}) {
                registry.RegisterProcess(MakeIdentity(id));
                UNIT_ASSERT_EXCEPTION(registry.SetQuery(MakeIdentity(id), nullptr), yexception);
                registry.SetQuery(MakeIdentity(id), id ? other.Query : query.Query);
                SetCapacity(registry, MakeIdentity(id), 3);
            }
            auto& source = registry.GetStateVerified(MakeIdentity(0));
            auto& destination = registry.GetStateVerified(MakeIdentity(1));
            const auto now = TMonotonic::Now();
            for (ui32 i = 0; i < 3; ++i) {
                UNIT_ASSERT(std::holds_alternative<TMonotonic>(destination.TryStart(now)));
                UNIT_ASSERT(destination.HasWorksCapacity());
                UNIT_ASSERT_VALUES_EQUAL(other.Query->CpuThrottle.load(), 1);
            }
            std::optional<TSchedulerLease> first(std::get<TSchedulerLease>(source.TryStart(now)));
            auto second = std::get<TSchedulerLease>(source.TryStart(now));
            std::optional<TSchedulerLease> third(std::get<TSchedulerLease>(source.TryStart(now)));
            UNIT_ASSERT_EXCEPTION(registry.MovePendingQueryToService(MakeIdentity(0)), yexception);
            UNIT_ASSERT(source.IsReady());
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 3);
            registry.PrepareWorkCapacity(MakeIdentity(0), 1);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 3);
            registry.ApplyWorkCapacity(MakeIdentity(0));
            UNIT_ASSERT(!source.HasWorksCapacity());
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 3);
            third.reset();
            *first = std::move(second);
            UNIT_ASSERT(!second);
            registry.ApplyWorkCapacity(MakeIdentity(0));
            registry.ApplyWorkCapacity(MakeIdentity(0));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(other.Query->CpuMaxDemand.load(), 3);

            TSchedulerQueryState moved(std::move(source));
            UNIT_ASSERT(!source.IsReadyToRelease());
            moved.PrepareWorkCapacity(4);
            moved.ApplyWorkCapacity();
            auto another = std::get<TSchedulerLease>(moved.TryStart(now));
            moved.PrepareWorkCapacity(1);
            moved.ApplyWorkCapacity();
            UNIT_ASSERT(!moved.HasWorksCapacity());
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 4);
            destination = std::move(moved);
            UNIT_ASSERT_VALUES_EQUAL(other.Query->CpuThrottle.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(other.Query->CpuMaxDemand.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 2);
            first.reset();
            destination.ApplyWorkCapacity();
            UNIT_ASSERT(!destination.HasWorksCapacity());
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 1);
            {
                auto finalLease = std::move(another);
                UNIT_ASSERT(finalLease);
            }
            UNIT_ASSERT(destination.HasWorksCapacity());
            Y_UNUSED(destination.TryStart(now)); // Empty-batch path releases its lease immediately.
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
            destination.PrepareForRemoval();
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);

            registry.RegisterProcess(kServiceQueryIdentity);
            const auto pending = MakeIdentity(2);
            registry.RegisterProcess(pending);
            registry.RegisterProcess(pending);
            UNIT_ASSERT_VALUES_EQUAL(registry.MovePendingQueryToService(pending), 2);
            UNIT_ASSERT(registry.RegisterProcess(pending));
            UNIT_ASSERT(!registry.GetStateVerified(pending).IsReady());
            SetCapacity(registry, kServiceQueryIdentity, 1);
            UNIT_ASSERT(std::holds_alternative<TSchedulerLease>(
                registry.GetStateVerified(kServiceQueryIdentity).TryStart(now)));
            for (ui32 i = 0; i < 2; ++i) {
                registry.UnregisterProcess(kServiceQueryIdentity);
                UNIT_ASSERT(!registry.TryReleaseQuery(kServiceQueryIdentity));
            }
            registry.UnregisterProcess(kServiceQueryIdentity);
            UNIT_ASSERT(registry.TryReleaseQuery(kServiceQueryIdentity));
            UNIT_ASSERT(!registry.TryReleaseQuery(kServiceQueryIdentity));
        }

        /* Scenario:
            Removing a busy managed fallback link waits for its accounting result, not the ACK.
            Replacing the pending snapshot applies only the last mode; restoring it cancels StopPrepare.
            Registrations and factory delivery during the wait use the installed topology.
         */
        Y_UNIT_TEST_TWIN(PendingFallbackModeUpdateCanBeReplaced, RestoreInitial) {
            auto initial = BuildConfig({1, 1}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
            initial.MutableWorkerPools(0)->SetSchedulingMode(NConfig::TProtoWorkerPool::NonSchedulable);
            auto first = MakeSchedulerQuery(MakeIdentity(0));
            auto second = MakeSchedulerQuery(MakeIdentity(1));
            TSchedulerRuntimeFixture fixture(initial);
            NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
            TAtomicCounter executed, independent, service;
            fixture.RegisterProcess(1, MakeIdentity(0));
            fixture.SendQueryResponse(first.Query);
            fixture.Submit(executed, 1);
            fixture.WaitFor([&] { return results.size() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(results.front()->Get()->GetWorkersPoolId(), 1);
            const ui64 originalCapacity = first.Query->CpuMaxDemand.load();
            auto target = initial;
            target.MutableWorkerPools(0)->SetSchedulingMode(NConfig::TProtoWorkerPool::All);
            fixture.UpdateConfig(target);
            fixture.RegisterProcess(2, MakeIdentity(1), ESpecialTaskCategory::Scan, "new-query");
            fixture.RegisterProcess(3, MakeIdentity(1), ESpecialTaskCategory::Scan, "cancelled");
            fixture.UnregisterProcess(3);
            fixture.SendQueryResponse(second.Query);
            fixture.Submit(executed, 2);
            fixture.Submit(independent, 0, ESpecialTaskCategory::Insert);
            fixture.WaitFor([&] { return results.size() == 2 && independent.Val() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(second.Query->CpuMaxDemand.load(), originalCapacity);
            if (RestoreInitial) {
                fixture.UpdateConfig(initial);
                fixture.WaitFor([&] { return results.size() == 3; });
                UNIT_ASSERT_VALUES_EQUAL(results.back()->Get()->GetWorkersPoolId(), 1);
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 2);
            } else {
                target.MutableWorkerPools(0)->SetSchedulingMode(NConfig::TProtoWorkerPool::Schedulable);
                fixture.UpdateConfig(target);
                fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 1);
                results.Unblock(1);
                fixture.WaitFor([&] { return results.size() == 2 && executed.Val() == 2; });
                UNIT_ASSERT_VALUES_EQUAL(results.back()->Get()->GetWorkersPoolId(), 2);
                UNIT_ASSERT_VALUES_EQUAL(second.Query->CpuMaxDemand.load(), 1);
            }
            results.Unblock();
            fixture.WaitFor([&] {
                return first.Query->GetParent()->CpuUsage.load() == 0 && second.Query->GetParent()->CpuUsage.load() == 0;
            });
            fixture.Submit(service, 0);
            fixture.WaitFor([&] { return results.size() == 1 && service.Val() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(results.front()->Get()->GetWorkersPoolId(), RestoreInitial ? 2 : 0);
            UNIT_ASSERT(results.front()->Get()->GetQueryIdentity() == kServiceQueryIdentity);
            results.Stop().Unblock();
            fixture.UnregisterProcess(1);
            fixture.UnregisterProcess(2);
            fixture.WaitFor([&] { return fixture.Removes[0] == 1 && fixture.Removes[1] == 1; });
            UNIT_ASSERT_VALUES_EQUAL(first.Query->GetParent()->CpuUsage.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(second.Query->GetParent()->CpuUsage.load(), 0);
        }

        /* Scenario:
            A mode change renames an unnamed pool and waits for its old batch before slot reuse.
            The queued managed task runs in the replacement, while service falls back to pool zero.
         */
        Y_UNIT_TEST(UnnamedModeChangeRecreatesPool) {
            auto proto = BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}});
            proto.MutableWorkerPools(0)->ClearName();
            auto query = MakeSchedulerQuery(MakeIdentity(0));
            TSchedulerRuntimeFixture fixture(proto);
            NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
            TAtomicCounter executed;
            fixture.RegisterProcess(1, MakeIdentity(0));
            fixture.SendQueryResponse(query.Query);
            fixture.Submit(executed, 1);
            fixture.WaitFor([&] { return results.size() == 1; });
            const auto oldWorker = results.front()->Sender;
            proto.MutableWorkerPools(0)->SetSchedulingMode(NConfig::TProtoWorkerPool::Schedulable);
            fixture.UpdateConfig(proto);
            fixture.Submit(executed, 1);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 1);
            results.Unblock();
            fixture.WaitFor([&] { return results.size() == 1 && executed.Val() == 2; });
            UNIT_ASSERT_VALUES_EQUAL(results.front()->Get()->GetWorkersPoolId(), 2);
            UNIT_ASSERT(results.front()->Sender != oldWorker);
            TAtomicCounter service;
            fixture.Submit(service, 0);
            fixture.WaitFor([&] { return results.size() == 2 && service.Val() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(results.back()->Get()->GetWorkersPoolId(), 0);
            UNIT_ASSERT(results.back()->Get()->GetQueryIdentity() == kServiceQueryIdentity);
            results.Stop().Unblock();
            fixture.UnregisterProcess(1);
            fixture.WaitFor([&] { return fixture.Removes[0] == 1; });
        }

        /* Scenario:
            - An unset/false feature flag or disabled local scheduler routes credentialed processes to service.
            - Processes share service identity, but preserve their scopes, queues and accounting.
            - Late accounting and subsequent registrations preserve service; the empty managed pool still updates NoTasks.
         */
        Y_UNIT_TEST(DisabledSchedulingKeepsSharedServiceIdentity) {
            for (const auto flag : {std::optional<bool>{}, {false}, {true}}) {
                auto proto = BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}});
                proto.MutableWorkerPools(0)->SetSchedulingMode(NConfig::TProtoWorkerPool::NonSchedulable);
                TSchedulerRuntimeFixture fixture(proto, flag);
                fixture.SetLocalSchedulerEnabled(!flag.value_or(false));
                const auto noTasks = fixture.FindNoTasks("WP::DEFAULT_SCHEDULABLE", ESpecialTaskCategory::Scan);
                ui64 batches = 0;
                auto routing = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetWorkersPoolId(), 2);
                    ++batches;
                });
                TAtomicCounter executed;
                NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
                fixture.RegisterProcess(1, MakeIdentity(0));
                fixture.Submit(executed, 1);
                fixture.WaitFor([&] { return results.size() == 1; });
                UNIT_ASSERT(results.front()->Get()->GetQueryIdentity() == kServiceQueryIdentity);
                const auto scope = results.front()->Get()->GetResults().front().GetScope();
                UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 1);

                fixture.RegisterProcess(2, MakeIdentity(7));
                fixture.RegisterProcess(3, MakeIdentity(42), ESpecialTaskCategory::Insert, "insert");
                fixture.Submit(executed, 2);
                fixture.Submit(executed, 3, ESpecialTaskCategory::Insert);
                fixture.UnregisterProcess(1);
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.HdrfEvents, 0);

                ui64 processed = 0;
                auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                    UNIT_ASSERT(ev->Get()->GetQueryIdentity() == kServiceQueryIdentity);
                    for (const auto& task : ev->Get()->GetResults()) {
                        if (task.GetProcessId() == 2) {
                            UNIT_ASSERT(task.GetScope() == scope);
                        }
                        ++processed;
                    }
                });
                results.Stop().Unblock();
                fixture.WaitFor([&] { return executed.Val() == 3 && processed == 3 && scope->GetCountInFlight() == 0; });
                fixture.UnregisterProcess(2);
                fixture.UnregisterProcess(3, ESpecialTaskCategory::Insert);

                fixture.Submit(executed, 0);
                fixture.RegisterProcess(4, MakeIdentity(0));
                fixture.Submit(executed, 4);
                fixture.WaitFor([&] { return executed.Val() == 5 && processed == 5; });
                fixture.UnregisterProcess(4);
                fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(fixture.HdrfEvents, 0);
                UNIT_ASSERT(batches > 0);
                UNIT_ASSERT(noTasks->Val() > 0);
            }
        }

        /* Scenario:
            - Empty candidates still update NoTasks once for each category without queued tasks.
            - A category with scope-blocked tasks is not counted as empty.
         */
        Y_UNIT_TEST(NoTasksCountsEmptyCategoriesWithoutCandidates) {
            TManagerFixture fixture;
            TAtomicCounter executed;
            fixture.Run(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}}),
                        [&](TTasksManager& manager) {
                            const auto& counters = manager.MutableWorkersPool(2).GetCounters();
                            const auto scan = counters->GetCategorySignals(ESpecialTaskCategory::Scan)->NoTasks;
                            const auto insert = counters->GetCategorySignals(ESpecialTaskCategory::Insert)->NoTasks;
                            UNIT_ASSERT(!manager.DrainTasks());
                            UNIT_ASSERT_VALUES_EQUAL(scan->Val(), 1);
                            UNIT_ASSERT_VALUES_EQUAL(insert->Val(), 1);

                            manager.RegisterProcess(ESpecialTaskCategory::Scan, "blocked", 1, TCPULimitsConfig(1), kServiceQueryIdentity);
                            auto& category = manager.MutableCategoryVerified(ESpecialTaskCategory::Scan);
                            auto& scope = category.MutableProcessScope("blocked");
                            scope.IncInFlight();
                            category.RegisterTask(1, std::make_shared<TCounterTask>(executed));
                            UNIT_ASSERT(!manager.DrainTasks());
                            UNIT_ASSERT_VALUES_EQUAL(scan->Val(), 1);
                            UNIT_ASSERT_VALUES_EQUAL(insert->Val(), 2);
                            UNIT_ASSERT_VALUES_EQUAL(category.GetWaitingQueueSize(), 1);

                            scope.DecInFlight();
                            manager.UnregisterProcess(ESpecialTaskCategory::Scan, 1);
                            UNIT_ASSERT(!manager.DrainTasks());
                            UNIT_ASSERT_VALUES_EQUAL(scan->Val(), 2);
                            UNIT_ASSERT_VALUES_EQUAL(insert->Val(), 3);
                            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
                        });
        }

        /* Scenario:
            - Runtime feature-flag and local-scheduler changes affect new processes only, even for the same TxId and scope.
            - Existing processes retain their mode and leases across both switching directions.
            - Removing the managed process does not affect the service process, or vice versa.
         */
        Y_UNIT_TEST_TWIN(RuntimeSchedulingChangesAffectNewProcessesOnly, InitiallyEnabled) {
            for (const bool localScheduler : {false, true}) {
                const auto identity = MakeIdentity(0);
                auto query = MakeSchedulerQuery(identity, TDuration::MicroSeconds(10), 2);
                TSchedulerRuntimeFixture fixture(BuildConfig({2}, {{{ESpecialTaskCategory::Scan, 1}}}), localScheduler || InitiallyEnabled);
                fixture.SetLocalSchedulerEnabled(!localScheduler || InitiallyEnabled);
                TAtomicCounter executed;
                NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
                fixture.RegisterProcess(1, identity);
                if (InitiallyEnabled) {
                    fixture.SendQueryResponse(query.Query);
                }
                fixture.Submit(executed, 1);
                fixture.WaitFor([&] { return results.size() == 1; });
                const auto scope = results.front()->Get()->GetResults().front().GetScope();

                if (localScheduler) {
                    fixture.SetLocalSchedulerEnabled(!InitiallyEnabled);
                } else {
                    fixture.UpdateSchedulingFlag(!InitiallyEnabled);
                }
                fixture.RegisterProcess(2, identity);
                if (!InitiallyEnabled) {
                    fixture.SendQueryResponse(query.Query);
                }
                fixture.Submit(executed, 2);
                fixture.WaitFor([&] { return results.size() == 2; });
                fixture.Submit(executed, 1);

                const ui64 managedProcess = InitiallyEnabled ? 1 : 2;
                const ui64 serviceProcess = InitiallyEnabled ? 2 : 1;
                ui64 processed = 0;
                auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                    for (const auto& task : ev->Get()->GetResults()) {
                        UNIT_ASSERT(task.GetScope() == scope);
                        UNIT_ASSERT(ev->Get()->GetQueryIdentity() == (task.GetProcessId() == managedProcess ? identity : kServiceQueryIdentity));
                        ++processed;
                    }
                });
                results.Stop().Unblock();
                fixture.WaitFor([&] { return processed == 3 && scope->GetCountInFlight() == 0 && query.Query->GetParent()->CpuUsage.load() == 0; });
                fixture.UnregisterProcess(managedProcess);
                fixture.WaitFor([&] { return fixture.Removes[0] == 1; });
                fixture.Submit(executed, serviceProcess);
                fixture.WaitFor([&] { return processed == 4 && scope->GetCountInFlight() == 0; });
                fixture.UnregisterProcess(serviceProcess);
                fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 4);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Adds[0], 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[0], 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.HdrfEvents, 4);
            }
        }

        /* Scenario:
            - Disable scheduling while a managed registration is waiting for QueryResponse.
            - New processes use service; the pending registration still completes as managed.
            - Re-enabling reuses the existing managed query without another HDRF Add.
         */
        Y_UNIT_TEST(RuntimeFlagChangePreservesPendingRegistration) {
            const auto identity = MakeIdentity(0);
            auto query = MakeSchedulerQuery(identity);
            TSchedulerRuntimeFixture fixture(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
            fixture.SetLocalSchedulerEnabled(true);
            TAtomicCounter executed;
            ui64 processed = 0;
            auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                for (const auto& task : ev->Get()->GetResults()) {
                    UNIT_ASSERT(ev->Get()->GetQueryIdentity() == (task.GetProcessId() == 2 ? kServiceQueryIdentity : identity));
                    ++processed;
                }
            });
            fixture.RegisterProcess(1, identity);
            fixture.Submit(executed, 1);
            fixture.UpdateSchedulingFlag(false);
            fixture.RegisterProcess(2, identity);
            fixture.Submit(executed, 2);
            fixture.WaitFor([&] { return processed == 1; });
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 1);
            fixture.SendQueryResponse(query.Query);
            fixture.WaitFor([&] { return processed == 2 && query.Query->GetParent()->CpuUsage.load() == 0; });
            fixture.UpdateSchedulingFlag(true);
            fixture.RegisterProcess(3, identity);
            fixture.Submit(executed, 3);
            fixture.WaitFor([&] { return processed == 3 && query.Query->GetParent()->CpuUsage.load() == 0; });
            fixture.UnregisterProcess(1);
            fixture.UnregisterProcess(2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[0], 0);
            fixture.UnregisterProcess(3);
            fixture.WaitFor([&] { return fixture.Removes[0] == 1; });
            UNIT_ASSERT_VALUES_EQUAL(fixture.Adds[0], 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.HdrfEvents, 4);
        }

        /* Scenario:
            - Grow and shrink a pool while a worker holds a lease and another task is throttled.
            - Preserve the running work, clear removed throttle demand and follow the updated pool topology.
         */
        Y_UNIT_TEST_TWIN(RegistrationAndTopologyPreserveLeases, SeveralWorkers) {
            const ui64 workers = SeveralWorkers ? 3 : 1;
            const auto identity = MakeIdentity(0);
            auto query = MakeSchedulerQuery(identity, TDuration::Seconds(1), workers);
            const auto initial = BuildConfig({double(workers), 2}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
            TSchedulerRuntimeFixture fixture(initial);
            fixture.RegisterProcess(1, identity);
            TWorkerEventBlocker<TEvInternal::TEvNewTask> results(fixture.Runtime);
            TAtomicCounter executed;
            for (ui64 i = 0; i < workers; ++i) {
                fixture.Submit(executed, 1);
            }
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(results.empty());
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
            fixture.SendQueryResponse(query.Query);
            fixture.WaitFor([&] { return results.size() == workers; });
            for (const auto& batch : results) {
                UNIT_ASSERT(batch->Get()->GetQueryIdentity() == identity);
            }
            for (ui64 id = 1; id < 1024; ++id) {
                fixture.RegisterProcess(id + 1, MakeIdentity(id));
            }
            fixture.UpdateConfig(BuildConfig({double(workers + 2), 2}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}}));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), workers + 2);
            fixture.Submit(executed, 1);
            fixture.WaitFor([&] { return query.Query->CpuThrottle.load() == 1; });

            fixture.UpdateConfig(initial);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), workers);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), workers);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 0);
            results.Stop().Unblock();
            fixture.WaitFor([&] { return executed.Val() == workers + 1 && query.Query->GetParent()->CpuUsage.load() == 0; });

            fixture.UpdateConfig(BuildConfig({1, 2}, {{{ESpecialTaskCategory::Insert, 1}}, {{ESpecialTaskCategory::Scan, 1}}}));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 2);
            const auto targetPool = ParseConfig(initial).GetCategoryConfig(ESpecialTaskCategory::Insert).GetWorkerPools().front();
            ui64 received = 0;
            auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetWorkersPoolId(), targetPool);
                ++received;
            });
            fixture.Submit(executed, 1);
            fixture.WaitFor([&] { return received == 1 && query.Query->GetParent()->CpuUsage.load() == 0; });
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), workers + 2);
            fixture.UnregisterProcess(1);
            fixture.WaitFor([&] { return fixture.Removes[identity.QueryId] == 1; });
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);
        }

        /* Scenario:
            Real dispatched batches own their leases; queued tasks do not execute at shutdown.
            Drop a batch before shutdown or retain it past registry and HDRF tree destruction.
            Started/throttled usage is stopped once; late event destruction is safe.
         */
        Y_UNIT_TEST_TWIN(ShutdownAndDroppedBatchesReleaseWork, KeepBatch) {
            const auto identity = MakeIdentity(0);
            auto query = MakeSchedulerQuery(identity);
            TAtomicCounter executed;
            TEvInternal::TEvNewTask::TPtr lateBatch;
            std::vector<std::weak_ptr<ITask>> queued;
            {
                TSchedulerRuntimeFixture fixture(BuildConfig({2}, {{{ESpecialTaskCategory::Scan, 1}}}));
                fixture.RegisterProcess(1, identity);
                fixture.SendQueryResponse(query.Query);
                TWorkerEventBlocker<TEvInternal::TEvNewTask> batches(fixture.Runtime);
                fixture.Submit(executed, 1);
                fixture.WaitFor([&] { return batches.size() == 1; });
                lateBatch = std::move(batches.front());
                batches.pop_front();
                fixture.Submit(executed, 1); // The second work is throttled by HDRF.
                fixture.WaitFor([&] { return query.Query->CpuThrottle.load() == 1; });
                fixture.Submit(executed, 0); // Occupy the other worker with service work.
                fixture.WaitFor([&] { return batches.size() == 1; });
                fixture.RegisterProcess(2, MakeIdentity(2)); // Leave registration pending.
                for (ui64 id : {0, 1, 2}) {
                    auto task = std::make_shared<TCounterTask>(executed);
                    queued.emplace_back(task);
                    fixture.Runtime.Send(fixture.Distributor, fixture.Sink,
                                         new TEvExecution::TEvNewTask(std::move(task), ESpecialTaskCategory::Scan, id));
                }
                fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);
                for (const auto& task : queued) {
                    UNIT_ASSERT(!task.expired());
                }
                if (!KeepBatch) {
                    lateBatch.Reset();
                    UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), KeepBatch ? 1 : 0);
            query.Root.reset();
            lateBatch.Reset(); // Must not touch the destroyed tree or stop twice.
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);
            for (const auto& task : queued) {
                UNIT_ASSERT(task.expired());
            }
        }

        /* Scenario:
            - Two Distributors and an external owner share one canonical scheduler query.
            - Each Distributor acquires one registration, regardless of its process count.
            - Releasing both Distributors preserves the external owner's registration.
         */
        Y_UNIT_TEST(RegistrationsAreSharedPerDistributorNotPerProcess) {
            const auto proto = BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}});
            TSchedulerRuntimeFixture fixture(proto);
            const auto identity = MakeIdentity(1);

            auto counters = MakeIntrusive<NKqp::TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
            NKqp::NScheduler::TOptions options{
                .DelayParams = {
                    .MaxDelay = TDuration::MilliSeconds(10),
                    .MinDelay = TDuration::MicroSeconds(10),
                    .AttemptBonus = TDuration::MicroSeconds(5),
                    .MaxRandomDelay = TDuration::MicroSeconds(100),
                },
            };
            auto scheduler = std::make_shared<NKqp::NScheduler::TComputeScheduler>(counters, options);
            scheduler->AddOrUpdateDatabase("database", {});
            scheduler->AddOrUpdatePool("database", "pool", {});
            auto query = scheduler->AddOrUpdateQuery("database", "pool", identity.QueryId, {});
            scheduler->ToggleEnabled(true);
            fixture.Runtime.GetAppData().KqpComputeScheduler = scheduler;
            auto service = fixture.Runtime.Register(NKqp::CreateKqpComputeSchedulerService(TDuration::MilliSeconds(1)));
            fixture.Runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(fixture.Runtime.GetNodeId(0)), service);
            fixture.Runtime.EnableScheduleForActor(service, true);

            fixture.RegisterProcess(1, identity);
            fixture.RegisterProcess(2, identity);
            TAtomicCounter counter;
            fixture.Submit(counter, 1);
            fixture.WaitFor([&] {
                return counter.Val() == 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(query->CpuMaxDemand.load(), 1);
            const auto firstDistributor = fixture.Distributor;
            const auto secondDistributor = fixture.Runtime.Register(CreateService(ParseConfig(proto), MakeIntrusive<NMonitoring::TDynamicCounters>()));
            fixture.Runtime.EnableScheduleForActor(secondDistributor, true);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            fixture.Distributor = secondDistributor;
            fixture.RegisterProcess(3, identity);
            fixture.WaitFor([&] { return query->CpuMaxDemand.load() == 2; });
            fixture.Distributor = firstDistributor;
            fixture.UnregisterProcess(1);
            fixture.UnregisterProcess(2);
            fixture.WaitFor([&] { return query->CpuMaxDemand.load() == 1; });
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(query->GetParent()->GetQuery(identity.QueryId) == query);
            fixture.Distributor = secondDistributor;
            fixture.Submit(counter, 3);
            fixture.WaitFor([&] { return counter.Val() == 2 && query->GetParent()->CpuUsage.load() == 0; });
            fixture.UnregisterProcess(3);
            fixture.WaitFor([&] { return query->CpuMaxDemand.load() == 0; });
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Adds[identity.QueryId], 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], 2);
            UNIT_ASSERT(scheduler->RemoveQuery(identity.QueryId));
            UNIT_ASSERT(!scheduler->RemoveQuery(identity.QueryId));
        }

        /* Scenario:
            - A nullptr response moves pending processes across categories to service, including TxId 0.
            - Preserve queued tasks, shared scope limits and CPU accounting while another batch is held.
            - Later registrations and default processes still work without HDRF Remove.
         */
        Y_UNIT_TEST_TWIN(NullResponsePreservesProcessesScopesAndAccounting, ZeroTxId) {
            TSchedulerRuntimeFixture fixture(BuildConfig({2, 1},
                                                         {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}}));
            fixture.SetLocalSchedulerEnabled(true);
            const auto identity = MakeIdentity(ZeroTxId ? 0 : 42);
            TAtomicCounter busy, scans, inserts, service;
            NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> held(fixture.Runtime);
            fixture.RegisterProcess(10, kServiceQueryIdentity, ESpecialTaskCategory::Scan, "scope", 1);
            fixture.Submit(busy, 10);
            fixture.WaitFor([&] { return held.size() == 1; });
            held.Stop();
            const auto scope = held.front()->Get()->GetResults().front().GetScope();
            const auto usage = scope->GetCPUUsage();
            TDuration expectedUsage = held.front()->Get()->GetResults().front().GetDuration();
            const auto oldUsage = usage->GetDuration();
            const auto oldPredicted = usage->GetPredictedDuration();
            auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                UNIT_ASSERT(ev->Get()->GetQueryIdentity() == kServiceQueryIdentity);
                for (const auto& task : ev->Get()->GetResults()) {
                    if (task.GetProcessId() == 1 || task.GetProcessId() == 2) {
                        UNIT_ASSERT(task.GetScope() == scope);
                        UNIT_ASSERT(task.GetScope()->GetCPUUsage() == usage);
                        expectedUsage += task.GetDuration();
                    }
                }
            });
            for (ui64 id : {1, 2}) {
                fixture.RegisterProcess(id, identity, ESpecialTaskCategory::Scan, "scope", 1);
                fixture.Submit(scans, id);
            }
            fixture.RegisterProcess(3, identity, ESpecialTaskCategory::Insert);
            fixture.Submit(inserts, 3, ESpecialTaskCategory::Insert);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(scans.Val() + inserts.Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Adds[identity.QueryId], 1);

            fixture.SetLocalSchedulerEnabled(false);
            fixture.RegisterProcess(4, identity, ESpecialTaskCategory::Scan, "other-scope");
            fixture.Submit(service, 4);
            fixture.WaitFor([&] { return service.Val() == 1; });
            fixture.SendQueryResponse(nullptr);
            fixture.WaitFor([&] { return inserts.Val() == 1; });
            // There is a free scan worker, but the original scope still owns its slot.
            UNIT_ASSERT_VALUES_EQUAL(scans.Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 1);
            UNIT_ASSERT_VALUES_EQUAL(usage->GetDuration(), oldUsage);
            UNIT_ASSERT_VALUES_EQUAL(usage->GetPredictedDuration(), oldPredicted);
            held.Unblock();
            fixture.WaitFor([&] { return scans.Val() == 2 && scope->GetCountInFlight() == 0; });
            UNIT_ASSERT_VALUES_EQUAL(usage->GetDuration(), expectedUsage);
            for (ui64 id : {1, 2, 4, 10}) {
                fixture.UnregisterProcess(id);
            }
            fixture.UnregisterProcess(3, ESpecialTaskCategory::Insert);
            fixture.RegisterProcess(5, identity);
            fixture.Submit(service, 5);
            fixture.WaitFor([&] { return service.Val() == 2; });
            fixture.UnregisterProcess(5);
            fixture.Submit(service, 0); // Default processes must remain usable after migration cleanup.
            fixture.WaitFor([&] { return service.Val() == 3; });
            UNIT_ASSERT_VALUES_EQUAL(fixture.HdrfEvents, 3);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], 0);
        }

        /* Scenario:
            - Use Now when no deadlines exist and ignore states without a deadline.
            - Compute the average of throttled deadlines, including values near ui64's maximum.
         */
        Y_UNIT_TEST(DeadlineReplacementAndOverflowSafeAverage) {
            for (const auto now : {TMonotonic::MicroSeconds(100), TMonotonic::FromValue(Max<ui64>() - 1000)}) {
                auto first = MakeSchedulerQuery(MakeIdentity(1), TDuration::MicroSeconds(100), 0);
                auto second = MakeSchedulerQuery(MakeIdentity(2), TDuration::MicroSeconds(201), 0);
                auto readyWithoutDeadline = MakeSchedulerQuery(MakeIdentity(3));
                TQueryRegistry registry;
                UNIT_ASSERT_VALUES_EQUAL(registry.GetAverageWakeUpDeadline(now), now);
                for (ui64 id : {1, 2, 3}) {
                    registry.RegisterProcess(MakeIdentity(id));
                }
                registry.SetQuery(MakeIdentity(3), readyWithoutDeadline.Query);
                SetCapacity(registry, MakeIdentity(3), 1);
                registry.SetQuery(MakeIdentity(1), first.Query);
                SetCapacity(registry, MakeIdentity(1), 1);
                auto& state = registry.GetStateVerified(MakeIdentity(1));
                for (const i64 offset : {0, 10, -10}) {
                    const auto attempt = offset < 0 ? now - TDuration::MicroSeconds(-offset) : now + TDuration::MicroSeconds(offset);
                    UNIT_ASSERT(std::holds_alternative<TMonotonic>(state.TryStart(attempt)));
                    UNIT_ASSERT_VALUES_EQUAL(registry.GetAverageWakeUpDeadline(now), attempt + TDuration::MicroSeconds(100));
                }
                for (ui64 id : {1, 2}) {
                    registry.SetQuery(MakeIdentity(id), id == 1 ? first.Query : second.Query);
                    SetCapacity(registry, MakeIdentity(id), 1);
                    Y_UNUSED(registry.GetStateVerified(MakeIdentity(id)).TryStart(now));
                    UNIT_ASSERT_VALUES_EQUAL(registry.GetAverageWakeUpDeadline(now).GetValue(), now.GetValue() + (id == 1 ? 100 : 150));
                }
            }
        }

        /* Scenario:
            - Release quota before fractional-worker sleep; unregister during sleep discards queued tasks.
            - Remove the query before delayed accounting; optionally register the same identity again.
            - Balance each registration once and reconcile the old scope's in-flight accounting.
         */
        Y_UNIT_TEST_TWIN(LeaseReleaseAndLateAccounting, ReRegister) {
            for (const ui64 processes : {1, 2}) {
                const auto identity = MakeIdentity(0);
                auto query = MakeSchedulerQuery(identity);
                TSchedulerRuntimeFixture fixture(BuildConfig({0.5}, {{{ESpecialTaskCategory::Scan, 1}}}));
                fixture.PrimeBatching();
                TAtomicCounter executed;
                for (ui64 id = 1; id <= processes; ++id) {
                    fixture.RegisterProcess(id, identity);
                    fixture.Submit(executed, id);
                }
                NActors::TActorId workerId;
                auto workerObserver = fixture.Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
                    workerId = ev->Recipient;
                });
                TWorkerEventBlocker<NActors::TEvents::TEvWakeup> wakeups(fixture.Runtime,
                                                                         [&](const auto& ev) { return ev->Recipient == workerId; });
                NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
                fixture.SendQueryResponse(query.Query);
                fixture.WaitFor([&] { return wakeups.size() == 1; });
                UNIT_ASSERT(results.empty());
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), processes);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
                // Unregister while the worker is sleeping, not just while the result is held.
                TAtomicCounter cancelled;
                auto queued = std::make_shared<TCounterTask>(cancelled);
                const std::weak_ptr<ITask> queuedRef = queued;
                fixture.Runtime.Send(fixture.Distributor, fixture.Sink,
                                     new TEvExecution::TEvNewTask(std::move(queued), ESpecialTaskCategory::Scan, 1));
                for (ui64 id = 1; id <= processes; ++id) {
                    fixture.UnregisterProcess(id);
                }
                UNIT_ASSERT(queuedRef.expired());
                UNIT_ASSERT_VALUES_EQUAL(cancelled.Val(), 0);
                fixture.WaitFor([&] { return fixture.Removes[identity.QueryId] == 1; });
                wakeups.Stop().Unblock();
                fixture.WaitFor([&] { return !results.empty(); });
                UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(results.front()->Get()->GetResults().size(), processes);
                UNIT_ASSERT(results.front()->Get()->GetQueryIdentity() == identity);
                THashSet<ui64> completed;
                const auto scope = results.front()->Get()->GetResults().front().GetScope();
                for (const auto& task : results.front()->Get()->GetResults()) {
                    UNIT_ASSERT(task.GetScope() == scope);
                    UNIT_ASSERT(completed.emplace(task.GetProcessId()).second);
                }
                UNIT_ASSERT_VALUES_EQUAL(completed.size(), processes);
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), processes);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], 1);
                if (ReRegister) {
                    fixture.RegisterProcess(processes + 1, identity);
                    fixture.SendQueryResponse(query.Query);
                    fixture.Submit(executed, processes + 1);
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Adds[identity.QueryId], 2);
                }
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), processes); // No worker reuse before old accounting.
                UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 1);
                auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                    for (const auto& task : ev->Get()->GetResults()) {
                        if (task.GetProcessId() > processes) {
                            UNIT_ASSERT(task.GetScope() != scope);
                        }
                    }
                });
                results.Stop().Unblock();
                fixture.WaitFor([&] { return std::cmp_equal(executed.Val(), processes + ReRegister) && scope->GetCountInFlight() == 0 && query.Query->GetParent()->CpuUsage.load() == 0; });
                UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 0);
                if (ReRegister) {
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], 1);
                    fixture.UnregisterProcess(processes + 1);
                }
                fixture.WaitFor([&] { return fixture.Removes[identity.QueryId] == (ReRegister ? 2 : 1); });
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);
            }
        }

        /* Scenario:
            - Unregister some or all processes, optionally re-registering one, before QueryResponse.
            - A query response balances its HDRF registration; nullptr migrates survivors without Remove.
            - Cancelled tasks never execute, and cleanup permits a fresh registration of the same identity.
         */
        Y_UNIT_TEST_TWIN(LateQueryResponsePreservesRegistrationLifecycle, NullResponse) {
            // At response time: no processes, one survivor, or a new process after count reached zero.
            for (const ui64 stateAtReply : {0, 1, 2}) {
                const auto identity = MakeIdentity(0);
                auto query = MakeSchedulerQuery(identity);
                TSchedulerRuntimeFixture fixture(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
                fixture.SetLocalSchedulerEnabled(true);
                fixture.RegisterProcess(1, identity);
                fixture.RegisterProcess(2, identity);
                TAtomicCounter cancelled;
                auto task = std::make_shared<TCounterTask>(cancelled);
                const std::weak_ptr<ITask> queuedTask = task;
                fixture.Runtime.Send(fixture.Distributor, fixture.Sink,
                                     new TEvExecution::TEvNewTask(std::move(task), ESpecialTaskCategory::Scan, 1));
                UNIT_ASSERT(!queuedTask.expired());
                fixture.UnregisterProcess(1);
                UNIT_ASSERT(queuedTask.expired());
                if (stateAtReply != 1) {
                    fixture.UnregisterProcess(2);
                }
                if (stateAtReply == 2) {
                    fixture.RegisterProcess(3, identity);
                }
                const ui64 survivor = stateAtReply == 1 ? 2 : stateAtReply == 2 ? 3
                                                                                : 0;
                TAtomicCounter executed;
                if (survivor) {
                    fixture.Submit(executed, survivor);
                }
                fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Adds[identity.QueryId], 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], 0);
                if (NullResponse) {
                    fixture.SetLocalSchedulerEnabled(false);
                }
                ui64 results = 0;
                auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                    UNIT_ASSERT(ev->Get()->GetQueryIdentity() == (NullResponse ? kServiceQueryIdentity : identity));
                    results += ev->Get()->GetResults().size();
                });
                fixture.SendQueryResponse(NullResponse ? nullptr : query.Query);
                if (survivor) {
                    fixture.WaitFor([&] { return results == 1 && query.Query->GetParent()->CpuUsage.load() == 0; });
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], 0);
                    fixture.UnregisterProcess(survivor);
                }
                fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], NullResponse ? 0 : 1);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);
                // Cleanup must remove the old key, including after an empty migration.
                fixture.SetLocalSchedulerEnabled(true);
                fixture.RegisterProcess(4, identity);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Adds[identity.QueryId], 2);
                fixture.SendQueryResponse(query.Query);
                fixture.UnregisterProcess(4);
                fixture.WaitFor([&] { return fixture.Removes[identity.QueryId] == (NullResponse ? 1 : 2); });
                UNIT_ASSERT_VALUES_EQUAL(cancelled.Val(), 0);
            }
        }

        /* Scenario:
            - Queue tasks for two managed queries sharing one scope.
            - Build separate multi-task batches per identity and balance scope accounting and both registrations.
         */
        Y_UNIT_TEST(BatchesDoNotMixQueriesSharingScope) {
            const auto first = MakeIdentity(0);
            const auto second = MakeIdentity(1);
            auto query1 = MakeSchedulerQuery(first);
            auto query2 = MakeSchedulerQuery(second);
            TSchedulerRuntimeFixture fixture(BuildConfig({0.5}, {{{ESpecialTaskCategory::Scan, 1}}}));
            fixture.PrimeBatching();
            fixture.RegisterProcess(1, first);
            fixture.RegisterProcess(2, second);
            TAtomicCounter counter;
            for (ui32 i = 0; i < 5; ++i) {
                fixture.Submit(counter, 1);
                fixture.Submit(counter, 2);
            }
            std::shared_ptr<TProcessScope> scope;
            THashSet<ui64> batchedQueries;
            auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                const auto& result = *ev->Get();
                UNIT_ASSERT(result.GetQueryIdentity() == first || result.GetQueryIdentity() == second);
                UNIT_ASSERT(result.GetResults().size() > 1);
                batchedQueries.emplace(result.GetQueryIdentity().QueryId);
                const auto expectedProcess = result.GetQueryIdentity() == first ? 1 : 2;
                for (const auto& task : result.GetResults()) {
                    UNIT_ASSERT_VALUES_EQUAL(task.GetProcessId(), expectedProcess);
                    if (!scope) {
                        scope = task.GetScope();
                    }
                    UNIT_ASSERT(task.GetScope() == scope);
                }
            });
            fixture.SendQueryResponse(query1.Query);
            fixture.SendQueryResponse(query2.Query);
            fixture.WaitFor([&] { return counter.Val() == 10 && query1.Query->GetParent()->CpuUsage.load() == 0
                && query2.Query->GetParent()->CpuUsage.load() == 0; });
            UNIT_ASSERT_VALUES_EQUAL(batchedQueries.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 0);
            fixture.UnregisterProcess(1);
            fixture.UnregisterProcess(2);
            fixture.WaitFor([&] { return fixture.Removes[first.QueryId] == 1 && fixture.Removes[second.QueryId] == 1; });
        }

        /* Scenario:
            - Hold a query's lease during a pool shrink or removal.
            - An independent query initializes and runs while the topology update is pending.
            - Returning the lease applies the new capacity to the first query.
         */
        Y_UNIT_TEST_TWIN(PendingPoolUpdateDoesNotBlockIndependentQuery, RemovePool) {
            const auto initial = RemovePool
                ? BuildConfig({2, 3, 1}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}})
                : BuildConfig({5, 1}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
            auto target = BuildConfig({3, 1}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
            if (RemovePool) {
                target.MutableWorkerPools(0)->SetName("pool-1");
                target.MutableWorkerPools(1)->SetName("pool-2");
            }
            const auto first = MakeIdentity(1);
            const auto second = MakeIdentity(2);
            auto query1 = MakeSchedulerQuery(first, TDuration::MicroSeconds(10), 5);
            auto query2 = MakeSchedulerQuery(second);
            TSchedulerRuntimeFixture fixture(initial);
            fixture.RegisterProcess(1, first);
            fixture.SendQueryResponse(query1.Query);

            TWorkerEventBlocker<TEvInternal::TEvNewTask> batches(fixture.Runtime,
                                                                 [&](const auto& ev) { return ev->Get()->GetQueryIdentity() == first; });
            TAtomicCounter counter1;
            TAtomicCounter counter2;
            fixture.Submit(counter1, 1);
            fixture.WaitFor([&] { return batches.size() == 1; });
            fixture.UpdateConfig(target);
            UNIT_ASSERT_VALUES_EQUAL(query1.Query->CpuMaxDemand.load(), 5);
            fixture.RegisterProcess(2, second, ESpecialTaskCategory::Insert);
            fixture.Submit(counter2, 2, ESpecialTaskCategory::Insert);
            fixture.SendQueryResponse(query2.Query);
            fixture.WaitFor([&] { return counter2.Val() == 1 && query2.Query->GetParent()->CpuUsage.load() == 0; });
            UNIT_ASSERT_VALUES_EQUAL(query2.Query->CpuMaxDemand.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(query1.Query->CpuMaxDemand.load(), 5);
            batches.Stop().Unblock();
            fixture.WaitFor([&] { return counter1.Val() == 1 && query1.Query->CpuMaxDemand.load() == 3 && query1.Query->GetParent()->CpuUsage.load() == 0; });
        }

        /* Scenario:
            - Throttle two queries and schedule one wakeup at their minimum deadline after a full drain.
            - A blocked query keeps its priority deadline but does not request a new timer.
            - When scopes are released, the original priority ordering is preserved.
         */
        Y_UNIT_TEST(DrainSchedulesOnlyFreshRetriesAndPreservesPriority) {
            TManagerFixture fixture;
            auto& runtime = fixture.Runtime;
            const auto& deadlines = fixture.Deadlines;
            auto query1 = MakeSchedulerQuery(MakeIdentity(1), TDuration::MicroSeconds(1), 0);
            auto query2 = MakeSchedulerQuery(MakeIdentity(2), TDuration::Seconds(2), 0);
            auto config = BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}});
            TAtomicCounter executed;
            TWorkerEventBlocker<TEvInternal::TEvNewTask> held(runtime);
            fixture.Run(config, [&](TTasksManager& manager) {
                for (ui64 id : {1, 2}) {
                    manager.RegisterProcess(ESpecialTaskCategory::Scan, ::ToString(id), id, TCPULimitsConfig(1), MakeIdentity(id));
                    manager.SetQuery(MakeIdentity(id), id == 1 ? query1.Query : query2.Query);
                    manager.MutableCategoryVerified(ESpecialTaskCategory::Scan).RegisterTask(id, std::make_shared<TCounterTask>(executed));
                }
                const auto before = TMonotonic::Now();
                UNIT_ASSERT(!manager.DrainTasks());
                const auto after = TMonotonic::Now();
                UNIT_ASSERT_VALUES_EQUAL(deadlines.size(), 1);
                UNIT_ASSERT(deadlines[0].GetValue() >= (before + TDuration::MicroSeconds(1)).GetValue());
                UNIT_ASSERT(deadlines[0].GetValue() <= (after + TDuration::MicroSeconds(1)).GetValue());
                // Suppress runnable tasks without touching the already recorded query deadlines.
                auto& category = manager.MutableCategoryVerified(ESpecialTaskCategory::Scan);
                auto& firstScope = category.MutableProcessScope("1");
                auto& secondScope = category.MutableProcessScope("2");
                firstScope.IncInFlight();
                const auto secondBefore = TMonotonic::Now();
                UNIT_ASSERT(!manager.DrainTasks());
                UNIT_ASSERT_VALUES_EQUAL(deadlines.size(), 2);
                UNIT_ASSERT(deadlines[1].GetValue() >= (secondBefore + TDuration::Seconds(2)).GetValue());
                secondScope.IncInFlight();
                UNIT_ASSERT(!manager.DrainTasks());
                UNIT_ASSERT_VALUES_EQUAL(deadlines.size(), 2);
                firstScope.DecInFlight();
                secondScope.DecInFlight();
                query1.SetFairShare(1);
                query2.SetFairShare(1);
                category.RegisterTask(0, std::make_shared<TCounterTask>(executed));
                std::optional<TSchedulerQueryIdentity> selected;
                const auto previousFilter = runtime.SetEventFilter([&](auto&, auto& ev) {
                    if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                        selected = ev->template Get<TEvInternal::TEvNewTask>()->GetQueryIdentity();
                    }
                    return false;
                });
                UNIT_ASSERT(manager.DrainTasks());
                UNIT_ASSERT(selected && *selected == MakeIdentity(1));
                UNIT_ASSERT_VALUES_EQUAL(deadlines.size(), 2);
                runtime.SetEventFilter(previousFilter);
            });
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
        }

    } // Y_UNIT_TEST_SUITE(CompositeConveyorScheduler)

} // namespace NKikimr::NConveyorComposite
