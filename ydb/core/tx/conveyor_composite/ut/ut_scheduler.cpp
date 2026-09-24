#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/tree/snapshot.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/conveyor_composite/service/manager.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConveyorComposite {

    namespace {

        using TLinkConfig = std::pair<ESpecialTaskCategory, double>;

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
            const TSchedulerQueryIdentity& identity, const TDuration delay = TDuration::MicroSeconds(10), const ui64 fairShare = 1) {
            using namespace NKqp::NScheduler;
            auto delayParams = std::make_shared<TDelayParams>(TDelayParams{
                .MaxDelay = delay,
                .MinDelay = delay,
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

            void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
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
            NActors::TTestActorRuntime Runtime;
            NActors::TActorId Sink;
            NActors::TActorId Distributor;
            NActors::TActorId Scheduler;

            explicit TSchedulerRuntimeFixture(const NKikimrConfig::TCompositeConveyorConfig& proto) {
                Runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
                Sink = Runtime.AllocateEdgeActor();
                Scheduler = Runtime.AllocateEdgeActor();
                Runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(Runtime.GetNodeId(0)), Scheduler);
                auto config = ParseConfig(proto);
                Distributor = Runtime.Register(CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
                Runtime.EnableScheduleForActor(Distributor, true);
                Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }

            void RegisterProcess(const ui64 processId, const TSchedulerQueryIdentity& identity,
                                 const ESpecialTaskCategory category = ESpecialTaskCategory::Scan, const TString& scopeId = "scope") {
                const auto schedulerPool = identity.IsServiceQuery ? std::nullopt : std::make_optional(MakeSchedulerPool());
                Runtime.Send(Distributor, Sink, new TEvExecution::TEvRegisterProcess(
                    TCPULimitsConfig(1000), category, scopeId, processId, identity.QueryId, schedulerPool));
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

            void SetLocalSchedulerEnabled(bool enabled) {
                auto& scheduler = Runtime.GetAppData().KqpComputeScheduler;
                if (!scheduler) {
                    scheduler = std::make_shared<NKqp::NScheduler::TComputeScheduler>(
                        MakeIntrusive<NKqp::TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()), NKqp::NScheduler::TOptions{});
                }
                scheduler->ToggleEnabled(enabled);
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
        Y_UNIT_TEST(ProcessGuardForwardsIdentityAndNormalizesNullopt) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
            const auto service = runtime.AllocateEdgeActor();
            const auto identity = MakeIdentity(42);

            runtime.RunCall([&] {
                TProcessGuard managed(ESpecialTaskCategory::Scan, "scope", 1, TCPULimitsConfig(), service,
                    identity.QueryId, MakeSchedulerPool());
                managed.Finish();
                TProcessGuard unmanaged(ESpecialTaskCategory::Scan, "scope", 2, TCPULimitsConfig(), service);
                unmanaged.Finish();
                return true;
            });

            auto managed = runtime.GrabEdgeEvent<TEvExecution::TEvRegisterProcess>(service);
            auto unmanaged = runtime.GrabEdgeEvent<TEvExecution::TEvRegisterProcess>(service);
            const auto& schedulerPool = managed->Get()->GetSchedulerPool();
            UNIT_ASSERT(schedulerPool);
            UNIT_ASSERT_VALUES_EQUAL(managed->Get()->GetTxId(), identity.QueryId);
            UNIT_ASSERT_VALUES_EQUAL(schedulerPool->DatabaseId, "database");
            UNIT_ASSERT_VALUES_EQUAL(schedulerPool->PoolId, "pool");
            UNIT_ASSERT(!unmanaged->Get()->GetSchedulerPool());
        }

        Y_UNIT_TEST(SharedGuardLifetimeAndMoveUnregisterExactlyOnce) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
            const auto service = runtime.AllocateEdgeActor();
            std::shared_ptr<TProcessGuard> callbackGuard;
            runtime.RunCall([&] {
                auto ownerGuard = std::make_shared<TProcessGuard>(ESpecialTaskCategory::Scan, "scope", 1,
                    TCPULimitsConfig(), service, 0, MakeSchedulerPool());
                callbackGuard = ownerGuard;
                ownerGuard.reset();
                return true;
            });
            auto registration = runtime.GrabEdgeEvent<TEvExecution::TEvRegisterProcess>(service);
            runtime.Schedule(new NActors::IEventHandle(service, {}, new NActors::TEvents::TEvWakeup()), TDuration::MilliSeconds(2));
            UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvExecution::TEvUnregisterProcess>(service, TDuration::MilliSeconds(1)));
            runtime.RunCall([&] {
                TProcessGuard moved(std::move(*callbackGuard));
                callbackGuard.reset();
                return true;
            });
            auto removal = runtime.GrabEdgeEvent<TEvExecution::TEvUnregisterProcess>(service);
            UNIT_ASSERT_VALUES_EQUAL(removal->Get()->GetInternalProcessId(), registration->Get()->GetInternalProcessId());
            runtime.Schedule(new NActors::IEventHandle(service, {}, new NActors::TEvents::TEvWakeup()), TDuration::MilliSeconds(2));
            UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvExecution::TEvUnregisterProcess>(service, TDuration::MilliSeconds(1)));
        }

        Y_UNIT_TEST(SameScopeCanBeSharedByDifferentQueries) {
            const auto proto = BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}});
            const auto config = ParseConfig(proto);
            TCounters counters("TEST", MakeIntrusive<NMonitoring::TDynamicCounters>());
            TProcessCategory category(config.GetCategoryConfig(ESpecialTaskCategory::Scan), counters);
            const auto identity1 = MakeIdentity(1);
            const auto identity2 = MakeIdentity(2);

            auto scope1 = category.UpsertScope("shared", TCPULimitsConfig(1000));
            const auto* sharedScope = scope1.get();
            category.RegisterProcess(1, std::move(scope1), identity1);
            auto scope2 = category.UpsertScope("shared", TCPULimitsConfig(1000));
            UNIT_ASSERT_VALUES_EQUAL(scope2.get(), sharedScope);
            category.RegisterProcess(2, std::move(scope2), identity2);

            UNIT_ASSERT(category.HasProcesses(identity1));
            UNIT_ASSERT(category.HasProcesses(identity2));

            UNIT_ASSERT(category.UnregisterProcess(1) == identity1);
            UNIT_ASSERT(category.UnregisterProcess(2) == identity2);
        }

        Y_UNIT_TEST(DefaultIdentityUsesAlwaysReadyFactory) {
            TQueryRegistry registry;
            const auto identity = kServiceQueryIdentity;
            registry.RegisterProcess(identity);
            SetCapacity(registry, identity, 1);

            auto result = registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100));
            UNIT_ASSERT(std::holds_alternative<TSchedulerLease>(result));
            {
                auto lease = std::get<TSchedulerLease>(std::move(result));
                UNIT_ASSERT(lease);
            }
            registry.UnregisterProcess(identity);
            UNIT_ASSERT(!registry.RegisterProcess(identity));
            registry.UnregisterProcess(identity);
        }

        Y_UNIT_TEST(ManagedQueryIsNotReadyBeforeFactory) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            UNIT_ASSERT(registry.RegisterProcess(identity));
            UNIT_ASSERT(!registry.RegisterProcess(identity));
            SetCapacity(registry, identity, 3);

            const auto& state = registry.GetStateVerified(identity);
            UNIT_ASSERT(!state.IsReady());
            registry.UnregisterProcess(identity);
            UNIT_ASSERT(!state.IsWaitRelease());
            registry.UnregisterProcess(identity);
            UNIT_ASSERT(state.IsWaitRelease());
            UNIT_ASSERT(!registry.TryReleaseQuery(identity));
        }

        Y_UNIT_TEST(FactoryCreatesOneWorkPerPotentialWorker) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            SetCapacity(registry, identity, 4);

            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 4);
        }

        Y_UNIT_TEST(QueryDeadlineIsReplacedAcrossRetries) {
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity, TDuration::MicroSeconds(20), 0);
            TQueryRegistry registry;
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            SetCapacity(registry, identity, 1);

            auto& state = registry.GetStateVerified(identity);
            UNIT_ASSERT(std::holds_alternative<TMonotonic>(state.TryStart(TMonotonic::MicroSeconds(100))));
            UNIT_ASSERT(std::holds_alternative<TMonotonic>(state.TryStart(TMonotonic::MicroSeconds(110))));
            UNIT_ASSERT_VALUES_EQUAL(state.GetWakeUpDeadline()->GetValue(), TMonotonic::MicroSeconds(130).GetValue());
            Y_UNUSED(state.TryStart(TMonotonic::MicroSeconds(90)));
            UNIT_ASSERT_VALUES_EQUAL(state.GetWakeUpDeadline()->GetValue(), TMonotonic::MicroSeconds(110).GetValue());
        }

        Y_UNIT_TEST(AverageDeadlineIgnoresQueriesWithoutDeadline) {
            const auto identity1 = MakeIdentity(1);
            const auto identity2 = MakeIdentity(2);
            const auto identity3 = MakeIdentity(3);
            auto query1 = MakeSchedulerQuery(identity1, TDuration::MicroSeconds(20), 0);
            auto query2 = MakeSchedulerQuery(identity2, TDuration::MicroSeconds(40), 0);
            auto query3 = MakeSchedulerQuery(identity3);
            TQueryRegistry registry;
            for (const auto& identity : {identity1, identity2, identity3}) {
                registry.RegisterProcess(identity);
            }
            UNIT_ASSERT(registry.SetQuery(identity1, query1.Query));
            UNIT_ASSERT(registry.SetQuery(identity2, query2.Query));
            UNIT_ASSERT(registry.SetQuery(identity3, query3.Query));
            SetCapacity(registry, identity1, 1);
            SetCapacity(registry, identity2, 1);
            SetCapacity(registry, identity3, 1);

            Y_UNUSED(registry.GetStateVerified(identity1).TryStart(TMonotonic::MicroSeconds(100)));
            Y_UNUSED(registry.GetStateVerified(identity2).TryStart(TMonotonic::MicroSeconds(100)));
            UNIT_ASSERT_VALUES_EQUAL(
                registry.GetAverageWakeUpDeadline(TMonotonic::MicroSeconds(100)).GetValue(), TMonotonic::MicroSeconds(130).GetValue());
        }

        Y_UNIT_TEST(LeaseStopsWorkExactlyOnce) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            SetCapacity(registry, identity, 1);

            auto result = registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100));
            std::optional<TSchedulerLease> holder;
            holder.emplace(std::get<TSchedulerLease>(std::move(result)));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);
            holder.reset();
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
        }

        Y_UNIT_TEST(RegistryRehashKeepsActiveLeaseValid) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            SetCapacity(registry, identity, 1);

            auto result = registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100));
            {
                auto lease = std::get<TSchedulerLease>(std::move(result));
                for (ui64 queryId = 2; queryId < 1024; ++queryId) {
                    registry.RegisterProcess(MakeIdentity(queryId));
                }
                UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
        }

        Y_UNIT_TEST(UnregisterStopsThrottledWorks) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity, TDuration::Seconds(1), 0);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            SetCapacity(registry, identity, 2);
            Y_UNUSED(registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100)));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 1);

            registry.UnregisterProcess(identity);
            UNIT_ASSERT(registry.TryReleaseQuery(identity));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 0);
            UNIT_ASSERT(registry.RegisterProcess(identity));
        }

        Y_UNIT_TEST(UnregisterWaitsForStartedWorkAndAllowsReRegistration) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            SetCapacity(registry, identity, 1);
            auto result = registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100));
            {
                auto lease = std::get<TSchedulerLease>(std::move(result));
                registry.UnregisterProcess(identity);
                UNIT_ASSERT(registry.GetStateVerified(identity).IsWaitRelease());
                UNIT_ASSERT(!registry.TryReleaseQuery(identity));
                UNIT_ASSERT(!registry.RegisterProcess(identity));
                UNIT_ASSERT(!registry.GetStateVerified(identity).IsWaitRelease());
                registry.UnregisterProcess(identity);
                UNIT_ASSERT(!registry.TryReleaseQuery(identity));
            }
            UNIT_ASSERT(registry.TryReleaseQuery(identity));
            UNIT_ASSERT(registry.RegisterProcess(identity));
        }

        Y_UNIT_TEST(CapacityShrinkRemovesOnlyNonStartedWorks) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            SetCapacity(registry, identity, 3);
            auto result = registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100));
            {
                auto lease = std::get<TSchedulerLease>(std::move(result));
                Y_UNUSED(registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100)));

                SetCapacity(registry, identity, 1);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 0);
            }
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
        }

        Y_UNIT_TEST(ManagedTasksWaitForFactoryAndLeaseLivesUntilResult) {
            const auto proto = BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}});
            TSchedulerRuntimeFixture fixture(proto);
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            TAtomicCounter counter;
            fixture.RegisterProcess(1, identity);
            fixture.Submit(counter, 1);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(2));
            UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 0);

            TAutoPtr<NActors::IEventHandle> heldTask;
            auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
                if (!heldTask && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                    heldTask = ev.Release();
                    return NActors::TTestActorRuntime::EEventAction::DROP;
                }
                return NActors::TTestActorRuntime::EEventAction::PROCESS;
            });
            fixture.SendQueryResponse(query.Query);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(2));
            fixture.Runtime.SetObserverFunc(previousObserver);
            UNIT_ASSERT(heldTask);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);

            fixture.Runtime.Send(heldTask.Release(), 0, true);
            fixture.WaitFor([&] {
                return counter.Val() == 1 && query.Query->GetParent()->CpuUsage.load() == 0;
            });
        }

        Y_UNIT_TEST(OneCandidateFillsAllFreeWorkers) {
            const auto proto = BuildConfig({3}, {{{ESpecialTaskCategory::Scan, 1}}});
            TSchedulerRuntimeFixture fixture(proto);
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity, TDuration::MicroSeconds(10), 3);
            TAtomicCounter counter;
            fixture.RegisterProcess(1, identity);
            for (ui32 i = 0; i < 3; ++i) {
                fixture.Submit(counter, 1);
            }
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(2));
            UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 0);

            std::vector<TAutoPtr<NActors::IEventHandle>> heldTasks;
            auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                    heldTasks.emplace_back(ev.Release());
                    return NActors::TTestActorRuntime::EEventAction::DROP;
                }
                return NActors::TTestActorRuntime::EEventAction::PROCESS;
            });
            fixture.SendQueryResponse(query.Query);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(2));
            fixture.Runtime.SetObserverFunc(previousObserver);

            UNIT_ASSERT_VALUES_EQUAL(heldTasks.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 3);
            for (auto& task : heldTasks) {
                fixture.Runtime.Send(task.Release(), 0, true);
            }
            fixture.WaitFor([&] {
                return counter.Val() == 3 && query.Query->GetParent()->CpuUsage.load() == 0;
            });
        }

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
            UNIT_ASSERT_VALUES_EQUAL(query->CpuDemand.load(), 1);
            const auto firstDistributor = fixture.Distributor;
            const auto secondDistributor = fixture.Runtime.Register(CreateService(ParseConfig(proto), MakeIntrusive<NMonitoring::TDynamicCounters>()));
            fixture.Runtime.EnableScheduleForActor(secondDistributor, true);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            fixture.Distributor = secondDistributor;
            fixture.RegisterProcess(3, identity);
            fixture.WaitFor([&] { return query->CpuDemand.load() == 2; });
            fixture.Distributor = firstDistributor;
            fixture.UnregisterProcess(1);
            fixture.UnregisterProcess(2);
            fixture.WaitFor([&] { return query->CpuDemand.load() == 1; });
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(query->GetParent()->GetQuery(identity.QueryId) == query);
            fixture.Distributor = secondDistributor;
            fixture.Submit(counter, 3);
            fixture.WaitFor([&] { return counter.Val() == 2 && query->GetParent()->CpuUsage.load() == 0; });
            fixture.UnregisterProcess(3);
            fixture.WaitFor([&] { return query->CpuDemand.load() == 0; });
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(scheduler->RemoveQuery(identity.QueryId));
            UNIT_ASSERT(!scheduler->RemoveQuery(identity.QueryId));
        }

        Y_UNIT_TEST(ServiceIdentityAndManagedZeroAreDifferent) {
            TQueryRegistry registry;
            const auto managed = MakeIdentity(0);
            UNIT_ASSERT(managed != kServiceQueryIdentity);
            UNIT_ASSERT(registry.RegisterProcess(managed));
            UNIT_ASSERT(!registry.RegisterProcess(kServiceQueryIdentity));
            UNIT_ASSERT(!registry.GetStateVerified(managed).IsReady());
            UNIT_ASSERT(registry.GetStateVerified(kServiceQueryIdentity).IsReady());
            auto query = MakeSchedulerQuery(managed);
            UNIT_ASSERT(registry.SetQuery(managed, query.Query));
            SetCapacity(registry, managed, 1);
            registry.UnregisterProcess(managed);
            UNIT_ASSERT(registry.TryReleaseQuery(managed));
            UNIT_ASSERT(!registry.RegisterProcess(kServiceQueryIdentity));
        }

        Y_UNIT_TEST(PendingQueryMovesProcessReferencesToService) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(0);
            registry.RegisterProcess(kServiceQueryIdentity);
            registry.RegisterProcess(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT_EXCEPTION(registry.SetQuery(identity, nullptr), yexception);
            UNIT_ASSERT(!registry.GetStateVerified(identity).IsReady());
            UNIT_ASSERT_VALUES_EQUAL(registry.MovePendingQueryToService(identity), 2);
            UNIT_ASSERT(registry.RegisterProcess(identity));
            UNIT_ASSERT(!registry.GetStateVerified(identity).IsReady());
            SetCapacity(registry, kServiceQueryIdentity, 1);
            {
                auto result = registry.GetStateVerified(kServiceQueryIdentity).TryStart(TMonotonic::Now());
                UNIT_ASSERT(std::holds_alternative<TSchedulerLease>(result));
            }
            for (ui32 i = 0; i < 2; ++i) {
                registry.UnregisterProcess(kServiceQueryIdentity);
                UNIT_ASSERT(!registry.GetStateVerified(kServiceQueryIdentity).IsWaitRelease());
            }
            registry.UnregisterProcess(kServiceQueryIdentity);
            UNIT_ASSERT(registry.TryReleaseQuery(kServiceQueryIdentity));
        }

        Y_UNIT_TEST(ReadyQueryCannotMoveToService) {
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            TQueryRegistry registry;
            registry.RegisterProcess(identity);
            registry.SetQuery(identity, query.Query);
            SetCapacity(registry, identity, 1);
            auto lease = registry.GetStateVerified(identity).TryStart(TMonotonic::Now());
            UNIT_ASSERT(std::holds_alternative<TSchedulerLease>(lease));
            UNIT_ASSERT_EXCEPTION(registry.MovePendingQueryToService(identity), yexception);
            UNIT_ASSERT(registry.GetStateVerified(identity).IsReady());
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);
        }

        Y_UNIT_TEST_TWIN(NullResponseMovesAllPendingProcessesAndQueuedTasks, ZeroTxId) {
            ui64 adds = 0;
            ui64 removes = 0;
            TSchedulerRuntimeFixture fixture(BuildConfig({1, 1}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}}));
            fixture.SetLocalSchedulerEnabled(true);
            fixture.Runtime.SetEventFilter([&](auto&, auto& ev) {
                using namespace NKqp::NScheduler;
                adds += ev->GetTypeRewrite() == TEvAddDatabase::EventType
                    || ev->GetTypeRewrite() == TEvAddPool::EventType || ev->GetTypeRewrite() == TEvAddQuery::EventType;
                removes += ev->GetTypeRewrite() == TEvRemoveQuery::EventType;
                return false;
            });
            const auto identity = MakeIdentity(ZeroTxId ? 0 : 42);
            TAtomicCounter queued;
            TAtomicCounter service;
            ui64 results = 0;
            auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                UNIT_ASSERT(ev->Get()->GetQueryIdentity() == kServiceQueryIdentity);
                results += ev->Get()->GetResults().size();
            });
            fixture.RegisterProcess(1, identity);
            fixture.Submit(queued, 1);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            // These registrations join the identity whose first handler is suspended.
            fixture.RegisterProcess(2, identity);
            fixture.Submit(queued, 2);
            fixture.RegisterProcess(3, identity, ESpecialTaskCategory::Insert);
            fixture.Submit(queued, 3, ESpecialTaskCategory::Insert);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(queued.Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(adds, 3);

            fixture.SetLocalSchedulerEnabled(false);
            fixture.RegisterProcess(4, identity);
            fixture.Submit(service, 4);
            fixture.WaitFor([&] { return service.Val() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(queued.Val(), 0);
            fixture.SendQueryResponse(nullptr);
            fixture.WaitFor([&] { return queued.Val() == 3 && results == 4; });
            fixture.UnregisterProcess(1);
            fixture.UnregisterProcess(2);
            fixture.UnregisterProcess(3, ESpecialTaskCategory::Insert);
            fixture.UnregisterProcess(4);
            fixture.RegisterProcess(5, identity);
            fixture.Submit(service, 5);
            fixture.WaitFor([&] { return service.Val() == 2 && results == 5; });
            fixture.UnregisterProcess(5);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(adds, 3);
            UNIT_ASSERT_VALUES_EQUAL(removes, 0);
        }

        Y_UNIT_TEST(DisabledSchedulerRegistersDirectlyInService) {
            ui64 hdrfEvents = 0;
            TSchedulerRuntimeFixture fixture(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
            fixture.SetLocalSchedulerEnabled(false);
            fixture.Runtime.SetEventFilter([&](auto&, auto& ev) {
                using namespace NKqp::NScheduler;
                hdrfEvents += ev->GetTypeRewrite() == TEvAddDatabase::EventType || ev->GetTypeRewrite() == TEvAddPool::EventType
                    || ev->GetTypeRewrite() == TEvAddQuery::EventType || ev->GetTypeRewrite() == TEvRemoveQuery::EventType;
                return false;
            });
            TAtomicCounter counter;
            ui64 results = 0;
            auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                UNIT_ASSERT(ev->Get()->GetQueryIdentity() == kServiceQueryIdentity);
                results += ev->Get()->GetResults().size();
            });
            for (ui64 id : {0, 1, 2}) {
                fixture.RegisterProcess(id + 1, MakeIdentity(id));
                fixture.Submit(counter, id + 1);
                fixture.WaitFor([&] { return results == id + 1; });
                fixture.UnregisterProcess(id + 1);
            }
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 3);
            UNIT_ASSERT_VALUES_EQUAL(hdrfEvents, 0);
        }

        Y_UNIT_TEST_TWIN(NullResponseAfterUnregisterDoesNotRemoveHdrf, AllRemoved) {
            ui64 removes = 0;
            TSchedulerRuntimeFixture fixture(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
            fixture.SetLocalSchedulerEnabled(true);
            fixture.Runtime.SetEventFilter([&](auto&, auto& ev) {
                removes += ev->GetTypeRewrite() == NKqp::NScheduler::TEvRemoveQuery::EventType;
                return false;
            });
            const auto identity = MakeIdentity(0);
            fixture.RegisterProcess(1, identity);
            fixture.RegisterProcess(2, identity);
            fixture.UnregisterProcess(1);
            if (AllRemoved) {
                fixture.UnregisterProcess(2);
            }
            fixture.SetLocalSchedulerEnabled(false);
            fixture.SendQueryResponse(nullptr);
            TAtomicCounter counter;
            if (!AllRemoved) {
                fixture.Submit(counter, 2);
                fixture.WaitFor([&] { return counter.Val() == 1; });
                fixture.UnregisterProcess(2);
            }
            fixture.RegisterProcess(3, identity);
            fixture.Submit(counter, 3);
            fixture.WaitFor([&] { return counter.Val() == (AllRemoved ? 1 : 2); });
            fixture.UnregisterProcess(3);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(removes, 0);
        }

        Y_UNIT_TEST(MoveToServicePreservesScopesQueuesAndAccounting) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
            const auto sink = runtime.AllocateEdgeActor();
            const auto identity = MakeIdentity(0);
            auto config = ParseConfig(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
            TCounters counters("TEST", MakeIntrusive<NMonitoring::TDynamicCounters>());
            TAtomicCounter executed;
            std::unique_ptr<TTasksManager> manager;
            std::shared_ptr<TProcessScope> scope;
            std::shared_ptr<TCPUUsage> usage;
            runtime.RunCall([&] {
                manager = std::make_unique<TTasksManager>("TEST", config, sink, counters);
                manager->RegisterProcess(ESpecialTaskCategory::Scan, "shared", 3, TCPULimitsConfig(1), kServiceQueryIdentity);
                auto& category = manager->MutableCategoryVerified(ESpecialTaskCategory::Scan);
                scope = category.GetProcessScopePtrVerified("shared");
                usage = scope->GetCPUUsage();
                category.RegisterTask(3, std::make_shared<TCounterTask>(executed));
                UNIT_ASSERT(manager->DrainTasks());
                for (ui64 id : {1, 2}) {
                    manager->RegisterProcess(ESpecialTaskCategory::Scan, "shared", id, TCPULimitsConfig(1), identity);
                    category.RegisterTask(id, std::make_shared<TCounterTask>(executed));
                }
                UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 1);
                UNIT_ASSERT_VALUES_EQUAL(category.GetWaitingQueueSize(), 2);
                const auto duration = usage->GetDuration();
                const auto predicted = usage->GetPredictedDuration();
                manager->MovePendingQueryToService(identity);
                UNIT_ASSERT(!category.HasProcesses(identity));
                UNIT_ASSERT(category.GetProcessScopePtrVerified("shared") == scope);
                UNIT_ASSERT(scope->GetCPUUsage() == usage);
                UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 1);
                UNIT_ASSERT_VALUES_EQUAL(category.GetWaitingQueueSize(), 2);
                UNIT_ASSERT_VALUES_EQUAL(usage->GetDuration(), duration);
                UNIT_ASSERT_VALUES_EQUAL(usage->GetPredictedDuration(), predicted);
                UNIT_ASSERT(!category.GetMinProcessUsage(kServiceQueryIdentity));
                UNIT_ASSERT(!manager->DrainTasks());
                return true;
            });
            for (ui64 completed = 1; completed <= 3; ++completed) {
                auto result = runtime.GrabEdgeEvent<TEvInternal::TEvTaskProcessedResult>(sink);
                UNIT_ASSERT(result->Get()->GetQueryIdentity() == kServiceQueryIdentity);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetResults().size(), 1);
                UNIT_ASSERT(result->Get()->GetResults().front().GetScope() == scope);
                const auto duration = result->Get()->GetResults().front().GetDuration();
                const auto previousUsage = usage->GetDuration();
                runtime.RunCall([&] {
                    auto& pool = manager->MutableWorkersPool(result->Get()->GetWorkersPoolId());
                    pool.PutTaskResults(result->Get()->DetachResults(), result->Get()->GetWorkersPoolId(), result->Get()->GetWorkerIdx());
                    pool.ReleaseWorker(result->Get()->GetWorkerIdx());
                    UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(usage->GetDuration(), previousUsage + duration);
                    if (completed < 3) {
                        UNIT_ASSERT(manager->DrainTasks());
                    }
                    return true;
                });
            }
            runtime.RunCall([&] {
                for (ui64 id : {1, 2, 3}) {
                    UNIT_ASSERT(manager->UnregisterProcess(ESpecialTaskCategory::Scan, id) == kServiceQueryIdentity);
                }
                UNIT_ASSERT(!manager->TryReleaseQuery(kServiceQueryIdentity));
                UNIT_ASSERT(manager->RegisterProcess(ESpecialTaskCategory::Scan, "new", 4, TCPULimitsConfig(1), identity));
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 3);
        }

        Y_UNIT_TEST(RuntimeDisableDoesNotMigrateReadyQueries) {
            TSchedulerRuntimeFixture fixture(BuildConfig({2}, {{{ESpecialTaskCategory::Scan, 1}}}));
            fixture.SetLocalSchedulerEnabled(true);
            const auto identity = MakeIdentity(0);
            auto query = MakeSchedulerQuery(identity);
            fixture.RegisterProcess(1, identity);
            fixture.SendQueryResponse(query.Query);
            fixture.SetLocalSchedulerEnabled(false);
            fixture.RegisterProcess(2, identity);
            TAtomicCounter executed;
            ui64 results = 0;
            auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                for (const auto& result : ev->Get()->GetResults()) {
                    UNIT_ASSERT(ev->Get()->GetQueryIdentity() == (result.GetProcessId() == 1 ? identity : kServiceQueryIdentity));
                    ++results;
                }
            });
            fixture.Submit(executed, 1);
            fixture.Submit(executed, 2);
            fixture.WaitFor([&] { return results == 2 && query.Query->GetParent()->CpuUsage.load() == 0; });
            fixture.UnregisterProcess(2);
            fixture.UnregisterProcess(1);
            auto remove = fixture.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(fixture.Scheduler);
            UNIT_ASSERT_VALUES_EQUAL(remove->Get()->QueryId, 0);
            UNIT_ASSERT(!remove->Get()->IsForceRemove);
        }

        Y_UNIT_TEST(AverageDeadlineDoesNotOverflow) {
            auto first = MakeSchedulerQuery(MakeIdentity(1), TDuration::MicroSeconds(100), 0);
            auto second = MakeSchedulerQuery(MakeIdentity(2), TDuration::MicroSeconds(201), 0);
            TQueryRegistry registry;
            const auto now = TMonotonic::FromValue(Max<ui64>() - 1000);
            UNIT_ASSERT_VALUES_EQUAL(registry.GetAverageWakeUpDeadline(now), now);
            for (ui64 id : {1, 2}) {
                const auto identity = MakeIdentity(id);
                registry.RegisterProcess(identity);
                registry.SetQuery(identity, id == 1 ? first.Query : second.Query);
                SetCapacity(registry, identity, 1);
                Y_UNUSED(registry.GetStateVerified(identity).TryStart(now));
            }
            UNIT_ASSERT_VALUES_EQUAL(registry.GetAverageWakeUpDeadline(now).GetValue(), now.GetValue() + 150);
            UNIT_ASSERT_VALUES_EQUAL(registry.GetMinWakeUpDeadline()->GetValue(), now.GetValue() + 100);
        }

        Y_UNIT_TEST(PrepareAndApplyCapacityArePersonalAndRetryDeferredShrink) {
            TQueryRegistry registry;
            auto first = MakeSchedulerQuery(MakeIdentity(1), TDuration::MicroSeconds(10), 3);
            auto second = MakeSchedulerQuery(MakeIdentity(2));
            for (ui64 id : {1, 2}) {
                const auto identity = MakeIdentity(id);
                registry.RegisterProcess(identity);
                registry.SetQuery(identity, id == 1 ? first.Query : second.Query);
                SetCapacity(registry, identity, 3);
            }
            std::vector<TSchedulerLease> leases;
            for (ui32 i = 0; i < 3; ++i) {
                leases.emplace_back(std::get<TSchedulerLease>(registry.GetStateVerified(MakeIdentity(1)).TryStart(TMonotonic::Now())));
            }
            registry.PrepareWorkCapacity(MakeIdentity(1), 1);
            UNIT_ASSERT_VALUES_EQUAL(first.Query->CpuDemand.load(), 3);
            registry.ApplyWorkCapacity(MakeIdentity(1));
            UNIT_ASSERT_VALUES_EQUAL(first.Query->CpuDemand.load(), 3);
            leases.pop_back();
            leases.pop_back();
            registry.ApplyWorkCapacity(MakeIdentity(1));
            registry.ApplyWorkCapacity(MakeIdentity(1));
            UNIT_ASSERT_VALUES_EQUAL(first.Query->CpuDemand.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(second.Query->CpuDemand.load(), 3);
            registry.PrepareWorkCapacity(MakeIdentity(1), 100);
            registry.ApplyWorkCapacity(MakeIdentity(1));
            UNIT_ASSERT_VALUES_EQUAL(first.Query->CpuDemand.load(), 100);
            UNIT_ASSERT_VALUES_EQUAL(first.Query->GetParent()->CpuUsage.load(), 1);
            leases.clear();
            UNIT_ASSERT_VALUES_EQUAL(first.Query->GetParent()->CpuUsage.load(), 0);
        }

        Y_UNIT_TEST(UnregisterBeforeResponseReleasesExactlyOneRegistration) {
            TSchedulerRuntimeFixture fixture(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
            auto query = MakeSchedulerQuery(MakeIdentity(0));
            fixture.RegisterProcess(1, MakeIdentity(0));
            fixture.RegisterProcess(2, MakeIdentity(0));
            fixture.UnregisterProcess(1);
            fixture.UnregisterProcess(2);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(2));
            fixture.SendQueryResponse(query.Query);
            auto remove = fixture.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(fixture.Scheduler);
            UNIT_ASSERT_VALUES_EQUAL(remove->Get()->QueryId, 0);
            UNIT_ASSERT(!remove->Get()->IsForceRemove);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 0);
            UNIT_ASSERT(!fixture.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(fixture.Scheduler, TDuration::MilliSeconds(1)));

            // A new lifetime must register again, not reuse a deleted state.
            fixture.RegisterProcess(3, MakeIdentity(0));
            fixture.SendQueryResponse(nullptr);
            TAtomicCounter counter;
            fixture.Submit(counter, 3);
            fixture.WaitFor([&] { return counter.Val() == 1; });
            fixture.UnregisterProcess(3);
            UNIT_ASSERT(!fixture.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(fixture.Scheduler, TDuration::MilliSeconds(2)));
        }

        Y_UNIT_TEST(ReRegistrationDuringAwaitKeepsSingleAdd) {
            TSchedulerRuntimeFixture fixture(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
            auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            fixture.RegisterProcess(1, identity);
            fixture.UnregisterProcess(1);
            fixture.RegisterProcess(2, identity);
            TAtomicCounter counter;
            fixture.Submit(counter, 2);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(2));
            UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 0);
            fixture.SendQueryResponse(query.Query);
            fixture.WaitFor([&] { return counter.Val() == 1 && query.Query->GetParent()->CpuUsage.load() == 0; });
            UNIT_ASSERT(!fixture.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddQuery>(fixture.Scheduler, TDuration::MilliSeconds(1)));
            fixture.UnregisterProcess(2);
            auto remove = fixture.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(fixture.Scheduler);
            UNIT_ASSERT_VALUES_EQUAL(remove->Get()->QueryId, identity.QueryId);
        }

        Y_UNIT_TEST(UnregisterWaitsForDelayedAccountingResult) {
            TSchedulerRuntimeFixture fixture(BuildConfig({0.5}, {{{ESpecialTaskCategory::Scan, 1}}}));
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            fixture.RegisterProcess(1, identity);
            fixture.SendQueryResponse(query.Query);
            TAutoPtr<NActors::IEventHandle> heldResult;
            auto observer = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvInternal::TEvTaskProcessedResult::EventType) {
                    UNIT_ASSERT(ev->Get<TEvInternal::TEvTaskProcessedResult>()->GetQueryIdentity() == identity);
                    heldResult = ev.Release();
                    return NActors::TTestActorRuntime::EEventAction::DROP;
                }
                if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                    fixture.Runtime.EnableScheduleForActor(ev->Recipient, true);
                }
                return NActors::TTestActorRuntime::EEventAction::PROCESS;
            });
            TAtomicCounter counter;
            fixture.Submit(counter, 1);
            fixture.WaitFor([&] { return bool(heldResult); });
            UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
            fixture.UnregisterProcess(1);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(2));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);
            UNIT_ASSERT(!fixture.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(fixture.Scheduler, TDuration::MilliSeconds(1)));
            fixture.Runtime.SetObserverFunc(observer);
            fixture.Runtime.Send(heldResult.Release(), 0, true);
            auto remove = fixture.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(fixture.Scheduler);
            UNIT_ASSERT(!remove->Get()->IsForceRemove);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 0);
        }

        Y_UNIT_TEST(BatchesDoNotMixQueriesSharingScope) {
            TSchedulerRuntimeFixture fixture(BuildConfig({2}, {{{ESpecialTaskCategory::Scan, 1}}}));
            const auto first = MakeIdentity(0);
            const auto second = MakeIdentity(1);
            auto query1 = MakeSchedulerQuery(first);
            auto query2 = MakeSchedulerQuery(second);
            fixture.RegisterProcess(1, first);
            fixture.RegisterProcess(2, second);
            TAtomicCounter counter;
            for (ui32 i = 0; i < 5; ++i) {
                fixture.Submit(counter, 1);
                fixture.Submit(counter, 2);
            }
            auto observer = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvInternal::TEvTaskProcessedResult::EventType) {
                    auto* result = ev->Get<TEvInternal::TEvTaskProcessedResult>();
                    const auto expectedProcess = result->GetQueryIdentity() == first ? 1 : 2;
                    for (const auto& task : result->GetResults()) {
                        UNIT_ASSERT_VALUES_EQUAL(task.GetProcessId(), expectedProcess);
                    }
                }
                return NActors::TTestActorRuntime::EEventAction::PROCESS;
            });
            fixture.SendQueryResponse(query1.Query);
            fixture.SendQueryResponse(query2.Query);
            fixture.WaitFor([&] { return counter.Val() == 10 && query1.Query->GetParent()->CpuUsage.load() == 0
                && query2.Query->GetParent()->CpuUsage.load() == 0; });
            fixture.Runtime.SetObserverFunc(observer);
        }

        Y_UNIT_TEST(SeveralProcessesInBatchReleaseAfterSharedLease) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
            const auto sink = runtime.AllocateEdgeActor();
            const auto identity = MakeIdentity(42);
            auto query = MakeSchedulerQuery(identity);
            auto config = ParseConfig(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
            TCounters counters("TEST", MakeIntrusive<NMonitoring::TDynamicCounters>());
            TAtomicCounter executed;
            std::unique_ptr<TTasksManager> manager;
            runtime.RunCall([&] {
                manager = std::make_unique<TTasksManager>("TEST", config, sink, counters);
                const auto poolId = config.GetCategoryConfig(ESpecialTaskCategory::Scan).GetWorkerPools().front();
                manager->MutableWorkersPool(poolId).AddDeliveryDuration(TDuration::Seconds(1));
                for (ui64 id : {1, 2}) {
                    manager->RegisterProcess(ESpecialTaskCategory::Scan, "scope", id, TCPULimitsConfig(1000), identity);
                    manager->MutableCategoryVerified(ESpecialTaskCategory::Scan).RegisterTask(id, std::make_shared<TCounterTask>(executed));
                }
                manager->SetQuery(identity, query.Query);
                UNIT_ASSERT(manager->DrainTasks());
                return true;
            });
            auto result = runtime.GrabEdgeEvent<TEvInternal::TEvTaskProcessedResult>(sink);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetResults().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 2);
            UNIT_ASSERT(result->Get()->GetQueryIdentity() == identity);
            runtime.RunCall([&] {
                manager->UnregisterProcess(ESpecialTaskCategory::Scan, 1);
                manager->UnregisterProcess(ESpecialTaskCategory::Scan, 2);
                UNIT_ASSERT(!manager->MutableCategoryVerified(ESpecialTaskCategory::Scan).MutableProcessScopeOptional("scope"));
                UNIT_ASSERT(!manager->TryReleaseQuery(identity));
                auto& pool = manager->MutableWorkersPool(result->Get()->GetWorkersPoolId());
                pool.PutTaskResults(result->Get()->DetachResults(), result->Get()->GetWorkersPoolId(), result->Get()->GetWorkerIdx());
                pool.ReleaseWorker(result->Get()->GetWorkerIdx());
                UNIT_ASSERT(manager->TryReleaseQuery(identity));
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 0);
        }

        Y_UNIT_TEST_TWIN(PendingPoolUpdateDoesNotBlockIndependentQuery, RemovePool) {
            const auto initial = RemovePool
                ? BuildConfig({2, 3, 1}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}})
                : BuildConfig({5, 1}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
            auto target = BuildConfig({3, 1}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
            if (RemovePool) {
                target.MutableWorkerPools(0)->SetName("pool-1");
                target.MutableWorkerPools(1)->SetName("pool-2");
            }
            TSchedulerRuntimeFixture fixture(initial);
            const auto first = MakeIdentity(1);
            const auto second = MakeIdentity(2);
            auto query1 = MakeSchedulerQuery(first, TDuration::MicroSeconds(10), 5);
            auto query2 = MakeSchedulerQuery(second);
            fixture.RegisterProcess(1, first);
            fixture.SendQueryResponse(query1.Query);

            TAutoPtr<NActors::IEventHandle> heldTask;
            auto observer = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType
                    && ev->Get<TEvInternal::TEvNewTask>()->GetQueryIdentity() == first) {
                    heldTask = ev.Release();
                    return NActors::TTestActorRuntime::EEventAction::DROP;
                }
                return NActors::TTestActorRuntime::EEventAction::PROCESS;
            });
            TAtomicCounter counter1;
            TAtomicCounter counter2;
            fixture.Submit(counter1, 1);
            fixture.WaitFor([&] { return bool(heldTask); });
            fixture.UpdateConfig(target);
            UNIT_ASSERT_VALUES_EQUAL(query1.Query->CpuDemand.load(), 5);
            fixture.RegisterProcess(2, second, ESpecialTaskCategory::Insert);
            fixture.Submit(counter2, 2, ESpecialTaskCategory::Insert);
            fixture.SendQueryResponse(query2.Query);
            fixture.WaitFor([&] { return counter2.Val() == 1 && query2.Query->GetParent()->CpuUsage.load() == 0; });
            UNIT_ASSERT_VALUES_EQUAL(query2.Query->CpuDemand.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(query1.Query->CpuDemand.load(), 5);
            fixture.Runtime.SetObserverFunc(observer);
            fixture.Runtime.Send(heldTask.Release(), 0, true);
            fixture.WaitFor([&] { return counter1.Val() == 1 && query1.Query->CpuDemand.load() == 3
                && query1.Query->GetParent()->CpuUsage.load() == 0; });
        }

        Y_UNIT_TEST(DrainSchedulesOneMinimumWakeupEvenWithoutNewAttempts) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
            runtime.UpdateCurrentTime(TInstant::FromValue(TMonotonic::Now().GetValue()), true);
            const auto sink = runtime.AllocateEdgeActor();
            std::vector<TInstant> deadlines;
            runtime.SetScheduledEventFilter([&](auto&, auto& ev, TDuration, TInstant& deadline) {
                if (ev->Recipient == sink && ev->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType) {
                    deadlines.push_back(deadline);
                    return true;
                }
                return false;
            });
            auto query1 = MakeSchedulerQuery(MakeIdentity(1), TDuration::Seconds(1), 0);
            auto query2 = MakeSchedulerQuery(MakeIdentity(2), TDuration::Seconds(2), 0);
            auto config = ParseConfig(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
            TCounters counters("TEST", MakeIntrusive<NMonitoring::TDynamicCounters>());
            TAtomicCounter executed;
            runtime.RunCall([&] {
                TTasksManager manager("TEST", config, sink, counters);
                for (ui64 id : {1, 2}) {
                    manager.RegisterProcess(ESpecialTaskCategory::Scan, "scope", id, TCPULimitsConfig(1), MakeIdentity(id));
                    manager.SetQuery(MakeIdentity(id), id == 1 ? query1.Query : query2.Query);
                    manager.MutableCategoryVerified(ESpecialTaskCategory::Scan).RegisterTask(id, std::make_shared<TCounterTask>(executed));
                }
                const auto before = TMonotonic::Now();
                UNIT_ASSERT(!manager.DrainTasks());
                const auto after = TMonotonic::Now();
                UNIT_ASSERT_VALUES_EQUAL(deadlines.size(), 1);
                UNIT_ASSERT(deadlines[0].GetValue() >= (before + TDuration::Seconds(1)).GetValue());
                UNIT_ASSERT(deadlines[0].GetValue() <= (after + TDuration::Seconds(1)).GetValue());
                // Suppress runnable tasks without touching the already recorded query deadlines.
                auto& scope = manager.MutableCategoryVerified(ESpecialTaskCategory::Scan).MutableProcessScope("scope");
                scope.IncInFlight();
                UNIT_ASSERT(!manager.DrainTasks());
                UNIT_ASSERT_VALUES_EQUAL(deadlines.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(deadlines[1], deadlines[0]);
                scope.DecInFlight();
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
        }

        Y_UNIT_TEST(TopologyChangesReconcilePotentialWorkerCapacity) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
            const auto sink = runtime.AllocateEdgeActor();
            auto initial = ParseConfig(BuildConfig({2, 3}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}}));
            auto grown = ParseConfig(BuildConfig({4, 3}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}}));
            auto relinked = ParseConfig(BuildConfig({4, 1}, {{{ESpecialTaskCategory::Insert, 1}}, {{ESpecialTaskCategory::Scan, 1}}}));
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            TCounters counters("TEST", MakeIntrusive<NMonitoring::TDynamicCounters>());

            runtime.RunCall([&] {
                TTasksManager manager("TEST", initial, sink, counters);
                manager.RegisterProcess(ESpecialTaskCategory::Scan, "scope", 1, TCPULimitsConfig(1000), identity);
                manager.SetQuery(identity, query.Query);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 2);

                manager.PrepareConfigUpdate(grown);
                UNIT_ASSERT(manager.IsReadyForUpdate());
                manager.ApplyConfigUpdate(grown, sink, counters);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 4);

                manager.PrepareConfigUpdate(relinked);
                UNIT_ASSERT(manager.IsReadyForUpdate());
                manager.ApplyConfigUpdate(relinked, sink, counters);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 1);
                manager.UnregisterProcess(ESpecialTaskCategory::Scan, 1);
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 0);
        }
    } // Y_UNIT_TEST_SUITE(CompositeConveyorScheduler)

} // namespace NKikimr::NConveyorComposite
