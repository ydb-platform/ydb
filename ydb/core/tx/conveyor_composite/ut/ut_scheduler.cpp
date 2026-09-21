#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/kqp/runtime/scheduler/tree/snapshot.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/conveyor_composite/service/manager.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConveyorComposite {

    namespace {

        using TLinkConfig = std::pair<ESpecialTaskCategory, double>;

        TSchedulerQueryIdentity MakeIdentity(const ui64 queryId, const TString& databaseId = "database", const TString& poolId = "pool") {
            return {
                .DatabaseId = databaseId,
                .PoolId = poolId,
                .QueryId = queryId,
            };
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
            auto database = std::make_shared<NHdrf::NDynamic::TDatabase>(identity.DatabaseId);
            auto pool = std::make_shared<NHdrf::NDynamic::TPool>(identity.PoolId, counters);
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

            explicit TSchedulerRuntimeFixture(const NKikimrConfig::TCompositeConveyorConfig& proto) {
                Runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
                Sink = Runtime.AllocateEdgeActor();
                auto config = ParseConfig(proto);
                Distributor = Runtime.Register(CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
                Runtime.EnableScheduleForActor(Distributor, true);
                Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }

            void RegisterProcess(const ui64 processId, const TSchedulerQueryIdentity& identity,
                                 const ESpecialTaskCategory category = ESpecialTaskCategory::Scan, const TString& scopeId = "scope") {
                Runtime.Send(Distributor, Sink, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), category, scopeId, processId, identity));
            }

            void SendQueryResponse(const NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr& query) {
                auto response = std::make_unique<NKqp::NScheduler::TEvQueryResponse>();
                response->Query = query;
                Runtime.Send(Distributor, Sink, response.release());
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
                TProcessGuard managed(ESpecialTaskCategory::Scan, "scope", 1, TCPULimitsConfig(), service, identity);
                managed.Finish();
                TProcessGuard unmanaged(ESpecialTaskCategory::Scan, "scope", 2, TCPULimitsConfig(), service);
                unmanaged.Finish();
                return true;
            });

            auto managed = runtime.GrabEdgeEvent<TEvExecution::TEvRegisterProcess>(service);
            auto unmanaged = runtime.GrabEdgeEvent<TEvExecution::TEvRegisterProcess>(service);
            UNIT_ASSERT(managed->Get()->GetSchedulerQueryIdentity() == identity);
            UNIT_ASSERT(unmanaged->Get()->GetSchedulerQueryIdentity().IsDefault());
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
            const TSchedulerQueryIdentity identity;
            registry.RegisterProcess(identity);
            registry.UpdateWorkCapacity(identity, 1);

            auto result = registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100));
            UNIT_ASSERT(std::holds_alternative<TSchedulerLease>(result));
            {
                auto lease = std::get<TSchedulerLease>(std::move(result));
                UNIT_ASSERT(lease);
            }
            UNIT_ASSERT(!registry.UnregisterProcess(identity));
            UNIT_ASSERT(!registry.RegisterProcess(identity));
            UNIT_ASSERT(!registry.UnregisterProcess(identity));
        }

        Y_UNIT_TEST(ManagedQueryIsNotReadyBeforeFactory) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            UNIT_ASSERT(registry.RegisterProcess(identity));
            UNIT_ASSERT(!registry.RegisterProcess(identity));
            registry.UpdateWorkCapacity(identity, 3);

            const auto& state = registry.GetStateVerified(identity);
            UNIT_ASSERT(!state.IsReady());
            UNIT_ASSERT_VALUES_EQUAL(state.GetProcessesCount(), 2);
        }

        Y_UNIT_TEST(FactoryCreatesOneWorkPerPotentialWorker) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            registry.UpdateWorkCapacity(identity, 4);

            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuDemand.load(), 4);
        }

        Y_UNIT_TEST(QueryDeadlineKeepsMinimumAcrossRetries) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity, TDuration::MicroSeconds(20), 0);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            registry.UpdateWorkCapacity(identity, 1);

            auto& state = registry.GetStateVerified(identity);
            UNIT_ASSERT(std::holds_alternative<TMonotonic>(state.TryStart(TMonotonic::MicroSeconds(100))));
            UNIT_ASSERT(std::holds_alternative<TMonotonic>(state.TryStart(TMonotonic::MicroSeconds(110))));
            UNIT_ASSERT_VALUES_EQUAL(state.GetWakeUpDeadline()->GetValue(), TMonotonic::MicroSeconds(120).GetValue());
        }

        Y_UNIT_TEST(AverageDeadlineIgnoresQueriesWithoutDeadline) {
            TQueryRegistry registry;
            const auto identity1 = MakeIdentity(1);
            const auto identity2 = MakeIdentity(2);
            const auto identity3 = MakeIdentity(3);
            for (const auto& identity : {identity1, identity2, identity3}) {
                registry.RegisterProcess(identity);
            }
            auto query1 = MakeSchedulerQuery(identity1, TDuration::MicroSeconds(20), 0);
            auto query2 = MakeSchedulerQuery(identity2, TDuration::MicroSeconds(40), 0);
            auto query3 = MakeSchedulerQuery(identity3);
            UNIT_ASSERT(registry.SetQuery(identity1, query1.Query));
            UNIT_ASSERT(registry.SetQuery(identity2, query2.Query));
            UNIT_ASSERT(registry.SetQuery(identity3, query3.Query));
            registry.UpdateWorkCapacity(identity1, 1);
            registry.UpdateWorkCapacity(identity2, 1);
            registry.UpdateWorkCapacity(identity3, 1);

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
            registry.UpdateWorkCapacity(identity, 1);

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
            registry.UpdateWorkCapacity(identity, 1);

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
            registry.UpdateWorkCapacity(identity, 2);
            Y_UNUSED(registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100)));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 1);

            UNIT_ASSERT(registry.UnregisterProcess(identity));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 0);
            UNIT_ASSERT(registry.RegisterProcess(identity));
        }

        Y_UNIT_TEST(UnregisterRejectsStartedWork) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            registry.UpdateWorkCapacity(identity, 1);
            auto result = registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100));
            {
                auto lease = std::get<TSchedulerLease>(std::move(result));
                UNIT_ASSERT_EXCEPTION(registry.UnregisterProcess(identity), yexception);
            }
            UNIT_ASSERT(!registry.RegisterProcess(identity));
            UNIT_ASSERT(registry.UnregisterProcess(identity));
            UNIT_ASSERT(registry.RegisterProcess(identity));
        }

        Y_UNIT_TEST(CapacityShrinkRemovesOnlyNonStartedWorks) {
            TQueryRegistry registry;
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity);
            registry.RegisterProcess(identity);
            UNIT_ASSERT(registry.SetQuery(identity, query.Query));
            registry.UpdateWorkCapacity(identity, 3);
            auto result = registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100));
            {
                auto lease = std::get<TSchedulerLease>(std::move(result));
                Y_UNUSED(registry.GetStateVerified(identity).TryStart(TMonotonic::MicroSeconds(100)));

                registry.UpdateWorkCapacity(identity, 1);
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

        Y_UNIT_TEST(ExistingSchedulerQueryInitializesNewManagedIdentity) {
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
            scheduler->AddOrUpdateDatabase(identity.DatabaseId, {});
            scheduler->AddOrUpdatePool(identity.DatabaseId, identity.PoolId, {});
            auto query = scheduler->AddOrUpdateQuery(identity.DatabaseId, identity.PoolId, identity.QueryId, {});
            fixture.Runtime.GetAppData().KqpComputeScheduler = scheduler;

            fixture.RegisterProcess(1, identity);
            fixture.RegisterProcess(2, identity);
            TAtomicCounter counter;
            fixture.Submit(counter, 1);
            fixture.WaitFor([&] {
                return counter.Val() == 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(query->CpuDemand.load(), 1);
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
