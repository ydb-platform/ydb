#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/tree/snapshot.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/conveyor_composite/service/manager.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

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

            explicit TSchedulerRuntimeFixture(const NKikimrConfig::TCompositeConveyorConfig& proto) {
                Runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
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
                Distributor = Runtime.Register(CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
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

        Y_UNIT_TEST(TopologyChangesKeepStartedLeasesAndReconcileCapacity) {
            const auto identity = MakeIdentity(1);
            auto query = MakeSchedulerQuery(identity, TDuration::Seconds(1));
            const auto initial = BuildConfig({1, 2}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
            TSchedulerRuntimeFixture fixture(initial);
            fixture.RegisterProcess(1, identity);
            fixture.SendQueryResponse(query.Query);
            NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
            TAtomicCounter executed;
            fixture.Submit(executed, 1);
            fixture.WaitFor([&] { return results.size() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(results.front()->Get()->GetWorkerIdx(), 0);
            fixture.UpdateConfig(BuildConfig({3, 2}, {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}}));
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 3);
            fixture.Submit(executed, 1);
            fixture.WaitFor([&] { return query.Query->CpuThrottle.load() == 1; });

            fixture.UpdateConfig(initial);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuThrottle.load(), 0);
            results.Stop().Unblock();
            fixture.WaitFor([&] { return executed.Val() == 2 && query.Query->GetParent()->CpuUsage.load() == 0; });

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
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 3);
        }

        Y_UNIT_TEST_TWIN(FactoryGatesBatchesAndLeasesSurviveRehash, SeveralWorkers) {
            const ui64 workers = SeveralWorkers ? 3 : 1;
            const auto identity = MakeIdentity(0);
            auto query = MakeSchedulerQuery(identity, TDuration::MicroSeconds(10), workers);
            TSchedulerRuntimeFixture fixture(BuildConfig({double(workers)}, {{{ESpecialTaskCategory::Scan, 1}}}));
            TAtomicCounter executed;
            fixture.RegisterProcess(1, identity);
            for (ui64 i = 0; i < workers; ++i) {
                fixture.Submit(executed, 1);
            }
            NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> batches(fixture.Runtime);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(batches.empty());
            UNIT_ASSERT_VALUES_EQUAL(executed.Val(), 0);
            fixture.SendQueryResponse(query.Query);
            fixture.WaitFor([&] { return batches.size() == workers; });
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), workers);
            UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), workers);
            for (const auto& batch : batches) {
                UNIT_ASSERT(batch->Get()->GetQueryIdentity() == identity);
            }
            // Grow the registry while every scheduler lease is still held by a worker.
            for (ui64 id = 1; id < 1024; ++id) {
                fixture.RegisterProcess(id + 1, MakeIdentity(id));
            }
            batches.Stop().Unblock();
            fixture.WaitFor([&] { return executed.Val() == workers && query.Query->GetParent()->CpuUsage.load() == 0; });
            fixture.UnregisterProcess(1);
            fixture.WaitFor([&] { return fixture.Removes[identity.QueryId] == 1; });
            UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);
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

        Y_UNIT_TEST(AverageDeadlineHandlesEmptyMissingAndOverflowCases) {
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
                for (ui64 id : {1, 2}) {
                    registry.SetQuery(MakeIdentity(id), id == 1 ? first.Query : second.Query);
                    SetCapacity(registry, MakeIdentity(id), 1);
                    Y_UNUSED(registry.GetStateVerified(MakeIdentity(id)).TryStart(now));
                    UNIT_ASSERT_VALUES_EQUAL(registry.GetAverageWakeUpDeadline(now).GetValue(), now.GetValue() + (id == 1 ? 100 : 150));
                }
                UNIT_ASSERT_VALUES_EQUAL(registry.GetMinWakeUpDeadline()->GetValue(), now.GetValue() + 100);
            }
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
            UNIT_ASSERT_VALUES_EQUAL(first.Query->CpuMaxDemand.load(), 3);
            registry.ApplyWorkCapacity(MakeIdentity(1));
            UNIT_ASSERT_VALUES_EQUAL(first.Query->CpuMaxDemand.load(), 3);
            leases.pop_back();
            leases.pop_back();
            registry.ApplyWorkCapacity(MakeIdentity(1));
            registry.ApplyWorkCapacity(MakeIdentity(1));
            UNIT_ASSERT_VALUES_EQUAL(first.Query->CpuMaxDemand.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(second.Query->CpuMaxDemand.load(), 3);
            registry.PrepareWorkCapacity(MakeIdentity(1), 100);
            registry.ApplyWorkCapacity(MakeIdentity(1));
            UNIT_ASSERT_VALUES_EQUAL(first.Query->CpuMaxDemand.load(), 100);
            UNIT_ASSERT_VALUES_EQUAL(first.Query->GetParent()->CpuUsage.load(), 1);
            leases.clear();
            UNIT_ASSERT_VALUES_EQUAL(first.Query->GetParent()->CpuUsage.load(), 0);
        }

        Y_UNIT_TEST_TWIN(QueryReleaseWaitsForBatchAccounting, ReRegister) {
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
                NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> results(fixture.Runtime);
                fixture.SendQueryResponse(query.Query);
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
                for (ui64 id = 1; id <= processes; ++id) {
                    fixture.UnregisterProcess(id);
                }
                UNIT_ASSERT_VALUES_EQUAL(executed.Val(), processes);
                UNIT_ASSERT_VALUES_EQUAL(query.Query->GetParent()->CpuUsage.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], 0);
                if (ReRegister) {
                    fixture.RegisterProcess(processes + 1, identity);
                    fixture.Submit(executed, processes + 1);
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Adds[identity.QueryId], 1);
                }
                auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                    for (const auto& task : ev->Get()->GetResults()) {
                        if (task.GetProcessId() > processes) {
                            UNIT_ASSERT(task.GetScope() != scope);
                        }
                    }
                });
                results.Stop().Unblock();
                fixture.WaitFor([&] { return std::cmp_equal(executed.Val(), processes + ReRegister) && query.Query->GetParent()->CpuUsage.load() == 0; });
                UNIT_ASSERT_VALUES_EQUAL(scope->GetCountInFlight(), 0);
                if (ReRegister) {
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Removes[identity.QueryId], 0);
                    fixture.UnregisterProcess(processes + 1);
                }
                fixture.WaitFor([&] { return fixture.Removes[identity.QueryId] == 1; });
                UNIT_ASSERT_VALUES_EQUAL(query.Query->CpuMaxDemand.load(), 0);
            }
        }

        Y_UNIT_TEST_TWIN(LateQueryResponsePreservesRegistrationLifecycle, NullResponse) {
            // At response time: no processes, one survivor, or a new process after count reached zero.
            for (const ui64 stateAtReply : {0, 1, 2}) {
                const auto identity = MakeIdentity(0);
                auto query = MakeSchedulerQuery(identity);
                TSchedulerRuntimeFixture fixture(BuildConfig({1}, {{{ESpecialTaskCategory::Scan, 1}}}));
                fixture.SetLocalSchedulerEnabled(true);
                fixture.RegisterProcess(1, identity);
                fixture.RegisterProcess(2, identity);
                fixture.UnregisterProcess(1);
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
            }
        }

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

            NActors::TBlockEvents<TEvInternal::TEvTaskProcessedResult> batches(fixture.Runtime,
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

    } // Y_UNIT_TEST_SUITE(CompositeConveyorScheduler)

} // namespace NKikimr::NConveyorComposite
