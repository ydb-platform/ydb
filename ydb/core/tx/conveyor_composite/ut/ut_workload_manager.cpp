#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_work_factory.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/conveyor_composite/service/manager.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConveyorComposite {
    namespace {

        class TTestTask: public ITask {
            void DoExecute(const std::shared_ptr<ITask>&) override {
            }

        public:
            TString GetTaskClassIdentifier() const override {
                return "WORKLOAD_MANAGER_IDENTITY";
            }
        };

        Y_UNIT_TEST_SUITE(TCompositeConveyorWorkloadManager) {
            Y_UNIT_TEST(ProcessQueryContextLifetime) {
                NActors::TTestActorRuntime runtime;
                runtime.Initialize(TAppPrepare().Unwrap());
                const auto edge = runtime.AllocateEdgeActor();
                auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
                auto scheduler = NKqp::CreateKqpComputeScheduler(counters, NKikimrConfig::TAppConfig());
                const TWorkloadManagerQueryIdentity identity("db", "pool", 42);
                scheduler->AddOrUpdateDatabase(identity.GetDatabaseId(), {});
                scheduler->AddOrUpdatePool(identity.GetDatabaseId(), identity.GetPoolId(), {});
                auto query = scheduler->AddOrUpdateQuery(identity.GetDatabaseId(), identity.GetPoolId(), identity.GetQueryId(), {});
                auto context = std::make_shared<NKqp::NScheduler::TSchedulableWorkFactory>(query, true);

                UNIT_ASSERT(runtime.RunCall([&] {
                    TCounters signals("test", counters);
                    TTasksManager manager("test", NConfig::TConfig::BuildDefault(), edge, signals);
                    const auto categoryId = ESpecialTaskCategory::Scan;
                    auto& category = manager.MutableCategoryVerified(categoryId);
                    auto registerProcess = [&](ui64 processId, const std::optional<TWorkloadManagerQueryIdentity>& queryIdentity) {
                        return manager.RegisterProcess(
                            categoryId, processId, category.UpsertScope("scope", TCPULimitsConfig()), queryIdentity);
                    };
                    auto getQuery = [&](ui64 processId) -> const std::shared_ptr<TWorkloadManagerQuery>& {
                        return category.GetProcessVerified(processId).GetWorkloadManagerQuery();
                    };

                    UNIT_ASSERT(!getQuery(0));
                    UNIT_ASSERT(registerProcess(1, identity) == identity);
                    UNIT_ASSERT(getQuery(1)->GetIdentity() == identity);
                    UNIT_ASSERT(!getQuery(1)->GetSchedulerContext());
                    UNIT_ASSERT(!registerProcess(2, identity));
                    UNIT_ASSERT(getQuery(1) == getQuery(2));
                    UNIT_ASSERT(manager.SetWorkloadManagerQueryContext(identity, context));
                    UNIT_ASSERT(getQuery(1)->GetSchedulerContext() == context);
                    UNIT_ASSERT(getQuery(2)->GetSchedulerContext() == context);
                    UNIT_ASSERT(!registerProcess(3, identity));
                    UNIT_ASSERT(getQuery(3)->GetSchedulerContext() == context);

                    UNIT_ASSERT(!registerProcess(4, std::nullopt));
                    UNIT_ASSERT(!getQuery(4));
                    UNIT_ASSERT(!manager.UnregisterProcess(categoryId, 4));

                    const TWorkloadManagerQueryIdentity otherDatabase("other-db", "pool", 42);
                    const TWorkloadManagerQueryIdentity otherPool("db", "other-pool", 42);
                    const TWorkloadManagerQueryIdentity otherQuery("db", "pool", 43);
                    for (const auto& other : {otherDatabase, otherPool, otherQuery}) {
                        UNIT_ASSERT(registerProcess(5, other) == other);
                        UNIT_ASSERT(getQuery(5)->GetIdentity() == other);
                        UNIT_ASSERT(!getQuery(5)->GetSchedulerContext());
                        UNIT_ASSERT(manager.UnregisterProcess(categoryId, 5) == other);
                    }

                    UNIT_ASSERT(!manager.UnregisterProcess(categoryId, 1));
                    UNIT_ASSERT(!manager.UnregisterProcess(categoryId, 2));
                    UNIT_ASSERT(getQuery(3)->GetSchedulerContext() == context);
                    std::weak_ptr<NYql::NDq::IDqSchedulableWorkFactory> weakContext = context;
                    context.reset();
                    UNIT_ASSERT(!weakContext.expired());
                    UNIT_ASSERT(manager.UnregisterProcess(categoryId, 3) == identity);
                    UNIT_ASSERT(weakContext.expired());

                    auto lateContext = std::make_shared<NKqp::NScheduler::TSchedulableWorkFactory>(query, true);
                    UNIT_ASSERT(!manager.SetWorkloadManagerQueryContext(identity, std::move(lateContext)));
                    UNIT_ASSERT(registerProcess(6, identity) == identity);
                    UNIT_ASSERT(!getQuery(6)->GetSchedulerContext());
                    UNIT_ASSERT(manager.UnregisterProcess(categoryId, 6) == identity);
                    UNIT_ASSERT_VALUES_EQUAL(query->CpuDemand.load(), 0);
                    return true;
                }));
            }

            Y_UNIT_TEST(ProcessGuardMovePreservesQueryIdentity) {
                for (bool explicitFinish : {false, true}) {
                    NActors::TTestActorRuntime runtime;
                    runtime.Initialize(TAppPrepare().Unwrap());
                    const auto service = runtime.AllocateEdgeActor();
                    const TWorkloadManagerQueryIdentity identity("db", "pool", 42);
                    ui64 processId = 0;
                    ui64 registrations = 0;
                    ui64 unregistrations = 0;
                    runtime.SetEventFilter([&](auto&, TAutoPtr<NActors::IEventHandle>& ev) {
                        if (ev->GetTypeRewrite() == TEvExecution::TEvRegisterProcess::EventType) {
                            const auto* event = ev->Get<TEvExecution::TEvRegisterProcess>();
                            UNIT_ASSERT(event->GetWorkloadManagerQueryIdentity() == identity);
                            processId = event->GetInternalProcessId();
                            ++registrations;
                        } else if (ev->GetTypeRewrite() == TEvExecution::TEvUnregisterProcess::EventType) {
                            UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvExecution::TEvUnregisterProcess>()->GetInternalProcessId(), processId);
                            ++unregistrations;
                        }
                        return false;
                    });

                    UNIT_ASSERT(runtime.RunCall([&] {
                        TProcessGuard process(ESpecialTaskCategory::Scan, "pool", 1, TCPULimitsConfig(), service, identity);
                        UNIT_ASSERT_VALUES_EQUAL(processId, process.GetInternalProcessId());
                        TProcessGuard moved(std::move(process));
                        UNIT_ASSERT(moved.GetWorkloadManagerQueryIdentity() == identity);
                        UNIT_ASSERT(moved.SendTaskToExecute(std::make_shared<TTestTask>()));
                        if (explicitFinish) {
                            moved.Finish();
                            TProcessGuard finished(std::move(moved));
                            UNIT_ASSERT(finished.GetWorkloadManagerQueryIdentity() == identity);
                        }
                        return true;
                    }));

                    runtime.GrabEdgeEvent<TEvExecution::TEvRegisterProcess>(service);
                    const auto task = runtime.GrabEdgeEvent<TEvExecution::TEvNewTask>(service);
                    UNIT_ASSERT_VALUES_EQUAL(task->Get()->GetInternalProcessId(), processId);
                    runtime.GrabEdgeEvent<TEvExecution::TEvUnregisterProcess>(service);
                    UNIT_ASSERT_VALUES_EQUAL(registrations, 1);
                    UNIT_ASSERT_VALUES_EQUAL(unregistrations, 1);
                    runtime.SetEventFilter(NActors::TTestActorRuntime::DefaultFilterFunc);
                }
            }

            Y_UNIT_TEST(QueryBelongsToProcessAcrossTaskExecution) {
                NActors::TTestActorRuntime runtime;
                runtime.Initialize(TAppPrepare().Unwrap());
                TCounters signals("test", MakeIntrusive<NMonitoring::TDynamicCounters>());
                TProcessCategory category(NConfig::TCategory(ESpecialTaskCategory::Scan), signals);
                const TWorkloadManagerQueryIdentity identity("db", "pool", 42);
                auto query = std::make_shared<TWorkloadManagerQuery>(identity);
                category.RegisterProcess(1, category.RegisterScope("pool", TCPULimitsConfig()), query);
                category.RegisterTask(1, std::make_shared<TTestTask>());
                auto workerSignals = signals.GetWorkersPoolSignals("test")->GetCategorySignals(ESpecialTaskCategory::Scan);
                THashSet<TString> scopes;
                auto task = category.ExtractTaskWithPrediction(workerSignals, scopes);
                UNIT_ASSERT(task);
                UNIT_ASSERT_VALUES_EQUAL(task->GetProcessId(), 1);
                UNIT_ASSERT(category.GetProcessVerified(task->GetProcessId()).GetWorkloadManagerQuery() == query);
                const auto now = TMonotonic::Now();
                auto result = task->GetResult(now, now);
                UNIT_ASSERT_VALUES_EQUAL(result.GetProcessId(), 1);
                scopes.clear();
                category.PutTaskResult(std::move(result), scopes);
                UNIT_ASSERT(category.GetProcessVerified(1).GetWorkloadManagerQuery()->GetIdentity() == identity);
                category.UnregisterProcess(1);
            }

            Y_UNIT_TEST(DistributorRegistersQueryWithProcesses) {
                NActors::TTestActorRuntime runtime;
                runtime.Initialize(TAppPrepare().Unwrap());
                const auto scheduler = runtime.AllocateEdgeActor();
                runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(runtime.GetNodeId(0)), scheduler);
                const auto configDispatcher = runtime.AllocateEdgeActor();
                runtime.RegisterService(NConsole::MakeConfigsDispatcherID(runtime.GetNodeId(0)), configDispatcher);
                const auto distributor = runtime.Register(
                    new TDistributor(NConfig::TConfig::BuildDefault(), MakeIntrusive<NMonitoring::TDynamicCounters>()));
                runtime.GrabEdgeEvent<NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest>(configDispatcher);
                const TWorkloadManagerQueryIdentity identity("db", "pool", 42);
                const TWorkloadManagerQueryIdentity otherIdentity("db", "pool", 43);
                ui64 registrations = 0;
                ui64 unregistrations = 0;
                runtime.SetEventFilter([&](auto&, TAutoPtr<NActors::IEventHandle>& ev) {
                    if (ev->GetTypeRewrite() == NKqp::NScheduler::TEvAddQuery::EventType) {
                        ++registrations;
                    } else if (ev->GetTypeRewrite() == NKqp::NScheduler::TEvRemoveQuery::EventType) {
                        ++unregistrations;
                    }
                    return false;
                });
                auto registerProcess = [&](ui64 processId, const std::optional<TWorkloadManagerQueryIdentity>& queryIdentity) {
                    runtime.Send(distributor, scheduler, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(), ESpecialTaskCategory::Scan, "scope", processId, queryIdentity));
                };
                auto unregisterProcess = [&](ui64 processId) {
                    runtime.Send(distributor, scheduler, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Scan, processId));
                };

                registerProcess(1, identity);
                const auto addDatabase = runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddDatabase>(scheduler);
                UNIT_ASSERT_VALUES_EQUAL(addDatabase->Get()->DatabaseId, identity.GetDatabaseId());
                const auto addPool = runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddPool>(scheduler);
                UNIT_ASSERT_VALUES_EQUAL(addPool->Get()->DatabaseId, identity.GetDatabaseId());
                UNIT_ASSERT_VALUES_EQUAL(addPool->Get()->PoolId, identity.GetPoolId());
                const auto addQuery = runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddQuery>(scheduler);
                UNIT_ASSERT_VALUES_EQUAL(addQuery->Get()->DatabaseId, identity.GetDatabaseId());
                UNIT_ASSERT_VALUES_EQUAL(addQuery->Get()->PoolId, identity.GetPoolId());
                UNIT_ASSERT_VALUES_EQUAL(addQuery->Get()->QueryId, identity.GetQueryId());
                UNIT_ASSERT_VALUES_EQUAL(addQuery->Cookie, identity.GetQueryId());

                registerProcess(2, identity);
                registerProcess(3, std::nullopt);
                unregisterProcess(1);
                registerProcess(4, otherIdentity);
                const auto otherQuery = runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddQuery>(scheduler);
                UNIT_ASSERT_VALUES_EQUAL(otherQuery->Get()->QueryId, otherIdentity.GetQueryId());
                UNIT_ASSERT_VALUES_EQUAL(registrations, 2);
                UNIT_ASSERT_VALUES_EQUAL(unregistrations, 0);

                unregisterProcess(2);
                const auto removeQuery = runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(scheduler);
                UNIT_ASSERT_VALUES_EQUAL(removeQuery->Get()->QueryId, identity.GetQueryId());
                unregisterProcess(3);
                unregisterProcess(4);
                const auto removeOtherQuery = runtime.GrabEdgeEvent<NKqp::NScheduler::TEvRemoveQuery>(scheduler);
                UNIT_ASSERT_VALUES_EQUAL(removeOtherQuery->Get()->QueryId, otherIdentity.GetQueryId());
                UNIT_ASSERT_VALUES_EQUAL(registrations, 2);
                UNIT_ASSERT_VALUES_EQUAL(unregistrations, 2);
                runtime.SetEventFilter(NActors::TTestActorRuntime::DefaultFilterFunc);
            }
        } // Y_UNIT_TEST_SUITE(TCompositeConveyorWorkloadManager)

    } // namespace
} // namespace NKikimr::NConveyorComposite
