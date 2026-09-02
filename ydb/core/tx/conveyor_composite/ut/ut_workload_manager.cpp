#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/runtime/scheduler/tree/common.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/service/workload.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConveyorComposite {

    namespace {

        constexpr ui64 LimitedQueryId = 1;

        TWorkloadContext MakeWorkloadContext(ui64 queryId) {
            return {
                .DatabaseId = "database",
                .PoolId = "limited",
                .QueryId = queryId,
            };
        }

        class TCountingTask: public NConveyor::ITask {
        public:
            explicit TCountingTask(TAtomicCounter& counter, TDuration executionTime = TDuration::Zero())
                : Counter(counter)
                , ExecutionTime(executionTime)
            {
            }

            TString GetTaskClassIdentifier() const override {
                return "WM_COUNTING_TASK";
            }

        private:
            void DoExecute(const std::shared_ptr<ITask>&) override {
                const auto deadline = TMonotonic::Now() + ExecutionTime;
                while (TMonotonic::Now() < deadline) {
                }
                Counter.Inc();
            }

            TAtomicCounter& Counter;
            const TDuration ExecutionTime;
        };

        NConfig::TConfig BuildConveyorConfig(ui32 workersCount = 2, ui32 maxBatchSize = 1) {
            NKikimrConfig::TCompositeConveyorConfig proto;
            proto.SetEnabled(true);
            auto* pool = proto.AddWorkerPools();
            pool->SetName("wm-test");
            pool->SetWorkersCount(workersCount);
            pool->SetMaxBatchSize(maxBatchSize);
            pool->AddLinks()->SetCategory(::ToString(ESpecialTaskCategory::Scan));
            return NConfig::TConfig::BuildFromProto(proto).DetachResult();
        }

        NKqp::NScheduler::TComputeSchedulerPtr BuildScheduler(
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 cpuLimit = 1, bool addQuery = true) {
            auto scheduler = std::make_shared<NKqp::NScheduler::TComputeScheduler>(
                MakeIntrusive<NKqp::TKqpCounters>(counters),
                NKqp::NScheduler::TOptions{
                    .Enabled = true,
                    .DelayParams = {
                        .MaxDelay = TDuration::Seconds(1),
                        .MinDelay = TDuration::MilliSeconds(1),
                        .AttemptBonus = TDuration::MicroSeconds(1),
                        .MaxRandomDelay = TDuration::MicroSeconds(1),
                    },
                });
            scheduler->SetTotalCpuLimit(cpuLimit);
            scheduler->AddOrUpdateDatabase("database", {});
            scheduler->AddOrUpdatePool("database", "limited", {
                                                                  .CpuLimit = cpuLimit,
                                                              });
            if (addQuery) {
                scheduler->AddOrUpdateQuery("database", "limited", LimitedQueryId, {
                                                                                       .CpuLimit = cpuLimit,
                                                                                   });
            }
            scheduler->UpdateFairShare();
            return scheduler;
        }

        void WaitForHeldTasks(NActors::TTestActorRuntime& runtime,
                              const std::vector<TAutoPtr<NActors::IEventHandle>>& heldTasks, size_t expected) {
            for (ui32 attempt = 0; attempt < 100 && heldTasks.size() < expected; ++attempt) {
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }
            UNIT_ASSERT_VALUES_EQUAL(heldTasks.size(), expected);
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TCompositeConveyorWorkloadManager) {
        Y_UNIT_TEST(SchedulableWorksCanFinishOutOfOrder) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = std::make_shared<NKqp::NScheduler::TComputeScheduler>(
                MakeIntrusive<NKqp::TKqpCounters>(counters),
                NKqp::NScheduler::TOptions{
                    .Enabled = true,
                    .DelayParams = {
                        .MaxDelay = TDuration::Seconds(1),
                        .MinDelay = TDuration::MilliSeconds(1),
                        .AttemptBonus = TDuration::MicroSeconds(1),
                        .MaxRandomDelay = TDuration::MicroSeconds(1),
                    },
                });
            scheduler->SetTotalCpuLimit(2);
            scheduler->AddOrUpdateDatabase("database", {});
            scheduler->AddOrUpdatePool("database", "limited", {
                                                                  .CpuLimit = 2,
                                                              });

            auto query = scheduler->AddOrUpdateQuery("database", "limited", LimitedQueryId, {
                                                                                                .CpuLimit = 2,
                                                                                            });
            auto queryState = std::make_shared<TWorkloadQueryState>(
                MakeWorkloadContext(LimitedQueryId), query, std::make_shared<TWorkloadPoolState>());
            auto first = std::make_unique<TConveyorWorkUnit>(queryState);
            auto second = std::make_unique<TConveyorWorkUnit>(queryState);
            auto third = std::make_unique<TConveyorWorkUnit>(queryState);
            auto extraDemand = std::make_unique<TConveyorWorkUnit>(queryState);
            scheduler->UpdateFairShare();

            UNIT_ASSERT(first->TryStart());
            UNIT_ASSERT(second->TryStart());
            UNIT_ASSERT(!third->TryStart());

            second->Finish(TDuration::MilliSeconds(2));
            UNIT_ASSERT(third->TryStart());

            third->Finish(TDuration::MilliSeconds(1));
            first->Finish(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(query->CpuUsage.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query->GetParent()->CpuUsage.load(), 0);
            Y_UNUSED(extraDemand);
        }

        Y_UNIT_TEST(WorkUnitLifetimeTracksDemand) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters);
            auto query = scheduler->GetQuery("database", "limited", LimitedQueryId);
            UNIT_ASSERT_VALUES_EQUAL(query->CpuDemand.load(), 0);

            {
                auto queryState = std::make_shared<TWorkloadQueryState>(
                    MakeWorkloadContext(LimitedQueryId), query, std::make_shared<TWorkloadPoolState>());
                auto work = std::make_unique<TConveyorWorkUnit>(queryState);
                UNIT_ASSERT_VALUES_EQUAL(query->CpuDemand.load(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(query->CpuDemand.load(), 0);
        }

        Y_UNIT_TEST(QueryCpuTimeQuotaUsesActualDuration) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters);
            scheduler->UpdateQueriesInPool("database", "limited", {
                                                                      .CpuRefillRateUsPerSecond = 0,
                                                                      .CpuBurstCapacityUs = 10,
                                                                  });

            TWorkloadScheduler workloadScheduler(scheduler);
            const auto context = MakeWorkloadContext(LimitedQueryId);
            workloadScheduler.RegisterProcess(context);

            TConveyorWorkUnits firstBatch;
            UNIT_ASSERT(workloadScheduler.TryAddToBatch(context, firstBatch));
            firstBatch.at(LimitedQueryId)->Finish(TDuration::MicroSeconds(11));
            firstBatch.clear();

            TConveyorWorkUnits secondBatch;
            UNIT_ASSERT(!workloadScheduler.TryAddToBatch(context, secondBatch));
            UNIT_ASSERT(workloadScheduler.ExtractNextWakeup());
            workloadScheduler.UnregisterProcess(context);
        }

        Y_UNIT_TEST(PoolCpuTimeQuotaIsSharedByQueries) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters);
            scheduler->AddOrUpdatePool("database", "limited", {
                                                                  .CpuRefillRateUsPerSecond = 0,
                                                                  .CpuBurstCapacityUs = 10,
                                                              });
            scheduler->AddOrUpdateQuery("database", "limited", 2, {.CpuLimit = 1});
            scheduler->UpdateFairShare();

            TWorkloadScheduler workloadScheduler(scheduler);
            const auto firstContext = MakeWorkloadContext(LimitedQueryId);
            const auto secondContext = MakeWorkloadContext(2);
            workloadScheduler.RegisterProcess(firstContext);
            workloadScheduler.RegisterProcess(secondContext);

            TConveyorWorkUnits firstBatch;
            UNIT_ASSERT(workloadScheduler.TryAddToBatch(firstContext, firstBatch));
            firstBatch.at(LimitedQueryId)->Finish(TDuration::MicroSeconds(11));
            firstBatch.clear();

            TConveyorWorkUnits secondBatch;
            UNIT_ASSERT(!workloadScheduler.TryAddToBatch(secondContext, secondBatch));
            workloadScheduler.UnregisterProcess(firstContext);
            workloadScheduler.UnregisterProcess(secondContext);
        }

        Y_UNIT_TEST(CpuTimeQuotaUpdateAffectsExistingQueryState) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters);
            scheduler->UpdateQueriesInPool("database", "limited", {
                                                                      .CpuRefillRateUsPerSecond = 0,
                                                                      .CpuBurstCapacityUs = 0,
                                                                  });

            TWorkloadScheduler workloadScheduler(scheduler);
            const auto context = MakeWorkloadContext(LimitedQueryId);
            workloadScheduler.RegisterProcess(context);

            TConveyorWorkUnits batch;
            UNIT_ASSERT(!workloadScheduler.TryAddToBatch(context, batch));

            scheduler->UpdateQueriesInPool("database", "limited", {
                                                                      .CpuRefillRateUsPerSecond = std::numeric_limits<double>::infinity(),
                                                                      .CpuBurstCapacityUs = std::numeric_limits<double>::infinity(),
                                                                  });
            scheduler->UpdateFairShare();
            UNIT_ASSERT(workloadScheduler.TryAddToBatch(context, batch));
            batch.at(LimitedQueryId)->Finish(TDuration::MicroSeconds(1));
            batch.clear();
            workloadScheduler.UnregisterProcess(context);
        }

        Y_UNIT_TEST(QueryLimitIsIndependentForDifferentQueries) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters, 2);
            auto firstQuery = scheduler->AddOrUpdateQuery("database", "limited", LimitedQueryId, {.CpuLimit = 1});
            auto secondQuery = scheduler->AddOrUpdateQuery("database", "limited", 2, {.CpuLimit = 1});
            auto poolState = std::make_shared<TWorkloadPoolState>();
            auto firstQueryState = std::make_shared<TWorkloadQueryState>(
                MakeWorkloadContext(LimitedQueryId), firstQuery, poolState);
            auto secondQueryState = std::make_shared<TWorkloadQueryState>(MakeWorkloadContext(2), secondQuery, poolState);
            auto first = std::make_unique<TConveyorWorkUnit>(firstQueryState);
            auto blocked = std::make_unique<TConveyorWorkUnit>(firstQueryState);
            auto second = std::make_unique<TConveyorWorkUnit>(secondQueryState);
            scheduler->UpdateFairShare();

            UNIT_ASSERT(first->TryStart());
            UNIT_ASSERT(!blocked->TryStart());
            UNIT_ASSERT(second->TryStart());

            first->Finish(TDuration::MilliSeconds(1));
            second->Finish(TDuration::MilliSeconds(1));
        }

        Y_UNIT_TEST(QueryLimitUpdateAffectsExistingQuery) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters, 2);
            auto query = scheduler->GetQuery("database", "limited", LimitedQueryId);

            scheduler->UpdateQueriesInPool("database", "limited", {.CpuLimit = 1});
            auto queryState = std::make_shared<TWorkloadQueryState>(
                MakeWorkloadContext(LimitedQueryId), query, std::make_shared<TWorkloadPoolState>());
            auto first = std::make_unique<TConveyorWorkUnit>(queryState);
            auto second = std::make_unique<TConveyorWorkUnit>(queryState);
            scheduler->UpdateFairShare();

            UNIT_ASSERT(first->TryStart());
            UNIT_ASSERT(!second->TryStart());
            first->Finish(TDuration::MilliSeconds(1));
        }

        Y_UNIT_TEST(PoolLimitCapsDifferentQueries) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters);
            auto secondQuery = scheduler->AddOrUpdateQuery("database", "limited", 2, {.CpuLimit = 1});
            auto poolState = std::make_shared<TWorkloadPoolState>();
            auto secondQueryState = std::make_shared<TWorkloadQueryState>(MakeWorkloadContext(2), secondQuery, poolState);
            auto second = std::make_unique<TConveyorWorkUnit>(secondQueryState);
            {
                auto firstQuery = scheduler->GetQuery("database", "limited", LimitedQueryId);
                auto firstQueryState = std::make_shared<TWorkloadQueryState>(
                    MakeWorkloadContext(LimitedQueryId), firstQuery, poolState);
                auto first = std::make_unique<TConveyorWorkUnit>(firstQueryState);
                scheduler->UpdateFairShare();

                UNIT_ASSERT(first->TryStart());
                UNIT_ASSERT(!second->TryStart());
                first->Finish(TDuration::MilliSeconds(1));
            }
            scheduler->UpdateFairShare();
            UNIT_ASSERT(second->TryStart());
            second->Finish(TDuration::MilliSeconds(1));
        }

        Y_UNIT_TEST(QueryIsMaterializedOnConveyorNode) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());

            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters, 1, false);
            runtime.GetAppData(0).KqpComputeScheduler = scheduler;
            const auto schedulerService = runtime.Register(
                NKqp::CreateKqpComputeSchedulerService(TDuration::MilliSeconds(1)));
            runtime.RegisterService(
                NKqp::MakeKqpSchedulerServiceId(runtime.GetNodeId(0)), schedulerService);

            const auto distributor = runtime.Register(CreateService(BuildConveyorConfig(1), counters, scheduler));
            const auto edge = runtime.AllocateEdgeActor();
            runtime.EnableScheduleForActor(distributor, true);
            runtime.EnableScheduleForActor(schedulerService, true);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            const TWorkloadContext context{
                .DatabaseId = "database",
                .PoolId = "limited",
                .QueryId = LimitedQueryId,
            };
            constexpr ui64 processId = 1;
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvRegisterProcess(
                             TCPULimitsConfig(1000), ESpecialTaskCategory::Scan, "scope", processId, context));

            TAtomicCounter counter;
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvNewTask(
                             std::make_shared<TCountingTask>(counter), ESpecialTaskCategory::Scan, processId, context));
            for (ui32 attempt = 0; attempt < 100 && counter.Val() != 1; ++attempt) {
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }
            UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
            UNIT_ASSERT(scheduler->GetQuery("database", "limited", LimitedQueryId));

            runtime.Send(distributor, edge,
                         new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Scan, processId));
            for (ui32 attempt = 0; attempt < 100 && scheduler->GetQuery("database", "limited", LimitedQueryId); ++attempt) {
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }
            UNIT_ASSERT(!scheduler->GetQuery("database", "limited", LimitedQueryId));
        }

        Y_UNIT_TEST(TasksOfSameQueryShareOneWorkUnitInBatch) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());

            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters, 2);
            const auto distributor = runtime.Register(CreateService(BuildConveyorConfig(1, 30), counters, scheduler));
            const auto edge = runtime.AllocateEdgeActor();
            runtime.EnableScheduleForActor(distributor, true);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            constexpr ui64 limitedProcess = 1;
            constexpr ui64 unlimitedProcess = 2;
            const TWorkloadContext limitedContext{
                .DatabaseId = "database",
                .PoolId = "limited",
                .QueryId = LimitedQueryId,
            };
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvRegisterProcess(
                             TCPULimitsConfig(1000), ESpecialTaskCategory::Scan, "limited-scope", limitedProcess, limitedContext));
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Scan, "unlimited-scope", unlimitedProcess));

            TAutoPtr<NActors::IEventHandle> heldTask;
            auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType && !heldTask) {
                    heldTask = ev.Release();
                    return NActors::TTestActorRuntime::EEventAction::DROP;
                }
                return NActors::TTestActorRuntime::EEventAction::PROCESS;
            });

            TAtomicCounter limitedCounter;
            TAtomicCounter unlimitedCounter;
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvNewTask(std::make_shared<TCountingTask>(unlimitedCounter),
                                                      ESpecialTaskCategory::Scan, unlimitedProcess));
            for (ui32 attempt = 0; attempt < 100 && !heldTask; ++attempt) {
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }
            UNIT_ASSERT(heldTask);

            runtime.Send(distributor, edge,
                         new TEvExecution::TEvNewTask(std::make_shared<TCountingTask>(limitedCounter, TDuration::MilliSeconds(1)),
                                                      ESpecialTaskCategory::Scan, limitedProcess, limitedContext));
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvNewTask(std::make_shared<TCountingTask>(limitedCounter, TDuration::MilliSeconds(1)),
                                                      ESpecialTaskCategory::Scan, limitedProcess, limitedContext));
            scheduler->UpdateFairShare();

            runtime.SetObserverFunc(previousObserver);
            std::vector<ui64> batchSizes;
            auto resultObserver = runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
                batchSizes.emplace_back(ev->Get()->GetResults().size());
            });
            runtime.EnableScheduleForActor(heldTask->Recipient, true);
            runtime.Send(heldTask.Release(), 0, true);

            auto query = scheduler->GetQuery("database", "limited", LimitedQueryId);
            for (ui32 attempt = 0; attempt < 100 && (limitedCounter.Val() != 2 || query->CpuUsage.load() != 0); ++attempt) {
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }
            UNIT_ASSERT_VALUES_EQUAL(unlimitedCounter.Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(limitedCounter.Val(), 2);
            UNIT_ASSERT_VALUES_EQUAL(query->CpuUsage.load(), 0);
            UNIT_ASSERT_GE(query->CpuBurstUsage.load(), TDuration::MilliSeconds(2).MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(batchSizes.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(batchSizes[0], 1);
            UNIT_ASSERT_VALUES_EQUAL(batchSizes[1], 2);
        }

        Y_UNIT_TEST(ThrottledPoolDoesNotOccupyFreeWorker) {
            NActors::TTestActorRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());

            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters);
            const auto distributor = runtime.Register(CreateService(BuildConveyorConfig(), counters, scheduler));
            const auto edge = runtime.AllocateEdgeActor();
            runtime.EnableScheduleForActor(distributor, true);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            constexpr ui64 limitedProcess = 1;
            constexpr ui64 unlimitedProcess = 2;
            const TWorkloadContext limitedContext{
                .DatabaseId = "database",
                .PoolId = "limited",
                .QueryId = LimitedQueryId,
            };
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvRegisterProcess(
                             TCPULimitsConfig(1000), ESpecialTaskCategory::Scan, "limited-scope", limitedProcess, limitedContext));
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Scan, "unlimited-scope", unlimitedProcess));

            std::vector<TAutoPtr<NActors::IEventHandle>> heldTasks;
            auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                    heldTasks.emplace_back(ev.Release());
                    return NActors::TTestActorRuntime::EEventAction::DROP;
                }
                return NActors::TTestActorRuntime::EEventAction::PROCESS;
            });

            TAtomicCounter limitedCounter;
            TAtomicCounter unlimitedCounter;
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvNewTask(std::make_shared<TCountingTask>(limitedCounter),
                                                      ESpecialTaskCategory::Scan, limitedProcess, limitedContext));
            WaitForHeldTasks(runtime, heldTasks, 1);

            scheduler->UpdateFairShare();

            runtime.Send(distributor, edge,
                         new TEvExecution::TEvNewTask(std::make_shared<TCountingTask>(limitedCounter),
                                                      ESpecialTaskCategory::Scan, limitedProcess, limitedContext));
            runtime.SimulateSleep(TDuration::MilliSeconds(5));
            UNIT_ASSERT_VALUES_EQUAL(heldTasks.size(), 1);

            runtime.Send(distributor, edge,
                         new TEvExecution::TEvNewTask(std::make_shared<TCountingTask>(unlimitedCounter),
                                                      ESpecialTaskCategory::Scan, unlimitedProcess));
            WaitForHeldTasks(runtime, heldTasks, 2);

            runtime.SetObserverFunc(previousObserver);
            for (auto& task : heldTasks) {
                runtime.EnableScheduleForActor(task->Recipient, true);
                runtime.Send(task.Release(), 0, true);
            }

            for (ui32 attempt = 0; attempt < 100 && (limitedCounter.Val() != 2 || unlimitedCounter.Val() != 1); ++attempt) {
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }
            UNIT_ASSERT_VALUES_EQUAL(limitedCounter.Val(), 2);
            UNIT_ASSERT_VALUES_EQUAL(unlimitedCounter.Val(), 1);
        }
    } // Y_UNIT_TEST_SUITE(TCompositeConveyorWorkloadManager)

} // namespace NKikimr::NConveyorComposite
