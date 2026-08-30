#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/runtime/scheduler/tree/common.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConveyorComposite {

    namespace {

        class TCountingTask: public NConveyor::ITask {
        public:
            explicit TCountingTask(TAtomicCounter& counter)
                : Counter(counter)
            {
            }

            TString GetTaskClassIdentifier() const override {
                return "WM_COUNTING_TASK";
            }

        private:
            void DoExecute(const std::shared_ptr<ITask>&) override {
                Counter.Inc();
            }

            TAtomicCounter& Counter;
        };

        NConfig::TConfig BuildConveyorConfig() {
            NKikimrConfig::TCompositeConveyorConfig proto;
            proto.SetEnabled(true);
            auto* pool = proto.AddWorkerPools();
            pool->SetName("wm-test");
            pool->SetWorkersCount(2);
            pool->SetMaxBatchSize(1);
            pool->AddLinks()->SetCategory(::ToString(ESpecialTaskCategory::Scan));
            return NConfig::TConfig::BuildFromProto(proto).DetachResult();
        }

        NKqp::NScheduler::TComputeSchedulerPtr BuildScheduler(
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
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
            scheduler->SetTotalCpuLimit(1);
            scheduler->AddOrUpdateDatabase("database", {});
            scheduler->AddOrUpdatePool("database", "limited", {
                                                                  .CpuLimit = 1,
                                                                  .ReadLimit = TDuration::Seconds(1),
                                                              });
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
        Y_UNIT_TEST(QuotaStateSurvivesCompletedTask) {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto scheduler = BuildScheduler(counters);
            TWorkloadQuotaController quota(scheduler);
            const TWorkloadContext context{
                .DatabaseId = "database",
                .PoolId = "limited",
            };

            auto first = quota.TryReserve(context, TDuration::MilliSeconds(1));
            UNIT_ASSERT(first.Allowed);
            scheduler->UpdateFairShare();
            quota.Finish(std::move(first.Reservation), TDuration::Seconds(2));

            UNIT_ASSERT(!quota.TryReserve(context, TDuration::MilliSeconds(1)).Allowed);
        }

        Y_UNIT_TEST(QuotaReservationsCanFinishOutOfOrder) {
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
                                                                  .ReadLimit = TDuration::Seconds(1),
                                                              });

            auto query = scheduler->GetReadQuery("database", "limited");
            auto read = std::make_shared<NKqp::NScheduler::TSchedulableRead>(query);
            std::vector<NKqp::NScheduler::TSchedulableReadPtr> extraDemand;
            for (ui32 i = 0; i < 3; ++i) {
                extraDemand.emplace_back(std::make_shared<NKqp::NScheduler::TSchedulableRead>(query));
            }
            scheduler->UpdateFairShare();

            auto first = read->TryConsumeQuota(TDuration::MilliSeconds(1));
            auto second = read->TryConsumeQuota(TDuration::MilliSeconds(1));
            UNIT_ASSERT(first);
            UNIT_ASSERT(second);
            UNIT_ASSERT(!read->TryConsumeQuota(TDuration::MilliSeconds(1)));

            read->ReturnQuota(std::move(*second), TDuration::MilliSeconds(2));
            auto third = read->TryConsumeQuota(TDuration::MilliSeconds(1));
            UNIT_ASSERT(third);

            read->ReturnQuota(std::move(*third), TDuration::MilliSeconds(1));
            read->ReturnQuota(std::move(*first), TDuration::MilliSeconds(1));
            Y_UNUSED(extraDemand);
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
            runtime.Send(distributor, edge,
                         new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Scan, "limited-scope", limitedProcess));
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
            const TWorkloadContext limitedContext{
                .DatabaseId = "database",
                .PoolId = "limited",
            };

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
