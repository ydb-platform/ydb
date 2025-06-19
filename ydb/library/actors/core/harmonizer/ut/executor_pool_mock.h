#pragma once

#include "defs.h"
#include "debug.h"
#include <ydb/library/actors/core/executor_pool.h>

#include <util/string/builder.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NActors {

    struct TMockExecutorPoolParams {
        i16 DefaultFullThreadCount = 4;
        i16 MinFullThreadCount = 4;
        i16 MaxFullThreadCount = 8;
        float DefaultThreadCount = 4.0f;
        float MinThreadCount = 4.0f;
        float MaxThreadCount = 8.0f;
        i16 Priority = 0;
        TString Name = "MockPool";
        ui32 PoolId = 0;

        TString ToString() const {
            return TStringBuilder() << "PoolId: " << PoolId << ", Name: " << Name << ", DefaultFullThreadCount: " << DefaultFullThreadCount << ", MinFullThreadCount: " << MinFullThreadCount << ", MaxFullThreadCount: " << MaxFullThreadCount << ", DefaultThreadCount: " << DefaultThreadCount << ", MinThreadCount: " << MinThreadCount << ", MaxThreadCount: " << MaxThreadCount << ", Priority: " << Priority;
        }
    };

    struct TCpuConsumptionModel {
        TCpuConsumption value;
        TCpuConsumptionModel() : value() {}
        TCpuConsumptionModel(const TCpuConsumption& other) : value(other) {}
        operator TCpuConsumption() const {
            return value;
        }
        void Increase(const TCpuConsumption& other) {
            value.ElapsedUs += other.ElapsedUs;
            value.CpuUs += other.CpuUs;
            value.NotEnoughCpuExecutions += other.NotEnoughCpuExecutions;
        }
    };

    class TMockExecutorPool : public IExecutorPool {
    public:
        TMockExecutorPool(const TMockExecutorPoolParams& params = TMockExecutorPoolParams())
            : IExecutorPool(params.PoolId)
            , Params(params)
            , ThreadCount(params.DefaultFullThreadCount)
            , ThreadCpuConsumptions(params.MaxFullThreadCount, TCpuConsumption{0.0, 0.0})
        {}

        TMockExecutorPoolParams Params;
        i16 ThreadCount = 0;
        std::vector<TCpuConsumptionModel> ThreadCpuConsumptions;
        std::vector<TSharedExecutorThreadCtx*> SharedThreads;

        i16 GetDefaultFullThreadCount() const override { return Params.DefaultFullThreadCount; }
        i16 GetMinFullThreadCount() const override { return Params.MinFullThreadCount; }
        i16 GetMaxFullThreadCount() const override { return Params.MaxFullThreadCount; }
        void SetFullThreadCount(i16 count) override {
            HARMONIZER_DEBUG_PRINT(Params.PoolId, Params.Name, "set full thread count", count);
            ThreadCount = Max(Params.MinFullThreadCount, Min(Params.MaxFullThreadCount, count));
        }
        i16 GetFullThreadCount() const override { return ThreadCount; }
        float GetDefaultThreadCount() const override { return Params.DefaultThreadCount; }
        float GetMinThreadCount() const override { return Params.MinThreadCount; }
        float GetMaxThreadCount() const override { return Params.MaxThreadCount; }
        i16 GetPriority() const override { return Params.Priority; }
        TString GetName() const override { return Params.Name; }

        // Дополнительные методы из IExecutorPool
        void Prepare(TActorSystem* /*actorSystem*/, NSchedulerQueue::TReader** /*scheduleReaders*/, ui32* /*scheduleSz*/) override {}
        void Start() override {}
        void PrepareStop() override {}
        void Shutdown() override {}
        bool Cleanup() override { return true; }

        TMailbox* GetReadyActivation(ui64 /*revolvingCounter*/) override { return nullptr; }
        TMailbox* ResolveMailbox(ui32 /*hint*/) override { return nullptr; }

        void Schedule(TInstant /*deadline*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}
        void Schedule(TMonotonic /*deadline*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}
        void Schedule(TDuration /*delta*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}

        bool Send(TAutoPtr<IEventHandle>& /*ev*/) override { return true; }
        bool SpecificSend(TAutoPtr<IEventHandle>& /*ev*/) override { return true; }
        void ScheduleActivation(TMailbox* /*activation*/) override {}
        void SpecificScheduleActivation(TMailbox* /*activation*/) override {}
        void ScheduleActivationEx(TMailbox* /*activation*/, ui64 /*revolvingCounter*/) override {}
        TActorId Register(IActor* /*actor*/, TMailboxType::EType /*mailboxType*/, ui64 /*revolvingCounter*/, const TActorId& /*parentId*/) override { return TActorId(); }
        TActorId Register(IActor* /*actor*/, TMailboxCache& /*cache*/, ui64 /*revolvingCounter*/, const TActorId& /*parentId*/) override { return TActorId(); }
        TActorId Register(IActor* /*actor*/, TMailbox* /*mailbox*/, const TActorId& /*parentId*/) override { return TActorId(); }
        TActorId RegisterAlias(TMailbox*, IActor*) override { return TActorId(); }
        void UnregisterAlias(TMailbox*, const TActorId&) override {}

        TAffinity* Affinity() const override { return nullptr; }

        ui32 GetThreads() const override { return static_cast<ui32>(ThreadCount); }
        float GetThreadCount() const override { return static_cast<float>(ThreadCount); }

        void IncreaseThreadCpuConsumption(TCpuConsumption consumption, i16 start = 0, i16 count = -1) {
            if (count == -1) {
                count = Params.MaxFullThreadCount - start;
            }
            for (i16 i = start; i < start + count; ++i) {
                ThreadCpuConsumptions[i].Increase(consumption);
            }
        }

        void SetThreadCpuConsumption(TCpuConsumption consumption, i16 start = 0, i16 count = -1) {
            if (count == -1) {
                count = Params.MaxFullThreadCount - start;
            }
            for (i16 i = start; i < start + count; ++i) {
                ThreadCpuConsumptions[i] = consumption;
            }
        }

        TCpuConsumption GetThreadCpuConsumption(i16 threadIdx) override {
            UNIT_ASSERT_GE(threadIdx, 0);
            UNIT_ASSERT_LE(static_cast<ui16>(threadIdx), ThreadCpuConsumptions.size());
            return ThreadCpuConsumptions[threadIdx];
        }

        ui64 TimePerMailboxTs() const override {
            return 1000000;
        }

        ui32 EventsPerMailbox() const override {
            return 1;
        }
    };

} // namespace NActors
