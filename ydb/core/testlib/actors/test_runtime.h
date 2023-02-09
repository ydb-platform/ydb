#pragma once

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/mon/mon.h>

#include <library/cpp/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/tests_data.h>

namespace NKikimr {
    struct TAppData;
}

namespace NKikimrProto {
    class TKeyConfig;
}

namespace NActors {
    struct IDestructable { virtual ~IDestructable() = default; };

    using TKeyConfigGenerator = std::function<NKikimrProto::TKeyConfig (ui32)>;


    class TTestActorRuntime
        : private TPortManager
        , public TTestActorRuntimeBase
    {
    private:
        struct TNodeData: public TNodeDataBase {
            void Stop();
            ~TNodeData();
            ui64 GetLoggerPoolId() const override;
            THolder<NActors::TMon> Mon;
        };

        struct TNodeFactory: public INodeFactory {
            TIntrusivePtr<TNodeDataBase> CreateNode() override {
                return MakeIntrusive<TNodeData>();
            }
        };

    public:
        struct TEgg {
            TAutoPtr<NKikimr::TAppData> App0;
            TAutoPtr<NActors::IDestructable> Opaque;
            TKeyConfigGenerator KeyConfigGenerator;
        };

        TTestActorRuntime(THeSingleSystemEnv d);
        TTestActorRuntime(ui32 nodeCount, ui32 dataCenterCount, bool UseRealThreads);
        TTestActorRuntime(ui32 nodeCount, ui32 dataCenterCount);
        TTestActorRuntime(ui32 nodeCount = 1, bool useRealThreads = false);

        ~TTestActorRuntime();

        void AddAppDataInit(std::function<void(ui32, NKikimr::TAppData&)> callback);
        virtual void Initialize(TEgg);

        ui16 GetMonPort(ui32 nodeIndex = 0) const;

        void SimulateSleep(TDuration duration);

        void SendToPipe(ui64 tabletId, const TActorId& sender, IEventBase* payload, ui32 nodeIndex = 0,
            const NKikimr::NTabletPipe::TClientConfig& pipeConfig = NKikimr::NTabletPipe::TClientConfig(), TActorId clientId = TActorId(), ui64 cookie = 0);
        void SendToPipe(TActorId clientId, const TActorId& sender, IEventBase* payload,
                                           ui32 nodeIndex = 0, ui64 cookie = 0);
        TActorId ConnectToPipe(ui64 tabletId, const TActorId& sender, ui32 nodeIndex, const NKikimr::NTabletPipe::TClientConfig& pipeConfig);
        void ClosePipe(TActorId clientId, const TActorId& sender, ui32 nodeIndex);
        void DisconnectNodes(ui32 fromNodeIndex, ui32 toNodeIndex, bool async = true);
        NKikimr::TAppData& GetAppData(ui32 nodeIndex = 0);

        TPortManager& GetPortManager() {
            return *this;
        }

        static bool DefaultScheduledFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline);

    private:
        void Initialize() override;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> GetCountersForComponent(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const char* component) override;
        void InitActorSystemSetup(TActorSystemSetup& setup) override;

        TNodeData* GetNodeById(size_t idx) override {
            return static_cast<TNodeData*>(TTestActorRuntimeBase::GetNodeById(idx));
        }

        void InitNodeImpl(TNodeDataBase*, size_t) override;

    private:
        using TTestActorRuntimeBase::Initialize;

    private:
        THolder<NKikimr::TAppData> App0;
        TKeyConfigGenerator KeyConfigGenerator;
        THolder<IDestructable> Opaque;
        TVector<ui16> MonPorts;
        TActorId SleepEdgeActor;
        TVector<std::function<void(ui32, NKikimr::TAppData&)>> AppDataInit_;
    };
} // namespace NActors
