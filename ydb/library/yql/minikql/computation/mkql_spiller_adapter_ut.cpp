#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/computation/mkql_spiller_adapter.h>
#include <ydb/library/yql/dq/actors/spilling/compute_storage_actor.h>
#include <ydb/library/yql/dq/actors/spilling/spilling_file.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/services/services.pb.h>

#include <vector>

namespace NKikimr::NMiniKQL {

namespace {

using namespace NActors;
using namespace NYql::NDq;

class TTestActorRuntime: public TTestActorRuntimeBase {
public:

    TTestActorRuntime() : TTestActorRuntimeBase(100, true){}

    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        node->LogSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );
        TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
    }

    ~TTestActorRuntime() {
        if (SpillingRoot_ && SpillingRoot_.Exists()) {
            SpillingRoot_.ForceDelete();
        }
    }

    void Initialize() override {
        TTestActorRuntimeBase::Initialize();
        SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_ERROR);
    }

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters() {
        static auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        return counters;
    }

    static TString GetSpillingPrefix() {
        static TString str = Sprintf("%s_%d", "dq_spilling", (int)getpid());
        return str;
    }

    TActorId StartSpillingService(ui64 maxTotalSize = 1000, ui64 maxFileSize = 500,
        ui64 maxFilePartSize = 100, const TFsPath& root = TFsPath::Cwd() / GetSpillingPrefix())
    {
        SpillingRoot_ = root;

        auto config = TFileSpillingServiceConfig{
            .Root = root.GetPath(),
            .MaxTotalSize = maxTotalSize,
            .MaxFileSize = maxFileSize,
            .MaxFilePartSize = maxFilePartSize
        };

        auto counters = Counters();
        counters->ResetCounters();

        auto spillingService = CreateDqLocalFileSpillingService(config, MakeIntrusive<TSpillingCounters>(counters));
        auto spillingServiceActorId = Register(spillingService);
        EnableScheduleForActor(spillingServiceActorId);
        RegisterService(MakeDqLocalFileSpillingServiceID(GetNodeId()), spillingServiceActorId);

        return spillingServiceActorId;
    }

    void RegisterSpillingActor(IActor* spillingActor) {
        auto spillingActorId = Register(spillingActor);
        EnableScheduleForActor(spillingActorId);
    }

    TActorSystem* ActorSystem() {
        return GetAnyNodeActorSystem();
    }

    void WaitBootstrap() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(DispatchEvents(options));
    }

    const TFsPath& GetSpillingRoot() const {
        return SpillingRoot_;
    }

private:
    TFsPath SpillingRoot_;
};


}

Y_UNIT_TEST_SUITE(TestWideSpillerAdapter) {
    constexpr size_t itemWidth = 3;
    constexpr size_t chunkSize = 100;
    NYql::NDq::TTxId txid;

    Y_UNIT_TEST(TestWriteExtract) {
        TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);
        TTypeEnvironment env(alloc);

        TTestActorRuntime runtime;
        runtime.Initialize();
        runtime.StartSpillingService();
        auto spiller = NYql::NDq::CreateDqComputeStorageActor(txid, runtime.ActorSystem());
        runtime.RegisterSpillingActor(spiller->GetActor());
        runtime.WaitBootstrap();

        std::vector<TType*> itemTypes(itemWidth, TDataType::Create(NUdf::TDataType<char*>::Id, env));
        TWideUnboxedValuesSpillerAdapter wideUVSpiller(spiller, TMultiType::Create(itemWidth, itemTypes.data(), env), chunkSize);
        std::vector<NUdf::TUnboxedValue> wideValue(itemWidth);
        constexpr size_t rowCount = chunkSize*10+3;
        for (size_t row = 0; row != rowCount; ++row) {
            for(size_t i = 0; i != itemWidth; ++i) {
                wideValue[i] = NUdf::TUnboxedValuePod(NUdf::TStringValue(TStringBuilder() << "Long enough string: " << row * 10 + i));
            }
            if (auto r = wideUVSpiller.WriteWideItem(wideValue)) {
                r->Wait();
                wideUVSpiller.AsyncWriteCompleted(r->GetValue());
            }
        }
        auto r = wideUVSpiller.FinishWriting();
        if (r) {
            r->Wait();
            wideUVSpiller.AsyncWriteCompleted(r->GetValue());
        }

        r->Wait();
        wideUVSpiller.AsyncWriteCompleted(r->GetValue());
        for (size_t row = 0; row != rowCount; ++row) {
            UNIT_ASSERT(!wideUVSpiller.Empty());
            if (auto r = wideUVSpiller.ExtractWideItem(wideValue)) {
                r->Wait();
                wideUVSpiller.AsyncReadCompleted(r->ExtractValue(), holderFactory);
                r = wideUVSpiller.ExtractWideItem(wideValue);
                UNIT_ASSERT(!r.has_value());
            }
            for (size_t i = 0; i != itemWidth; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(TStringBuf(wideValue[i].AsStringRef()) , TStringBuilder() << "Long enough string: " << row * 10 + i);
            }
        }
        UNIT_ASSERT(!wideUVSpiller.Empty());
    }
}

} //namespace NKikimr::NMiniKQL