#include "cloud_function_transform.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/actors/testlib/test_runtime.h>

#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/common/http_gateway/mock/yql_http_mock_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <thread>
#include <chrono>

namespace {

using namespace NYql;
using namespace NActors;
using namespace NKikimr;
using namespace NMiniKQL;
using namespace NDq;

using namespace std::chrono_literals;

struct TTransformSystem {
    IDqOutputChannel::TPtr InputChannel;
    IDqOutputChannel::TPtr OutputChannel;
    NActors::TActorId TransformActorId;
    NActors::TActorId ComputeActorId;
    IHTTPMockGateway::TPtr Gateway;
};

class TCloudFunctionTransformTest: public TTestBase {
public:
    UNIT_TEST_SUITE(TCloudFunctionTransformTest);
    UNIT_TEST(TestEmptyChannel)
    UNIT_TEST(TestMultiplicationTransform)
    UNIT_TEST(TestSeveralIterations)
    UNIT_TEST(TestPrivateFunction)
    UNIT_TEST_SUITE_END();

    TCloudFunctionTransformTest()
    : Alloc(std::make_unique<TScopedAlloc>(NKikimr::TAlignedPagePoolCounters(), false))
    , TypeEnv(std::make_unique<TTypeEnvironment>(*Alloc))
    , FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
    , ProgramBuilder(*TypeEnv, *FunctionRegistry)
    , MemInfo("Mem")
    {
        HolderFactory = std::make_unique<NMiniKQL::THolderFactory>(Alloc->Ref(), MemInfo);
        ValueBuilder = std::make_unique<TDefaultValueBuilder>(*HolderFactory);
    }

    TTransformSystem StartUpTransformSystem(ui32 nodeId, TString transformName,
                                            TString connectionName = {}, THashMap<TString, TString> secureParams = {}) {
        auto system = TTransformSystem();

        system.Gateway = IHTTPMockGateway::Make();

        NDqProto::TDqTransform taskTransform;

        taskTransform.SetType(NDqProto::ETransformType::TRANSFORM_CLOUD_FUNCTION);
        taskTransform.SetFunctionName(transformName);
        if (!connectionName.empty()) {
            taskTransform.SetConnectionName(connectionName);
        }

        TStructMember inMembers[] = {
            {"A", TDataType::Create(NUdf::TDataType<bool>::Id, *TypeEnv)},
            {"C", TDataType::Create(NUdf::TDataType<ui32>::Id, *TypeEnv)}
        };
        NMiniKQL::TType* inputType = TStructType::Create(2, inMembers, *TypeEnv);
        taskTransform.SetInputType(NYql::NCommon::WriteTypeToYson(inputType));

        TStructMember outMembers[] = {
            {"A", TDataType::Create(NUdf::TDataType<bool>::Id, *TypeEnv)},
            {"C", TDataType::Create(NUdf::TDataType<ui64>::Id, *TypeEnv)}
        };
        NMiniKQL::TType* outputType = TStructType::Create(2, outMembers, *TypeEnv);
        taskTransform.SetOutputType(NYql::NCommon::WriteTypeToYson(outputType));

        TDqOutputChannelSettings settings;
        settings.MaxStoredBytes = 10000;
        settings.MaxChunkBytes = 2_MB;
        settings.TransportVersion = NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0;
        settings.CollectProfileStats = false;
        settings.AllowGeneratorsInUnboxedValues = false;

        TLogFunc logger = [](const TString& message) {
            Y_UNUSED(message);
        };

        system.InputChannel = CreateDqOutputChannel(nodeId + 100, inputType, *TypeEnv, *HolderFactory, settings, logger);
        system.OutputChannel = CreateDqOutputChannel(nodeId + 400, outputType, *TypeEnv, *HolderFactory, settings, logger);
        auto outputConsumer = CreateOutputMapConsumer(system.OutputChannel);

        auto credentialsFactory = CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory("", true, "");
        system.ComputeActorId = ActorRuntime_->AllocateEdgeActor();
        auto args = TDqTransformActorFactory::TArguments{
            .ComputeActorId = system.ComputeActorId,
            .TransformInput = system.InputChannel,
            .TransformOutput = outputConsumer,
            .HolderFactory = *HolderFactory,
            .TypeEnv = *TypeEnv,
            .ProgramBuilder = ProgramBuilder,
            .SecureParams = secureParams
        };

        NActors::IActor* transformActor;
        std::tie(std::ignore, transformActor) = CreateCloudFunctionTransformActor(taskTransform, system.Gateway,
                                                                                         credentialsFactory, std::move(args));
        system.TransformActorId = ActorRuntime_->Register(transformActor, nodeId);

        return system;
    }

    void SetUp() override {
        const ui32 nodesNumber = 6;
        ActorRuntime_.Reset(new NActors::TTestActorRuntimeBase(nodesNumber, false));
        ActorRuntime_->Initialize();
    }

    void TearDown() override {
        ActorRuntime_.Reset();
    }

    void TestEmptyChannel() {
        auto context = StartUpTransformSystem(0, "emptyChannel");

        context.InputChannel->Finish();
        auto executeEv = new TCloudFunctionTransformActor::TCFTransformEvent::TEvExecuteTransform();
        ActorRuntime_->Send(new IEventHandle(context.TransformActorId, context.ComputeActorId, executeEv));

        auto event = ActorRuntime_->GrabEdgeEvent<NTransformActor::TEvTransformCompleted>(context.ComputeActorId, TDuration::Seconds(1));

        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(true, context.OutputChannel->IsFinished());
    }


    void TestMultiplicationTransform() {
        auto context = StartUpTransformSystem(0, "multiplyC");

        IHTTPMockGateway::TDataResponse multiplyCResponse = []() {
            return IHTTPGateway::TContent("[{\"A\":true,\"C\":1110}]", 200);
        };

        auto url = "https://functions.yandexcloud.net/multiplyC";
        auto headers = IHTTPGateway::THeaders();
        auto postBody = "[{\"A\":\"true\",\"C\":\"555\"}]";
        context.Gateway->AddDownloadResponse(url, headers, postBody, multiplyCResponse);

        NUdf::TUnboxedValue* items;
        auto value = ValueBuilder->NewArray(2, items);
        items[0] = NUdf::TUnboxedValuePod(true);
        items[1] = NUdf::TUnboxedValuePod(ui32(555));

        context.InputChannel->Push(std::move(value));
        context.InputChannel->Finish();

        auto executeEv = new TCloudFunctionTransformActor::TCFTransformEvent::TEvExecuteTransform();
        ActorRuntime_->Send(new IEventHandle(context.TransformActorId, context.ComputeActorId, executeEv));
        auto event = ActorRuntime_->GrabEdgeEvent<NTransformActor::TEvTransformCompleted>(context.ComputeActorId, TDuration::Seconds(1));

        UNIT_ASSERT(event);
        NKikimr::NMiniKQL::TUnboxedValueVector transformedRows;
        context.OutputChannel->PopAll(transformedRows);
        UNIT_ASSERT_VALUES_EQUAL(transformedRows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(true, context.OutputChannel->IsFinished());
        transformedRows.clear();
    }

    void TestSeveralIterations() {
        auto context = StartUpTransformSystem(0, "multiplyCBatch");

        IHTTPMockGateway::TDataResponse multiplyCResponse1 = []() {
            return IHTTPGateway::TContent("[{\"A\":true,\"C\":860}]", 200);
        };

        IHTTPMockGateway::TDataResponse multiplyCResponse2 = []() {
            return IHTTPGateway::TContent("[{\"A\":false,\"C\":800}]", 200);
        };

        auto url = "https://functions.yandexcloud.net/multiplyCBatch";
        auto headers = IHTTPGateway::THeaders();
        auto postBody1 = "[{\"A\":\"true\",\"C\":\"430\"}]";
        context.Gateway->AddDownloadResponse(url, headers, postBody1, multiplyCResponse1);
        auto postBody2 = "[{\"A\":\"false\",\"C\":\"800\"}]";
        context.Gateway->AddDownloadResponse(url, headers, postBody2, multiplyCResponse2);

        NUdf::TUnboxedValue* items1;
        auto value1 = ValueBuilder->NewArray(2, items1);
        items1[0] = NUdf::TUnboxedValuePod(true);
        items1[1] = NUdf::TUnboxedValuePod(ui32(430));
        context.InputChannel->Push(std::move(value1));
        auto executeEv1 = new TCloudFunctionTransformActor::TCFTransformEvent::TEvExecuteTransform();
        ActorRuntime_->Send(new IEventHandle(context.TransformActorId, context.ComputeActorId, executeEv1));

        ActorRuntime_->GrabEdgeEvent<NTransformActor::TEvTransformNewData>(context.ComputeActorId, TDuration::MilliSeconds(500));

        NUdf::TUnboxedValue* items2;
        auto value2 = ValueBuilder->NewArray(2, items2);
        items2[0] = NUdf::TUnboxedValuePod(false);
        items2[1] = NUdf::TUnboxedValuePod(ui32(800));
        context.InputChannel->Push(std::move(value2));
        context.InputChannel->Finish();

        auto executeEv2 = new TCloudFunctionTransformActor::TCFTransformEvent::TEvExecuteTransform();
        ActorRuntime_->Send(new IEventHandle(context.TransformActorId, context.ComputeActorId, executeEv2));

        auto event = ActorRuntime_->GrabEdgeEvent<NTransformActor::TEvTransformCompleted>(context.ComputeActorId, TDuration::Seconds(1));

        UNIT_ASSERT(event);
        NKikimr::NMiniKQL::TUnboxedValueVector transformedRows;
        context.OutputChannel->PopAll(transformedRows);
        UNIT_ASSERT_VALUES_EQUAL(transformedRows.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(true, context.OutputChannel->IsFinished());
        transformedRows.clear();
    }

    void TestPrivateFunction() {
        const auto token = "{\"token\":\"yVVeGG6W0ccToZw\"}";
        const THashMap<TString, TString> secureParams = {{"private_cf_connection", token}};
        auto context = StartUpTransformSystem(0, "multiplyPrivate", "private_cf_connection", secureParams);

        IHTTPMockGateway::TDataResponse multiplyPrivateResponse = []() {
            return IHTTPGateway::TContent("[{\"A\":true,\"C\":360}]", 200);
        };

        auto url = "https://functions.yandexcloud.net/multiplyPrivate";
        auto headers = IHTTPGateway::THeaders{"Authorization: Bearer yVVeGG6W0ccToZw"};
        auto postBody = "[{\"A\":\"true\",\"C\":\"180\"}]";
        context.Gateway->AddDownloadResponse(url, headers, postBody, multiplyPrivateResponse);

        NUdf::TUnboxedValue* items;
        auto value = ValueBuilder->NewArray(2, items);
        items[0] = NUdf::TUnboxedValuePod(true);
        items[1] = NUdf::TUnboxedValuePod(ui32(180));

        context.InputChannel->Push(std::move(value));
        context.InputChannel->Finish();

        auto executeEv = new TCloudFunctionTransformActor::TCFTransformEvent::TEvExecuteTransform();
        ActorRuntime_->Send(new IEventHandle(context.TransformActorId, context.ComputeActorId, executeEv));
        auto event = ActorRuntime_->GrabEdgeEvent<NTransformActor::TEvTransformCompleted>(context.ComputeActorId, TDuration::Seconds(1));

        UNIT_ASSERT(event);
        NKikimr::NMiniKQL::TUnboxedValueVector transformedRows;
        context.OutputChannel->PopAll(transformedRows);
        transformedRows.clear();
    }

private:
    THolder<NActors::TTestActorRuntimeBase> ActorRuntime_;

    std::unique_ptr<NMiniKQL::TScopedAlloc> Alloc;
    std::unique_ptr<NMiniKQL::TTypeEnvironment> TypeEnv;

    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    NMiniKQL::TProgramBuilder ProgramBuilder;
    NMiniKQL::TMemoryUsageInfo MemInfo;
    std::unique_ptr<NMiniKQL::THolderFactory> HolderFactory;

    std::unique_ptr<TDefaultValueBuilder> ValueBuilder;
};

UNIT_TEST_SUITE_REGISTRATION(TCloudFunctionTransformTest)
}