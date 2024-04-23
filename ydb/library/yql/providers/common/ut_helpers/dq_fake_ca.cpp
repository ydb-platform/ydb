#include "dq_fake_ca.h"

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/core/testlib/basics/appdata.h>

#include <util/system/env.h>

#include <condition_variable>
#include <thread>

namespace NYql::NDq {

using namespace NActors;

NYql::NDqProto::TCheckpoint CreateCheckpoint(ui64 id) {
    auto checkpoint = NYql::NDqProto::TCheckpoint();
    checkpoint.SetGeneration(0);
    checkpoint.SetId(id);
    return checkpoint;
}

TFakeActor::TFakeActor(TAsyncInputPromises& sourcePromises, TAsyncOutputPromises& asyncOutputPromises)
    : TActor<TFakeActor>(&TFakeActor::StateFunc)
    , Alloc(__LOCATION__)
    , MemoryInfo("test")
    , HolderFactory(Alloc.Ref(), MemoryInfo)
    , TypeEnv(Alloc)
    , FunctionRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry()))
    , ProgramBuilder(TypeEnv, *FunctionRegistry)
    , ValueBuilder(HolderFactory)
    , AsyncInputEvents(*this)
    , AsyncOutputCallbacks(*this)
    , AsyncInputPromises(sourcePromises)
    , AsyncOutputPromises(asyncOutputPromises)
{
    Alloc.Release();
}

TFakeActor::~TFakeActor() {
    Alloc.Acquire();
}

void TFakeActor::InitAsyncOutput(IDqComputeActorAsyncOutput* dqAsyncOutput, IActor* dqAsyncOutputAsActor) {
    DqAsyncOutputActorId = RegisterWithSameMailbox(dqAsyncOutputAsActor),
    DqAsyncOutput = dqAsyncOutput;
    DqAsyncOutputAsActor = dqAsyncOutputAsActor;
}

void TFakeActor::InitAsyncInput(IDqComputeActorAsyncInput* dqAsyncInput, IActor* dqAsyncInputAsActor) {
    DqAsyncInputActorId = RegisterWithSameMailbox(dqAsyncInputAsActor),
    DqAsyncInput = dqAsyncInput;
    DqAsyncInputAsActor = dqAsyncInputAsActor;
}

void TFakeActor::Terminate() {
    if (DqAsyncInputActorId) {
        DqAsyncInput->PassAway();

        DqAsyncInputActorId = std::nullopt;
        DqAsyncInput = nullptr;
        DqAsyncInputAsActor = nullptr;
    }

    if (DqAsyncOutputActorId) {
        DqAsyncOutput->PassAway();

        DqAsyncOutputActorId = std::nullopt;
        DqAsyncOutput = nullptr;
        DqAsyncOutputAsActor = nullptr;
    }
}

TFakeActor::TAsyncOutputCallbacks& TFakeActor::GetAsyncOutputCallbacks() {
    return AsyncOutputCallbacks;
}

NKikimr::NMiniKQL::THolderFactory& TFakeActor::GetHolderFactory() {
    return HolderFactory;
}

TFakeCASetup::TFakeCASetup()
    : Runtime(new NActors::TTestBasicRuntime(1, true))
    , FakeActorId(0, "FakeActor")
{
    Runtime->AddLocalService(
        FakeActorId,
        NActors::TActorSetupCmd(
            new TFakeActor(AsyncInputPromises, AsyncOutputPromises),
            NActors::TMailboxType::Simple,
            0));

    Runtime->SetLogBackend(CreateStderrBackend());

    TAutoPtr<NKikimr::TAppPrepare> app = new NKikimr::TAppPrepare();
    Runtime->Initialize(app->Unwrap());

    Runtime->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::EPriority::PRI_TRACE);
}

TFakeCASetup::~TFakeCASetup() {
    Execute([](TFakeActor& actor) {
        actor.Terminate();
    });
}

void TFakeCASetup::AsyncOutputWrite(const TWriteValueProducer valueProducer, TMaybe<NDqProto::TCheckpoint> checkpoint, bool finish) {
    Execute([&valueProducer, checkpoint, finish](TFakeActor& actor) {
        auto batch = valueProducer(actor.GetHolderFactory());
        
        Y_ASSERT(actor.DqAsyncOutput);
        actor.DqAsyncOutput->SendData(std::move(batch), 0, checkpoint, finish);
    });
}

void TFakeCASetup::SaveSourceState(NDqProto::TCheckpoint checkpoint, TSourceState& state) {
    Execute([&state, &checkpoint](TFakeActor& actor) {
        Y_ASSERT(actor.DqAsyncInput);
        actor.DqAsyncInput->SaveState(checkpoint, state);
    });
}

void TFakeCASetup::LoadSource(const TSourceState& state) {
    Execute([&state](TFakeActor& actor) {
        Y_ASSERT(actor.DqAsyncInput);
        actor.DqAsyncInput->LoadState(state);
    });
}

void TFakeCASetup::LoadSink(const TSinkState& state) {
    Execute([&state](TFakeActor& actor) {
        Y_ASSERT(actor.DqAsyncOutput);
        actor.DqAsyncOutput->LoadState(state);
    });
}

void TFakeCASetup::Execute(TCallback callback) {
    std::exception_ptr exception_ptr = nullptr;
    const TActorId& edgeId = Runtime->AllocateEdgeActor();
    auto promise = NThreading::NewPromise();
    Runtime->Send(new IEventHandle(FakeActorId, edgeId, new TEvPrivate::TEvExecute(promise, callback, exception_ptr)));
    auto future = promise.GetFuture();
    future.Wait();
    if (exception_ptr) {
        std::rethrow_exception(exception_ptr);
    }
}

} // namespace NKikimr::NMiniKQL
