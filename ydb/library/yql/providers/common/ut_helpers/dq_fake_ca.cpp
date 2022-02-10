#include "dq_fake_ca.h" 
 
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
 
TFakeActor::TFakeActor(TSourcePromises& sourcePromises, TSinkPromises& sinkPromises) 
    : TActor<TFakeActor>(&TFakeActor::StateFunc) 
    , MemoryInfo("test") 
    , HolderFactory(Alloc.Ref(), MemoryInfo) 
    , SourceCallbacks(*this) 
    , SinkCallbacks(*this) 
    , SourcePromises(sourcePromises) 
    , SinkPromises(sinkPromises) 
{ 
    Alloc.Release(); 
} 
 
TFakeActor::~TFakeActor() { 
    Alloc.Acquire(); 
} 
 
void TFakeActor::InitSink(IDqSinkActor* dqSink, IActor* dqSinkAsActor) { 
    DqSinkActorId = RegisterWithSameMailbox(dqSinkAsActor), 
    DqSinkActor = dqSink; 
    DqSinkActorAsActor = dqSinkAsActor; 
} 
 
void TFakeActor::InitSource(IDqSourceActor* dqSource, IActor* dqSourceAsActor) { 
    DqSourceActorId = RegisterWithSameMailbox(dqSourceAsActor), 
    DqSourceActor = dqSource; 
    DqSourceActorAsActor = dqSourceAsActor; 
} 
 
void TFakeActor::Terminate() { 
    if (DqSourceActorId) { 
        DqSourceActor->PassAway();
 
        DqSourceActorId = std::nullopt; 
        DqSourceActor = nullptr; 
        DqSourceActorAsActor = nullptr; 
    } 
 
    if (DqSinkActorId) { 
        DqSinkActor->PassAway();
 
        DqSinkActorId = std::nullopt; 
        DqSinkActor = nullptr; 
        DqSinkActorAsActor = nullptr; 
    } 
} 
 
TFakeActor::TSourceCallbacks& TFakeActor::GetSourceCallbacks() { 
    return SourceCallbacks; 
} 
 
TFakeActor::TSinkCallbacks& TFakeActor::GetSinkCallbacks() { 
    return SinkCallbacks; 
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
            new TFakeActor(SourcePromises, SinkPromises), 
            NActors::TMailboxType::Simple, 
            0)); 
 
    TAutoPtr<NKikimr::TAppPrepare> app = new NKikimr::TAppPrepare(); 
    Runtime->Initialize(app->Unwrap()); 
} 
 
TFakeCASetup::~TFakeCASetup() { 
    Execute([](TFakeActor& actor) { 
        actor.Terminate(); 
    }); 
} 
 
void TFakeCASetup::SinkWrite(const TWriteValueProducer valueProducer, TMaybe<NDqProto::TCheckpoint> checkpoint) { 
    Execute([&valueProducer, checkpoint](TFakeActor& actor) { 
        auto batch = valueProducer(actor.GetHolderFactory()); 
        Y_ASSERT(actor.DqSinkActor); 
        actor.DqSinkActor->SendData(std::move(batch), 0, checkpoint, false); 
    }); 
} 
 
void TFakeCASetup::SaveSourceState(NDqProto::TCheckpoint checkpoint, NDqProto::TSourceState& state) {
    Execute([&state, &checkpoint](TFakeActor& actor) { 
        Y_ASSERT(actor.DqSourceActor); 
        actor.DqSourceActor->SaveState(checkpoint, state);
    }); 
} 
 
void TFakeCASetup::LoadSource(const NDqProto::TSourceState& state) {
    Execute([&state](TFakeActor& actor) { 
        Y_ASSERT(actor.DqSourceActor); 
        actor.DqSourceActor->LoadState(state); 
    }); 
} 
 
void TFakeCASetup::LoadSink(const NDqProto::TSinkState& state) {
    Execute([&state](TFakeActor& actor) { 
        Y_ASSERT(actor.DqSinkActor); 
        actor.DqSinkActor->LoadState(state); 
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
