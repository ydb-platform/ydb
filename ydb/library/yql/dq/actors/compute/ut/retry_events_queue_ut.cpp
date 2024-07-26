#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>
//#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <chrono>
#include <thread>


using namespace NActors;

namespace {

class ClientActor : public TActorBootstrapped<ClientActor>, public NYql::NDq::TRetryEventsQueue::ICallbacks {
public:
    ClientActor(NActors::TActorId serverActorId)
     : EventsQueue(this)
     , ServerActorId(serverActorId) {}

    void Bootstrap() {
        std::cerr << "Bootstrap" << std::endl;
        Become(&ClientActor::StateFunc);
        Init();
        std::cerr << "Bootstrap end" << std::endl;
    }

    void SendSomething() {
       EventsQueue.Send(new NFq::TEvRowDispatcher::TEvGetNextBatch());
    }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ) {
        std::cerr << "TEvRetry" << std::endl;
      //  EventsQueue.Retry();
    }

    STRICT_STFUNC(StateFunc,
       // hFunc(NYql::NDq::TEvDqCompute::TEvRun, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        
    )

    void Init() {
        EventsQueue.Init("TxId", SelfId(), SelfId(), 777);
        EventsQueue.OnNewRecipientId(ServerActorId);
    }

    virtual void SessionClosed(ui64 ) {}

    NYql::NDq::TRetryEventsQueue EventsQueue;
    NActors::TActorId ServerActorId;
};

struct TRuntime: public NActors::TTestActorRuntime
{
    
public:

    TRuntime() 
    : NActors::TTestActorRuntime(2, false){
        Initialize(NKikimr::TAppPrepare().Unwrap());
        SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);

        ServerActorId = AllocateEdgeActor(1);
        
        Client = new ClientActor(ServerActorId);
        ClientActorId = Register(Client, 0);
        EnableScheduleForActor(ClientActorId, true);

        std::cerr << " wait Bootstrap " << std::endl;

         NActors::TDispatchOptions options;
         options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
         DispatchEvents(options);

         std::cerr << "Bootstrap success" << std::endl;
    }

    // void TearDown(NUnitTest::TTestContext&) override {
    //   //  Reset();
    // }

    ClientActor* Client;
    NActors::TActorId ClientActorId;
    NActors::TActorId ServerActorId;
    //NKikimr::TActorSystemStub ActorSystemStub;
};


Y_UNIT_TEST_SUITE(TRetryEventsQueueTest) {
    Y_UNIT_TEST(Empty) { 
        TRuntime runtime;
        runtime.Client->SendSomething();

        NActors::TDispatchOptions options;
        options.FinalEvents.emplace_back(NYql::NDq::TEvRetryQueuePrivate::EEv::EvRetry, 1);
        runtime.DispatchEvents(options);
    }
}

}
