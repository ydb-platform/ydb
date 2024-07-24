#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>
//#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
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


    // void Handle(const NYql::NDq::TEvDqCompute::TEvRun::TPtr& ev) {
    //     std::cerr << "TEvRun" << std::endl;
    //     Send(ev->Sender,new NYql::NDq::TEvDqCompute::TEvRun());

        
    // }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ) {
        std::cerr << "TEvRetry" << std::endl;
        EventsQueue.Retry();
    }

    STRICT_STFUNC(StateFunc,
       // hFunc(NYql::NDq::TEvDqCompute::TEvRun, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        
    )

    void Init() {
        EventsQueue.Init("TxId", SelfId(), SelfId(), 777);
        EventsQueue.OnNewRecipientId(ServerActorId);
       // EventsQueue.Send(new NYql::NDq::TEvRetryQueuePrivate::TEvRetry(1));
    }

    virtual void SessionClosed(ui64 ) {}

    NYql::NDq::TRetryEventsQueue EventsQueue;
    NActors::TActorId ServerActorId;
};

// class ServerActor : public TActorBootstrapped<ServerActor>, public NYql::NDq::TRetryEventsQueue::ICallbacks {
// public:
//     ServerActor()
//      : EventsQueue(this) {}

//     void Bootstrap() {
//         Become(&ServerActor::StateFunc);
//     }

//     void Handle(const NYql::NDq::TEvDqCompute::TEvRun::TPtr&) {
//         std::cerr << "TEvRun" << std::endl;
//     }


//     STRICT_STFUNC(StateFunc,
//         hFunc(NYql::NDq::TEvDqCompute::TEvRun, Handle);
//     )


//     virtual void SessionClosed(ui64 ) {}

//     NYql::NDq::TRetryEventsQueue EventsQueue;
// };

struct TRuntime: public NActors::TTestActorRuntime
{
    
public:

    TRuntime() 
    : NActors::TTestActorRuntime(1, true){
       // const ui32 nodesNumber = 1;
        //ActorSystem.Reset(new NActors::TTestActorRuntimeBase(nodesNumber));

        //Initialize(MakeEgg());

        Initialize(NKikimr::TAppPrepare().Unwrap());

        ServerActorId = AllocateEdgeActor(0);

        // Server = new ServerActor();
        // ServerActorId = Register(Server);
        
        Client = new ClientActor(ServerActorId);
        ClientActorId = Register(Client, 0);

      //  EnableScheduleForActor(ServerActorId, true);
        EnableScheduleForActor(ClientActorId, true);
        //Start();

        // for (ui32 i = 1; i < nodesNumber; i++) {
        //     ActorRuntime_->GetLogSettings(i)->Append(
        //         NKikimrServices::EServiceKikimr_MIN,
        //         NKikimrServices::EServiceKikimr_MAX,
        //         NKikimrServices::EServiceKikimr_Name
        //     );
        //     TString explanation;
        //     auto err = ActorRuntime_->GetLogSettings(i)->SetLevel(NActors::NLog::PRI_EMERG, NKikimrServices::KQP_COMPUTE, explanation); //do not care about CA errors in this test"
        //     Y_ABORT_IF(err);
        // }


        std::cerr << " wait Bootstrap " << std::endl;

         NActors::TDispatchOptions options;
         options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
         DispatchEvents(options);

         std::cerr << "Bootstrap success" << std::endl;

       // DispatchEvents({}, TDuration::Zero());

        // auto statusEv = MakeHolder<TEvClusterStatus>();
        // const auto statusSender = ActorRuntime_->AllocateEdgeActor();
        // ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), statusSender, statusEv.Release()));
        // auto resp = ActorRuntime_->GrabEdgeEvent<TEvClusterStatusResponse>(statusSender);

    }

    TTestActorRuntime::TEgg MakeEgg()
    {
        return { new NKikimr::TAppData(0, 0, 0, 0, { }, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {} };
    }


    // void TearDown(NUnitTest::TTestContext&) override {
    //   //  Reset();
    // }

   // THolder<NActors::TTestActorRuntimeBase> ActorSystem;
    ClientActor* Client;
   // ServerActor* Server;

    NActors::TActorId ClientActorId;
    NActors::TActorId ServerActorId;
 //   NKikimr::TActorSystemStub ActorSystemStub;
};


Y_UNIT_TEST_SUITE(TRetryEventsQueueTest) {
    Y_UNIT_TEST(Empty) { 
        // Client->Init(ServerActorId);
        TRuntime runtime;

        //runtime.Send(new IEventHandle(runtime.ClientActorId, runtime.ServerActorId, new NYql::NDq::TEvDqCompute::TEvRun()), 0, true);
        // runtime.GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvRun>(runtime.ServerActorId);
        //  runtime.DispatchEvents({}, TDuration::Seconds(5));


        // TAutoPtr<IEventHandle> handle;
        // runtime.GrabEdgeEvent<NYql::NDq::TEvRetryQueuePrivate::TEvRetry>(handle);


        // TAutoPtr<IEventHandle> handle;
        // runtime.GrabEdgeEvent<NActors::TEvInterconnect::TEvConnectNode>(handle);

        //  const TActorId proxy = runtime.GetInterconnectProxy(0, 1);
        //  std::cerr << "proxy2 " << proxy.ToString() << std::endl;

     //   runtime.Send(proxy, TActorId(), new NActors::TEvInterconnect::TEvConnectNode(), 0, true);

        //Wait for event TEvInterconnect::EvNodeDisconnected
        // TDispatchOptions options;
        // options.FinalEvents.emplace_back(NActors::TEvInterconnect::EvConnectNode);
        // runtime.DispatchEvents(options);

        //std::this_thread::sleep_for(std::chrono::milliseconds(20000));
    }
}

}
