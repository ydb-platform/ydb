#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>


//using namespace NYql;
using namespace NActors;
//using namespace NProto;
//using namespace NDqs;

namespace {


class ClientActor : public TActorBootstrapped<ClientActor> {
public:
    void Bootstrap() {}

    NYql::NDq::TRetryEventsQueue EventsQueue;
};

class ServerActor : public TActorBootstrapped<ServerActor> {
public:
    void Bootstrap() {}

    NYql::NDq::TRetryEventsQueue EventsQueue;
};

struct TFixture: public NUnitTest::TBaseFixture
{
public:

    void SetUp(NUnitTest::TTestContext& /* context */) override {
        const ui32 nodesNumber = 2;
        ActorSystem.Reset(new NActors::TTestActorRuntimeBase(nodesNumber, true));

        ClientActorId = ActorSystem->Register(new ClientActor());
        ServerActorId = ActorSystem->Register(new ServerActor());

        ActorSystem->Initialize();

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

        // NActors::TDispatchOptions options;
        // options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, nodesNumber);
        // ActorRuntime_->DispatchEvents(options);
        ActorSystem->DispatchEvents({}, TDuration::Zero());

        // auto statusEv = MakeHolder<TEvClusterStatus>();
        // const auto statusSender = ActorRuntime_->AllocateEdgeActor();
        // ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), statusSender, statusEv.Release()));
        // auto resp = ActorRuntime_->GrabEdgeEvent<TEvClusterStatusResponse>(statusSender);

    }
    THolder<NActors::TTestActorRuntimeBase> ActorSystem;
    NActors::TActorId ClientActorId;
    NActors::TActorId ServerActorId;
};

Y_UNIT_TEST_SUITE(TRetryEventsQueueTest) {
    Y_UNIT_TEST_F(Empty, TFixture) { 

    }
}

}
