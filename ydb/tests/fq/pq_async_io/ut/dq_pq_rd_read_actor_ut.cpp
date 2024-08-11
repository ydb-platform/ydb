#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <thread>

namespace NYql::NDq {

Y_UNIT_TEST_SUITE(TDqPqRdReadActorTest) {
    Y_UNIT_TEST_F(TestReadFromTopic, TPqIoTestFixture) {

        InitRdSource(BuildPqTopicSourceSettings("topicName"));
        SourceRead<TString>(UVParser);
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe>(LocalRowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvCoordinatorChanged(Coordinator1Id);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, LocalRowDispatcherId, event));
        });
        
        auto eventHolder2 = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorRequest>(Coordinator1Id, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder2.Get() != nullptr);

        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvCoordinatorResult();
            auto* partitions = event->Record.AddPartitions();
            partitions->AddPartitionId(0);
            ActorIdToProto(RemoteRowDispatcher, partitions->MutableActorId());
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, Coordinator1Id, event));
        });

    }
}
} // NYql::NDq
