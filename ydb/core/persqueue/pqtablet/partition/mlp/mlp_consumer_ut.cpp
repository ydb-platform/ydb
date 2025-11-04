#include "mlp_storage.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPConsumerTests) {

Y_UNIT_TEST(Reload) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    CreateTopic(setup, "/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddConsumer()
                .ConsumerName("mlp-consumer")
                .AddAttribute("_mlp", "1")
            .EndAddConsumer());

    // Write many messaes because small snapshot do not write wal
    WriteMany(setup, "/Root/topic1", 0, 16, 113);

    Cerr << ">>>>> BEGIN DESCRIBE" << Endl;

    ui64 tabletId;
    {
        CreateDescriberActor(runtime, "/Root", "/Root/topic1");
        auto result = GetDescriberResponse(runtime);
        tabletId = result->Topics["/Root/topic1"].Info->PartitionGraph->GetPartition(0)->TabletId;
    }

    Sleep(TDuration::Seconds(2));
    
    Cerr << ">>>>> BEGIN READ" << Endl;

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .VisibilityTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .VisibilityTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Cerr << ">>>>> BEGIN COMMIT" << Endl;

    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Cerr << ">>>>> BEGIN REBOOT " << tabletId << Endl;

    ForwardToTablet(runtime, tabletId, runtime.AllocateEdgeActor(), new TEvents::TEvPoison());

    Sleep(TDuration::Seconds(2));

    ForwardToTablet(runtime, tabletId, runtime.AllocateEdgeActor(),
        new NKikimr::TEvPQ::TEvGetMLPConsumerStateRequest("/Root/topic1", "mlp-consumer", 0));
    auto result = runtime.GrabEdgeEvent<NKikimr::TEvPQ::TEvGetMLPConsumerStateResponse>();

    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[2].Status, TStorage::EMessageStatus::Unprocessed);
}

}

} // namespace NKikimr::NPQ::NMLP
