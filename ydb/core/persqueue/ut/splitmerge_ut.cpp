#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;


// TODO
static constexpr ui64 SS = 72057594046644480l;

auto CreateTransaction(const TString& parentPath, ::NKikimrSchemeOp::TPersQueueGroupDescription& scheme) {
    NKikimrSchemeOp::TModifyScheme tx;
    tx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
    tx.SetWorkingDir(parentPath);
    tx.MutableAlterPersQueueGroup()->CopyFrom(scheme);
    return tx;
}

TEvTx* CreateRequest(ui64 txId, NKikimrSchemeOp::TModifyScheme&& tx) {
    auto ev = new TEvTx(txId, SS);
    *ev->Record.AddTransaction() = std::move(tx);
    return ev;
}

void DoRequest(TTopicSdkTestSetup& setup, ui64& txId, NKikimrSchemeOp::TPersQueueGroupDescription& scheme) {
    Cerr << "ALTER_SCHEME: " << scheme << Endl;

    const auto sender = setup.GetRuntime().AllocateEdgeActor();
    const auto request = CreateRequest(txId, CreateTransaction("/Root", scheme));
    setup.GetRuntime().Send(new IEventHandle(
            MakeTabletResolverID(),
            sender,
            new TEvTabletResolver::TEvForward(
                    SS,
                    new IEventHandle(TActorId(), sender, request),
                    { },
                    TEvTabletResolver::TEvForward::EActor::Tablet
            )),
            0);

    auto subscriber = CreateNotificationSubscriber(setup.GetRuntime(), SS);
    setup.GetRuntime().Send(new IEventHandle(subscriber, sender, new TEvSchemeShard::TEvNotifyTxCompletion(txId)));
    TAutoPtr<IEventHandle> handle;
    auto event = setup.GetRuntime().GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
    UNIT_ASSERT(event);
    UNIT_ASSERT_EQUAL(event->Record.GetTxId(), txId);

    auto e = setup.GetRuntime().GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
    UNIT_ASSERT_EQUAL_C(e->Record.GetStatus(), TEvSchemeShard::EStatus::StatusAccepted,
        "Unexpected status " << NKikimrScheme::EStatus_Name(e->Record.GetStatus()) << " " << e->Record.GetReason());
}

void SplitPartition(TTopicSdkTestSetup& setup, ui64& txId, const ui32 partition, TString boundary) {
    ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
    scheme.SetName(TEST_TOPIC);
    auto* split = scheme.AddSplit();
    split->SetPartition(partition);
    split->SetSplitBoundary(boundary);

    DoRequest(setup, txId, scheme);
}

void MergePartition(TTopicSdkTestSetup& setup, ui64& txId, const ui32 partitionLeft, const ui32 partitionRight) {
    ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
    scheme.SetName(TEST_TOPIC);
    auto* merge = scheme.AddMerge();
    merge->SetPartition(partitionLeft);
    merge->SetAdjacentPartition(partitionRight);

    DoRequest(setup, txId, scheme);
}

auto Msg(const TString& data, ui64 seqNo) {
    TWriteMessage msg(data);
    msg.SeqNo(seqNo);
    return msg;
}



Y_UNIT_TEST_SUITE(TopicSplitMerge) {
    Y_UNIT_TEST(PartitionSplit) {
        TTopicSdkTestSetup setup("TopicSplitMerge", TTopicSdkTestSetup::MakeServerSettings(), false);

        auto& ff = setup.GetRuntime().GetAppData().FeatureFlags;
        ff.SetEnableTopicSplitMerge(true);
        ff.SetEnablePQConfigTransactionsAtSchemeShard(true);

        setup.CreateTopic();

        TTopicClient client = setup.MakeClient();

        setup.GetRuntime().SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        setup.GetRuntime().SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_TRACE);

        TString producer1 = "producer-1";
        TString producer2 = "producer-2";
        TString producer3 = "producer-3";
        TString producer4 = "producer-4";

        auto writeSettings1 = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .ProducerId(producer1)
                        .MessageGroupId(producer1);
        auto writeSession1 = client.CreateSimpleBlockingWriteSession(writeSettings1);

        auto writeSettings2 = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .ProducerId(producer2)
                        .MessageGroupId(producer2);
        auto writeSession2 = client.CreateSimpleBlockingWriteSession(writeSettings2);

        auto writeSettings3 = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .ProducerId(producer3)
                        .PartitionId(0);
        auto writeSession3 = client.CreateSimpleBlockingWriteSession(writeSettings3);

        Cerr << ">>>>> 1 " << Endl;

        struct MsgInfo {
            ui64 PartitionId;
            ui64 SeqNo;
            ui64 Offset;
            TString Data;
        };

        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        std::vector<MsgInfo> receivedMessages;
        std::set<size_t> partitions;

        auto readSettings = TReadSessionSettings()
            .ConsumerName(TEST_CONSUMER)
            .AppendTopics(TEST_TOPIC);

        readSettings.EventHandlers_.SimpleDataHandlers(
            [&]
            (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
            auto& messages = ev.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];

                Cerr << ">>>>> Received TDataReceivedEvent message partitionId=" << message.GetPartitionSession()->GetPartitionId() 
                     << ", message=" << message.GetData() 
                     << ", seqNo=" << message.GetSeqNo()
                     << ", offset=" << message.GetOffset()
                     << Endl;
                receivedMessages.push_back({message.GetPartitionSession()->GetPartitionId(),
                                            message.GetSeqNo(),
                                            message.GetOffset(),
                                            message.GetData()});
            }

            if (receivedMessages.size() == 6) {
                checkedPromise.SetValue();
            }
        });

        readSettings.EventHandlers_.StartPartitionSessionHandler(
            [&]
            (TReadSessionEvent::TStartPartitionSessionEvent& ev) mutable {
                Cerr << ">>>>> Received TStartPartitionSessionEvent message " << ev.DebugString() << Endl;
                partitions.insert(ev.GetPartitionSession()->GetPartitionId());
                ev.Confirm();
        });

        auto readSession = client.CreateReadSession(readSettings);

        Cerr << ">>>>> 2 " << Endl;

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));
        UNIT_ASSERT(writeSession3->Write(Msg("message_3.1", 1)));

        Cerr << ">>>>> 3 " << Endl;

        ui64 txId = 0;
        SplitPartition(setup, ++txId, 0, "a");

        Cerr << ">>>>> 4 " << Endl;

        writeSession1->Write(Msg("message_1.2_2", 2)); // Will be ignored because duplicated SeqNo
        writeSession3->Write(Msg("message_3.2", 11)); // Will be fail because partition is not writable after split
        writeSession1->Write(Msg("message_1.2", 5));
        writeSession2->Write(Msg("message_2.2", 7));

        auto writeSettings4 = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .DeduplicationEnabled(false)
                        .PartitionId(1);
        auto writeSession4 = client.CreateSimpleBlockingWriteSession(writeSettings4);
        writeSession4->Write(TWriteMessage("message_4.1"));

        Cerr << ">>>>> 5 " << Endl;

        checkedPromise.GetFuture().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(6, receivedMessages.size());

        Cerr << ">>>>> 6 " << Endl;

        for(const auto& info : receivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_2.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(3, info.SeqNo);
            } else if (info.Data == "message_1.2") {
                UNIT_ASSERT_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_EQUAL(5, info.SeqNo);
            } else if (info.Data == "message_2.2") {
                UNIT_ASSERT_EQUAL(1, info.PartitionId);
                UNIT_ASSERT_EQUAL(7, info.SeqNo);
            } else if (info.Data == "message_3.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(1, info.SeqNo);
            } else if (info.Data == "message_4.1") {
                UNIT_ASSERT_C(1, info.PartitionId);
                UNIT_ASSERT_C(1, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        Cerr << ">>>>> 7 " << Endl;

        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
        writeSession3->Close(TDuration::Seconds(1));
        readSession->Close(TDuration::Seconds(1));

        Cerr << ">>>>> 8 " << Endl;
    }

    Y_UNIT_TEST(PartitionMerge) {
        TTopicSdkTestSetup setup("TopicSplitMerge", TTopicSdkTestSetup::MakeServerSettings(), false);

        auto& ff = setup.GetRuntime().GetAppData().FeatureFlags;
        ff.SetEnableTopicSplitMerge(true);
        ff.SetEnablePQConfigTransactionsAtSchemeShard(true);

        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);

        TTopicClient client = setup.MakeClient();

        setup.GetRuntime().SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        setup.GetRuntime().SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_TRACE);

        TString producer1 = "producer-1";
        TString producer2 = "producer-2";
        TString producer3 = "producer-3";

        auto writeSettings1 = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .ProducerId(producer1)
                        .MessageGroupId(producer1)
                        .PartitionId(0);
        auto writeSession1 = client.CreateSimpleBlockingWriteSession(writeSettings1);

        auto writeSettings2 = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .ProducerId(producer2)
                        .MessageGroupId(producer2)
                        .PartitionId(1);
        auto writeSession2 = client.CreateSimpleBlockingWriteSession(writeSettings2);

        Cerr << ">>>>> 1 " << Endl;

        struct MsgInfo {
            ui64 PartitionId;
            ui64 SeqNo;
            ui64 Offset;
            TString Data;
        };

        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        std::vector<MsgInfo> receivedMessages;
        std::set<size_t> partitions;

        auto readSettings = TReadSessionSettings()
            .ConsumerName(TEST_CONSUMER)
            .AppendTopics(TEST_TOPIC);

        readSettings.EventHandlers_.SimpleDataHandlers(
            [&]
            (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
            auto& messages = ev.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];

                Cerr << ">>>>> Received message partitionId=" << message.GetPartitionSession()->GetPartitionId() 
                     << ", message=" << message.GetData() 
                     << ", seqNo=" << message.GetSeqNo()
                     << ", offset=" << message.GetOffset()
                     << Endl;
                receivedMessages.push_back({message.GetPartitionSession()->GetPartitionId(),
                                            message.GetSeqNo(),
                                            message.GetOffset(),
                                            message.GetData()});
            }

            if (receivedMessages.size() == 3) {
                checkedPromise.SetValue();
            }
        });

        readSettings.EventHandlers_.StartPartitionSessionHandler(
            [&]
            (TReadSessionEvent::TStartPartitionSessionEvent& ev) mutable {
                Cerr << ">>>>> Received TStartPartitionSessionEvent for partition " << ev.GetPartitionSession()->GetPartitionId() << Endl;
                partitions.insert(ev.GetPartitionSession()->GetPartitionId());
                ev.Confirm();
        });

        auto readSession = client.CreateReadSession(readSettings);

        Cerr << ">>>>> 2 " << Endl;

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));

        Cerr << ">>>>> 3 " << Endl;

        ui64 txId = 0;
        MergePartition(setup, ++txId, 0, 1);

        Cerr << ">>>>> 4 " << Endl;

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.2", 5))); // Will be fail because partition is not writable after merge
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.2", 7))); // Will be fail because partition is not writable after merge

        auto writeSettings3 = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .ProducerId(producer1)
                        .MessageGroupId(producer1);
        auto writeSession3 = client.CreateSimpleBlockingWriteSession(writeSettings3);

        UNIT_ASSERT(writeSession3->Write(Msg("message_3.1", 2)));  // Will be ignored because duplicated SeqNo
        UNIT_ASSERT(writeSession3->Write(Msg("message_3.2", 11)));


        Cerr << ">>>>> 5 " << Endl;

        checkedPromise.GetFuture().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(3, receivedMessages.size());

        Cerr << ">>>>> 6 " << Endl;

        for(const auto& info : receivedMessages) {
            if (info.Data == TString("message_1.1")) {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(2, info.SeqNo);
            } else if (info.Data == TString("message_2.1")) {
                UNIT_ASSERT_EQUAL(1, info.PartitionId);
                UNIT_ASSERT_EQUAL(3, info.SeqNo);
            } else if (info.Data == TString("message_3.2")) {
                UNIT_ASSERT_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_EQUAL(11, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        Cerr << ">>>>> 7 " << Endl;

        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
        writeSession3->Close(TDuration::Seconds(1));
        readSession->Close(TDuration::Seconds(1));

        Cerr << ">>>>> 8 " << Endl;
    }

}

} // namespace NKikimr
