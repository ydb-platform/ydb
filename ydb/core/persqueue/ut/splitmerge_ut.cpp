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
    Sleep(TDuration::Seconds(1));

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

    Sleep(TDuration::Seconds(1));
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

TTopicSdkTestSetup CreateSetup() {
    NKikimrConfig::TFeatureFlags ff;
    ff.SetEnableTopicSplitMerge(true);
    ff.SetEnablePQConfigTransactionsAtSchemeShard(true);

    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetFeatureFlags(ff);

    auto setup = TTopicSdkTestSetup("TopicSplitMerge", settings, false);

    setup.GetRuntime().SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    setup.GetRuntime().SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_TRACE);
    setup.GetRuntime().SetLogPriority(NKikimrServices::PQ_PARTITION_CHOOSER, NActors::NLog::PRI_TRACE);

    setup.GetRuntime().GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    setup.GetRuntime().GetAppData().PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
    setup.GetRuntime().GetAppData().PQConfig.SetBalancerWakeupIntervalSec(1);

    return setup;
}

std::shared_ptr<ISimpleBlockingWriteSession> CreateWriteSession(TTopicClient& client, const TString& producer, std::optional<ui32> partition = std::nullopt) {
    auto writeSettings = TWriteSessionSettings()
                    .Path(TEST_TOPIC)
                    .ProducerId(producer);
    if (partition) {
        writeSettings.PartitionId(*partition);
    } else {
        writeSettings.MessageGroupId(producer);
    }

    return client.CreateSimpleBlockingWriteSession(writeSettings);
}

struct TTestReadSession {
    struct MsgInfo {
        ui64 PartitionId;
        ui64 SeqNo;
        ui64 Offset;
        TString Data;

        TReadSessionEvent::TDataReceivedEvent::TMessage Msg;
        bool Commited;
    };

    bool AutoCommit;

    std::shared_ptr<IReadSession> Session;

    NThreading::TPromise<void> Promise = NThreading::NewPromise<void>();
    std::vector<MsgInfo> ReceivedMessages;
    std::set<size_t> Partitions;

    TTestReadSession(TTopicClient& client, size_t expectedMessagesCount, bool autoCommit = true)
        : AutoCommit(autoCommit) {
        auto readSettings = TReadSessionSettings()
            .ConsumerName(TEST_CONSUMER)
            .AppendTopics(TEST_TOPIC);

        readSettings.EventHandlers_.SimpleDataHandlers(
            [&, expectedMessagesCount]
            (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
            auto& messages = ev.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];

                Cerr << ">>>>> Received TDataReceivedEvent message partitionId=" << message.GetPartitionSession()->GetPartitionId()
                     << ", message=" << message.GetData()
                     << ", seqNo=" << message.GetSeqNo()
                     << ", offset=" << message.GetOffset()
                     << Endl;
                ReceivedMessages.push_back({message.GetPartitionSession()->GetPartitionId(),
                                            message.GetSeqNo(),
                                            message.GetOffset(),
                                            message.GetData(),
                                            message,
                                            AutoCommit});

                if (AutoCommit) {
                    message.Commit();
                }
            }

            if (ReceivedMessages.size() == expectedMessagesCount) {
                Promise.SetValue();
            }
        });

        readSettings.EventHandlers_.StartPartitionSessionHandler(
            [&]
            (TReadSessionEvent::TStartPartitionSessionEvent& ev) mutable {
                Cerr << ">>>>> Received TStartPartitionSessionEvent message " << ev.DebugString() << Endl;
                Partitions.insert(ev.GetPartitionSession()->GetPartitionId());
                ev.Confirm();
        });

        Session = client.CreateReadSession(readSettings);
    }

    void WaitAllMessages() {
        Promise.GetFuture().GetValueSync();
    }

    void Commit() {
        for (auto& m : ReceivedMessages) {
            if (!m.Commited) {
                m.Msg.Commit();
                m.Commited = true;
            }
        }
    }
};

Y_UNIT_TEST_SUITE(TopicSplitMerge) {

    Y_UNIT_TEST(Simple) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic();

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1");
        auto writeSession2 = CreateWriteSession(client, "producer-2");

        TTestReadSession ReadSession(client, 2);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));

        ReadSession.WaitAllMessages();

        for(const auto& info : ReadSession.ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_2.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(3, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(PartitionSplit) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1");

        TTestReadSession ReadSession(client, 2, false);

        UNIT_ASSERT(writeSession->Write(Msg("message_1.1", 2)));

        ui64 txId = 1006;
        SplitPartition(setup, ++txId, 0, "a");

        UNIT_ASSERT(writeSession->Write(Msg("message_1.2", 3)));

        Sleep(TDuration::Seconds(1)); // Wait read session events

        UNIT_ASSERT_EQUAL_C(1, ReadSession.Partitions.size(), "We are reading only one partitions because offset is not commited");
        ReadSession.Commit();

        ReadSession.WaitAllMessages();

        for(const auto& info : ReadSession.ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_1.2") {
                UNIT_ASSERT(1 == info.PartitionId || 2 == info.PartitionId);
                UNIT_ASSERT_EQUAL(3, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        writeSession->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(PartitionSplit_PreferedPartition) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1");
        auto writeSession2 = CreateWriteSession(client, "producer-2");
        auto writeSession3 = CreateWriteSession(client, "producer-3", 0);

        TTestReadSession ReadSession(client, 6);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));
        UNIT_ASSERT(writeSession3->Write(Msg("message_3.1", 1)));

        ui64 txId = 1006;
        SplitPartition(setup, ++txId, 0, "a");

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

        ReadSession.WaitAllMessages();

        Cerr << ">>>>> All messages received" << Endl;

        for(const auto& info : ReadSession.ReceivedMessages) {
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
                UNIT_ASSERT_EQUAL(2, info.PartitionId);
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

        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
        writeSession3->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(PartitionMerge_PreferedPartition) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1", 0);
        auto writeSession2 = CreateWriteSession(client, "producer-2", 1);

        TTestReadSession ReadSession(client, 3);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));

        ui64 txId = 1012;
        MergePartition(setup, ++txId, 0, 1);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.2", 5))); // Will be fail because partition is not writable after merge
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.2", 7))); // Will be fail because partition is not writable after merge

        auto writeSession3 = CreateWriteSession(client, "producer-2", 2);

        UNIT_ASSERT(writeSession3->Write(Msg("message_3.1", 2)));  // Will be ignored because duplicated SeqNo
        UNIT_ASSERT(writeSession3->Write(Msg("message_3.2", 11)));

        ReadSession.WaitAllMessages();

        for(const auto& info : ReadSession.ReceivedMessages) {
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

        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
        writeSession3->Close(TDuration::Seconds(1));
    }

}

} // namespace NKikimr
