#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;

NKikimrSchemeOp::TModifyScheme CreateTransaction(const TString& parentPath, ::NKikimrSchemeOp::TPersQueueGroupDescription& scheme) {
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

TWriteMessage Msg(const TString& data, ui64 seqNo) {
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

std::shared_ptr<ISimpleBlockingWriteSession> CreateWriteSession(TTopicClient& client, const TString& producer, std::optional<ui32> partition) {
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

TTestReadSession::TTestReadSession(TTopicClient& client, size_t expectedMessagesCount, bool autoCommit)
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

    readSettings.EventHandlers_.StopPartitionSessionHandler(
        [&]
        (TReadSessionEvent::TStopPartitionSessionEvent& ev) mutable {
            Cerr << ">>>>> Received TStopPartitionSessionEvent message " << ev.DebugString() << Endl;
            Partitions.erase(ev.GetPartitionSession()->GetPartitionId());
            ev.Confirm();
    });

    Session = client.CreateReadSession(readSettings);
}

void TTestReadSession::WaitAllMessages() {
    Promise.GetFuture().GetValueSync();
}

void TTestReadSession::Commit() {
    for (auto& m : ReceivedMessages) {
        if (!m.Commited) {
            m.Msg.Commit();
            m.Commited = true;
        }
    }
}

}
