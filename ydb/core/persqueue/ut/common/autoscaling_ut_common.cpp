#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>


static inline IOutputStream& operator<<(IOutputStream& o, const std::set<size_t> t) {
    o << "[" << JoinRange(", ", t.begin(), t.end()) << "]";

    return o;
}
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

    Cerr << "ALTER_SCHEME: " << scheme << Endl << Flush;

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

std::shared_ptr<ISimpleBlockingWriteSession> CreateWriteSession(TTopicClient& client, const TString& producer, std::optional<ui32> partition, TString topic) {
    auto writeSettings = TWriteSessionSettings()
                    .Path(topic)
                    .ProducerId(producer);
    if (partition) {
        writeSettings.PartitionId(*partition);
    } else {
        writeSettings.MessageGroupId(producer);
    }

    return client.CreateSimpleBlockingWriteSession(writeSettings);
}


TTestReadSession::TTestReadSession(const TString& name, TTopicClient& client, size_t expectedMessagesCount, bool autoCommit, std::set<ui32> partitions, bool autoscalingSupport) {
    Impl = std::make_shared<TImpl>(name, autoCommit);

    Impl->Acquire();

    auto readSettings = TReadSessionSettings()
        .ConsumerName(TEST_CONSUMER)
        .AppendTopics(TEST_TOPIC)
        .AutoscalingSupport(autoscalingSupport);
    for (auto partitionId : partitions) {
        readSettings.Topics_[0].AppendPartitionIds(partitionId);
    }

    readSettings.EventHandlers_.SimpleDataHandlers(
        [impl=Impl, expectedMessagesCount]
        (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
        auto& messages = ev.GetMessages();
        for (size_t i = 0u; i < messages.size(); ++i) {
            auto& message = messages[i];

            Cerr << ">>>>> " << impl->Name << " Received TDataReceivedEvent message partitionId=" << message.GetPartitionSession()->GetPartitionId()
                    << ", message=" << message.GetData()
                    << ", seqNo=" << message.GetSeqNo()
                    << ", offset=" << message.GetOffset()
                    << Endl << Flush;
            impl->ReceivedMessages.push_back({message.GetPartitionSession()->GetPartitionId(),
                                        message.GetSeqNo(),
                                        message.GetOffset(),
                                        message.GetData(),
                                        message,
                                        impl->AutoCommit});

            if (impl->AutoCommit) {
                message.Commit();
            }
        }

        if (impl->ReceivedMessages.size() == expectedMessagesCount) {
            impl->DataPromise.SetValue(impl->ReceivedMessages);
        }
    });

    readSettings.EventHandlers_.StartPartitionSessionHandler(
            [impl=Impl]
            (TReadSessionEvent::TStartPartitionSessionEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TStartPartitionSessionEvent message " << ev.DebugString() << Endl << Flush;
                auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                auto offset = impl->GetOffset(partitionId);
                impl->Modify([&](std::set<size_t>& s) { s.insert(partitionId); });
                if (offset) {
                    Cerr << ">>>>> " << impl->Name << " Start reading partition " << partitionId << " from offset " << offset.value() << Endl << Flush;
                    ev.Confirm(offset.value(), TMaybe<ui64>());
                } else {
                    Cerr << ">>>>> " << impl->Name << " Start reading partition " << partitionId << " without offset" << Endl << Flush;
                    ev.Confirm();
                }
    });

    readSettings.EventHandlers_.StopPartitionSessionHandler(
            [impl=Impl]
            (TReadSessionEvent::TStopPartitionSessionEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TStopPartitionSessionEvent message " << ev.DebugString() << Endl << Flush;
                auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                impl->Modify([&](std::set<size_t>& s) { s.erase(partitionId); });
                Cerr << ">>>>> " << impl->Name << " Stop reading partition " << partitionId << Endl << Flush;
                ev.Confirm();
    });

    readSettings.EventHandlers_.PartitionSessionClosedHandler(
            [impl=Impl]
            (TReadSessionEvent::TPartitionSessionClosedEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TPartitionSessionClosedEvent message " << ev.DebugString() << Endl << Flush;
                auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                impl->Modify([&](std::set<size_t>& s) { s.erase(partitionId); });
                Cerr << ">>>>> " << impl->Name << " Stop (closed) reading partition " << partitionId << Endl << Flush;
    });

    readSettings.EventHandlers_.SessionClosedHandler(
                    [impl=Impl]
            (const TSessionClosedEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TSessionClosedEvent message " << ev.DebugString() << Endl << Flush;
    });

    readSettings.EventHandlers_.EndPartitionSessionHandler(
            [impl=Impl]
            (TReadSessionEvent::TEndPartitionSessionEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TEndPartitionSessionEvent message " << ev.DebugString() << Endl << Flush;
                auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                impl->EndedPartitions.insert(partitionId);
                impl->EndedPartitionEvents.push_back(ev);

                ev.Confirm();
    });


    Session = client.CreateReadSession(readSettings);
}

void TTestReadSession::WaitAllMessages() {
    Impl->DataPromise.GetFuture().GetValue(TDuration::Seconds(5));
}

void TTestReadSession::Commit() {
    Cerr << ">>>>> " << Impl->Name << " Commit all received messages" << Endl << Flush;
    for (auto& m : Impl->ReceivedMessages) {
        if (!m.Commited) {
            m.Msg.Commit();
            m.Commited = true;
        }
    }
}

void TTestReadSession::TImpl::Acquire() {
    Cerr << ">>>>> " << Name << " Acquire()" << Endl << Flush;
    Semaphore.Acquire();
}

void TTestReadSession::TImpl::Release() {
    Cerr << ">>>>> " << Name << " Release()" << Endl << Flush;
    Semaphore.Release();
}

NThreading::TFuture<std::set<size_t>> TTestReadSession::TImpl::Wait(std::set<size_t> partitions, const TString& message) {
    Cerr << ">>>>> " << Name << " Wait partitions " << partitions << " " << message << Endl << Flush;

    with_lock (Lock) {
        ExpectedPartitions = partitions;
        PartitionsPromise = NThreading::NewPromise<std::set<size_t>>();

        if (Partitions == ExpectedPartitions.value()) {
            PartitionsPromise.SetValue(ExpectedPartitions.value());
        }
    }

    return PartitionsPromise.GetFuture();
}

void TTestReadSession::Assert(const std::set<size_t>& expected, NThreading::TFuture<std::set<size_t>> f, const TString& message) {
    auto actual = f.HasValue() ? f.GetValueSync() : GetPartitions();
    Cerr << ">>>>> " << Impl->Name << " Partitions " << actual << " received #2" << Endl << Flush;
    UNIT_ASSERT_VALUES_EQUAL_C(expected, actual, message);
    Impl->Release();
}

void TTestReadSession::WaitAndAssertPartitions(std::set<size_t> partitions, const TString& message) {
    auto f = Impl->Wait(partitions, message);
    f.Wait(TDuration::Seconds(60));
    Assert(partitions, f, message);
}

void TTestReadSession::Run() {
    Impl->ExpectedPartitions = std::nullopt;
    Impl->Semaphore.TryAcquire();
    Impl->Release();
}

void TTestReadSession::Close() {
    Run();
    Cerr << ">>>>> " << Impl->Name << " Closing reading session " << Endl << Flush;
    Session->Close();
    Session.reset();
}

std::set<size_t> TTestReadSession::GetPartitions() {
    with_lock (Impl->Lock) {
        return Impl->Partitions;
    }
}

void TTestReadSession::TImpl::Modify(std::function<void (std::set<size_t>&)> modifier) {
    bool found = false;

    with_lock (Lock) {
        modifier(Partitions);

        if (ExpectedPartitions && Partitions == ExpectedPartitions.value()) {
            ExpectedPartitions = std::nullopt;
            PartitionsPromise.SetValue(Partitions);
            found = true;
        }
    }

    if (found) {
        Acquire();
    }
}

std::optional<ui64> TTestReadSession::TImpl::GetOffset(ui32 partitionId) const {
    with_lock (Lock) {
        auto it = Offsets.find(partitionId);
        if (it == Offsets.end()) {
            return std::nullopt;
        }
        return it->second;
    }
}

void TTestReadSession::SetOffset(ui32 partitionId, std::optional<ui64> offset) {
    with_lock (Impl->Lock) {
        if (offset) {
            Impl->Offsets[partitionId] = offset.value();
        } else {
            Impl->Offsets.erase(partitionId);
        }
    }
}

}
