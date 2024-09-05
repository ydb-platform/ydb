#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

namespace NKikimr::NPQ::NTest {

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
    DoRequest(setup.GetRuntime(), txId, scheme);
}

void DoRequest(NActors::TTestActorRuntime& runtime, ui64& txId, NKikimrSchemeOp::TPersQueueGroupDescription& scheme) {
    Sleep(TDuration::Seconds(1));

    Cerr << "ALTER_SCHEME: " << scheme << Endl << Flush;

    const auto sender = runtime.AllocateEdgeActor();
    const auto request = CreateRequest(txId, CreateTransaction("/Root", scheme));
    runtime.Send(new IEventHandle(
            MakeTabletResolverID(),
            sender,
            new TEvTabletResolver::TEvForward(
                    SS,
                    new IEventHandle(TActorId(), sender, request),
                    { },
                    TEvTabletResolver::TEvForward::EActor::Tablet
            )),
            0);

    auto subscriber = CreateNotificationSubscriber(runtime, SS);
    runtime.Send(new IEventHandle(subscriber, sender, new TEvSchemeShard::TEvNotifyTxCompletion(txId)));
    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
    UNIT_ASSERT(event);
    UNIT_ASSERT_EQUAL(event->Record.GetTxId(), txId);

    auto e = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
    UNIT_ASSERT_EQUAL_C(e->Record.GetStatus(), TEvSchemeShard::EStatus::StatusAccepted,
        "Unexpected status " << NKikimrScheme::EStatus_Name(e->Record.GetStatus()) << " " << e->Record.GetReason());

    Sleep(TDuration::Seconds(1));
}

void SplitPartition(TTopicSdkTestSetup& setup, ui64& txId, const ui32 partition, TString boundary) {
    SplitPartition(setup.GetRuntime(), txId, partition, boundary);
}

void SplitPartition(NActors::TTestActorRuntime& runtime, ui64& txId, const ui32 partition, TString boundary) {
    ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
    scheme.SetName(TEST_TOPIC);
    auto* split = scheme.AddSplit();
    split->SetPartition(partition);
    split->SetSplitBoundary(boundary);

    DoRequest(runtime, txId, scheme);
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
    //ff.SetEnableTopicServiceTx(true);

    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetFeatureFlags(ff);

    auto setup = TTopicSdkTestSetup("TopicSplitMerge", settings, false);

    setup.GetRuntime().SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    setup.GetRuntime().SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_TRACE);
    setup.GetRuntime().SetLogPriority(NKikimrServices::PQ_PARTITION_CHOOSER, NActors::NLog::PRI_TRACE);
    setup.GetRuntime().SetLogPriority(NKikimrServices::PQ_READ_PROXY, NActors::NLog::PRI_TRACE);

    setup.GetRuntime().GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    setup.GetRuntime().GetAppData().PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
    setup.GetRuntime().GetAppData().PQConfig.SetBalancerWakeupIntervalSec(1);

    return setup;
}

std::shared_ptr<ISimpleBlockingWriteSession> CreateWriteSession(TTopicClient& client, const TString& producer, std::optional<ui32> partition, TString topic, bool useCodec) {
    auto writeSettings = TWriteSessionSettings()
                    .Path(topic)
                    .ProducerId(producer);
    if (!useCodec) {
        writeSettings.Codec(ECodec::RAW);
    }
    if (partition) {
        writeSettings.PartitionId(*partition);
    } else {
        writeSettings.MessageGroupId(producer);
    }

    return client.CreateSimpleBlockingWriteSession(writeSettings);
}

template<SdkVersion Sdk>
struct TTestReadSession : public ITestReadSession {

    using TSdkReadSession = typename std::conditional_t<Sdk == SdkVersion::Topic,
        NYdb::NTopic::IReadSession,
        NYdb::NPersQueue::IReadSession>;

    static constexpr size_t SemCount = 1;

    TTestReadSession(TestReadSessionSettings settings);
    ~TTestReadSession() = default;

    void WaitAllMessages() override;

    void Assert(const std::set<size_t>& expected, NThreading::TFuture<std::unordered_map<TString, std::set<size_t>>> f, const TString& message) override;
    void Assert(const std::unordered_map<TString, std::set<size_t>>& expected, NThreading::TFuture<std::unordered_map<TString, std::set<size_t>>> f, const TString& message) override;
    void WaitAndAssertPartitions(std::set<size_t> partitions, const TString& message) override;
    void WaitAndAssert(std::unordered_map<TString, std::set<size_t>> partitions, const TString& message) override;

    void Run() override;
    void Commit() override;
    void SetAutoCommit(bool value) override { Impl->AutoCommit = value; }

    void Close() override;

    std::set<size_t> GetPartitions() override;
    std::unordered_map<TString, std::set<size_t>> GetPartitionsA() override;
    std::vector<MsgInfo> GetReceivedMessages() override;
    std::vector<EvEndMsg> GetEndedPartitionEvents() override;

    void SetOffset(ui32 partitionId, std::optional<ui64> offset) override;

    struct TImpl {

        TImpl(const TString& name, bool autoCommit)
            : Name(name)
            , AutoCommit(autoCommit)
            , Semaphore((TStringBuilder() << name << "::" << RandomNumber<size_t>()).c_str(), SemCount) {}

        TString Name;
        std::unordered_map<ui32, ui64> Offsets;

        bool AutoCommit;

        NThreading::TPromise<std::vector<MsgInfo>> DataPromise = NThreading::NewPromise<std::vector<MsgInfo>>();
        NThreading::TPromise<std::unordered_map<TString, std::set<size_t>>> PartitionsPromise = NThreading::NewPromise<std::unordered_map<TString, std::set<size_t>>>();

        std::vector<MsgInfo> ReceivedMessages;
        std::unordered_map<TString, std::set<size_t>> Partitions;
        std::optional<std::unordered_map<TString, std::set<size_t>>> ExpectedPartitions;

        std::set<size_t> EndedPartitions;
        std::vector<TReadSessionEvent::TEndPartitionSessionEvent> EndedPartitionEvents;

        TMutex Lock;
        TSemaphore Semaphore;

        std::optional<ui64> GetOffset(ui32 partitionId) const;
        void Modify(std::function<void (std::unordered_map<TString, std::set<size_t>>&)> modifier);

        void Acquire();
        void Release();

        NThreading::TFuture<std::unordered_map<TString, std::set<size_t>>> Wait(std::unordered_map<TString, std::set<size_t>> partitions, const TString& message);
    };

    std::shared_ptr<TSdkReadSession> Create(const TestReadSessionSettings& settings);

    std::shared_ptr<TSdkReadSession> Session;
    std::shared_ptr<TImpl> Impl;
};


template<SdkVersion Sdk>
TTestReadSession<Sdk>::TTestReadSession(TestReadSessionSettings settings) {
    Impl = std::make_shared<TImpl>(settings.Name, settings.AutoCommit);
    Impl->Acquire();

    Session = Create(settings);
}


template<>
std::shared_ptr<TTestReadSession<SdkVersion::Topic>::TSdkReadSession> TTestReadSession<SdkVersion::Topic>::Create(const TestReadSessionSettings& settings) {
    auto readSettings = TReadSessionSettings()
        .ConsumerName(TEST_CONSUMER)
        .AutoPartitioningSupport(settings.AutoPartitioningSupport);
    for (auto& topic : settings.Topics) {
        readSettings.AppendTopics(topic);
    }
    for (auto partitionId : settings.Partitions) {
        readSettings.Topics_[0].AppendPartitionIds(partitionId);
    }

    struct MsgWrapper : public IMessage {

        MsgWrapper(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage msg)
            : Msg(msg) {
        }
        ~MsgWrapper() = default;

        NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage Msg;

        void Commit() override {
            Msg.Commit();
        };
    };

    readSettings.EventHandlers_.SimpleDataHandlers(
        [impl=Impl, expectedMessagesCount=settings.ExpectedMessagesCount]
        (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
        auto& messages = ev.GetMessages();
        for (size_t i = 0u; i < messages.size(); ++i) {
            auto& message = messages[i];

            Cerr << ">>>>> " << impl->Name << " Received TDataReceivedEvent message partitionId=" << message.GetPartitionSession()->GetPartitionId()
                    << ", message=" << message.GetData()
                    << ", seqNo=" << message.GetSeqNo()
                    << ", offset=" << message.GetOffset()
                    << Endl << Flush;

            auto msg = MsgInfo(message.GetPartitionSession()->GetPartitionId(),
                                        message.GetSeqNo(),
                                        message.GetOffset(),
                                        message.GetData(),
                                        impl->AutoCommit)
                                    .WithMsg(new MsgWrapper(message));

            impl->ReceivedMessages.push_back(msg);

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
                auto topic = ev.GetPartitionSession()->GetTopicPath();
                auto offset = impl->GetOffset(partitionId);
                impl->Modify([&](std::unordered_map<TString, std::set<size_t>>& s) { s[topic].insert(partitionId); });
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
                auto topic = ev.GetPartitionSession()->GetTopicPath();
                impl->Modify([&](std::unordered_map<TString, std::set<size_t>>& s) { s[topic].erase(partitionId); });
                Cerr << ">>>>> " << impl->Name << " Stop reading partition " << partitionId << Endl << Flush;
                ev.Confirm();
    });

    readSettings.EventHandlers_.PartitionSessionClosedHandler(
            [impl=Impl]
            (TReadSessionEvent::TPartitionSessionClosedEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TPartitionSessionClosedEvent message " << ev.DebugString() << Endl << Flush;
                auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                auto topic = ev.GetPartitionSession()->GetTopicPath();
                impl->Modify([&](std::unordered_map<TString, std::set<size_t>>& s) { s[topic].erase(partitionId); });
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


    return settings.Setup.MakeClient().CreateReadSession(readSettings);
}

template<>
std::shared_ptr<TTestReadSession<SdkVersion::PQv1>::TSdkReadSession> TTestReadSession<SdkVersion::PQv1>::Create(const TestReadSessionSettings& settings) {

    NYdb::NPersQueue::TReadSessionSettings readSettings;
    readSettings
        .ConsumerName(TEST_CONSUMER);
    for (auto& topic : settings.Topics) {
        readSettings.AppendTopics(topic);
    }
    for (auto partitionId : settings.Partitions) {
        readSettings.Topics_[0].PartitionGroupIds_.push_back(partitionId + 1);
    }

    struct MsgWrapper : public IMessage {

        MsgWrapper(NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage msg)
            : Msg(msg) {
        }
        ~MsgWrapper() = default;

        NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage Msg;

        void Commit() override {
            Msg.Commit();
        };
    };

    readSettings.EventHandlers_.SimpleDataHandlers(
        [impl=Impl, expectedMessagesCount=settings.ExpectedMessagesCount]
        (NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& ev) mutable {
        auto& messages = ev.GetMessages();
        for (size_t i = 0u; i < messages.size(); ++i) {
            auto& message = messages[i];

            Cerr << ">>>>> " << impl->Name << " Received TDataReceivedEvent message partitionId=" << message.GetPartitionStream()->GetPartitionId()
                    << ", message=" << message.GetData()
                    << ", seqNo=" << message.GetSeqNo()
                    << ", offset=" << message.GetOffset()
                    << Endl << Flush;

            auto msg = MsgInfo(message.GetPartitionStream()->GetPartitionId(),
                                        message.GetSeqNo(),
                                        message.GetOffset(),
                                        message.GetData(),
                                        impl->AutoCommit)
                                    .WithMsg(new MsgWrapper(message));

            impl->ReceivedMessages.push_back(msg);

            if (impl->AutoCommit) {
                message.Commit();
            }
        }

        if (impl->ReceivedMessages.size() == expectedMessagesCount) {
            impl->DataPromise.SetValue(impl->ReceivedMessages);
        }
    });

    readSettings.EventHandlers_.CreatePartitionStreamHandler(
            [impl=Impl]
            (NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TCreatePartitionStreamEvent message " << ev.DebugString() << Endl << Flush;
                auto partitionId = ev.GetPartitionStream()->GetPartitionId();
                auto topic = ev.GetPartitionStream()->GetTopicPath();
                auto offset = impl->GetOffset(partitionId);
                impl->Modify([&](std::unordered_map<TString, std::set<size_t>>& s) { s[topic].insert(partitionId); });
                if (offset) {
                    Cerr << ">>>>> " << impl->Name << " Start reading partition " << partitionId << " from offset " << offset.value() << Endl << Flush;
                    ev.Confirm(offset.value(), TMaybe<ui64>());
                } else {
                    Cerr << ">>>>> " << impl->Name << " Start reading partition " << partitionId << " without offset" << Endl << Flush;
                    ev.Confirm();
                }
    });

    readSettings.EventHandlers_.DestroyPartitionStreamHandler(
            [impl=Impl]
            (NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TDestroyPartitionStreamEvent message " << ev.DebugString() << Endl << Flush;
                auto partitionId = ev.GetPartitionStream()->GetPartitionId();
                auto topic = ev.GetPartitionStream()->GetTopicPath();
                impl->Modify([&](std::unordered_map<TString, std::set<size_t>>& s) { s[topic].erase(partitionId); });
                Cerr << ">>>>> " << impl->Name << " Stop reading partition " << partitionId << Endl << Flush;
                ev.Confirm();
    });

    readSettings.EventHandlers_.PartitionStreamClosedHandler(
            [impl=Impl]
            (NYdb::NPersQueue::TReadSessionEvent::TReadSessionEvent::TPartitionStreamClosedEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TPartitionSessionClosedEvent message " << ev.DebugString() << Endl << Flush;
                auto partitionId = ev.GetPartitionStream()->GetPartitionId();
                auto topic = ev.GetPartitionStream()->GetTopicPath();
                impl->Modify([&](std::unordered_map<TString, std::set<size_t>>& s) { s[topic].erase(partitionId); });
                Cerr << ">>>>> " << impl->Name << " Stop (closed) reading partition " << partitionId << Endl << Flush;
    });

    readSettings.EventHandlers_.SessionClosedHandler(
                    [impl=Impl]
            (const TSessionClosedEvent& ev) mutable {
                Cerr << ">>>>> " << impl->Name << " Received TSessionClosedEvent message " << ev.DebugString() << Endl << Flush;
    });


    auto client = NYdb::NPersQueue::TPersQueueClient(*(settings.Setup.GetServer().AnnoyingClient->GetDriver()));
    return client.CreateReadSession(readSettings);
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::WaitAllMessages() {
    Cerr << ">>>>> " << Impl->Name << " WaitAllMessages " << Endl << Flush;
    Impl->DataPromise.GetFuture().GetValue(TDuration::Seconds(5));
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::Commit() {
    Cerr << ">>>>> " << Impl->Name << " Commit all received messages" << Endl << Flush;
    for (auto& m : Impl->ReceivedMessages) {
        if (!m.Commited) {
            m.Msg->Commit();
            m.Commited = true;
        }
    }
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::TImpl::Acquire() {
    Cerr << ">>>>> " << Name << " Acquire()" << Endl << Flush;
    Semaphore.Acquire();
    Cerr << ">>>>> " << Name << " Acquired" << Endl << Flush;
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::TImpl::Release() {
    Cerr << ">>>>> " << Name << " Release()" << Endl << Flush;
    Semaphore.Release();
}

template<SdkVersion Sdk>
NThreading::TFuture<std::unordered_map<TString, std::set<size_t>>> TTestReadSession<Sdk>::TImpl::Wait(std::unordered_map<TString, std::set<size_t>> partitions, const TString& message) {
    Cerr << ">>>>> " << Name << " Wait partitions " << partitions << " " << message << Endl << Flush;

    with_lock (Lock) {
        ExpectedPartitions = partitions;
        PartitionsPromise = NThreading::NewPromise<std::unordered_map<TString, std::set<size_t>>>();

        if (Partitions == ExpectedPartitions.value()) {
            PartitionsPromise.SetValue(ExpectedPartitions.value());
        }
    }

    return PartitionsPromise.GetFuture();
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::Assert(const std::set<size_t>& expected, NThreading::TFuture<std::unordered_map<TString, std::set<size_t>>> f, const TString& message) {
    Assert(std::unordered_map<TString, std::set<size_t>>{{TEST_TOPIC, expected}}, f, message);
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::Assert(const std::unordered_map<TString, std::set<size_t>>& expected, NThreading::TFuture<std::unordered_map<TString, std::set<size_t>>> f, const TString& message) {
    auto actual = f.HasValue() ? f.GetValueSync() : GetPartitionsA();
    Cerr << ">>>>> " << Impl->Name << " Partitions " << actual << " received #2" << Endl << Flush;
    UNIT_ASSERT_VALUES_EQUAL_C(expected, actual, message);
    Impl->Release();
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::WaitAndAssertPartitions(std::set<size_t> partitions, const TString& message) {
    WaitAndAssert(std::unordered_map<TString, std::set<size_t>>{{TEST_TOPIC, partitions}}, message);
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::WaitAndAssert(std::unordered_map<TString, std::set<size_t>> partitions, const TString& message) {
    auto f = Impl->Wait(partitions, message);
    f.Wait(TDuration::Seconds(60));
    Assert(partitions, f, message);
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::Run() {
    Impl->ExpectedPartitions = std::nullopt;
    Impl->Semaphore.TryAcquire();
    Impl->Release();
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::Close() {
    Run();
    Cerr << ">>>>> " << Impl->Name << " Closing reading session " << Endl << Flush;
    Session->Close();
    Session.reset();
}

template<SdkVersion Sdk>
std::set<size_t> TTestReadSession<Sdk>::GetPartitions() {
    return GetPartitionsA()[TEST_TOPIC];
}

template<SdkVersion Sdk>
std::unordered_map<TString, std::set<size_t>> TTestReadSession<Sdk>::GetPartitionsA() {
    with_lock (Impl->Lock) {
        return Impl->Partitions;
    }
}

template<SdkVersion Sdk>
std::vector<EvEndMsg> TTestReadSession<Sdk>::GetEndedPartitionEvents() {
    with_lock(Impl->Lock) {
        std::vector<EvEndMsg> result;
        for (auto& e : Impl->EndedPartitionEvents) {
            result.push_back({e.GetAdjacentPartitionIds(), e.GetChildPartitionIds()});
        }
        return result;
    }
}

template<SdkVersion Sdk>
std::vector<MsgInfo> TTestReadSession<Sdk>::GetReceivedMessages() {
    with_lock(Impl->Lock) {
        return Impl->ReceivedMessages;
    }
};

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::TImpl::Modify(std::function<void (std::unordered_map<TString, std::set<size_t>>&)> modifier) {
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

template<SdkVersion Sdk>
std::optional<ui64> TTestReadSession<Sdk>::TImpl::GetOffset(ui32 partitionId) const {
    with_lock (Lock) {
        auto it = Offsets.find(partitionId);
        if (it == Offsets.end()) {
            return std::nullopt;
        }
        return it->second;
    }
}

template<SdkVersion Sdk>
void TTestReadSession<Sdk>::SetOffset(ui32 partitionId, std::optional<ui64> offset) {
    with_lock (Impl->Lock) {
        if (offset) {
            Impl->Offsets[partitionId] = offset.value();
        } else {
            Impl->Offsets.erase(partitionId);
        }
    }
}

template struct TTestReadSession<SdkVersion::Topic>;
template struct TTestReadSession<SdkVersion::PQv1>;

std::shared_ptr<ITestReadSession> CreateTestReadSession(TestReadSessionSettings settings) {
    if (settings.Sdk == SdkVersion::Topic) {
        return std::make_shared<TTestReadSession<SdkVersion::Topic>>(settings);
    } else {
        return std::make_shared<TTestReadSession<SdkVersion::PQv1>>(settings);
    }
}

}
