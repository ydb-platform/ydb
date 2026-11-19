#include "private_events.h"
#include "dst_alterer.h"
#include "dst_remover.h"
#include "stream_remover.h"
#include "topic_mirror.h"

#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NKikimr::NReplication::NController {
namespace {
using namespace NTestHelpers;

class TMirrorReaderFactory : public NPQ::TPersQueueMirrorReaderFactory {
    class TReadSession final: public NYdb::NTopic::IReadSession {
        using TEvent = NYdb::NTopic::TReadSessionEvent::TEvent;
        using TSettings = NYdb::NTopic::TReadSessionGetEventSettings;

    public:
        TReadSession(const TMirrorReaderFactory& factory, std::shared_ptr<NYdb::NTopic::IReadSession> session)
            : Factory(factory)
            , Session(std::move(session))
        {}

        NThreading::TFuture<void> WaitEvent() override {
            TGuard<TMutex> guard(Factory.Mutex);
            if (Factory.Resumed.Initialized()) {
                return Factory.Resumed.GetFuture();
            }
            return Session->WaitEvent();
        }

        std::optional<TEvent> GetEvent(bool block, size_t maxByteSize) override {
            TGuard<TMutex> guard(Factory.Mutex);
            if (Factory.Resumed.Initialized()) {
                return std::nullopt;
            }
            return Session->GetEvent(block, maxByteSize);
        }

        std::optional<TEvent> GetEvent(const TSettings& settings) override {
            return Session->GetEvent(settings);
        }

        std::vector<TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) override {
            return Session->GetEvents(block, maxEventsCount, maxByteSize);
        }

        std::vector<TEvent> GetEvents(const TSettings& settings) override {
            return Session->GetEvents(settings);
        }

        bool Close(TDuration timeout) override {
            return Session->Close(timeout);
        }

        NYdb::NTopic::TReaderCounters::TPtr GetCounters() const override {
            return Session->GetCounters();
        }

        std::string GetSessionId() const override {
            return Session->GetSessionId();
        }

    private:
        const TMirrorReaderFactory& Factory;
        std::shared_ptr<NYdb::NTopic::IReadSession> Session;
    };

public:
    std::shared_ptr<NYdb::NTopic::IReadSession> GetReadSession(
            const NKikimrPQ::TMirrorPartitionConfig& config, ui32 partition,
            std::shared_ptr<NYdb::ICredentialsProviderFactory> credentials,
            ui64 maxMemoryUsageBytes, TMaybe<TLog> logger = Nothing()) const override
    {
        return std::make_shared<TReadSession>(*this, TPersQueueMirrorReaderFactory::GetReadSession(
            config, partition, std::move(credentials), maxMemoryUsageBytes, std::move(logger)));
    }

    void Pause() {
        TGuard<TMutex> guard(Mutex);
        UNIT_ASSERT(!Resumed.Initialized());
        Resumed = NThreading::NewPromise<void>();
    }

    void Resume() {
        NThreading::TPromise<void> resumed;
        {
            TGuard<TMutex> guard(Mutex);
            resumed = std::exchange(Resumed, {});
        }
        if (resumed.Initialized()) {
            resumed.SetValue();
        }
    }

private:
    mutable TMutex Mutex;
    NThreading::TPromise<void> Resumed;
};

class TTopicMirroringFixture {
    static constexpr const char* Consumer = "application";
    static constexpr const char* SourceTopic = "/Root/src/topic";
    static constexpr const char* DestinationTopic = "/Root/topic";

    static TFeatureFlags Flags() {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableTopicReplication(true);
        flags.FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
        return flags;
    }

    struct TSourceEnv: TEnv<> {
        TSourceEnv(): TEnv<>(Flags()) {
            UNIT_ASSERT_VALUES_EQUAL(MkDir("/Root", "src"), NMsgBusProxy::MSTATUS_OK);
            auto topic = MakeTopicDescription({.Name = "topic"});
            topic->MutablePQTabletConfig()->AddConsumers()->SetName(Consumer);
            UNIT_ASSERT_VALUES_EQUAL(CreateTopic("/Root/src", *topic), NMsgBusProxy::MSTATUS_OK);
        }
    };

    struct TOffsets {
        ui64 End;
        ui64 Committed;
    };

    static TOffsets GetOffsets(TEnv<>& env, const TString& topic, const TString& consumer) {
        NYdb::NTopic::TTopicClient client(env.GetDriver());
        const auto result = env.RunCall([&] {
            return client.DescribeConsumer(topic, consumer, NYdb::NTopic::TDescribeConsumerSettings()
                .IncludeStats(true)).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& partitions = result.GetConsumerDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        const auto& partition = partitions.front();
        UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);
        UNIT_ASSERT(partition.GetPartitionStats());
        UNIT_ASSERT(partition.GetPartitionConsumerStats());
        return {partition.GetPartitionStats()->GetEndOffset(), partition.GetPartitionConsumerStats()->GetCommittedOffset()};
    }

    TString MirrorConsumer;

public:
    TSourceEnv Source;
    TEnv<> Destination;
    const ui64 SchemeShard = Destination.GetPathId("/Root").OwnerId;

    explicit TTopicMirroringFixture(const TString& sourceHost = "localhost", bool stopBeforeCreateResult = false,
            TMirrorReaderFactory* mirrorReaderFactory = nullptr)
        : Destination(Flags(), true, stopBeforeCreateResult ? ERuntimeMode::Simulated : ERuntimeMode::RealThreads)
    {
        if (mirrorReaderFactory) {
            auto& runtime = Destination.GetRuntime();
            auto& appData = runtime.GetAppData();
            mirrorReaderFactory->Initialize(runtime.GetAnyNodeActorSystem(), appData.PQConfig.GetMirrorConfig().GetPQLibSettings());
            appData.PersQueueMirrorReaderFactory = mirrorReaderFactory;
        }

        std::atomic<bool> createResultBlocked = false;
        auto observer = Destination.GetRuntime().AddObserver<TEvPrivate::TEvCreateDstResult>(
            [&](TEvPrivate::TEvCreateDstResult::TPtr& ev) {
                if (stopBeforeCreateResult && ev->Get()->IsSuccess()) {
                    ev.Reset();
                    createResultBlocked = true;
                }
            });
        const auto endpoint = sourceHost + Source.GetEndpoint().substr(Source.GetEndpoint().rfind(':'));
        Exec(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/src/topic` AS `/Root/topic`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", endpoint.c_str()));
        if (stopBeforeCreateResult) {
            Wait([&] { return createResultBlocked.load(); });
            return;
        }
        WaitMirror(true);
        const auto config = Config();
        const auto& mirror = config.GetPartitionConfig().GetMirrorFrom();
        UNIT_ASSERT(mirror.GetMirrorConsumerOffsets());
        MirrorConsumer = mirror.GetConsumer();
        UNIT_ASSERT(!MirrorConsumer.empty());
    }

    void Exec(const TString& query) {
        NYdb::NTable::TTableClient client(Destination.GetDriver());
        auto session = Destination.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });
        const auto result = Destination.RunCall([&] { return session.ExecuteSchemeQuery(query).GetValueSync(); });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    NKikimrPQ::TPQTabletConfig Config() {
        const auto result = Destination.Send<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(SchemeShard,
            new NSchemeShard::TEvSchemeShard::TEvDescribeScheme("/Root/topic"));
        return result->Get()->GetRecord().GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
    }

    template <typename TCondition>
    void Wait(const TCondition& ready, const TString& description = "topic mirroring") {
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        while (!ready()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Timed out waiting for " << description);
            if (Destination.GetRuntime().IsRealThreads()) {
                Sleep(TDuration::MilliSeconds(10));
            } else {
                Destination.GetRuntime().SimulateSleep(TDuration::MilliSeconds(10));
            }
        }
    }

    void WaitMirror(bool enabled) {
        Wait([&] { return Config().GetPartitionConfig().HasMirrorFrom() == enabled; });
    }

    void Done() {
        Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');");
        Wait([&] { return Destination.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetState().HasDone(); });
    }

    void CheckDropByMissingId() {
        namespace TEvSchemeShard = NSchemeShard::TEvSchemeShard;
        const TPathId missing(SchemeShard, 100500);
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(200600, SchemeShard);
        auto& tx = *request->Record.AddTransaction();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropPersQueueGroup);
        tx.MutableDrop()->SetId(missing.LocalPathId);
        const auto drop = Destination.Send<TEvSchemeShard::TEvModifySchemeTransactionResult>(SchemeShard, std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(drop->Get()->Record.GetStatus(), NKikimrScheme::StatusNameConflict);

        Destination.GetRuntime().Register(CreateDstRemover(Destination.GetSender(), SchemeShard, {}, 1, 1,
            TReplication::ETargetKind::Topic, missing, Destination.GetPathId("/Root/replication")));
        const auto cleanup = Destination.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDropDstResult>(Destination.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(cleanup->Get()->Status, NKikimrScheme::StatusSuccess);
    }

    void CheckMissingDestination() {
        Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        WaitMirror(false);
        const auto pathId = Destination.GetPathId("/Root/topic");
        NYdb::NTopic::TTopicClient client(Destination.GetDriver());
        const auto dropped = client.DropTopic("/Root/topic").GetValueSync();
        UNIT_ASSERT_C(dropped.IsSuccess(), dropped.GetIssues().ToString());
        for (const auto state : {TReplication::EState::Ready, TReplication::EState::Done}) {
            Destination.GetRuntime().Register(CreateDstTopicAlterer(Destination.GetSender(), SchemeShard,
                1, 1, pathId, state, {}));
            const auto result = Destination.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvAlterDstResult>(Destination.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, state == TReplication::EState::Ready
                ? NKikimrScheme::StatusPathDoesNotExist : NKikimrScheme::StatusSuccess);
        }

        UNIT_ASSERT_VALUES_EQUAL(Destination.CreateTopic("/Root", *MakeTopicDescription({.Name = "topic"})), NMsgBusProxy::MSTATUS_OK);
        const auto replacement = Destination.GetPathId("/Root/topic");
        Exec("DROP ASYNC REPLICATION `/Root/replication` CASCADE;");
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetPathId("/Root/topic"), replacement);
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetDescription("/Root/replication").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
    }

    void DropSourceTopic() {
        NYdb::NTopic::TTopicClient client(Source.GetDriver());
        const auto dropped = client.DropTopic("/Root/src/topic").GetValueSync();
        UNIT_ASSERT_C(dropped.IsSuccess(), dropped.GetIssues().ToString());
    }

    void RemoveSourceDirectory() {
        NYdb::NScheme::TSchemeClient scheme(Source.GetDriver());
        const auto removed = scheme.RemoveDirectory("/Root/src").GetValueSync();
        UNIT_ASSERT_C(removed.IsSuccess(), removed.GetIssues().ToString());
    }

    void CheckDropWithMissingSource() {
        Exec("DROP ASYNC REPLICATION `/Root/replication`;");
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetDescription("/Root/replication").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
        UNIT_ASSERT(!Config().GetPartitionConfig().HasMirrorFrom());
    }

    NYdb::TStatus CommitSourceConsumerOffset(ui64 offset) {
        NYdb::NTopic::TTopicClient client(Source.GetDriver());
        return client.CommitOffset(SourceTopic, 0, Consumer, offset).GetValueSync();
    }

    ui64 GetSourceConsumerOffset() {
        return GetOffsets(Source, SourceTopic, Consumer).Committed;
    }

    void CommitAndWaitForConsumerOffset(ui64 offset) {
        const auto commit = CommitSourceConsumerOffset(offset);
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());

        NYdb::NTopic::TTopicClient destination(Destination.GetDriver());
        Wait([&] {
            const auto result = destination.DescribeConsumer(DestinationTopic, Consumer,
                NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& partitions = result.GetConsumerDescription().GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
            const auto& stats = partitions.front().GetPartitionConsumerStats();
            return stats && stats->GetCommittedOffset() == offset;
        });
    }

    void ReplicateFirstMessage() {
        Write({"first"});
        WaitForOffsets(1, 0);
    }

    void CheckBacklog(ui64 before, ui64 end) {
        const auto source = GetOffsets(Source, SourceTopic, MirrorConsumer);
        UNIT_ASSERT_VALUES_EQUAL(source.End, end);
        UNIT_ASSERT_VALUES_EQUAL(source.Committed, before);
        UNIT_ASSERT_VALUES_EQUAL(GetOffsets(Destination, DestinationTopic, Consumer).End, before);
        UNIT_ASSERT_C(before < end, "Expected a backlog on partition 0");
    }

    void WaitForOffsets(ui64 end, ui64 appOffset) {
        TString description;
        Wait([&] {
            const auto destination = GetOffsets(Destination, DestinationTopic, Consumer);
            description = TStringBuilder() << "destination partition 0 to reach end offset " << end
                << " and application offset " << appOffset << " (observed " << destination.End
                << " and " << destination.Committed << ')';
            return destination.End == end && destination.Committed == appOffset;
        }, description);

        Wait([&] {
            const auto source = GetOffsets(Source, SourceTopic, MirrorConsumer);
            description = TStringBuilder() << "source mirror consumer " << MirrorConsumer
                << " to commit partition 0 through offset " << end << " (observed " << source.Committed << ')';
            return source.End == end && source.Committed == end;
        }, description);
    }

    void Write(const TVector<TString>& payloads) {
        using namespace NYdb::NTopic;
        TTopicClient client(Source.GetDriver());
        auto writer = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
            .Path(SourceTopic).ProducerId("producer").MessageGroupId("producer").Codec(ECodec::RAW));
        for (const auto& payload : payloads) {
            UNIT_ASSERT(writer->Write(payload));
        }
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
    }

    void ReadBack(const TVector<TString>& payloads, ui64 firstOffset) {
        using namespace NYdb::NTopic;
        TTopicClient client(Destination.GetDriver());
        auto reader = client.CreateReadSession(TReadSessionSettings().WithoutConsumer()
            .AppendTopics(TTopicReadSettings(DestinationTopic).AppendPartitionIds(0)));
        size_t index = 0;
        Wait([&] {
            auto event = reader->GetEvent();
            if (!event) {
                return false;
            }
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                start->Confirm(firstOffset);
            } else if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& message : data->GetMessages()) {
                    UNIT_ASSERT(index < payloads.size());
                    UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), firstOffset + index);
                    UNIT_ASSERT_VALUES_EQUAL(message.GetData(), payloads[index]);
                    ++index;
                }
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
            return index == payloads.size();
        });
        UNIT_ASSERT(Destination.RunCall([&] { return reader->Close(TDuration::Seconds(10)); }));
    }

    void WriteAndReadBack(const TString& payload, ui64 offset) {
        Write({payload});
        ReadBack({payload}, offset);
    }
};

class TConsumerRemovalFixture {
public:
    TConsumerRemovalFixture() {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Parent = Runtime.AllocateEdgeActor();
        Proxy = Runtime.AllocateEdgeActor();
        Remover = Runtime.Register(CreateStreamRemover(Parent, Proxy, 1, 1,
            TReplication::ETargetKind::Topic, "/Root/topic", "consumer"));
        Runtime.EnableScheduleForActor(Remover);
        Runtime.GrabEdgeEvent<TEvPrivate::TEvRequestDropStream>(Parent);
        Runtime.Send(Remover, Parent, new TEvPrivate::TEvAllowDropStream());
    }

    void DenyAndWaitForRetry() {
        Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(Proxy);
        const auto start = Runtime.GetCurrentTime();
        Reply(NYdb::EStatus::UNAUTHORIZED);
        Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(Proxy);
        UNIT_ASSERT(Runtime.GetCurrentTime() - start >= TDuration::Seconds(10));
    }

    void CheckAmbiguousAbsence() {
        for (const auto status : {NYdb::EStatus::SUCCESS, NYdb::EStatus::UNAUTHORIZED}) {
            Reply(NYdb::EStatus::SCHEME_ERROR);
            const auto request = Runtime.GrabEdgeEvent<TEvYdbProxy::TEvListDirectoryRequest>(Proxy);
            UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(request->Get()->GetArgs()), "/Root");

            NYdb::NScheme::TSchemeEntry parent;
            parent.Type = NYdb::NScheme::ESchemeEntryType::SubDomain;
            NYdb::NScheme::TSchemeEntry topic;
            topic.Name = "topic";
            topic.Type = NYdb::NScheme::ESchemeEntryType::Topic;
            std::vector<NYdb::NScheme::TSchemeEntry> children;
            if (status == NYdb::EStatus::SUCCESS) {
                children.push_back(topic);
            }
            const auto start = Runtime.GetCurrentTime();
            Runtime.Send(Remover, Proxy, new TEvYdbProxy::TEvListDirectoryResponse(
                NYdb::NScheme::TListDirectoryResult(NYdb::TStatus(status, {}), parent, std::move(children))));
            Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(Proxy);
            UNIT_ASSERT(Runtime.GetCurrentTime() - start >= TDuration::Seconds(10));
        }
    }

    void FinishWithMissingConsumer() {
        Reply(NYdb::EStatus::NOT_FOUND);
        const auto result = Runtime.GrabEdgeEvent<TEvPrivate::TEvDropStreamResult>(Parent);
        UNIT_ASSERT(result->Get()->IsSuccess());
    }

    void Cancel() {
        Runtime.Send(Remover, Parent, new TEvents::TEvPoison());
        Runtime.GrabEdgeEvent<TEvPrivate::TEvCancelDropStream>(Parent);
    }

private:
    void Reply(NYdb::EStatus status) {
        Runtime.Send(Remover, Proxy, new TEvYdbProxy::TEvAlterTopicResponse(NYdb::TStatus(status, {})));
    }

    TTestActorRuntime Runtime;
    TActorId Parent;
    TActorId Proxy;
    TActorId Remover;
};

Y_UNIT_TEST_SUITE(TopicMirroring) {
    Y_UNIT_TEST(ConsumerCleanupRetriesUnauthorizedAndAcceptsMissing) {
        TConsumerRemovalFixture test;
        test.DenyAndWaitForRetry();
        test.CheckAmbiguousAbsence();
        test.FinishWithMissingConsumer();
    }

    Y_UNIT_TEST(ConsumerCleanupCanBeCancelledAfterFailure) {
        TConsumerRemovalFixture test;
        test.DenyAndWaitForRetry();
        test.Cancel();
    }

    Y_UNIT_TEST(DoneBeforeCreateResultDoesNotStartMirroring) {
        TTopicMirroringFixture test("localhost", true);
        test.Done();
        UNIT_ASSERT(!test.Config().GetPartitionConfig().HasMirrorFrom());
    }

    Y_UNIT_TEST_TWIN(DropSucceedsAfterSourceDirectoryWasDeleted, ReplaceParentWithTopic) {
        TTopicMirroringFixture test;
        test.DropSourceTopic();
        test.RemoveSourceDirectory();
        if (ReplaceParentWithTopic) {
            UNIT_ASSERT_VALUES_EQUAL(test.Source.CreateTopic("/Root", *MakeTopicDescription({.Name = "src"})), NMsgBusProxy::MSTATUS_OK);
        }
        test.CheckDropWithMissingSource();
    }

    Y_UNIT_TEST(DropSucceedsAfterDirectoryReplacesSourceTopic) {
        TTopicMirroringFixture test;
        test.DropSourceTopic();
        UNIT_ASSERT_VALUES_EQUAL(test.Source.MkDir("/Root/src", "topic"), NMsgBusProxy::MSTATUS_OK);
        test.CheckDropWithMissingSource();
    }

    Y_UNIT_TEST(TopicCleanupConfirmsMissingIdAfterNameConflict) {
        TTopicMirroringFixture test;
        test.CheckDropByMissingId();
    }

    Y_UNIT_TEST(MissingDestinationCannotResume) {
        TTopicMirroringFixture test;
        test.CheckMissingDestination();
    }

    Y_UNIT_TEST(CopiesMessagesAcrossPauseAndResume) {
        TTopicMirroringFixture test("[::1]");
        test.WriteAndReadBack("first", 0);
        test.WriteAndReadBack("second", 1);
        test.CommitAndWaitForConsumerOffset(1);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        test.WaitMirror(false);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'STANDBY');");
        test.WaitMirror(true);
        test.WriteAndReadBack("third", 2);
        test.CommitAndWaitForConsumerOffset(2);
        test.CommitAndWaitForConsumerOffset(3);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');");
        test.WaitMirror(false);
        test.Exec("DROP ASYNC REPLICATION `/Root/replication`;");
    }

    Y_UNIT_TEST(DrainsBacklogWhenSourceClientModificationsAreDisabled) {
        TMirrorReaderFactory mirrorReader;
        TTopicMirroringFixture test("[::1]", false, &mirrorReader);
        test.ReplicateFirstMessage();

        const TVector<TString> backlog = {"second", "third", "fourth"};
        const ui64 end = 1 + backlog.size();
        constexpr ui64 appOffset = 2;
        mirrorReader.Pause();
        Y_DEFER { mirrorReader.Resume(); };
        test.Write(backlog);
        const auto commit = test.CommitSourceConsumerOffset(appOffset);
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(test.GetSourceConsumerOffset(), appOffset);
        test.Source.GetRuntime().GetAppData().FeatureFlags.SetEnableClientModifications(false);

        const auto denied = test.CommitSourceConsumerOffset(end);
        UNIT_ASSERT_VALUES_EQUAL(denied.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT_STRING_CONTAINS(denied.GetIssues().ToString(), "Consumer offset commits are disabled");
        UNIT_ASSERT_VALUES_EQUAL(test.GetSourceConsumerOffset(), appOffset);

        test.CheckBacklog(1, end);
        mirrorReader.Resume();
        test.ReadBack(backlog, 1);
        test.WaitForOffsets(end, appOffset);
        UNIT_ASSERT_VALUES_EQUAL(test.GetSourceConsumerOffset(), appOffset);
    }

    Y_UNIT_TEST(DrainsBurstBacklog) {
        TMirrorReaderFactory mirrorReader;
        TTopicMirroringFixture test("[::1]", false, &mirrorReader);
        test.ReplicateFirstMessage();

        constexpr size_t messageCount = 10000;
        TVector<TString> burst;
        burst.reserve(messageCount);
        for (size_t index = 0; index < messageCount; ++index) {
            burst.emplace_back(TStringBuilder() << "burst-" << index);
        }

        mirrorReader.Pause();
        Y_DEFER { mirrorReader.Resume(); };
        test.Write(burst);
        const ui64 end = 1 + burst.size();
        test.CheckBacklog(1, end);

        const auto commit = test.CommitSourceConsumerOffset(end);
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        mirrorReader.Resume();

        test.ReadBack(burst, 1);
        test.WaitForOffsets(end, end);
    }
}
}
}
