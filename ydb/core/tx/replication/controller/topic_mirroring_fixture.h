#pragma once

#include "private_events.h"
#include "dst_alterer.h"
#include "dst_remover.h"
#include "stream_remover.h"
#include "topic_mirror.h"

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ut_helpers/test_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>

namespace NKikimr::NReplication::NController {
namespace {
using namespace NTestHelpers;

class TMirrorReaderFactory : public NPQ::TPersQueueMirrorReaderFactory {
    class TReadSession final: public NYdb::NTopic::IReadSession {
        using TEvent = NYdb::NTopic::TReadSessionEvent::TEvent;
        using TSettings = NYdb::NTopic::TReadSessionGetEventSettings;

    public:
        TReadSession(const TMirrorReaderFactory& factory, ui32 partition, std::shared_ptr<NYdb::NTopic::IReadSession> session)
            : Factory(factory)
            , Partition(partition)
            , Session(std::move(session))
        {}

        NThreading::TFuture<void> WaitEvent() override {
            TGuard<TMutex> guard(Factory.Mutex);
            if (Factory.IsPaused(Partition)) {
                return Factory.Resumed.GetFuture();
            }
            return Session->WaitEvent();
        }

        std::optional<TEvent> GetEvent(bool block, size_t maxByteSize) override {
            TGuard<TMutex> guard(Factory.Mutex);
            if (Factory.IsPaused(Partition)) {
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
        const ui32 Partition;
        std::shared_ptr<NYdb::NTopic::IReadSession> Session;
    };

public:
    std::shared_ptr<NYdb::NTopic::IReadSession> GetReadSession(
            const NKikimrPQ::TMirrorPartitionConfig& config, ui32 partition,
            std::shared_ptr<NYdb::ICredentialsProviderFactory> credentials,
            ui64 maxMemoryUsageBytes, TMaybe<TLog> logger = Nothing()) const override
    {
        return std::make_shared<TReadSession>(*this, partition, TPersQueueMirrorReaderFactory::GetReadSession(
            config, partition, std::move(credentials), maxMemoryUsageBytes, std::move(logger)));
    }

    void Pause(std::optional<ui32> partition = std::nullopt) {
        TGuard<TMutex> guard(Mutex);
        UNIT_ASSERT(!Resumed.Initialized());
        PausedPartition = partition;
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
    bool IsPaused(ui32 partition) const {
        return Resumed.Initialized() && (!PausedPartition || *PausedPartition == partition);
    }

    mutable TMutex Mutex;
    std::optional<ui32> PausedPartition;
    NThreading::TPromise<void> Resumed;
};

class TTopicMirroringFixture {

    static TFeatureFlags Flags() {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableTopicReplication(true);
        flags.FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
        return flags;
    }

    std::optional<TEnv<>> RemoteSource;

public:
    struct TOffsets {
        ui64 End;
        ui64 Committed;
    };

    static TOffsets GetOffsets(TEnv<>& env, const TString& topic, const TString& consumer, ui32 partitionId = 0) {
        NYdb::NTopic::TTopicClient client(env.GetDriver());
        const auto result = env.RunCall([&] {
            return client.DescribeConsumer(topic, consumer, NYdb::NTopic::TDescribeConsumerSettings()
                .IncludeStats(true)).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& partitions = result.GetConsumerDescription().GetPartitions();
        const auto selected = FindIf(partitions, [partitionId](const auto& partition) {
            return partition.GetPartitionId() == partitionId;
        });
        UNIT_ASSERT_C(selected != partitions.end(), "Missing partition " << partitionId << " on " << topic);
        const auto& partition = *selected;
        UNIT_ASSERT(partition.GetPartitionStats());
        UNIT_ASSERT(partition.GetPartitionConsumerStats());
        return {partition.GetPartitionStats()->GetEndOffset(), partition.GetPartitionConsumerStats()->GetCommittedOffset()};
    }

    static constexpr const char* Consumer = "application";
    static constexpr const char* SourceTopic = "/Root/src/topic";
    static constexpr const char* DestinationTopic = "/Root/topic";
    TString MirrorConsumer;

    TEnv<> Destination;
    TEnv<>& Source;
    const ui64 SchemeShard = Destination.GetPathId("/Root").OwnerId;

    enum class EStart {
        Immediate,
        Deferred,
    };

    // The simulated runtime installs a process-wide clock, so the DONE test uses one cluster.
    explicit TTopicMirroringFixture(const TString& sourceHost = "localhost", bool stopBeforeCreateResult = false,
            TMirrorReaderFactory* mirrorReaderFactory = nullptr, const TString& sourceWriteMode = {},
            EStart start = EStart::Immediate, ui32 partitionCount = 1)
        : RemoteSource(stopBeforeCreateResult ? std::nullopt : std::optional<TEnv<>>(std::in_place, Flags()))
        , Destination(Flags(), true, stopBeforeCreateResult ? ERuntimeMode::Simulated : ERuntimeMode::RealThreads)
        , Source(RemoteSource ? *RemoteSource : Destination)
    {
        Source.RunCall([&] {
            UNIT_ASSERT_VALUES_EQUAL(Source.MkDir("/Root", "src"), NMsgBusProxy::MSTATUS_OK);
            auto topic = MakeTopicDescription({.Name = "topic"});
            topic->SetTotalGroupCount(partitionCount);
            topic->MutablePQTabletConfig()->AddConsumers()->SetName(Consumer);
            UNIT_ASSERT_VALUES_EQUAL(Source.CreateTopic("/Root/src", *topic), NMsgBusProxy::MSTATUS_OK);
        });

        if (mirrorReaderFactory) {
            auto& runtime = Destination.GetRuntime();
            auto& appData = runtime.GetAppData();
            mirrorReaderFactory->Initialize(runtime.GetAnyNodeActorSystem(), appData.PQConfig.GetMirrorConfig().GetPQLibSettings());
            appData.PersQueueMirrorReaderFactory = mirrorReaderFactory;
        }

        if (!sourceWriteMode.empty()) {
            SetSourceWriteMode(sourceWriteMode);
        }
        if (start == EStart::Immediate) {
            Start(sourceHost, stopBeforeCreateResult);
        }
    }

    void Start(const TString& sourceHost, bool stopBeforeCreateResult = false) {
        bool createResultBlocked = false;
        auto observer = Destination.GetRuntime().AddObserver([&](IEventHandle::TPtr& ev) {
            if (!stopBeforeCreateResult || ev->GetTypeRewrite() != TEvPrivate::EvCreateDstResult || !ev->HasEvent()) {
                return;
            }

            // Other actors can use the same private event ID.
            if (const auto* result = dynamic_cast<TEvPrivate::TEvCreateDstResult*>(ev->GetBase())) {
                UNIT_ASSERT_C(result->IsSuccess(), result->Error);
                ev.Reset();
                createResultBlocked = true;
            }
        });
        const auto endpoint = sourceHost + Source.GetEndpoint().substr(Source.GetEndpoint().rfind(':'));
        Exec(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/src/topic` AS `/Root/topic`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", endpoint.c_str()));
        if (stopBeforeCreateResult) {
            Wait([&] { return createResultBlocked; });
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

    NKikimrPQ::TPQTabletConfig Config(const TString& path = "/Root/topic") {
        const auto result = Destination.Send<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(SchemeShard,
            new NSchemeShard::TEvSchemeShard::TEvDescribeScheme(path));
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

    void WaitMirror(bool enabled, const TString& path = "/Root/topic") {
        Wait([&] { return Config(path).GetPartitionConfig().HasMirrorFrom() == enabled; });
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

    NYdb::TStatus CommitSourceConsumerOffset(ui64 offset, ui32 partitionId = 0) {
        return CommitConsumerOffset(Source, SourceTopic, offset, partitionId);
    }

    NYdb::TStatus CommitDestinationConsumerOffset(ui64 offset, ui32 partitionId = 0) {
        return CommitConsumerOffset(Destination, DestinationTopic, offset, partitionId);
    }

    void CheckClientModificationsBlocked(ui64 offset) {
        CheckWriteBlocked(Source, SourceTopic);
        CheckWriteBlocked(Destination, DestinationTopic);

        for (const auto& commit : {CommitSourceConsumerOffset(offset), CommitDestinationConsumerOffset(offset)}) {
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED, commit.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(commit.GetIssues().ToString(), "Consumer offset commits are disabled");
        }
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

    void SetSourceWriteMode(const TString& mode) {
        NYdb::NTopic::TTopicClient client(Source.GetDriver());
        const auto result = Source.RunCall([&] {
            return client.AlterTopic("/Root/src/topic", NYdb::NTopic::TAlterTopicSettings()
                .BeginAlterAttributes().Add("_write_mode", mode).EndAlterAttributes()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void CheckBacklog(ui64 before, ui64 end, ui32 partitionId = 0) {
        const auto source = GetOffsets(Source, SourceTopic, MirrorConsumer, partitionId);
        UNIT_ASSERT_VALUES_EQUAL(source.End, end);
        UNIT_ASSERT_VALUES_EQUAL(source.Committed, before);
        UNIT_ASSERT_VALUES_EQUAL(GetOffsets(Destination, DestinationTopic, Consumer, partitionId).End, before);
        UNIT_ASSERT_C(before < end, "Expected a backlog on partition " << partitionId);
    }

    void WaitForOffsets(ui64 end, ui64 appOffset, ui32 partitionId = 0) {
        TString description;
        Wait([&] {
            const auto destination = GetOffsets(Destination, DestinationTopic, Consumer, partitionId);
            description = TStringBuilder() << "destination partition " << partitionId << " to reach end offset " << end
                << " and application offset " << appOffset << " (observed " << destination.End
                << " and " << destination.Committed << ')';
            return destination.End == end && destination.Committed == appOffset;
        }, description);

        Wait([&] {
            const auto source = GetOffsets(Source, SourceTopic, MirrorConsumer, partitionId);
            description = TStringBuilder() << "source mirror consumer " << MirrorConsumer
                << " to commit partition " << partitionId << " through offset " << end << " (observed " << source.Committed << ')';
            return source.End == end && source.Committed == end;
        }, description);
    }

    void Write(const TVector<TString>& payloads, ui32 partitionId = 0) {
        Write(Source, SourceTopic, "producer", payloads, partitionId);
    }

    void WriteDestination(const TVector<TString>& payloads, ui32 partitionId = 0) {
        Write(Destination, DestinationTopic, "destination-producer", payloads, partitionId);
    }

    void ReadBack(const TVector<TString>& payloads, ui64 firstOffset, ui32 partitionId = 0,
            const TString& topic = DestinationTopic) {
        ReadBack(Destination, topic, payloads, firstOffset, nullptr, partitionId);
    }

    void ReadSourceAsConsumer(const TVector<TString>& payloads) {
        ReadBack(Source, SourceTopic, payloads, 0, Consumer);
    }

    void ReadDestinationAsConsumer(const TVector<TString>& payloads, ui64 firstOffset, ui32 partitionId = 0) {
        ReadBack(Destination, DestinationTopic, payloads, firstOffset, Consumer, partitionId);
    }

    void WriteAndReadBack(const TString& payload, ui64 offset) {
        Write({payload});
        ReadBack({payload}, offset);
    }

private:
    static NYdb::TStatus CommitConsumerOffset(TEnv<>& env, const TString& topic, ui64 offset, ui32 partitionId) {
        NYdb::NTopic::TTopicClient client(env.GetDriver());
        return client.CommitOffset(topic, partitionId, Consumer, offset).GetValueSync();
    }

    void CheckWriteBlocked(TEnv<>& env, const TString& topic) {
        using namespace NYdb::NTopic;
        TTopicClient client(env.GetDriver());
        auto writer = client.CreateWriteSession(TWriteSessionSettings()
            .Path(topic).ProducerId("blocked-producer").MessageGroupId("blocked-producer").Codec(ECodec::RAW)
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy()));
        bool sent = false;
        Wait([&] {
            auto event = writer->GetEvent();
            if (!event) {
                return false;
            }
            if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                if (!sent) {
                    writer->Write(std::move(ready->ContinuationToken), "rejected-write");
                    sent = true;
                }
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_ASSERT_C(!closed->IsSuccess(), closed->DebugString());
                UNIT_ASSERT_STRING_CONTAINS(closed->GetIssues().ToString(), "Client data writes are disabled");
                return true;
            } else {
                UNIT_FAIL("Unexpected acknowledgement for a blocked write to " << topic);
            }
            return false;
        }, TStringBuilder() << "client write rejection on " << topic);
    }

    static void Write(TEnv<>& env, const TString& topic, const TString& producer,
            const TVector<TString>& payloads, ui32 partitionId)
    {
        using namespace NYdb::NTopic;
        TTopicClient client(env.GetDriver());
        const TString partitionProducer = TStringBuilder() << producer << "-" << partitionId;
        auto writer = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
            .Path(topic).ProducerId(partitionProducer).MessageGroupId(partitionProducer).PartitionId(partitionId).Codec(ECodec::RAW));
        for (const auto& payload : payloads) {
            UNIT_ASSERT_C(writer->Write(payload), "Write failed on " << topic << " partition " << partitionId);
        }
        UNIT_ASSERT_C(writer->Close(TDuration::Seconds(30)), "Writer close failed on " << topic << " partition " << partitionId);
    }

    void ReadBack(TEnv<>& env, const TString& topic, const TVector<TString>& payloads, ui64 firstOffset,
            const char* consumer, ui32 partitionId = 0)
    {
        using namespace NYdb::NTopic;
        TTopicClient client(env.GetDriver());
        TReadSessionSettings settings;
        if (consumer) {
            settings.ConsumerName(consumer);
        } else {
            settings.WithoutConsumer();
        }
        auto reader = client.CreateReadSession(settings.AppendTopics(TTopicReadSettings(topic).AppendPartitionIds(partitionId)));
        size_t index = 0;
        Wait([&] {
            auto event = reader->GetEvent();
            if (!event) {
                return false;
            }
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                if (consumer) {
                    UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), firstOffset);
                }
                start->Confirm(consumer && index == 0 ? std::nullopt : std::optional<ui64>(firstOffset + index));
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
        UNIT_ASSERT(env.RunCall([&] { return reader->Close(TDuration::Seconds(10)); }));
    }
public:
    void CreateSourceTable() {
        const auto table = MakeTableDescription({
            .Name = "table",
            .KeyColumns = {"Key"},
            .Columns = {
                {.Name = "Key", .Type = "Uint32"},
                {.Name = "Value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        });
        UNIT_ASSERT_VALUES_EQUAL(Source.CreateTable("/Root/src", *table), NMsgBusProxy::MSTATUS_OK);
    }

    void CreateTopicAndTableReplication() {
        const auto endpoint = "localhost" + Source.GetEndpoint().substr(Source.GetEndpoint().rfind(':'));
        Exec(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR
            `/Root/src/topic` AS `/Root/topic`, `/Root/src/table` AS `/Root/table`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", endpoint.c_str()));
        WaitMirrorConsumer();
        Wait([&] { return Destination.GetDescription("/Root/table").GetSchemeStatus() == NKikimrScheme::StatusSuccess; },
            "destination table creation");
    }

    void WaitMirrorConsumer() {
        WaitMirror(true);
        const auto config = Config();
        const auto& mirror = config.GetPartitionConfig().GetMirrorFrom();
        UNIT_ASSERT(mirror.GetMirrorConsumerOffsets());
        MirrorConsumer = mirror.GetConsumer();
        UNIT_ASSERT(!MirrorConsumer.empty());
    }

    void UpsertSourceTable(ui64 key, const TString& value) {
        NYdb::NTable::TTableClient client(Source.GetDriver());
        auto session = Source.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });
        const auto result = Source.RunCall([&] {
            return session.ExecuteDataQuery(
                Sprintf("UPSERT INTO `/Root/src/table` (Key, Value) VALUES (%lu, \"%s\");", key, value.c_str()),
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void WaitDestinationTableRows(size_t count) {
        NYdb::NTable::TTableClient client(Destination.GetDriver());
        auto session = Destination.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });
        Wait([&] {
            const auto result = Destination.RunCall([&] {
                return session.ExecuteDataQuery("SELECT COUNT(*) FROM `/Root/table`;",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto parser = result.GetResultSetParser(0);
            UNIT_ASSERT(parser.TryNextRow());
            return parser.ColumnParser(0).GetUint64() >= count;
        }, "destination table rows");
    }

    void BulkUpsertSourceTable(ui64 begin, ui64 count) {
        NYdb::NTable::TTableClient client(Source.GetDriver());
        auto rows = NYdb::TValueBuilder();
        rows.BeginList();
        for (ui64 i = 0; i < count; ++i) {
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint32(begin + i)
                    .AddMember("Value").Utf8(TStringBuilder() << "row-" << (begin + i))
                .EndStruct();
        }
        rows.EndList();
        const auto result = Source.RunCall([&] {
            return client.BulkUpsert("/Root/src/table", rows.Build()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void WriteMany(size_t count, const TString& prefix) {
        using namespace NYdb::NTopic;
        TTopicClient client(Source.GetDriver());
        auto writer = client.CreateWriteSession(TWriteSessionSettings()
            .Path(SourceTopic).ProducerId("bulk").MessageGroupId("bulk").Codec(ECodec::RAW));
        std::optional<TContinuationToken> token;
        for (size_t index = 0; index < count; ++index) {
            while (!token) {
                auto event = writer->GetEvent(true);
                UNIT_ASSERT(event);
                if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                    token = std::move(ready->ContinuationToken);
                } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                    UNIT_FAIL(closed->DebugString());
                }
            }
            const TString payload = TStringBuilder() << prefix << '-' << index;
            writer->Write(std::move(*token), TWriteMessage(payload));
            token.reset();
        }
        UNIT_ASSERT(writer->Close(TDuration::Seconds(120)));
    }

    void AlterSourcePartitioning(ui64 minActivePartitions, ui64 maxActivePartitions,
            NYdb::NTopic::EAutoPartitioningStrategy strategy) {
        NYdb::NTopic::TTopicClient client(Source.GetDriver());
        const auto result = Source.RunCall([&] {
            return client.AlterTopic(SourceTopic, NYdb::NTopic::TAlterTopicSettings()
                .BeginAlterPartitioningSettings()
                    .MinActivePartitions(minActivePartitions)
                    .MaxActivePartitions(maxActivePartitions)
                    .BeginAlterAutoPartitioningSettings()
                        .Strategy(strategy)
                    .EndAlterAutoPartitioningSettings()
                .EndAlterTopicPartitioningSettings()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void WaitActivePartitionCount(TEnv<>& env, const TString& topic, size_t count) {
        NYdb::NTopic::TTopicClient client(env.GetDriver());
        Wait([&] {
            const auto result = env.RunCall([&] { return client.DescribeTopic(topic).GetValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            size_t active = 0;
            for (const auto& partition : result.GetTopicDescription().GetPartitions()) {
                active += partition.GetActive();
            }
            return active == count;
        }, TStringBuilder() << topic << " active partition count " << count);
    }

    ui64 GetDestinationEndOffset() {
        return GetOffsets(Destination, DestinationTopic, Consumer).End;
    }

    NYdb::TStatus CommitSourceConsumerOffset(ui64 offset, const TString& consumer) {
        NYdb::NTopic::TTopicClient client(Source.GetDriver());
        return client.CommitOffset(SourceTopic, 0, consumer, offset).GetValueSync();
    }

    void CommitAndWaitForConsumerOffset(ui64 offset, const TString& consumer) {
        const auto commit = CommitSourceConsumerOffset(offset, consumer);
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());

        NYdb::NTopic::TTopicClient destination(Destination.GetDriver());
        Wait([&] {
            const auto result = destination.DescribeConsumer(DestinationTopic, consumer,
                NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& partitions = result.GetConsumerDescription().GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
            const auto& stats = partitions.front().GetPartitionConsumerStats();
            return stats && stats->GetCommittedOffset() == offset;
        });
    }

    void WaitReplicationError(const TString& issuePart = {}) {
        Wait([&] {
            const auto& state = Destination.GetDescription("/Root/replication")
                .GetPathDescription().GetReplicationDescription().GetState();
            return state.HasError()
                && (issuePart.empty() || state.GetError().DebugString().Contains(issuePart));
        }, TStringBuilder() << "replication error containing '" << issuePart << "'");
    }

    void RestartSourceTopicTablet() {
        const auto description = Source.GetDescription(SourceTopic);
        const auto& partitions = description.GetPathDescription().GetPersQueueGroup().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        KillTablet(Source, partitions.front().GetTabletId());
    }

    void RestartDestinationTopicTablet() {
        const auto description = Destination.GetDescription(DestinationTopic);
        const auto& partitions = description.GetPathDescription().GetPersQueueGroup().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        KillTablet(Destination, partitions.front().GetTabletId());
    }

    void RestartController() {
        const auto controllerId = Destination.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetControllerId();
        Destination.SendAsync(controllerId, new TEvents::TEvPoisonPill());
    }

    struct TMirroredMessage {
        TString Data;
        TString ProducerId;
        ui64 SeqNo = 0;
        TInstant CreateTime;
        TInstant WriteTime;
        ui64 Offset = 0;
        ui64 LogicalMessageCount = 0;
        TVector<std::pair<TString, TString>> Meta;
    };

    TVector<TMirroredMessage> ReadMessages(ui64 firstOffset, size_t count, ui32 partitionId = 0,
            const TString& topic = DestinationTopic) {
        using namespace NYdb::NTopic;
        TTopicClient client(Destination.GetDriver());
        auto reader = client.CreateReadSession(TReadSessionSettings().WithoutConsumer()
            .AppendTopics(TTopicReadSettings(topic).AppendPartitionIds(partitionId)));
        TVector<TMirroredMessage> messages;
        Wait([&] {
            auto event = reader->GetEvent();
            if (!event) {
                return false;
            }
            if (auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                start->Confirm(firstOffset + messages.size());
            } else if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& message : data->GetMessages()) {
                    UNIT_ASSERT(messages.size() < count);
                    TMirroredMessage mirrored;
                    mirrored.Data = message.GetData();
                    mirrored.ProducerId = message.GetProducerId();
                    mirrored.SeqNo = message.GetSeqNo();
                    mirrored.CreateTime = message.GetCreateTime();
                    mirrored.WriteTime = message.GetWriteTime();
                    mirrored.Offset = message.GetOffset();
                    mirrored.LogicalMessageCount = message.GetLogicalMessageCount();
                    for (const auto& field : message.GetMessageMeta()->Fields) {
                        mirrored.Meta.emplace_back(field.first, field.second);
                    }
                    messages.push_back(std::move(mirrored));
                }
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
            return messages.size() == count;
        });
        UNIT_ASSERT(Destination.RunCall([&] { return reader->Close(TDuration::Seconds(10)); }));
        return messages;
    }

private:
    void KillTablet(TEnv<>& env, ui64 tabletId) {
        auto& runtime = env.GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())));
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvForward(tabletId, nullptr)));
        const auto ev = runtime.GrabEdgeEventRethrow<TEvTabletResolver::TEvForwardResult>(sender);
        UNIT_ASSERT(ev && ev->Get()->Tablet);
        runtime.Send(new IEventHandle(ev->Get()->Tablet, sender, new TEvents::TEvPoisonPill()));
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())));
    }
};

}
}
