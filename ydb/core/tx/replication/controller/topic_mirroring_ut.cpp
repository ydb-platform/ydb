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
#include <util/string/printf.h>
#include <util/datetime/base.h>
#include <optional>

namespace NKikimr::NReplication::NController {
namespace {
using namespace NTestHelpers;

class TTopicMirroringFixture {
    static constexpr const char* Consumer = "application";

    static TFeatureFlags Flags() {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableTopicReplication(true);
        flags.FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
        return flags;
    }

    std::optional<TEnv<>> RemoteSource;

public:
    TEnv<> Destination;
    TEnv<>& Source;
    const ui64 SchemeShard = Destination.GetPathId("/Root").OwnerId;

    // The simulated runtime installs a process-wide clock, so the DONE test uses one cluster.
    explicit TTopicMirroringFixture(const TString& sourceHost = "localhost", bool stopBeforeCreateResult = false)
        : RemoteSource(stopBeforeCreateResult ? std::nullopt : std::optional<TEnv<>>(std::in_place, Flags()))
        , Destination(Flags(), true, stopBeforeCreateResult ? ERuntimeMode::Simulated : ERuntimeMode::RealThreads)
        , Source(RemoteSource ? *RemoteSource : Destination)
    {
        Source.RunCall([&] {
            UNIT_ASSERT_VALUES_EQUAL(Source.MkDir("/Root", "src"), NMsgBusProxy::MSTATUS_OK);
            auto topic = MakeTopicDescription({.Name = "topic"});
            topic->MutablePQTabletConfig()->AddConsumers()->SetName(Consumer);
            UNIT_ASSERT_VALUES_EQUAL(Source.CreateTopic("/Root/src", *topic), NMsgBusProxy::MSTATUS_OK);
        });

        auto& runtime = Destination.GetRuntime();
        if (runtime.IsRealThreads()) {
            const auto& appData = runtime.GetAppData(0);
            appData.PersQueueMirrorReaderFactory->Initialize(
                runtime.GetActorSystem(0), appData.PQConfig.GetMirrorConfig().GetPQLibSettings());
        }

        bool createResultBlocked = false;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
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
        UNIT_ASSERT(Config().GetPartitionConfig().GetMirrorFrom().GetMirrorConsumerOffsets());
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
    void Wait(const TCondition& ready) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        while (!ready()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Timed out waiting for topic mirroring");
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

    void WriteAndReadBack(const TString& payload, ui64 offset) {
        using namespace NYdb::NTopic;
        TTopicClient sourceClient(Source.GetDriver()), destinationClient(Destination.GetDriver());
        auto writer = sourceClient.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
            .Path("/Root/src/topic").ProducerId("producer").MessageGroupId("producer").Codec(ECodec::RAW));
        UNIT_ASSERT(writer->Write(payload));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));

        auto reader = destinationClient.CreateReadSession(TReadSessionSettings().WithoutConsumer()
            .AppendTopics(TTopicReadSettings("/Root/topic").AppendPartitionIds(0)));
        Wait([&] {
            auto event = reader->GetEvent();
            if (!event) {
                return false;
            }
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                start->Confirm(offset);
            } else if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                const auto& messages = data->GetMessages();
                UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(messages.front().GetOffset(), offset);
                UNIT_ASSERT_VALUES_EQUAL(messages.front().GetData(), payload);
                return true;
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
            return false;
        });
        UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
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
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        test.WaitMirror(false);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'STANDBY');");
        test.WaitMirror(true);
        test.WriteAndReadBack("third", 2);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');");
        test.WaitMirror(false);
        test.Exec("DROP ASYNC REPLICATION `/Root/replication`;");
    }
}
}
}
