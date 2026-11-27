#include "topic_mirroring_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {
namespace {

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
    Y_UNIT_TEST(DoesNotInheritSourceWriteMode) {
        for (const auto* mode : {"unavailable", "prohibited"}) {
            TTopicMirroringFixture test("localhost", false, nullptr, mode);
            const auto config = test.Config();
            UNIT_ASSERT(!config.HasWriteMode());
            UNIT_ASSERT(config.GetWriteMode() == NKikimrPQ::TPQTabletConfig::WRITE_AVAILABLE);

            NYdb::NTopic::TTopicClient client(test.Destination.GetDriver());
            const auto description = client.DescribeTopic("/Root/topic").GetValueSync();
            UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
            UNIT_ASSERT(!description.GetTopicDescription().GetAttributes().contains("_write_mode"));

            test.SetSourceWriteMode("available");
            test.WriteAndReadBack("after-reopening-source", 0);
        }
    }

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

    Y_UNIT_TEST(TopicFailoverDrainsBacklogWithClientModificationsDisabled) {
        TMirrorReaderFactory mirrorReader;
        TTopicMirroringFixture test("[::1]", false, &mirrorReader, {}, TTopicMirroringFixture::EStart::Deferred);

        const TVector<TString> consumed = {"first", "second"};
        TVector<TString> unread;
        constexpr size_t messageCount = 10000;
        unread.reserve(messageCount);
        for (size_t index = 0; index < messageCount; ++index) {
            unread.emplace_back(TStringBuilder() << "unread-" << index);
        }
        const ui64 end = consumed.size() + unread.size();
        test.Write(consumed);
        test.ReadSourceAsConsumer(consumed);
        const auto sourceCommit = test.CommitSourceConsumerOffset(consumed.size());
        UNIT_ASSERT_C(sourceCommit.IsSuccess(), sourceCommit.GetIssues().ToString());
        test.Write(unread);

        mirrorReader.Pause();
        Y_DEFER { mirrorReader.Resume(); };
        test.Start("[::1]");
        test.Source.GetRuntime().GetAppData().FeatureFlags.SetEnableClientModifications(false);
        test.Destination.GetRuntime().GetAppData().FeatureFlags.SetEnableClientModifications(false);

        test.CheckClientModificationsBlocked(end);
        test.CheckBacklog(0, end);
        mirrorReader.Resume();
        test.WaitForOffsets(end, consumed.size());
        test.ReadDestinationAsConsumer(unread, consumed.size());

        test.Done();
        test.WaitMirror(false);
        test.Exec("DROP ASYNC REPLICATION `/Root/replication`;");
        UNIT_ASSERT_VALUES_EQUAL(test.Destination.GetDescription("/Root/replication").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);

        test.Destination.GetRuntime().GetAppData().FeatureFlags.SetEnableClientModifications(true);
        const auto destinationCommit = test.CommitDestinationConsumerOffset(end);
        UNIT_ASSERT_C(destinationCommit.IsSuccess(), destinationCommit.GetIssues().ToString());
        test.WriteDestination({"after-failover"});
        test.ReadDestinationAsConsumer({"after-failover"}, end);
    }

    Y_UNIT_TEST(TopicFailoverAcrossPartitions) {
        constexpr ui32 partitionCount = 3;
        constexpr ui32 laggingPartition = partitionCount - 1;
        TMirrorReaderFactory mirrorReader;
        TTopicMirroringFixture test("[::1]", false, &mirrorReader, {},
            TTopicMirroringFixture::EStart::Deferred, partitionCount);

        TVector<TVector<TString>> unread(partitionCount);
        TVector<ui64> ends(partitionCount);
        for (ui32 partition = 0; partition < partitionCount; ++partition) {
            TVector<TString> consumed(partition + 1, TStringBuilder() << "consumed-" << partition);
            for (ui32 index = 0; index < (partition + 1) * 32; ++index) {
                unread[partition].push_back(TStringBuilder() << "unread-" << partition << "-" << index);
            }
            test.Write(consumed, partition);
            const auto commit = test.CommitSourceConsumerOffset(consumed.size(), partition);
            UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
            test.Write(unread[partition], partition);
            ends[partition] = consumed.size() + unread[partition].size();
        }

        mirrorReader.Pause(laggingPartition);
        Y_DEFER { mirrorReader.Resume(); };
        test.Start("[::1]");
        test.Source.GetRuntime().GetAppData().FeatureFlags.SetEnableClientModifications(false);

        for (ui32 partition = 0; partition < laggingPartition; ++partition) {
            test.WaitForOffsets(ends[partition], partition + 1, partition);
        }
        test.CheckBacklog(0, ends[laggingPartition], laggingPartition);
        mirrorReader.Resume();
        test.WaitForOffsets(ends[laggingPartition], laggingPartition + 1, laggingPartition);

        test.Done();
        test.WaitMirror(false);
        for (ui32 partition = 0; partition < partitionCount; ++partition) {
            test.ReadDestinationAsConsumer(unread[partition], partition + 1, partition);
            const auto commit = test.CommitDestinationConsumerOffset(ends[partition], partition);
            UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());

            const TVector<TString> local = {TStringBuilder() << "local-" << partition};
            test.WriteDestination(local, partition);
            test.ReadDestinationAsConsumer(local, ends[partition], partition);
        }
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

    Y_UNIT_TEST(DrainsPreexistingBacklog) {
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);
        const TVector<TString> preexisting = {"before-1", "before-2", "before-3"};
        test.Write(preexisting);
        test.Start("localhost");
        test.ReadBack(preexisting, 0);
        test.WaitForOffsets(preexisting.size(), 0);
    }

    Y_UNIT_TEST(DestinationCopiesSourceSettingsExceptMirrorConsumer) {
        using namespace NYdb::NTopic;
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);

        TTopicClient source(test.Source.GetDriver());
        const auto altered = test.Source.RunCall([&] {
            return source.AlterTopic(test.SourceTopic, TAlterTopicSettings()
                .BeginAddConsumer("other").EndAddConsumer()
                .AlterAttributes({{"custom-attribute", "custom-value"}})).GetValueSync();
        });
        UNIT_ASSERT_C(altered.IsSuccess(), altered.GetIssues().ToString());

        test.Start("localhost");

        const auto sourceDescription = test.Source.RunCall([&] {
            return source.DescribeTopic(test.SourceTopic).GetValueSync();
        });
        UNIT_ASSERT_C(sourceDescription.IsSuccess(), sourceDescription.GetIssues().ToString());
        const auto& sourceAttributes = sourceDescription.GetTopicDescription().GetAttributes();
        const auto replicationConsumer = sourceAttributes.find("_ydb_replication_consumer");
        UNIT_ASSERT(replicationConsumer != sourceAttributes.end());
        UNIT_ASSERT_VALUES_EQUAL(replicationConsumer->second, test.MirrorConsumer);

        TTopicClient destination(test.Destination.GetDriver());
        const auto destinationDescription = test.Destination.RunCall([&] {
            return destination.DescribeTopic(test.DestinationTopic).GetValueSync();
        });
        UNIT_ASSERT_C(destinationDescription.IsSuccess(), destinationDescription.GetIssues().ToString());
        const auto& description = destinationDescription.GetTopicDescription();

        const auto& attributes = description.GetAttributes();
        const auto customAttribute = attributes.find("custom-attribute");
        UNIT_ASSERT(customAttribute != attributes.end());
        UNIT_ASSERT_VALUES_EQUAL(customAttribute->second, "custom-value");
        UNIT_ASSERT(!attributes.contains("_ydb_replication_consumer"));

        const auto& consumers = description.GetConsumers();
        const auto hasConsumer = [&](const TString& name) {
            return FindIf(consumers, [&](const auto& consumer) {
                return consumer.GetConsumerName() == name;
            }) != consumers.end();
        };
        UNIT_ASSERT(hasConsumer(test.Consumer));
        UNIT_ASSERT(hasConsumer("other"));
        UNIT_ASSERT(!hasConsumer(test.MirrorConsumer));
    }

    Y_UNIT_TEST(DropWithoutCascadeKeepsDestinationTopic) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("kept", 0);
        test.Exec("DROP ASYNC REPLICATION `/Root/replication`;");
        UNIT_ASSERT_VALUES_EQUAL(test.Destination.GetDescription("/Root/replication").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
        test.WaitMirror(false);
        test.ReadBack({"kept"}, 0);
    }

    Y_UNIT_TEST(DropCascadeRemovesDestinationTopic) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("dropped", 0);
        test.Exec("DROP ASYNC REPLICATION `/Root/replication` CASCADE;");
        UNIT_ASSERT_VALUES_EQUAL(test.Destination.GetDescription("/Root/replication").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
        UNIT_ASSERT_VALUES_EQUAL(test.Destination.GetDescription("/Root/topic").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
    }

    Y_UNIT_TEST(DoneFromPausedDemotesDestination) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("done-paused", 0);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        test.WaitMirror(false);
        test.Done();
        UNIT_ASSERT(!test.Config().GetPartitionConfig().HasMirrorFrom());
        test.ReadBack({"done-paused"}, 0);
    }

    Y_UNIT_TEST(TopicMirrorSettingsValidateEndpointAndCredentials) {
        const auto settings = [](const NKikimrReplication::TConnectionParams& params) {
            return TTopicMirrorSettings(params, "/Root/src/topic", "consumer");
        };

        NKikimrReplication::TConnectionParams badEndpoint;
        badEndpoint.SetEndpoint("localhost:99999");
        UNIT_ASSERT_STRING_CONTAINS(settings(badEndpoint).Error, "Invalid source endpoint");

        NKikimrReplication::TConnectionParams caCert;
        caCert.SetEndpoint("localhost:2135");
        caCert.SetCaCert("dummy");
        UNIT_ASSERT_STRING_CONTAINS(settings(caCert).Error, "Custom source CA certificates are not supported");

        NKikimrReplication::TConnectionParams staticCreds;
        staticCreds.SetEndpoint("localhost:2135");
        staticCreds.MutableStaticCredentials()->SetUser("user");
        UNIT_ASSERT_STRING_CONTAINS(settings(staticCreds).Error, "OAuth source credentials only");

        NKikimrReplication::TConnectionParams emptyToken;
        emptyToken.SetEndpoint("localhost:2135");
        emptyToken.MutableOAuthToken();
        UNIT_ASSERT_STRING_CONTAINS(settings(emptyToken).Error, "must contain a token or secret reference");

        NKikimrReplication::TConnectionParams valid;
        valid.SetEndpoint("localhost:2135");
        valid.SetDatabase("/Root");
        valid.MutableOAuthToken()->SetToken("token");
        const auto ok = settings(valid);
        UNIT_ASSERT_VALUES_EQUAL(ok.Error, "");
        UNIT_ASSERT_VALUES_EQUAL(ok.Config->GetEndpoint(), "localhost");
        UNIT_ASSERT_VALUES_EQUAL(ok.Config->GetEndpointPort(), 2135);
        UNIT_ASSERT_VALUES_EQUAL(ok.Config->GetTopic(), "/Root/src/topic");
        UNIT_ASSERT_VALUES_EQUAL(ok.Config->GetConsumer(), "consumer");
        UNIT_ASSERT(ok.Config->GetMirrorConsumerOffsets());
    }

    Y_UNIT_TEST(SecretReferenceCredentialsKeepMirroring) {
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);
        test.Exec(R"(CREATE SECRET `/Root/secret` WITH (value = "root@builtin");)");
        const auto endpoint = "localhost" + test.Source.GetEndpoint().substr(test.Source.GetEndpoint().rfind(':'));
        test.Exec(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/src/topic` AS `/Root/topic`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN_SECRET_NAME = "/Root/secret",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", endpoint.c_str()));
        test.WaitMirrorConsumer();
        test.WriteAndReadBack("via-secret", 0);

        test.Exec(R"(ALTER SECRET `/Root/secret` WITH (value = "root@builtin");)");
        test.WriteAndReadBack("after-rotation", 1);
        test.WaitForOffsets(2, 0);
    }

    Y_UNIT_TEST(AuthFailureRecoversAfterTokenAlter) {
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);
        const auto endpoint = "localhost" + test.Source.GetEndpoint().substr(test.Source.GetEndpoint().rfind(':'));
        test.Exec(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/src/topic` AS `/Root/topic`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "bad@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", endpoint.c_str()));
        test.WaitReplicationError();

        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (TOKEN = 'root@builtin');");
        test.WaitMirror(true);
        test.WriteAndReadBack("recovered", 0);
        test.WaitForOffsets(1, 0);
    }

    Y_UNIT_TEST(SourceTopicDroppedWhileMirroring) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("before-drop", 0);
        test.DropSourceTopic();
        test.WaitReplicationError();

        test.ReadBack({"before-drop"}, 0);
        test.Exec("DROP ASYNC REPLICATION `/Root/replication`;");
        UNIT_ASSERT_VALUES_EQUAL(test.Destination.GetDescription("/Root/replication").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
    }

    Y_UNIT_TEST(FanOutTwoReplicationsFromOneSource) {
        TTopicMirroringFixture test;
        const auto endpoint = "localhost" + test.Source.GetEndpoint().substr(test.Source.GetEndpoint().rfind(':'));
        test.Exec(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication2` FOR `/Root/src/topic` AS `/Root/topic2`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", endpoint.c_str()));
        test.WaitMirror(true, "/Root/topic2");

        test.Write({"fan-out"});
        test.ReadBack({"fan-out"}, 0);
        test.ReadBack({"fan-out"}, 0, 0, "/Root/topic2");
    }

    Y_UNIT_TEST(DoneConsistentWaitsForBacklog) {
        TMirrorReaderFactory mirrorReader;
        TTopicMirroringFixture test("[::1]", false, &mirrorReader);
        test.ReplicateFirstMessage();

        mirrorReader.Pause();
        Y_DEFER { mirrorReader.Resume(); };
        test.Write({"pending"});
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE', FAILOVER_MODE = 'CONSISTENT');");
        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT(!test.Destination.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetState().HasDone());

        mirrorReader.Resume();
        test.Wait([&] { return test.Destination.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetState().HasDone(); });
    }

    Y_UNIT_TEST(DestinationConsumerSurvivesPauseResume) {
        TTopicMirroringFixture test;
        NYdb::NTopic::TTopicClient destination(test.Destination.GetDriver());
        const auto altered = destination.AlterTopic(test.DestinationTopic, NYdb::NTopic::TAlterTopicSettings()
            .BeginAddConsumer("extra").EndAddConsumer()).GetValueSync();
        UNIT_ASSERT_C(altered.IsSuccess(), altered.GetIssues().ToString());

        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        test.WaitMirror(false);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'STANDBY');");
        test.WaitMirror(true);

        const auto described = destination.DescribeTopic(test.DestinationTopic).GetValueSync();
        UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
        const auto& consumers = described.GetTopicDescription().GetConsumers();
        const auto extra = FindIf(consumers, [](const auto& consumer) {
            return consumer.GetConsumerName() == "extra";
        });
        UNIT_ASSERT(extra != consumers.end());
        test.WriteAndReadBack("still-flows", 0);
    }
}
}
}
