#include <ydb/library/testlib/helpers.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

constexpr TDuration WaitTimeout = TDuration::MilliSeconds(10000);

Y_UNIT_TEST_SUITE(TPqWriterTest) {
    Y_UNIT_TEST_F(TestWriteToTopic, TPqIoTestFixture) {
        const TString topicName = "WriteToTopic";
        PQCreateStream(topicName);
        InitAsyncOutput(topicName);
        const std::vector<TString> data = { "1", "2", "3", "4" };

        AsyncOutputWrite(data);
        auto result = PQReadUntil(topicName, 4);
        UNIT_ASSERT_EQUAL(result, data);
    }

    Y_UNIT_TEST_F(TestWriteToTopicMultiBatch, TPqIoTestFixture) {
        const TString topicName = "WriteToTopicMultiBatch";
        PQCreateStream(topicName);
        InitAsyncOutput(topicName);

        const std::vector<TString> data1 = { "1" };
        const std::vector<TString> data2 = { "2" };
        const std::vector<TString> data3 = { "3" };

        AsyncOutputWrite(data1);
        AsyncOutputWrite(data2);
        AsyncOutputWrite(data3);
        auto result = PQReadUntil(topicName, 3);

        std::vector<TString> expected = { "1", "2", "3" };
        UNIT_ASSERT_EQUAL(result, expected);
    }

    Y_UNIT_TEST_F(TestDeferredWriteToTopic, TPqIoTestFixture) {
        // In this case we are checking free space overflow
        const TString topicName = "DeferredWriteToTopic";
        PQCreateStream(topicName);
        InitAsyncOutput(topicName, 1);

        const std::vector<TString> data = { "1", "2", "3" };

        auto future = CaSetup->AsyncOutputPromises->ResumeExecution.GetFuture();
        AsyncOutputWrite(data);
        auto result = PQReadUntil(topicName, 3);

        UNIT_ASSERT_EQUAL(result, data);
        UNIT_ASSERT(future.Wait(WaitTimeout)); // Resume execution should be called

        const std::vector<TString> data2 = { "4", "5", "6" };

        AsyncOutputWrite(data2);
        auto result2 = PQReadUntil(topicName, 6);
        const std::vector<TString> expected = { "1", "2", "3", "4", "5", "6" };
        UNIT_ASSERT_EQUAL(result2, expected);
    }

    Y_UNIT_TEST_F(WriteNonExistentTopic, TPqIoTestFixture) {
        const TString topicName = "NonExistentTopic";
        InitAsyncOutput(topicName);

        const std::vector<TString> data = { "1" };
        auto future = CaSetup->AsyncOutputPromises->Issue.GetFuture();
        AsyncOutputWrite(data);

        UNIT_ASSERT(future.Wait(WaitTimeout));
        UNIT_ASSERT_STRING_CONTAINS(future.GetValue().ToString(), "Write session to topic \"NonExistentTopic\" was closed");
    }

    Y_UNIT_TEST(TestCheckpoints) {
        const TString topicName = "Checkpoints";
        PQCreateStream(topicName);
        const auto initSink = [&](TPqIoTestFixture& setup) {
            auto settings = BuildPqTopicSinkSettings(topicName);
            settings.SetEnableDeduplication(true);
            setup.InitAsyncOutput(std::move(settings));
        };

        TSinkState state1;
        NDqProto::TCheckpoint checkpoint;
        {
            TPqIoTestFixture setup;
            initSink(setup);

            const std::vector<TString> data1 = { "1" };
            setup.AsyncOutputWrite(data1);

            const std::vector<TString> data2 = { "2", "3" };
            checkpoint = CreateCheckpoint();
            auto future = setup.CaSetup->AsyncOutputPromises->StateSaved.GetFuture();
            setup.AsyncOutputWrite(data2, checkpoint);

            UNIT_ASSERT(future.Wait(WaitTimeout));
            state1 = future.GetValue();
        }

        {
            TPqIoTestFixture setup;
            initSink(setup);
            setup.LoadSink(state1, checkpoint);

            const std::vector<TString> data3 = { "4", "5" };
            setup.AsyncOutputWrite(data3);

            auto result = PQReadUntil(topicName, 5);
            const std::vector<TString> expected = { "1", "2", "3", "4", "5" };
            UNIT_ASSERT_EQUAL(result, expected);
        }

        {
            TPqIoTestFixture setup;
            initSink(setup);
            setup.LoadSink(state1, checkpoint);

            const std::vector<TString> data4 = { "4", "5" };
            auto future = setup.CaSetup->AsyncOutputPromises->StateSaved.GetFuture();
            setup.AsyncOutputWrite(data4, CreateCheckpoint(1)); // This write should be deduplicated
            UNIT_ASSERT(future.Wait(WaitTimeout));

            NYdb::NTopic::TTopicClient client(setup.Driver, NYdb::NTopic::TTopicClientSettings()
                .DiscoveryEndpoint(GetDefaultPqEndpoint()).Database(GetDefaultPqDatabase()));
            const auto description = client.DescribeTopic(topicName,
                NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValue(WaitTimeout);
            UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
            const auto& partitions = description.GetTopicDescription().GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
            UNIT_ASSERT(partitions.front().GetPartitionStats());
            UNIT_ASSERT_VALUES_EQUAL(partitions.front().GetPartitionStats()->GetEndOffset(), 5);

            auto result = PQReadUntil(topicName, 5);
            const std::vector<TString> expected = { "1", "2", "3", "4", "5" };
            UNIT_ASSERT_EQUAL(result, expected);
        }
    }

    Y_UNIT_TEST_F(TestCheckpointWithEmptyBatch, TPqIoTestFixture) {
        const TString topicName = "CheckpointsWithEmptyBatch";
        PQCreateStream(topicName);

        TSinkState state1;
        {
            InitAsyncOutput(topicName);

            const std::vector<TString> data = {};
            auto checkpoint = CreateCheckpoint();
            auto future = CaSetup->AsyncOutputPromises->StateSaved.GetFuture();
            AsyncOutputWrite(data, checkpoint);

            UNIT_ASSERT(future.Wait(WaitTimeout));
            state1 = future.GetValue();
        }
    }
}

namespace {

using namespace NActors;
using namespace NYdb;

using TPublicationRequest = NTestUtils::IMockPqDeferredPublishClient::TRequest;
using EPublicationMethod = NTestUtils::IMockPqDeferredPublishClient::EMethod;

struct TEvRequest : TEventLocal<TEvRequest, EventSpaceBegin(TEvents::ES_PRIVATE) + 100>, TPublicationRequest {
    explicit TEvRequest(TPublicationRequest request)
        : TPublicationRequest(std::move(request))
    {}
};

class TWriteActorFixture : public TPqIoTestFixture {
public:
    void Start(ui64 restoredPublicationId = 0, bool write = true) {
        Edge = CaSetup->Runtime->AllocateEdgeActor();
        Gateway = NTestUtils::CreateMockPqGateway();
        Gateway->GetDeferredPublishClientController().SetRequestHandler(
            [actorSystem = CaSetup->Runtime->GetActorSystem(0), edge = Edge](TPublicationRequest request) {
                actorSystem->Send(edge, new TEvRequest(std::move(request)));
            });
        CaSetup->Execute([&](TFakeActor& actor) {
            NPq::NProto::TDqPqTopicSink settings;
            settings.SetTopicPath("topic");
            settings.SetDatabase("/Root");
            settings.SetDeferredPublicationExtIdPrefix("query:execution");
            auto [sink, sinkActor] = CreateDqPqWriteActor(std::move(settings), 0, TCollectStatsLevel::None, TString("tx"), 7,
                {}, Driver, CredentialsFactory, &actor.GetAsyncOutputCallbacks(),
                Counters, Gateway, false, DqPqDefaultFreeSpace, 3, false, true);
            actor.InitAsyncOutput(sink, sinkActor);
            Error = CaSetup->AsyncOutputPromises->Issue.GetFuture();
        });

        if (restoredPublicationId) {
            NPq::NProto::TDqPqTopicSinkState proto;
            proto.SetSourceId("source");
            proto.SetDeferredPublicationIntId(restoredPublicationId);
            TSinkState state;
            state.Data.Version = 1;
            UNIT_ASSERT(proto.SerializeToString(&state.Data.Blob));
            NDqProto::TCheckpoint checkpoint;
            checkpoint.SetGeneration(2);
            checkpoint.SetId(42);
            LoadSink(state, checkpoint);
        }
        if (write) {
            Write(1);
        }
    }

    void Write(ui64 checkpointId = 0) {
        TMaybe<NDqProto::TCheckpoint> checkpoint;
        if (checkpointId) {
            checkpoint.ConstructInPlace();
            checkpoint->SetGeneration(3);
            checkpoint->SetId(checkpointId);
            CaSetup->Execute([&](TFakeActor&) {
                SavedState = CaSetup->AsyncOutputPromises->StateSaved.GetFuture();
            });
        }
        AsyncOutputWrite({"message"}, checkpoint);
    }

    void Commit(ui64 checkpointId, ui64 generation = 3) {
        CaSetup->Execute([&](TFakeActor& actor) {
            CommittedState = CaSetup->AsyncOutputPromises->StateCommitted.GetFuture();
            NDqProto::TCheckpoint checkpoint;
            checkpoint.SetId(checkpointId);
            checkpoint.SetGeneration(generation);
            actor.DqAsyncOutput->CommitState(checkpoint);
        });
    }

    void AssertSaved(ui64 publicationId = 1) {
        UNIT_ASSERT(SavedState.Wait(WaitTimeout));
        NPq::NProto::TDqPqTopicSinkState state;
        UNIT_ASSERT(state.ParseFromString(SavedState.GetValue().Data.Blob));
        UNIT_ASSERT_VALUES_EQUAL(state.GetDeferredPublicationIntId(), publicationId);
    }

    void AssertCommitted() {
        UNIT_ASSERT(CommittedState.Wait(WaitTimeout));
    }

    ui64 Counter(const TString& name) {
        const auto counter = Counters->GetSubgroup("sink", "PqSink")->FindCounter("DeferredPublication/" + name);
        UNIT_ASSERT_C(counter, name);
        return counter->Val();
    }

    TEvRequest::TPtr Request(EPublicationMethod method) {
        auto event = CaSetup->Runtime->GrabEdgeEvent<TEvRequest>(Edge, WaitTimeout);
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(event->Get()->Method), static_cast<int>(method));
        return event;
    }

    void AssertError(const TString& message) {
        UNIT_ASSERT(Error.Wait(WaitTimeout));
        UNIT_ASSERT_STRING_CONTAINS(Error.GetValue().ToOneLineString(), message);
    }

    void FailWriteSession() {
        Gateway->WaitWriteSession("topic")->AddCloseSessionEvent(EStatus::UNAVAILABLE);
        AssertError("was closed. Status: UNAVAILABLE");
    }

    void AssertNoRequests() {
        // The sink shares the fake compute actor's mailbox. Drain the reply
        // before checking that it did not issue another SDK request.
        CaSetup->Execute([](TFakeActor&) {});
        UNIT_ASSERT(CaSetup->Runtime->CaptureMailboxEvents(Edge.Hint(), Edge.NodeId()).empty());
    }

private:
    TActorId Edge;
    NTestUtils::IMockPqGateway::TPtr Gateway;
    const NMonitoring::TDynamicCounterPtr Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    NThreading::TFuture<TSinkState> SavedState;
    NThreading::TFuture<NDqProto::TCheckpoint> CommittedState;
    NThreading::TFuture<TIssues> Error;
};

} // namespace

Y_UNIT_TEST_SUITE(TDqPqWriteActor) {
    Y_UNIT_TEST_F(CleansOnlyStalePublicationsBeforeFirstCreation, TWriteActorFixture) {
        Start();
        auto list = Request(EPublicationMethod::List);
        UNIT_ASSERT_VALUES_EQUAL(list->Get()->WriterIdentity, "query:execution:7:0");
        Write(); // More input while listing must not start another cleanup or publication.
        Sleep(TDuration::MilliSeconds(20));
        list->Get()->Reply(EStatus::SUCCESS, {
            {101, "query:execution:7:0:1:0", "query:execution:7:0"},
            {102, "query:execution:7:0:2:42", "query:execution:7:0"},
            {103, "query:execution:7:0:3:0", "query:execution:7:0"},
            {104, "query:execution:7:0:4:0", "query:execution:7:0"},
            {110, "query:execution:7:0:1:extra", "query:execution:7:0"},
            {111, "query:execution:7:0:1", "query:execution:7:0"},
            {112, "query:execution:7:0:1:0:extra", "query:execution:7:0"},
            {113, "query:execution:7:0:-1:0", "query:execution:7:0"},
            {114, "query:execution:7:0:9223372036854775808:0", "query:execution:7:0"},
            {115, "query:execution:7:0:1:18446744073709551616", "query:execution:7:0"},
            {116, "query:other:7:0:1:0", "query:execution:7:0"},
        });
        auto cancel = Request(EPublicationMethod::Cancel);
        UNIT_ASSERT_VALUES_EQUAL(cancel->Get()->PublicationId, 102);
        Write(); // New input must still wait for cancellation.
        Sleep(TDuration::MilliSeconds(20));
        cancel->Get()->Reply(EStatus::NOT_FOUND, {});
        cancel = Request(EPublicationMethod::Cancel);
        UNIT_ASSERT_VALUES_EQUAL(cancel->Get()->PublicationId, 101);
        cancel->Get()->Reply(EStatus::SUCCESS, {});

        auto begin = Request(EPublicationMethod::Begin);
        UNIT_ASSERT_VALUES_EQUAL(begin->Get()->ExternalId, "query:execution:7:0:3:0");
        UNIT_ASSERT_VALUES_EQUAL(begin->Get()->WriterIdentity, "query:execution:7:0");
        begin->Get()->Reply(EStatus::SUCCESS, {});
        AssertSaved();

        UNIT_ASSERT_VALUES_EQUAL(Counter("Listed"), 11);
        UNIT_ASSERT_VALUES_EQUAL(Counter("Canceled"), 1); // NOT_FOUND did not cancel a publication.
        UNIT_ASSERT_VALUES_EQUAL(Counter("ListRequests"), 1);
        UNIT_ASSERT_VALUES_EQUAL(Counter("CancelRequests"), 2);
        UNIT_ASSERT(Counter("LastListLatencyUs") >= 20000);
        UNIT_ASSERT(Counter("LastAvgCancelLatencyMs") >= 10);

        // Advancing the checkpoint creates the next publication without listing again.
        begin = Request(EPublicationMethod::Begin);
        UNIT_ASSERT_VALUES_EQUAL(begin->Get()->ExternalId, "query:execution:7:0:3:1");
        begin->Get()->Reply(EStatus::SUCCESS, {});
    }

    Y_UNIT_TEST_F(CreatesPublicationAfterEmptyList, TWriteActorFixture) {
        Start();
        Request(EPublicationMethod::List)->Get()->Reply(EStatus::SUCCESS, {});
        Request(EPublicationMethod::Begin)->Get()->Reply(EStatus::SUCCESS, {});
        AssertSaved();
        UNIT_ASSERT_VALUES_EQUAL(Counter("Listed"), 0);
        UNIT_ASSERT_VALUES_EQUAL(Counter("Canceled"), 0);
        UNIT_ASSERT_VALUES_EQUAL(Counter("ListRequests"), 1);
        UNIT_ASSERT_VALUES_EQUAL(Counter("CancelRequests"), 0);
        UNIT_ASSERT_VALUES_EQUAL(Counter("LastAvgCancelLatencyMs"), 0);
    }

    Y_UNIT_TEST_TWIN_F(PreservesRestoredPublicationDuringCommit, CommitBeforeListReply, TWriteActorFixture) {
        Start(/* restoredPublicationId */ 103, /* write */ false);
        // The coordinator sends commit before resuming the graph, but does not
        // wait for the asynchronous publication commit to complete.
        Commit(42, 2);
        auto publish = Request(EPublicationMethod::Publish);
        UNIT_ASSERT_VALUES_EQUAL(publish->Get()->PublicationId, 103);
        Write(1);
        auto list = Request(EPublicationMethod::List);
        if constexpr (CommitBeforeListReply) {
            publish->Get()->Reply(EStatus::SUCCESS, {});
            AssertCommitted();
        }
        list->Get()->Reply(EStatus::SUCCESS, {
            {101, "query:execution:7:0:2:0", "query:execution:7:0"},
            {103, "query:execution:7:0:2:1", "query:execution:7:0"},
        });
        auto cancel = Request(EPublicationMethod::Cancel);
        UNIT_ASSERT_VALUES_EQUAL(cancel->Get()->PublicationId, 101);
        cancel->Get()->Reply(EStatus::SUCCESS, {});
        Request(EPublicationMethod::Begin)->Get()->Reply(EStatus::SUCCESS, {});
        AssertSaved();
        if constexpr (!CommitBeforeListReply) {
            publish->Get()->Reply(EStatus::SUCCESS, {});
            AssertCommitted();
        }
        UNIT_ASSERT_VALUES_EQUAL(Counter("Canceled"), 1);
        Commit(1);
        publish = Request(EPublicationMethod::Publish);
        UNIT_ASSERT_VALUES_EQUAL(publish->Get()->PublicationId, 1);
        publish->Get()->Reply(EStatus::SUCCESS, {});
        AssertCommitted();
    }

    Y_UNIT_TEST_F(PreservesPendingCommitWhileCreatingNextPublication, TWriteActorFixture) {
        Start();
        Request(EPublicationMethod::List)->Get()->Reply(EStatus::SUCCESS, {});
        Request(EPublicationMethod::Begin)->Get()->Reply(EStatus::SUCCESS, {});
        AssertSaved();

        // Publication 1 is checkpointed and waiting for the coordinator.
        Write();
        auto begin = Request(EPublicationMethod::Begin);
        UNIT_ASSERT_VALUES_EQUAL(begin->Get()->ExternalId, "query:execution:7:0:3:1");
        Commit(1);
        auto publish = Request(EPublicationMethod::Publish);
        UNIT_ASSERT_VALUES_EQUAL(publish->Get()->PublicationId, 1);
        begin->Get()->Reply(EStatus::SUCCESS, {});
        publish->Get()->Reply(EStatus::SUCCESS, {});
        AssertCommitted();

        Write(2);
        AssertSaved(2);
        Commit(2);
        publish = Request(EPublicationMethod::Publish);
        UNIT_ASSERT_VALUES_EQUAL(publish->Get()->PublicationId, 2);
        publish->Get()->Reply(EStatus::SUCCESS, {});
        AssertCommitted();
        UNIT_ASSERT_VALUES_EQUAL(Counter("ListRequests"), 1);
        UNIT_ASSERT_VALUES_EQUAL(Counter("CancelRequests"), 0);
    }

    Y_UNIT_TEST_F(ListFailurePreventsPublicationCreation, TWriteActorFixture) {
        Start();
        Request(EPublicationMethod::List)->Get()->Reply(EStatus::UNAVAILABLE, {});
        AssertError("Failed to list stale deferred publications. Status: UNAVAILABLE");
        UNIT_ASSERT_VALUES_EQUAL(Counter("ListRequests"), 1);
        UNIT_ASSERT_VALUES_EQUAL(Counter("Listed"), 0);
        UNIT_ASSERT_VALUES_EQUAL(Counter("CancelRequests"), 0);
    }

    Y_UNIT_TEST_F(CancelFailurePreventsPublicationCreation, TWriteActorFixture) {
        Start();
        Request(EPublicationMethod::List)->Get()->Reply(EStatus::SUCCESS, {{101, "query:execution:7:0:1:0", "query:execution:7:0"}});
        Request(EPublicationMethod::Cancel)->Get()->Reply(EStatus::UNAUTHORIZED, {});
        AssertError("Failed to cancel stale deferred publication #101. Status: UNAUTHORIZED");
        UNIT_ASSERT_VALUES_EQUAL(Counter("Listed"), 1);
        UNIT_ASSERT_VALUES_EQUAL(Counter("CancelRequests"), 1);
        UNIT_ASSERT_VALUES_EQUAL(Counter("Canceled"), 0);
    }

    Y_UNIT_TEST_TWIN_F(StopsAfterListReplyToFailedWriter, HasStalePublication, TWriteActorFixture) {
        Start();
        auto list = Request(EPublicationMethod::List);
        FailWriteSession();

        std::vector<NTopic::TPublicationSummary> publications;
        if constexpr (HasStalePublication) {
            publications.push_back({101, "query:execution:7:0:1:0", "query:execution:7:0"});
        }
        list->Get()->Reply(EStatus::SUCCESS, std::move(publications));
        AssertNoRequests();
    }

    Y_UNIT_TEST_TWIN_F(StopsAfterCancelReplyToFailedWriter, HasMoreStalePublications, TWriteActorFixture) {
        Start();
        std::vector<NTopic::TPublicationSummary> publications = {
            {101, "query:execution:7:0:1:0", "query:execution:7:0"},
        };
        if constexpr (HasMoreStalePublications) {
            publications.push_back({102, "query:execution:7:0:2:0", "query:execution:7:0"});
        }
        Request(EPublicationMethod::List)->Get()->Reply(EStatus::SUCCESS, std::move(publications));
        auto cancel = Request(EPublicationMethod::Cancel);
        FailWriteSession();

        cancel->Get()->Reply(EStatus::SUCCESS, {});
        AssertNoRequests();
    }
}

} // namespace NYql::NDq
