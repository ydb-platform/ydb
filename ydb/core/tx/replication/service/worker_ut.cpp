#include "table_writer.h"
#include "topic_reader.h"
#include "worker.h"
#include "service.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(Worker) {
    using namespace NTestHelpers;

    Y_UNIT_TEST(ReaderRestartBeforeSchemaCheckpointSkipsBufferedReplay) {
        class TWriter final : public TActor<TWriter> {
        public:
            TWriter(const TActorId& edge, const NKikimrReplication::TSchemaChange& schema)
                : TActor(&TThis::StateWork)
                , Edge(edge)
                , Schema(schema)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                Send(Worker, new TEvWorker::TEvHandshake());
            }

            void Handle(TEvWorker::TEvData::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvData(
                    ev->Get()->PartitionId, ev->Get()->Source, ev->Get()->Records));
                if (!BarrierReported) {
                    BarrierReported = true;
                    Send(Worker, new TEvWorker::TEvSchemaChange(Schema, 1));
                } else {
                    Send(Worker, new TEvWorker::TEvPoll());
                }
            }

            void Handle(TEvService::TEvSchemaChangeResult::TPtr&) {
                Send(Worker, new TEvWorker::TEvSchemaChangeApplied(Schema));
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvData, Handle);
                    hFunc(TEvService::TEvSchemaChangeResult, Handle);
                }
            }

            const TActorId Edge;
            const NKikimrReplication::TSchemaChange Schema;
            TActorId Worker;
            bool BarrierReported = false;
        };

        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto edge = env.GetSender();
        const auto ydbProxy = runtime.AllocateEdgeActor();
        const auto firstReadSession = runtime.AllocateEdgeActor();
        const auto replacementReadSession = runtime.AllocateEdgeActor();

        NKikimrReplication::TSchemaChange schema;
        schema.MutableVersion()->SetStep(1);
        schema.MutableVersion()->SetTxId(1);
        schema.SetSourceSchemaVersion(1);

        const auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("consumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path("/Root/topic")
                .AppendPartitionIds(0));

        const auto worker = runtime.Register(CreateWorker(edge,
            [ydbProxy, settings] { return CreateRemoteTopicReader(ydbProxy, settings); },
            [edge, schema] { return new TWriter(edge, schema); }));

        auto create = runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvCreateTopicReaderRequest>(ydbProxy);
        const auto firstReader = create->Sender;
        runtime.Send(new IEventHandle(firstReader, ydbProxy,
            new TEvYdbProxy::TEvCreateTopicReaderResponse(firstReadSession)));
        runtime.Send(new IEventHandle(firstReader, firstReadSession,
            new TEvYdbProxy::TEvStartTopicReadingSession("first", 0)));
        runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvReadTopicRequest>(firstReadSession);

        runtime.Send(new IEventHandle(firstReader, firstReadSession,
            new TEvYdbProxy::TEvReadTopicResponse(0, TVector<TTopicMessage>{
                TTopicMessage(0, "old-schema"),
                TTopicMessage(1, "schema"),
                TTopicMessage(2, "retained-suffix"),
            })));
        auto data = runtime.GrabEdgeEventRethrow<TEvWorker::TEvData>(edge);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 3);

        auto commit = runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvCommitOffsetRequest>(ydbProxy);
        UNIT_ASSERT_VALUES_EQUAL(std::get<3>(commit->Get()->GetArgs()), 1);

        runtime.Send(new IEventHandle(firstReader, firstReadSession,
            new TEvYdbProxy::TEvTopicReaderGone(
                NYdb::TStatus(NYdb::EStatus::UNAVAILABLE, {}))));

        create = runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvCreateTopicReaderRequest>(ydbProxy);
        const auto replacementReader = create->Sender;
        UNIT_ASSERT_VALUES_UNEQUAL(replacementReader, firstReader);
        runtime.Send(new IEventHandle(replacementReader, ydbProxy,
            new TEvYdbProxy::TEvCreateTopicReaderResponse(replacementReadSession)));
        runtime.Send(new IEventHandle(replacementReader, replacementReadSession,
            new TEvYdbProxy::TEvStartTopicReadingSession("replacement", 0)));

        commit = runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvCommitOffsetRequest>(ydbProxy);
        UNIT_ASSERT_VALUES_EQUAL(std::get<3>(commit->Get()->GetArgs()), 1);
        runtime.Send(new IEventHandle(replacementReader, ydbProxy,
            new TEvYdbProxy::TEvCommitOffsetResponse(
                NYdb::TStatus(NYdb::EStatus::SUCCESS, {}))));
        runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvCommitOffsetRequest>(replacementReadSession);
        runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);

        auto release = MakeHolder<TEvService::TEvSchemaChangeResult>();
        release->Record.MutableSchema()->CopyFrom(schema);
        release->Record.SetOffset(1);
        runtime.Send(new IEventHandle(worker, edge, release.Release()));
        auto report = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        UNIT_ASSERT(report->Get()->Record.GetApplied());

        auto applied = MakeHolder<TEvService::TEvSchemaChangeResult>();
        applied->Record.MutableSchema()->CopyFrom(schema);
        applied->Record.SetOffset(1);
        applied->Record.SetApplied(true);
        runtime.Send(new IEventHandle(worker, edge, applied.Release()));

        commit = runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvCommitOffsetRequest>(ydbProxy);
        UNIT_ASSERT_VALUES_EQUAL(std::get<3>(commit->Get()->GetArgs()), 2);
        runtime.Send(new IEventHandle(replacementReader, ydbProxy,
            new TEvYdbProxy::TEvCommitOffsetResponse(
                NYdb::TStatus(NYdb::EStatus::SUCCESS, {}))));
        runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvCommitOffsetRequest>(replacementReadSession);
        report = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        UNIT_ASSERT(report->Get()->Record.GetCompleted());

        auto completed = MakeHolder<TEvService::TEvSchemaChangeResult>();
        completed->Record.MutableSchema()->CopyFrom(schema);
        completed->Record.SetOffset(1);
        completed->Record.SetApplied(true);
        completed->Record.SetCompleted(true);
        runtime.Send(new IEventHandle(worker, edge, completed.Release()));

        data = runtime.GrabEdgeEventRethrow<TEvWorker::TEvData>(edge);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.front().GetOffset(), 2);
        runtime.GrabEdgeEventRethrow<TEvYdbProxy::TEvReadTopicRequest>(replacementReadSession);

        runtime.Send(new IEventHandle(replacementReader, replacementReadSession,
            new TEvYdbProxy::TEvReadTopicResponse(0, TVector<TTopicMessage>{
                TTopicMessage(0, "old-schema"),
                TTopicMessage(1, "schema"),
                TTopicMessage(2, "retained-suffix"),
                TTopicMessage(3, "new-data"),
            })));
        data = runtime.GrabEdgeEventRethrow<TEvWorker::TEvData>(edge);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.front().GetOffset(), 3);
    }

    Y_UNIT_TEST(ReaderRestartRetriesTransferWriterCheckpoint) {
        class TReader final : public TActor<TReader> {
        public:
            TReader(const TActorId& edge, ui32& generation, TActorId& latestReader)
                : TActor(&TThis::StateWork)
                , Edge(edge)
                , Generation(++generation)
                , LatestReader(latestReader)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                LatestReader = SelfId();
                if (Generation == 2) {
                    Send(Edge, new TEvents::TEvWakeup());
                } else {
                    Start();
                }
            }

            void Handle(TEvents::TEvWakeup::TPtr&) {
                Start();
            }

            void Start() {
                Send(Worker, new TEvWorker::TEvHandshake());
                Send(Worker, new TEvWorker::TEvReaderStarted(0));
            }

            void Handle(TEvWorker::TEvPoll::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvPoll(ev->Get()->SkipCommit));
            }

            void Handle(TEvWorker::TEvCommit::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvCommit(ev->Get()->Offset));
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvents::TEvWakeup, Handle);
                    hFunc(TEvWorker::TEvPoll, Handle);
                    hFunc(TEvWorker::TEvCommit, Handle);
                }
            }

            const TActorId Edge;
            const ui32 Generation;
            TActorId& LatestReader;
            TActorId Worker;
        };

        class TWriter final : public TActor<TWriter> {
        public:
            TWriter(const TActorId& edge, TActorId& latestWriter)
                : TActor(&TThis::StateWork)
                , Edge(edge)
                , LatestWriter(latestWriter)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                LatestWriter = SelfId();
                Send(Worker, new TEvWorker::TEvHandshake());
            }

            void Handle(TEvWorker::TEvData::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvData(
                    ev->Get()->PartitionId, ev->Get()->Source, ev->Get()->Records));
            }

            void Handle(TEvents::TEvWakeup::TPtr&) {
                Send(Worker, new TEvWorker::TEvCommit(3));
                Send(Worker, new TEvWorker::TEvPoll());
                Send(Worker, TEvWorker::TEvStatus::FromOperation(EWorkerOperation::NONE));
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvData, Handle);
                    hFunc(TEvents::TEvWakeup, Handle);
                }
            }

            const TActorId Edge;
            TActorId& LatestWriter;
            TActorId Worker;
        };

        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto edge = env.GetSender();

        ui32 readerGeneration = 0;
        TActorId latestReader;
        TActorId latestWriter;
        const auto worker = runtime.Register(CreateWorker(edge,
            [edge, &readerGeneration, &latestReader] {
                return new TReader(edge, readerGeneration, latestReader);
            },
            [edge, &latestWriter] { return new TWriter(edge, latestWriter); }));

        runtime.GrabEdgeEventRethrow<TEvWorker::TEvPoll>(edge);
        const auto reader = latestReader;
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvData(0, "source", {
            TTopicMessage(0, "first"),
            TTopicMessage(1, "second"),
            TTopicMessage(2, "third"),
        })));
        runtime.GrabEdgeEventRethrow<TEvWorker::TEvData>(edge);

        runtime.Send(new IEventHandle(worker, reader,
            new TEvWorker::TEvGone(TEvWorker::TEvGone::UNAVAILABLE)));
        auto replacementHandshake = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        const auto replacementReader = latestReader;
        UNIT_ASSERT_VALUES_EQUAL(readerGeneration, 2);
        UNIT_ASSERT_VALUES_EQUAL(replacementHandshake->Sender, replacementReader);

        runtime.Send(new IEventHandle(latestWriter, edge, new TEvents::TEvWakeup()));
        runtime.GrabEdgeEventRethrow<TEvWorker::TEvStatus>(edge);

        runtime.Send(new IEventHandle(replacementReader, edge, new TEvents::TEvWakeup()));
        auto checkpoint = runtime.GrabEdgeEventRethrow<TEvWorker::TEvCommit>(edge);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->Sender, replacementReader);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->Get()->Offset, 3);
        runtime.Send(new IEventHandle(worker, replacementReader,
            new TEvWorker::TEvCommitResult(3)));

        runtime.Send(new IEventHandle(worker, replacementReader, new TEvWorker::TEvData(0, "source", {
            TTopicMessage(0, "first"),
            TTopicMessage(1, "second"),
            TTopicMessage(2, "third"),
        })));
        auto poll = runtime.GrabEdgeEventRethrow<TEvWorker::TEvPoll>(edge);
        UNIT_ASSERT_VALUES_EQUAL(poll->Sender, replacementReader);
    }

    Y_UNIT_TEST(RecoversCompletionFromDurableConsumerOffset) {
        class TReader final : public TActor<TReader> {
        public:
            explicit TReader(const TActorId& edge)
                : TActor(&TThis::StateWork)
                , Edge(edge)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                Send(Worker, new TEvWorker::TEvHandshake());
                Send(Edge, new TEvents::TEvWakeup());
            }
            void Handle(TEvents::TEvWakeup::TPtr&) {
                Started = true;
                Send(Worker, new TEvWorker::TEvReaderStarted(43));
                Send(Edge, new TEvents::TEvWakeup(1));
            }
            void Handle(TEvWorker::TEvPoll::TPtr& ev) { Send(ev->Forward(Edge)); }
            void Handle(TEvWorker::TEvCommit::TPtr& ev) {
                UNIT_ASSERT(Started);
                Send(ev->Forward(Edge));
            }
            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvPoll, Handle);
                    hFunc(TEvWorker::TEvCommit, Handle);
                    hFunc(TEvents::TEvWakeup, Handle);
                }
            }
            const TActorId Edge;
            TActorId Worker;
            bool Started = false;
        };

        class TWriter final : public TActor<TWriter> {
        public:
            explicit TWriter(const NKikimrReplication::TSchemaChange& schema)
                : TActor(&TThis::StateWork)
                , Schema(schema)
            {}
        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                Send(Worker, new TEvWorker::TEvHandshake());
            }
            void Handle(TEvWorker::TEvData::TPtr& ev) {
                Send(Worker, new TEvWorker::TEvSchemaChange(Schema, ev->Get()->Records.front().GetOffset()));
            }
            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvData, Handle);
                }
            }
            const NKikimrReplication::TSchemaChange Schema;
            TActorId Worker;
        };

        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto edge = env.GetSender();
        NKikimrReplication::TSchemaChange schema;
        schema.MutableVersion()->SetStep(1);
        schema.MutableVersion()->SetTxId(1);
        schema.SetSourceSchemaVersion(1);

        auto nextSchema = schema;
        nextSchema.MutableVersion()->SetStep(2);
        nextSchema.MutableVersion()->SetTxId(2);

        const auto worker = runtime.Register(CreateWorker(edge,
            [edge] { return new TReader(edge); },
            [nextSchema] { return new TWriter(nextSchema); }));
        auto readerReady = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        const auto reader = readerReady->Sender;
        runtime.GrabEdgeEventRethrow<TEvWorker::TEvPoll>(edge);
        runtime.Send(new IEventHandle(reader, edge, new TEvents::TEvWakeup()));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);

        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvData(0, "source", {
            TTopicMessage(100, R"({})")
        })));
        runtime.GrabEdgeEventRethrow<TEvWorker::TEvCommit>(edge);
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvGone(TEvWorker::TEvGone::UNAVAILABLE)));
        auto replacementReady = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        const auto replacementReader = replacementReady->Sender;
        runtime.Send(new IEventHandle(replacementReader, edge, new TEvents::TEvWakeup()));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        auto retriedCheckpoint = runtime.GrabEdgeEventRethrow<TEvWorker::TEvCommit>(edge);
        UNIT_ASSERT_VALUES_EQUAL(retriedCheckpoint->Get()->Offset, 100);
        runtime.Send(new IEventHandle(worker, replacementReader, new TEvWorker::TEvCommitResult(100)));
        runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);

        auto applied = MakeHolder<TEvService::TEvSchemaChangeResult>();
        applied->Record.MutableSchema()->CopyFrom(schema);
        applied->Record.SetOffset(42);
        applied->Record.SetApplied(true);
        runtime.Send(new IEventHandle(worker, edge, applied.Release()));

        auto completed = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());
        UNIT_ASSERT_VALUES_EQUAL(completed->Get()->Record.GetOffset(), 42);
        UNIT_ASSERT_VALUES_EQUAL(completed->Get()->Record.GetSchema().SerializeAsString(), schema.SerializeAsString());
    }

    Y_UNIT_TEST(WriterRestartAfterAppliedReplaysRelease) {
        class TReader final : public TActor<TReader> {
        public:
            TReader(const TActorId& edge, ui64 committedOffset)
                : TActor(&TThis::StateWork)
                , Edge(edge)
                , CommittedOffset(committedOffset)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                Send(Worker, new TEvWorker::TEvHandshake());
                Send(Worker, new TEvWorker::TEvReaderStarted(CommittedOffset));
                Send(Edge, new TEvents::TEvWakeup());
            }

            void Handle(TEvWorker::TEvCommit::TPtr& ev) {
                Send(ev->Forward(Edge));
            }

            void Handle(TEvWorker::TEvPoll::TPtr&) {
                Send(Edge, new TEvWorker::TEvPoll());
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvCommit, Handle);
                    hFunc(TEvWorker::TEvPoll, Handle);
                }
            }

            const TActorId Edge;
            const ui64 CommittedOffset;
            TActorId Worker;
        };

        class TWriter final : public TActor<TWriter> {
        public:
            TWriter(const NKikimrReplication::TSchemaChange& schema, ui32 generation)
                : TActor(&TThis::StateWork)
                , Schema(schema)
                , Generation(generation)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                Send(Worker, new TEvWorker::TEvHandshake());
            }

            void Handle(TEvWorker::TEvData::TPtr& ev) {
                Send(Worker, new TEvWorker::TEvSchemaChange(Schema, ev->Get()->Records.front().GetOffset()));
            }

            void Handle(TEvService::TEvSchemaChangeResult::TPtr&) {
                Send(Worker, new TEvWorker::TEvSchemaChangeApplied(Schema));
                if (Generation == 1) {
                    Send(Worker, new TEvWorker::TEvGone(TEvWorker::TEvGone::UNAVAILABLE));
                }
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvData, Handle);
                    hFunc(TEvService::TEvSchemaChangeResult, Handle);
                }
            }

            const NKikimrReplication::TSchemaChange Schema;
            const ui32 Generation;
            TActorId Worker;
        };

        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto edge = env.GetSender();

        NKikimrReplication::TSchemaChange schema;
        schema.MutableVersion()->SetStep(1);
        schema.MutableVersion()->SetTxId(1);
        schema.SetSourceSchemaVersion(1);
        schema.AddColumns()->SetName("key");
        schema.MutableColumns(0)->SetType("Uint64");
        schema.AddPrimaryKeyColumnNames("key");

        ui32 readerGeneration = 0;
        ui32 writerGeneration = 0;
        auto worker = runtime.Register(CreateWorker(edge,
            [edge, &readerGeneration] { return new TReader(edge, readerGeneration++ ? 43 : 0); },
            [schema, &writerGeneration] { return new TWriter(schema, ++writerGeneration); }));

        auto ready = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        const auto reader = ready->Sender;
        auto poll = runtime.GrabEdgeEventRethrow<TEvWorker::TEvPoll>(edge);
        UNIT_ASSERT_VALUES_EQUAL(poll->Sender, reader);
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvData(0, "source", {
            TTopicMessage(42, R"({\"tableChanges\":[]})")
        })));

        auto checkpoint = runtime.GrabEdgeEventRethrow<TEvWorker::TEvCommit>(edge);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->Get()->Offset, 42);
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvCommitResult(42)));

        auto report = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        UNIT_ASSERT(!report->Get()->Record.GetApplied());
        auto release = MakeHolder<TEvService::TEvSchemaChangeResult>();
        release->Record.MutableSchema()->CopyFrom(schema);
        release->Record.SetOffset(42);
        runtime.Send(new IEventHandle(worker, edge, release.Release()));

        report = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        UNIT_ASSERT(report->Get()->Record.GetApplied());
        UNIT_ASSERT_VALUES_EQUAL(writerGeneration, 2);

        auto applied = MakeHolder<TEvService::TEvSchemaChangeResult>();
        applied->Record.MutableSchema()->CopyFrom(schema);
        applied->Record.SetOffset(42);
        applied->Record.SetApplied(true);
        runtime.Send(new IEventHandle(worker, edge, applied.Release()));

        checkpoint = runtime.GrabEdgeEventRethrow<TEvWorker::TEvCommit>(edge);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->Get()->Offset, 43);
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvCommitResult(43)));

        report = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        if (!report->Get()->Record.GetCompleted()) {
            UNIT_ASSERT(report->Get()->Record.GetApplied());
            report = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        }
        UNIT_ASSERT(report->Get()->Record.GetCompleted());

        // Recreate only the reader after the post-schema checkpoint is
        // durable. The controller completion acknowledgement still belongs
        // to PendingSchemaChange and must finish that active barrier rather
        // than enter historical completion recovery.
        runtime.Send(new IEventHandle(worker, reader,
            new TEvWorker::TEvGone(TEvWorker::TEvGone::UNAVAILABLE)));
        ready = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        const auto replacementReader = ready->Sender;
        UNIT_ASSERT_VALUES_UNEQUAL(replacementReader, reader);

        auto completion = MakeHolder<TEvService::TEvSchemaChangeResult>();
        completion->Record.MutableSchema()->CopyFrom(schema);
        completion->Record.SetOffset(42);
        completion->Record.SetApplied(true);
        completion->Record.SetCompleted(true);
        runtime.Send(new IEventHandle(worker, edge, completion.Release()));
        poll = runtime.GrabEdgeEventRethrow<TEvWorker::TEvPoll>(edge);
        UNIT_ASSERT_VALUES_EQUAL(poll->Sender, replacementReader);
        UNIT_ASSERT_VALUES_EQUAL(writerGeneration, 2);
    }

    Y_UNIT_TEST(CompletionWaitsForReplacementWriterRefresh) {
        class TReader final : public TActor<TReader> {
        public:
            TReader(const TActorId& edge, TActorId& latestReader, bool& pollReceived)
                : TActor(&TThis::StateWork)
                , Edge(edge)
                , LatestReader(latestReader)
                , PollReceived(pollReceived)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                LatestReader = SelfId();
                Send(Worker, new TEvWorker::TEvHandshake());
                Send(Worker, new TEvWorker::TEvReaderStarted(0));
            }

            void Handle(TEvWorker::TEvPoll::TPtr& ev) {
                PollReceived = true;
                Send(Edge, new TEvWorker::TEvPoll(ev->Get()->SkipCommit));
            }

            void Handle(TEvWorker::TEvCommit::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvCommit(ev->Get()->Offset));
            }

            void Handle(TEvents::TEvWakeup::TPtr&) {
                Send(Edge, new TEvents::TEvWakeup());
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvPoll, Handle);
                    hFunc(TEvWorker::TEvCommit, Handle);
                    hFunc(TEvents::TEvWakeup, Handle);
                }
            }

            const TActorId Edge;
            TActorId& LatestReader;
            bool& PollReceived;
            TActorId Worker;
        };

        class TWriter final : public TActor<TWriter> {
        public:
            TWriter(const TActorId& edge,
                    const NKikimrReplication::TSchemaChange& schema,
                    ui32& generation, TActorId& latestWriter)
                : TActor(&TThis::StateWork)
                , Edge(edge)
                , Schema(schema)
                , Generation(++generation)
                , LatestWriter(latestWriter)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                LatestWriter = SelfId();
                Send(Worker, new TEvWorker::TEvHandshake());
            }

            void Handle(TEvWorker::TEvData::TPtr& ev) {
                Send(Worker, new TEvWorker::TEvSchemaChange(
                    Schema, ev->Get()->Records.front().GetOffset()));
            }

            void Handle(TEvService::TEvSchemaChangeResult::TPtr&) {
                if (Generation == 2) {
                    Send(Edge, new TEvents::TEvWakeup());
                    return;
                }
                Send(Worker, new TEvWorker::TEvSchemaChangeApplied(Schema));
            }

            void Handle(TEvents::TEvWakeup::TPtr&) {
                Send(Worker, new TEvWorker::TEvSchemaChangeApplied(Schema));
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvData, Handle);
                    hFunc(TEvService::TEvSchemaChangeResult, Handle);
                    hFunc(TEvents::TEvWakeup, Handle);
                }
            }

            const TActorId Edge;
            const NKikimrReplication::TSchemaChange Schema;
            const ui32 Generation;
            TActorId& LatestWriter;
            TActorId Worker;
        };

        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto edge = env.GetSender();

        NKikimrReplication::TSchemaChange schema;
        schema.MutableVersion()->SetStep(1);
        schema.MutableVersion()->SetTxId(1);
        schema.SetSourceSchemaVersion(1);

        ui32 writerGeneration = 0;
        TActorId latestReader;
        TActorId latestWriter;
        bool pollReceived = false;
        const auto worker = runtime.Register(CreateWorker(edge,
            [edge, &latestReader, &pollReceived] {
                return new TReader(edge, latestReader, pollReceived);
            },
            [edge, schema, &writerGeneration, &latestWriter] {
                return new TWriter(edge, schema, writerGeneration, latestWriter);
            }));

        auto poll = runtime.GrabEdgeEventRethrow<TEvWorker::TEvPoll>(edge);
        pollReceived = false;
        const auto reader = latestReader;
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvData(0, "source", {
            TTopicMessage(42, "schema")
        })));

        auto checkpoint = runtime.GrabEdgeEventRethrow<TEvWorker::TEvCommit>(edge);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->Get()->Offset, 42);
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvCommitResult(42)));
        runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);

        auto release = MakeHolder<TEvService::TEvSchemaChangeResult>();
        release->Record.MutableSchema()->CopyFrom(schema);
        release->Record.SetOffset(42);
        runtime.Send(new IEventHandle(worker, edge, release.Release()));
        auto report = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        UNIT_ASSERT(report->Get()->Record.GetApplied());

        auto applied = MakeHolder<TEvService::TEvSchemaChangeResult>();
        applied->Record.MutableSchema()->CopyFrom(schema);
        applied->Record.SetOffset(42);
        applied->Record.SetApplied(true);
        runtime.Send(new IEventHandle(worker, edge, applied.Release()));
        checkpoint = runtime.GrabEdgeEventRethrow<TEvWorker::TEvCommit>(edge);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->Get()->Offset, 43);
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvCommitResult(43)));
        report = runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);
        UNIT_ASSERT(report->Get()->Record.GetCompleted());

        runtime.Send(new IEventHandle(worker, latestWriter,
            new TEvWorker::TEvGone(TEvWorker::TEvGone::UNAVAILABLE)));
        auto replacementRefresh = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT_VALUES_EQUAL(writerGeneration, 2);
        UNIT_ASSERT_VALUES_EQUAL(replacementRefresh->Sender, latestWriter);

        auto completed = MakeHolder<TEvService::TEvSchemaChangeResult>();
        completed->Record.MutableSchema()->CopyFrom(schema);
        completed->Record.SetOffset(42);
        completed->Record.SetApplied(true);
        completed->Record.SetCompleted(true);
        runtime.Send(new IEventHandle(worker, edge, completed.Release()));
        runtime.Send(new IEventHandle(worker, edge,
            TEvWorker::TEvStatus::FromOperation(EWorkerOperation::NONE)));
        runtime.GrabEdgeEventRethrow<TEvWorker::TEvStatus>(edge);
        runtime.Send(new IEventHandle(reader, edge, new TEvents::TEvWakeup()));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(!pollReceived);

        runtime.Send(new IEventHandle(latestWriter, edge, new TEvents::TEvWakeup()));
        poll = runtime.GrabEdgeEventRethrow<TEvWorker::TEvPoll>(edge);
        UNIT_ASSERT_VALUES_EQUAL(poll->Sender, reader);
    }

    Y_UNIT_TEST(SchemaReleaseWaitsForReplacementWriterBarrier) {
        class TReader final : public TActor<TReader> {
        public:
            TReader(const TActorId& edge, TActorId& latestReader)
                : TActor(&TThis::StateWork)
                , Edge(edge)
                , LatestReader(latestReader)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                LatestReader = SelfId();
                Send(Worker, new TEvWorker::TEvHandshake());
                Send(Worker, new TEvWorker::TEvReaderStarted(0));
            }

            void Handle(TEvWorker::TEvPoll::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvPoll(ev->Get()->SkipCommit));
            }

            void Handle(TEvWorker::TEvCommit::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvCommit(ev->Get()->Offset));
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvPoll, Handle);
                    hFunc(TEvWorker::TEvCommit, Handle);
                }
            }

            const TActorId Edge;
            TActorId& LatestReader;
            TActorId Worker;
        };

        class TWriter final : public TActor<TWriter> {
        public:
            TWriter(const TActorId& edge,
                    const NKikimrReplication::TSchemaChange& schema,
                    ui32& generation, TActorId& latestWriter)
                : TActor(&TThis::StateWork)
                , Edge(edge)
                , Schema(schema)
                , Generation(++generation)
                , LatestWriter(latestWriter)
            {}

        private:
            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                LatestWriter = SelfId();
                if (Generation == 2) {
                    Send(Edge, new TEvents::TEvWakeup());
                } else {
                    Send(Worker, new TEvWorker::TEvHandshake());
                }
            }

            void Handle(TEvWorker::TEvData::TPtr& ev) {
                Send(Worker, new TEvWorker::TEvSchemaChange(
                    Schema, ev->Get()->Records.front().GetOffset()));
            }

            void Handle(TEvService::TEvSchemaChangeResult::TPtr&) {
                Send(Edge, new TEvents::TEvWakeup());
            }

            void Handle(TEvents::TEvWakeup::TPtr&) {
                Send(Worker, new TEvWorker::TEvHandshake());
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvData, Handle);
                    hFunc(TEvService::TEvSchemaChangeResult, Handle);
                    hFunc(TEvents::TEvWakeup, Handle);
                }
            }

            const TActorId Edge;
            const NKikimrReplication::TSchemaChange Schema;
            const ui32 Generation;
            TActorId& LatestWriter;
            TActorId Worker;
        };

        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto edge = env.GetSender();

        NKikimrReplication::TSchemaChange schema;
        schema.MutableVersion()->SetStep(1);
        schema.MutableVersion()->SetTxId(1);
        schema.SetSourceSchemaVersion(1);

        ui32 writerGeneration = 0;
        TActorId latestReader;
        TActorId latestWriter;
        const auto worker = runtime.Register(CreateWorker(edge,
            [edge, &latestReader] { return new TReader(edge, latestReader); },
            [edge, schema, &writerGeneration, &latestWriter] {
                return new TWriter(edge, schema, writerGeneration, latestWriter);
            }));

        runtime.GrabEdgeEventRethrow<TEvWorker::TEvPoll>(edge);
        const auto reader = latestReader;
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvData(0, "source", {
            TTopicMessage(42, "schema")
        })));

        auto checkpoint = runtime.GrabEdgeEventRethrow<TEvWorker::TEvCommit>(edge);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->Get()->Offset, 42);
        runtime.Send(new IEventHandle(worker, reader, new TEvWorker::TEvCommitResult(42)));
        runtime.GrabEdgeEventRethrow<TEvService::TEvSchemaChangeReport>(edge);

        runtime.Send(new IEventHandle(worker, latestWriter,
            new TEvWorker::TEvGone(TEvWorker::TEvGone::UNAVAILABLE)));
        auto replacementHandshake = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT_VALUES_EQUAL(writerGeneration, 2);
        UNIT_ASSERT_VALUES_EQUAL(replacementHandshake->Sender, latestWriter);

        auto release = MakeHolder<TEvService::TEvSchemaChangeResult>();
        release->Record.MutableSchema()->CopyFrom(schema);
        release->Record.SetOffset(42);
        runtime.Send(new IEventHandle(worker, edge, release.Release()));
        runtime.Send(new IEventHandle(worker, edge,
            TEvWorker::TEvStatus::FromOperation(EWorkerOperation::NONE)));
        runtime.GrabEdgeEventRethrow<TEvWorker::TEvStatus>(edge);
        UNIT_ASSERT(runtime.CaptureMailboxEvents(latestWriter.Hint(), latestWriter.NodeId()).empty());

        runtime.Send(new IEventHandle(latestWriter, edge, new TEvents::TEvWakeup()));
        auto released = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT_VALUES_EQUAL(released->Sender, latestWriter);
    }

    Y_UNIT_TEST(Basic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(env.GetYdbProxy(),
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic",
                    NYdb::NTopic::TCreateTopicSettings()
                        .BeginAddConsumer()
                            .ConsumerName("consumer")
                        .EndAddConsumer()
            ));

            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }

        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        }));

        auto createReaderFn = [ydbProxy = env.GetYdbProxy()]() {
            return CreateRemoteTopicReader(ydbProxy,
                TEvYdbProxy::TTopicReaderSettings()
                    .ConsumerName("consumer")
                    .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                        .Path("/Root/topic")
                        .AppendPartitionIds(0)
                    )
            );
        };

        auto createWriterFn = [tablePathId = env.GetPathId("/Root/Table")]() {
            return CreateLocalTableWriter("/Root", tablePathId);
        };

        auto worker = env.GetRuntime().Register(CreateWorker(env.GetSender(), std::move(createReaderFn), std::move(createWriterFn)));
        Y_UNUSED(worker);

        UNIT_ASSERT(WriteTopic(env, "/Root/topic", R"({"key":[1], "update":{"value":"10"}})"));
        UNIT_ASSERT(WriteTopic(env, "/Root/topic", R"({"key":[2], "update":{"value":"20"}})"));
        UNIT_ASSERT(WriteTopic(env, "/Root/topic", R"({"key":[3], "update":{"value":"30"}})"));
    }
}

}
