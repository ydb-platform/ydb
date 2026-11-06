#include "table_writer.h"
#include "topic_reader.h"
#include "worker.h"
#include "service.h"

#include <ydb/core/tx/replication/common/schema_change.h>

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

#include <util/string/builder.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(ServiceCredentials) {
    using namespace NTestHelpers;

    // Reader creation stays blocked: these tests exercise cache identity and
    // ownership without creating SDK sessions or contacting IAM services.
    class TCredentialCacheEnv {
    public:
        TEnv<> Env{true, ERuntimeMode::Simulated};
        TTestActorRuntime& Runtime = Env.GetRuntime();
        const TActorId Edge = Env.GetSender();
        const TActorId Service = Runtime.Register(CreateReplicationService());
        NActors::TBlockEvents<TEvYdbProxy::TEvCreateTopicReaderRequest> Blocked{Runtime};
        THashSet<TActorId> Poisoned;

        TCredentialCacheEnv()
            : PoisonObserver(Runtime.AddObserver<TEvents::TEvPoison>([this](TEvents::TEvPoison::TPtr& ev) {
                Poisoned.insert(ev->GetRecipientRewrite());
            }))
        {
            Runtime.SetFastSimulatedTime(false);
            Env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
                .Name = "Table",
                .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
            }));
            Runtime.Send(new IEventHandle(Service, Edge, new TEvService::TEvHandshake(42, 1)));
            UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvService::TEvStatus>(Edge, TDuration::Seconds(30)));
        }

        TActorId Start(ui64 id, const NKikimrReplication::TConnectionParams& credentials) {
            auto run = MakeHolder<TEvService::TEvRunWorker>();
            run->Record.MutableController()->SetTabletId(42);
            run->Record.MutableController()->SetGeneration(1);
            auto* worker = run->Record.MutableWorker();
            worker->SetReplicationId(1);
            worker->SetTargetId(1);
            worker->SetWorkerId(id);
            auto* command = run->Record.MutableCommand();
            command->SetDatabase("/Root");
            auto* reader = command->MutableRemoteTopicReader();
            auto* connection = reader->MutableConnectionParams();
            connection->CopyFrom(credentials);
            connection->SetEndpoint(Env.GetEndpoint());
            connection->SetDatabase(Env.GetDatabase());
            reader->SetTopicPath("/Root/topic");
            reader->SetConsumerName("consumer");
            reader->SetTopicPartitionId(0);
            Env.GetPathId("/Root/Table").ToProto(command->MutableLocalTableWriter()->MutablePathId());
            const auto requestCount = Blocked.size();
            Runtime.Send(new IEventHandle(Service, Edge, run.Release()));
            RequireStatus(id, NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
            Runtime.WaitFor("remote reader request", [&] { return Blocked.size() == requestCount + 1; }, TDuration::Seconds(30));
            Workers.emplace(id, Blocked.back()->Sender);
            return Blocked.back()->GetRecipientRewrite();
        }

        void Stop(ui64 id) {
            auto stop = MakeHolder<TEvService::TEvStopWorker>();
            stop->Record.MutableController()->SetTabletId(42);
            stop->Record.MutableController()->SetGeneration(1);
            stop->Record.MutableWorker()->SetReplicationId(1);
            stop->Record.MutableWorker()->SetTargetId(1);
            stop->Record.MutableWorker()->SetWorkerId(id);
            Runtime.Send(new IEventHandle(Service, Edge, stop.Release()));
            RequireStatus(id, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED);
            const auto reader = Workers.at(id);
            Runtime.WaitFor("worker stopped its reader", [&] { return Poisoned.contains(reader); }, TDuration::Seconds(30));
            Workers.erase(id);
        }

        void Finish() {
            while (!Workers.empty()) {
                Stop(Workers.begin()->first);
            }
            THashSet<TActorId> readers;
            THashSet<TActorId> proxies;
            for (const auto& request : Blocked) {
                const auto reader = request->Sender;
                const auto proxy = request->GetRecipientRewrite();
                readers.insert(reader);
                proxies.insert(proxy);
                // A stopped reader awaits its pending create response. Complete
                // teardown only, without delivering the held SDK request.
                Runtime.Send(new IEventHandle(reader, Edge, new TEvents::TEvPoison()));
                Runtime.Send(new IEventHandle(reader, proxy, new TEvYdbProxy::TEvCreateTopicReaderResponse(TActorId{})));
            }
            Blocked.clear();
            Blocked.Stop();
            Runtime.WaitFor("stopped pending readers", [&] {
                for (const auto& reader : readers) {
                    if (Runtime.FindActor(reader)) {
                        return false;
                    }
                }
                return true;
            }, TDuration::Seconds(30));
            Runtime.Send(new IEventHandle(Service, Edge, new TEvents::TEvPoison()));
            Runtime.Send(new IEventHandle(Env.GetYdbProxy(), Edge, new TEvents::TEvPoison()));
            Runtime.WaitFor("cached proxies stopped", [&] {
                for (const auto& proxy : proxies) {
                    if (Runtime.FindActor(proxy)) {
                        return false;
                    }
                }
                return !Runtime.FindActor(Service) && !Runtime.FindActor(Env.GetYdbProxy());
            }, TDuration::Seconds(30));
            PoisonObserver.Remove();
        }

    private:
        void RequireStatus(ui64 id, NKikimrReplication::TEvWorkerStatus::EStatus expected) {
            auto status = Runtime.GrabEdgeEvent<TEvService::TEvWorkerStatus>(Edge, TDuration::Seconds(30));
            UNIT_ASSERT(status);
            const auto& record = status->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetWorker().GetReplicationId(), 1);
            UNIT_ASSERT_VALUES_EQUAL(record.GetWorker().GetTargetId(), 1);
            UNIT_ASSERT_VALUES_EQUAL(record.GetWorker().GetWorkerId(), id);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(record.GetStatus()), static_cast<int>(expected));
        }

        THashMap<ui64, TActorId> Workers;
        TTestActorRuntime::TEventObserverHolder PoisonObserver;
    };

    Y_UNIT_TEST(StaticCredentialsReuseProxyAndPreserveExistingUsers) {
        TCredentialCacheEnv env;
        NKikimrReplication::TConnectionParams first;
        first.MutableStaticCredentials()->SetUser("user");
        first.MutableStaticCredentials()->SetPassword("password1");
        const auto original = env.Start(1, first);
        NKikimrReplication::TConnectionParams same;
        same.CopyFrom(first);
        UNIT_ASSERT_VALUES_EQUAL(env.Start(2, same), original);
        auto changed = first;
        changed.MutableStaticCredentials()->SetPassword("password2");
        const auto replacement = env.Start(3, changed);
        UNIT_ASSERT(replacement != original);
        UNIT_ASSERT(env.Runtime.FindActor(original));
        UNIT_ASSERT(!env.Poisoned.contains(original));
        env.Stop(3);
        UNIT_ASSERT(env.Runtime.FindActor(original));
        UNIT_ASSERT(!env.Poisoned.contains(original));
        env.Finish();
    }

    Y_UNIT_TEST(IamCredentialsReuseProxyByAccountAndResource) {
        TCredentialCacheEnv env;
        NKikimrReplication::TConnectionParams credentials;
        credentials.MutableIamCredentials()->SetServiceAccountId("account1");
        credentials.MutableIamCredentials()->SetResourceId("resource1");
        const auto original = env.Start(1, credentials);
        UNIT_ASSERT_VALUES_EQUAL(env.Start(2, credentials), original);
        credentials.MutableIamCredentials()->SetServiceAccountId("account2");
        UNIT_ASSERT(env.Start(3, credentials) != original);
        credentials.MutableIamCredentials()->SetServiceAccountId("account1");
        credentials.MutableIamCredentials()->SetResourceId("resource2");
        UNIT_ASSERT(env.Start(4, credentials) != original);
        env.Finish();
    }

    Y_UNIT_TEST(OAuthCredentialsReuseProxyByToken) {
        TCredentialCacheEnv env;
        NKikimrReplication::TConnectionParams credentials;
        credentials.MutableOAuthToken()->SetToken("token1");
        const auto original = env.Start(1, credentials);
        UNIT_ASSERT_VALUES_EQUAL(env.Start(2, credentials), original);
        credentials.MutableOAuthToken()->SetToken("token2");
        UNIT_ASSERT(env.Start(3, credentials) != original);
        env.Finish();
    }

    Y_UNIT_TEST(CredentialKindsUseSeparateProxies) {
        TCredentialCacheEnv env;
        NKikimrReplication::TConnectionParams credentials;
        // Static and OAuth have equal identity/fingerprint payloads in the old
        // key representation, so their separation requires the credential kind.
        credentials.MutableStaticCredentials()->SetUser("");
        credentials.MutableStaticCredentials()->SetPassword("");
        const auto staticProxy = env.Start(1, credentials);
        credentials.MutableOAuthToken()->SetToken("");
        const auto oauthProxy = env.Start(2, credentials);
        credentials.MutableIamCredentials()->SetServiceAccountId("same-identity");
        credentials.MutableIamCredentials()->SetResourceId("test-resource");
        const auto iamProxy = env.Start(3, credentials);
        UNIT_ASSERT(staticProxy != oauthProxy);
        UNIT_ASSERT(staticProxy != iamProxy);
        UNIT_ASSERT(oauthProxy != iamProxy);
        env.Finish();
    }

    Y_UNIT_TEST(ChangedStaticPasswordStartsSourceReadOnSameService) {
        TEnv<true> env("user1", "password1", ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        runtime.SetFastSimulatedTime(false);
        const auto edge = env.GetSender();

        auto topic = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic",
                NYdb::NTopic::TCreateTopicSettings()
                    .BeginAddConsumer().ConsumerName("consumer").EndAddConsumer()));
        UNIT_ASSERT_C(topic->Get()->Result.IsSuccess(), topic->Get()->Result.GetIssues().ToString());
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
        }));

        const auto service = runtime.Register(CreateReplicationService());
        runtime.Send(new IEventHandle(service, edge, new TEvService::TEvHandshake(42, 1)), 0, true);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvStatus>(edge, TDuration::Seconds(30)));

        THashSet<TActorId> serverReadSessions;
        TTestActorRuntimeBase::TRegistrationObserver previousRegistrationObserver;
        previousRegistrationObserver = runtime.SetRegistrationObserverFunc(
            [&](TTestActorRuntimeBase& actorRuntime, const TActorId& parent, const TActorId& actorId) {
                previousRegistrationObserver(actorRuntime, parent, actorId);
                if (actorRuntime.FindActor(actorId)->GetActivityType() == NKikimrServices::TActivity::FRONT_PQ_READ) {
                    serverReadSessions.insert(actorId);
                }
            });
        Y_DEFER {
            runtime.SetRegistrationObserverFunc(std::move(previousRegistrationObserver));
        };

        auto oldRun = MakeHolder<TEvService::TEvRunWorker>();
        oldRun->Record.MutableController()->SetTabletId(42);
        oldRun->Record.MutableController()->SetGeneration(1);
        oldRun->Record.MutableWorker()->SetReplicationId(1);
        oldRun->Record.MutableWorker()->SetTargetId(1);
        oldRun->Record.MutableWorker()->SetWorkerId(0);
        auto* command = oldRun->Record.MutableCommand();
        command->SetDatabase("/Root");
        auto* reader = command->MutableRemoteTopicReader();
        auto* connection = reader->MutableConnectionParams();
        connection->SetEndpoint(env.GetEndpoint());
        connection->SetDatabase(env.GetDatabase());
        connection->MutableStaticCredentials()->SetUser("user1");
        connection->MutableStaticCredentials()->SetPassword("password1");
        reader->SetTopicPath("/Root/topic");
        reader->SetConsumerName("consumer");
        reader->SetTopicPartitionId(0);
        env.GetPathId("/Root/Table").ToProto(command->MutableLocalTableWriter()->MutablePathId());

        auto newRun = MakeHolder<TEvService::TEvRunWorker>();
        newRun->Record.CopyFrom(oldRun->Record);
        newRun->Record.MutableCommand()->MutableRemoteTopicReader()->MutableConnectionParams()
            ->MutableStaticCredentials()->SetPassword("password2");
        const auto workerId = oldRun->Record.GetWorker();

        auto requireStatus = [&](NKikimrReplication::TEvWorkerStatus::EStatus expected) {
            auto status = runtime.GrabEdgeEvent<TEvService::TEvWorkerStatus>(edge, TDuration::Seconds(30));
            UNIT_ASSERT(status);
            UNIT_ASSERT_VALUES_EQUAL(status->Get()->Record.GetWorker().SerializeAsString(), workerId.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(status->Get()->Record.GetStatus()), static_cast<int>(expected));
        };

        NActors::TBlockEvents<TEvYdbProxy::TEvCreateTopicReaderRequest> blocked(runtime);
        runtime.Send(new IEventHandle(service, edge, oldRun.Release()));
        requireStatus(NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
        runtime.WaitFor("first remote reader request", [&] { return blocked.size() == 1; }, TDuration::Seconds(30));
        const auto oldReader = blocked.front()->Sender;
        const auto oldProxy = blocked.front()->GetRecipientRewrite();

        auto stop = MakeHolder<TEvService::TEvStopWorker>();
        stop->Record.MutableController()->SetTabletId(42);
        stop->Record.MutableController()->SetGeneration(1);
        stop->Record.MutableWorker()->CopyFrom(workerId);
        runtime.Send(new IEventHandle(service, edge, stop.Release()));
        requireStatus(NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED);

        constexpr auto data = R"({"key":[7], "update":{"value":"after-rotation"}})";
        UNIT_ASSERT(env.RunCall([&] { return WriteTopic(env, "/Root/topic", data); }));

        env.RotatePassword("user1", "password2", "password1");

        TActorId oldSession;
        bool oldSessionPoisoned = false;
        auto observeResponse = runtime.AddObserver<TEvYdbProxy::TEvCreateTopicReaderResponse>(
            [&](TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
                if (ev->Sender == oldProxy && ev->GetRecipientRewrite() == oldReader) {
                    oldSession = ev->Get()->Result;
                }
            });
        auto observePoison = runtime.AddObserver<TEvents::TEvPoison>([&](TEvents::TEvPoison::TPtr& ev) {
            if (oldSession && ev->GetRecipientRewrite() == oldSession) {
                oldSessionPoisoned = true;
            }
        });
        blocked.Stop().Unblock(1);
        runtime.WaitFor("old reader cleanup", [&] { return oldSessionPoisoned; }, TDuration::Seconds(30));
        UNIT_ASSERT(oldSession);
        observeResponse.Remove();
        observePoison.Remove();

        struct TReadProof {
            ui32 Partition;
            ui64 Offset;
            TString Data;
        };
        TVector<TReadProof> reads;
        auto observeData = runtime.AddObserver<TEvWorker::TEvData>([&](TEvWorker::TEvData::TPtr& ev) {
            for (const auto& record : ev->Get()->Records) {
                reads.push_back({ev->Get()->PartitionId, record.GetOffset(), record.GetData()});
            }
        });
        TActorId newSession;
        TActorId newProxy;
        auto observeNewReader = runtime.AddObserver<TEvYdbProxy::TEvCreateTopicReaderResponse>(
            [&](TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
                newSession = ev->Get()->Result;
                newProxy = ev->Sender;
            });

        runtime.Send(new IEventHandle(service, edge, newRun.Release()));
        requireStatus(NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
        TDispatchOptions readOptions;
        readOptions.CustomFinalCondition = [&] { return !reads.empty(); };
        readOptions.FinalEvents.emplace_back([](IEventHandle&) { return false; });
        runtime.DispatchEvents(readOptions, TDuration::Seconds(30));
        UNIT_ASSERT_C(!reads.empty(), "replacement worker did not read source after password rotation");
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reads[0].Partition, 0);
        UNIT_ASSERT_VALUES_EQUAL(reads[0].Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(reads[0].Data, data);
        observeData.Remove();
        observeNewReader.Remove();
        UNIT_ASSERT(newSession);
        UNIT_ASSERT(newProxy);
        UNIT_ASSERT(newProxy != oldProxy);

        THashSet<TActorId> cleanupActors{service, env.GetYdbProxy(), oldReader, oldSession, oldProxy, newSession, newProxy};
        auto observeShutdown = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->HasEvent() && ev->GetTypeRewrite() == TEvents::TEvPoison::EventType
                && cleanupActors.contains(ev->Sender))
            {
                cleanupActors.insert(ev->GetRecipientRewrite());
            }
        });

        auto stopNew = MakeHolder<TEvService::TEvStopWorker>();
        stopNew->Record.MutableController()->SetTabletId(42);
        stopNew->Record.MutableController()->SetGeneration(1);
        stopNew->Record.MutableWorker()->CopyFrom(workerId);
        runtime.Send(new IEventHandle(service, edge, stopNew.Release()));
        requireStatus(NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED);
        runtime.WaitFor("source readers stopped", [&] {
            return !runtime.FindActor(oldReader) && !runtime.FindActor(oldSession) && !runtime.FindActor(newSession);
        }, TDuration::Seconds(30));
        runtime.Send(new IEventHandle(service, edge, new TEvents::TEvPoison()));
        runtime.Send(new IEventHandle(env.GetYdbProxy(), edge, new TEvents::TEvPoison()));
        runtime.WaitFor("owned replication actors stopped", [&] {
            for (const auto& actor : cleanupActors) {
                if (runtime.FindActor(actor)) {
                    return false;
                }
            }
            return true;
        }, TDuration::Seconds(30));
        UNIT_ASSERT_C(!serverReadSessions.empty(), "source read session was not registered on the server");
        for (const auto& actor : serverReadSessions) {
            if (runtime.FindActor(actor)) {
                runtime.Send(new IEventHandle(actor, edge, new NGRpcProxy::V1::TEvPQProxy::TEvDone()));
            }
        }
        runtime.WaitFor("server read sessions stopped", [&] {
            for (const auto& actor : serverReadSessions) {
                if (runtime.FindActor(actor)) {
                    return false;
                }
            }
            return true;
        }, TDuration::Seconds(30));
        observeShutdown.Remove();
    }
}

Y_UNIT_TEST_SUITE(ServiceSchemaProtocol) {
    using namespace NTestHelpers;

    Y_UNIT_TEST(ReportsMatchControllerIndexMetadataSupport) {
        for (const bool supportsIndexes : {false, true}) {
            TEnv env;
            auto& runtime = env.GetRuntime();
            const auto edge = env.GetSender();
            auto topic = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(env.GetYdbProxy(),
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic",
                    NYdb::NTopic::TCreateTopicSettings()
                        .BeginAddConsumer().ConsumerName("consumer").EndAddConsumer()));
            UNIT_ASSERT_C(topic->Get()->Result.IsSuccess(), topic->Get()->Result.GetIssues().ToString());
            env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
                .Name = "Table",
                .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
            }));

            const auto service = runtime.Register(CreateReplicationService());
            auto handshake = MakeHolder<TEvService::TEvHandshake>(42, 1);
            handshake->Record.SetSupportsIndexMetadata(supportsIndexes);
            runtime.Send(new IEventHandle(service, edge, handshake.Release()));
            runtime.GrabEdgeEvent<TEvService::TEvStatus>(edge);
            auto run = MakeHolder<TEvService::TEvRunWorker>();
            run->Record.MutableController()->SetTabletId(42);
            run->Record.MutableController()->SetGeneration(1);
            run->Record.MutableWorker()->SetReplicationId(1);
            run->Record.MutableWorker()->SetTargetId(1);
            run->Record.MutableWorker()->SetWorkerId(0);
            auto* command = run->Record.MutableCommand();
            command->SetDatabase("/Root");
            auto* reader = command->MutableRemoteTopicReader();
            auto* connection = reader->MutableConnectionParams();
            connection->SetEndpoint(env.GetEndpoint());
            connection->SetDatabase(env.GetDatabase());
            connection->MutableOAuthToken()->SetToken("root@builtin");
            reader->SetTopicPath("/Root/topic");
            reader->SetConsumerName("consumer");
            reader->SetTopicPartitionId(0);
            env.GetPathId("/Root/Table").ToProto(command->MutableLocalTableWriter()->MutablePathId());
            runtime.Send(new IEventHandle(service, edge, run.Release()));
            runtime.GrabEdgeEvent<TEvService::TEvWorkerStatus>(edge);
            UNIT_ASSERT(WriteTopic(env, "/Root/topic", R"json({
                "tableChanges": [{"table": {
                    "schemaVersion": 2,
                    "indexes": {},
                    "columns": {"key": {"type": "Uint32"}, "value": {"type": "Utf8"}},
                    "primaryKeyColumnNames": ["key"]
                }}],
                "ts": [1, 1]
            })json"));
            const auto first = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeReport>(edge);
            UNIT_ASSERT_VALUES_EQUAL(first->Get()->Record.GetSchema().HasIndexes(), supportsIndexes);
            auto next = MakeHolder<TEvService::TEvHandshake>(42, 2);
            next->Record.SetSupportsIndexMetadata(!supportsIndexes);
            runtime.Send(new IEventHandle(service, edge, next.Release()));
            runtime.GrabEdgeEvent<TEvService::TEvStatus>(edge);
            const auto replay = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeReport>(edge);
            UNIT_ASSERT_VALUES_EQUAL(replay->Get()->Record.GetSchema().HasIndexes(), !supportsIndexes);
        }
    }
}

Y_UNIT_TEST_SUITE(Worker) {
    using namespace NTestHelpers;

    Y_UNIT_TEST(SchemaEqualityAcceptsMissingIndexMetadata) {
        NKikimrReplication::TSchemaChange legacy;
        legacy.SetSourceSchemaVersion(1);
        auto empty = legacy;
        empty.MutableIndexes();
        auto indexed = empty;
        indexed.MutableIndexes()->AddItems()->SetName("by_value");
        UNIT_ASSERT(IsSameSchemaChange(legacy, empty));
        UNIT_ASSERT(IsSameSchemaChange(empty, legacy));
        UNIT_ASSERT(IsSameSchemaChange(legacy, indexed));
        UNIT_ASSERT(!IsSameSchemaChange(empty, indexed));
        auto conflicting = indexed;
        conflicting.MutableIndexes()->MutableItems(0)->SetName("other");
        UNIT_ASSERT(!IsSameSchemaChange(indexed, conflicting));
        conflicting.ClearIndexes();
        conflicting.SetSourceSchemaVersion(2);
        UNIT_ASSERT(!IsSameSchemaChange(indexed, conflicting));
    }

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
            new TEvYdbProxy::TEvStartTopicReadingSession("first", 0, std::nullopt)));
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
            new TEvYdbProxy::TEvStartTopicReadingSession("replacement", 0, std::nullopt)));

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
        schema.MutableIndexes();

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
        release->Record.MutableSchema()->ClearIndexes();
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

    // Model receiver rollback using a table with old rows and deduplication offsets.
    void CheckReceiverRollbackState(bool restoreSnapshot) {
        TEnv env;
        auto& runtime = env.GetRuntime();
        const TString topicPath = "/Root/topic";
        const TString consumer = "consumer";
        const TString baseline = R"({"key":[1],"update":{"value":"before-snapshot"}})";

        NYdb::NTopic::TTopicClient topicClient(env.GetDriver());
        const auto created = topicClient.CreateTopic(topicPath, NYdb::NTopic::TCreateTopicSettings()
            .BeginAddConsumer().ConsumerName(consumer).EndAddConsumer()).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());

        for (const TString& name : {TString("Live"), TString("Snapshot")}) {
            UNIT_ASSERT_VALUES_EQUAL(env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
                .Name = name,
                .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
            })), NMsgBusProxy::MSTATUS_OK);
        }

        auto waitCommitted = [&](ui64 offset, bool sessionClosed = false) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            while (TInstant::Now() < deadline) {
                const auto result = topicClient.DescribeConsumer(topicPath, consumer,
                    NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)
                        .ClientTimeout(TDuration::Seconds(5))).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                const auto& partitions = result.GetConsumerDescription().GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
                const auto& stats = partitions.front().GetPartitionConsumerStats();
                UNIT_ASSERT(stats);
                if (stats->GetCommittedOffset() == offset && (!sessionClosed || stats->GetReadSessionId().empty())) {
                    return;
                }
            }
            UNIT_FAIL("Timed out waiting for offset " << offset << ", sessionClosed=" << sessionClosed);
        };

        NYdb::NTable::TTableClient tableClient(env.GetDriver());
        const auto sessionResult = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        auto readKeys = [&](const TString& tablePath) {
            const auto result = session.ExecuteDataQuery(
                TStringBuilder() << "SELECT key FROM `" << tablePath << "` ORDER BY key",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto parser = result.GetResultSetParser(0);
            TVector<ui32> keys;
            while (parser.TryNextRow()) {
                keys.push_back(parser.ColumnParser(0).GetOptionalUint32().value());
            }
            return keys;
        };

        auto startWorker = [&](const TString& tablePath) {
            return runtime.Register(CreateWorker(env.GetSender(),
                [ydbProxy = env.GetYdbProxy(), topicPath, consumer] {
                    return CreateRemoteTopicReader(ydbProxy, TEvYdbProxy::TTopicReaderSettings()
                        .ConsumerName(consumer)
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path(topicPath).AppendPartitionIds(0)));
                },
                [tablePathId = env.GetPathId(tablePath)] {
                    return CreateLocalTableWriter("/Root", tablePathId);
                }));
        };

        // Seed both the old row and its durable offset using the reader's source identity.
        const auto snapshotWriter = runtime.Register(CreateLocalTableWriter("/Root", env.GetPathId("/Root/Snapshot")));
        env.Send<TEvWorker::TEvHandshake>(snapshotWriter, new TEvWorker::TEvHandshake());
        env.Send<TEvWorker::TEvPoll>(snapshotWriter, new TEvWorker::TEvData(0, "0", {TTopicMessage(0, baseline)}));
        env.SendAsync(snapshotWriter, new TEvents::TEvPoison());

        auto worker = startWorker("/Root/Live");
        UNIT_ASSERT(WriteTopic(env, topicPath, baseline));
        TVector<ui32> allKeys{1};
        for (ui32 key = 2; key <= 31; ++key) {
            UNIT_ASSERT(WriteTopic(env, topicPath,
                TStringBuilder() << R"({"key":[)" << key << R"(],"update":{"value":"after-snapshot"}})"));
            allKeys.push_back(key);
        }
        waitCommitted(31);
        UNIT_ASSERT_VALUES_EQUAL(readKeys("/Root/Live"), allKeys);
        UNIT_ASSERT_VALUES_EQUAL(readKeys("/Root/Snapshot"), (TVector<ui32>{1}));

        env.SendAsync(worker, new TEvents::TEvPoison());
        // Close the old SDK read session before assigning the partition again.
        waitCommitted(31, true);
        const TString resumedTable = restoreSnapshot ? "/Root/Snapshot" : "/Root/Live";
        worker = startWorker(resumedTable);
        UNIT_ASSERT(WriteTopic(env, topicPath, R"({"key":[32],"update":{"value":"after-restart"}})"));
        waitCommitted(32);
        const auto keys = readKeys(resumedTable);
        env.SendAsync(worker, new TEvents::TEvPoison());
        waitCommitted(32, true);

        // Characterize the existing bug: rollback loses the 30 acknowledged rows.
        allKeys.push_back(32);
        if (restoreSnapshot) {
            UNIT_ASSERT_VALUES_EQUAL(keys, (TVector<ui32>{1, 32}));
            UNIT_ASSERT_VALUES_UNEQUAL(keys, allKeys);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(keys, allKeys);
        }
    }

    Y_UNIT_TEST(ReceiverRollbackStateSkipsCommittedRows) {
        CheckReceiverRollbackState(true);
    }

    Y_UNIT_TEST(OrdinaryWorkerRestartPreservesCommittedRows) {
        CheckReceiverRollbackState(false);
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

    void CheckReaderRetryLimit(ui32 successfulBatches) {
        class TWriter: public TActorBootstrapped<TWriter> {
        public:
            explicit TWriter(TActorId edge)
                : Edge(edge)
            {
            }

            void Bootstrap() {
                Become(&TThis::StateWork);
            }

            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                Send(Worker, new TEvWorker::TEvHandshake());
            }

            void Handle(TEvWorker::TEvData::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvData(
                    ev->Get()->PartitionId, ev->Get()->Source, ev->Get()->Records));
                Send(Worker, new TEvWorker::TEvPoll());
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvData, Handle);
                    cFunc(TEvents::TEvPoison::EventType, PassAway);
                }
            }

        private:
            const TActorId Edge;
            TActorId Worker;
        };

        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto edge = env.GetSender();
        const auto topicSession = runtime.AllocateEdgeActor();
        const auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("replicationConsumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path("/Root/topic")
                .AppendPartitionIds(0));

        const auto worker = runtime.Register(CreateWorker(edge,
            [edge, settings] { return CreateRemoteTopicReader(edge, settings); },
            [edge] { return new TWriter(edge); }));

        TActorId previousReader;
        auto createReader = [&] {
            TAutoPtr<IEventHandle> ev;
            runtime.GrabEdgeEvents<TEvYdbProxy::TEvCreateTopicReaderRequest, TEvWorker::TEvGone>(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvYdbProxy::TEvCreateTopicReaderRequest::EventType);
            const auto reader = ev->Sender;
            UNIT_ASSERT(reader != previousReader);
            previousReader = reader;

            runtime.Send(new IEventHandle(reader, edge,
                new TEvYdbProxy::TEvCreateTopicReaderResponse(topicSession)));
            const auto read = runtime.GrabEdgeEvent<TEvYdbProxy::TEvReadTopicRequest>(topicSession);
            UNIT_ASSERT_VALUES_EQUAL(read->Sender, reader);
            return reader;
        };

        const TString issue = "Injected session failure after reader handshake";
        auto failReader = [&](TActorId reader) {
            runtime.Send(new IEventHandle(reader, topicSession,
                new TEvYdbProxy::TEvTopicReaderGone(NYdb::TStatus(NYdb::EStatus::UNAVAILABLE,
                    NYdb::NIssue::TIssues{NYdb::NIssue::TIssue(issue)}))));
        };

        for (ui32 batch = 0; batch < successfulBatches; ++batch) {
            // Use almost all of the retry budget before making progress. Each
            // successful batch must allow another series of transient failures.
            for (ui32 attempt = 0; attempt < 2; ++attempt) {
                failReader(createReader());
            }

            const auto reader = createReader();
            runtime.Send(new IEventHandle(reader, topicSession,
                new TEvYdbProxy::TEvReadTopicResponse(0, TVector<TTopicMessage>{
                    TTopicMessage(batch, "data"),
                })));
            const auto data = runtime.GrabEdgeEvent<TEvWorker::TEvData>(edge);
            UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.front().GetOffset(), batch);
            const auto read = runtime.GrabEdgeEvent<TEvYdbProxy::TEvReadTopicRequest>(topicSession);
            UNIT_ASSERT_VALUES_EQUAL(read->Sender, reader);
            failReader(reader);
        }

        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            failReader(createReader());
        }

        TAutoPtr<IEventHandle> ev;
        runtime.GrabEdgeEvents<TEvYdbProxy::TEvCreateTopicReaderRequest, TEvWorker::TEvGone>(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvWorker::TEvGone::EventType);
        UNIT_ASSERT_VALUES_EQUAL(ev->Sender, worker);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::UNAVAILABLE);
        UNIT_ASSERT_C(ev->Get<TEvWorker::TEvGone>()->ErrorDescription.Contains(issue),
            ev->Get<TEvWorker::TEvGone>()->ErrorDescription);
    }

    Y_UNIT_TEST(ReaderRetryLimitAfterHandshake) {
        CheckReaderRetryLimit(0);
    }

    Y_UNIT_TEST(ReaderRetryBudgetResetsAfterData) {
        CheckReaderRetryLimit(3);
    }
}

}
