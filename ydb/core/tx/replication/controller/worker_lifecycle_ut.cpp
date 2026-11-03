#include "controller_impl.h"
#include <ydb/core/transfer/transfer_writer.h>
#include <ydb/core/tx/replication/service/worker.h>
#include <ydb/library/testlib/helpers.h>
#include <atomic>

#include <ydb/core/base/statestorage.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/interconnect.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {

// Children of a split changefeed topic partition get their workers only when
// the controller handles the parent's TEvWorkerDataEnd. These tests cover the ways
// it was lost: node disconnect (session never restored), controller restart
// (DataEnd not resent), and re-adopted workers that were never rebooted.
Y_UNIT_TEST_SUITE(WorkerLifecycle) {
    using namespace NTestHelpers;

    struct TTransferErrorCounters {
        std::atomic<ui32> Starts{0};
        std::atomic<ui32> DataErrors{0};
        std::atomic<ui32> OtherStops{0};
        std::atomic<ui32> StopCommands{0};
    };

    // Runtime observers do not see ordinary actor deliveries with real threads.
    // Observe service input without changing or dropping any events.
    class TObservedReplicationService: public TDecorator {
    public:
        explicit TObservedReplicationService(std::shared_ptr<TTransferErrorCounters> counters)
            : TDecorator(THolder<IActor>(CreateReplicationService()))
            , Counters(std::move(counters))
        {}

        bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext&) override {
            if (ev->GetTypeRewrite() == TEvService::TEvRunWorker::EventType) {
                ++Counters->Starts;
            } else if (ev->GetTypeRewrite() == TEvService::TEvStopWorker::EventType) {
                ++Counters->StopCommands;
            } else if (ev->GetTypeRewrite() == NService::TEvWorker::TEvGone::EventType) {
                const auto* gone = ev->Get<NService::TEvWorker::TEvGone>();
                if (gone->Status == NService::TEvWorker::TEvGone::DATA_ERROR
                    && gone->ErrorDescription.Contains("Failed to decompress topic message:")) {
                    ++Counters->DataErrors;
                } else {
                    ++Counters->OtherStops;
                }
            }
            return true;
        }

    private:
        const std::shared_ptr<TTransferErrorCounters> Counters;
    };

    Y_UNIT_TEST_QUAD(BrokenCompressedTransferStopsWithIssue, Local, InternalDecompression) {
        for (const auto codec : {NYdb::NTopic::ECodec::GZIP, NYdb::NTopic::ECodec::ZSTD}) {
            TEnv env(TFeatureFlags().SetTransferInternalDataDecompression(InternalDecompression));
            auto& runtime = env.GetRuntime();
            runtime.GetAppData().TransferWriterFactory = std::make_shared<NTransfer::TTransferWriterFactory>();
            const auto counters = std::make_shared<TTransferErrorCounters>();
            runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)),
                runtime.Register(new TObservedReplicationService(counters)));

            NYdb::NTable::TTableClient tables(env.GetDriver());
            auto session = tables.CreateSession().GetValueSync().GetSession();
            auto scheme = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/sink` (partition Uint64 NOT NULL, key Uint64 NOT NULL, value Utf8, PRIMARY KEY (partition, key));
            )").GetValueSync();
            UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

            NYdb::NTopic::TTopicClient topics(env.GetDriver());
            auto topic = topics.CreateTopic("/Root/topic", NYdb::NTopic::TCreateTopicSettings()
                .BeginConfigurePartitioningSettings().MinActivePartitions(2).EndConfigurePartitioningSettings()
                .BeginAddConsumer().ConsumerName("consumer").EndAddConsumer()).GetValueSync();
            UNIT_ASSERT_C(topic.IsSuccess(), topic.GetIssues().ToString());
            auto write = [&](auto data, NYdb::NTopic::ECodec codec, ui32 partition = 0) {
                const auto producer = std::string("producer-") + std::to_string(partition);
                auto writer = topics.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
                    .Path("/Root/topic").ProducerId(producer).MessageGroupId(producer).PartitionId(partition).Codec(codec));
                UNIT_ASSERT(writer->Write(std::move(data)));
                UNIT_ASSERT(writer->Close());
            };
            write(std::string("good-message"), NYdb::NTopic::ECodec::RAW);

            TStringBuilder options;
            options << "CONSUMER = 'consumer', FLUSH_INTERVAL = Interval('PT1S')";
            if (!Local) {
                options << ", CONNECTION_STRING = 'grpc://" << env.GetEndpoint() << "/?database=/Root'";
            }
            scheme = session.ExecuteSchemeQuery(TStringBuilder() << R"(
                $l = ($x) -> { RETURN [<|partition: CAST($x._partition AS Uint64), key: $x._offset, value: CAST($x._data AS Utf8)|>]; };
                CREATE TRANSFER `/Root/broken_transfer` FROM `/Root/topic` TO `/Root/sink` USING $l
                WITH ()" << options << ");").GetValueSync();
            UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

            auto committedOffset = [&]() -> ui64 {
                auto consumer = topics.DescribeConsumer("/Root/topic", "consumer",
                    NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
                UNIT_ASSERT_C(consumer.IsSuccess(), consumer.GetIssues().ToString());
                const auto& partitions = consumer.GetConsumerDescription().GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 2);
                for (const auto& partition : partitions) {
                    if (partition.GetPartitionId() == 0) {
                        UNIT_ASSERT(partition.GetPartitionConsumerStats());
                        return partition.GetPartitionConsumerStats()->GetCommittedOffset();
                    }
                }
                UNIT_FAIL("Missing partition 0");
                return 0;
            };
            auto rows = [&]() -> ui64 {
                auto result = session.ExecuteDataQuery("SELECT COUNT(*) FROM `/Root/sink`",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto parser = result.GetResultSetParser(0);
                UNIT_ASSERT(parser.TryNextRow());
                return parser.ColumnParser(0).GetUint64();
            };
            const auto healthyDeadline = TInstant::Now() + TDuration::Seconds(60);
            while (TInstant::Now() < healthyDeadline && (rows() != 1 || committedOffset() != 1 || counters->Starts.load() != 2)) {
                Sleep(TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT_VALUES_EQUAL(rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(committedOffset(), 1);

            UNIT_ASSERT_VALUES_EQUAL(counters->Starts.load(), 2);
            write(NYdb::NTopic::TWriteMessage::CompressedMessage("baddata", codec, 12345), codec);

            const auto description = env.GetDescription("/Root/broken_transfer");
            const auto controllerId = description.GetPathDescription().GetReplicationDescription().GetControllerId();
            auto describe = [&]() {
                auto request = MakeHolder<TEvController::TEvDescribeReplication>();
                env.GetPathId("/Root/broken_transfer").ToProto(request->Record.MutablePathId());
                auto result = env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
                UNIT_ASSERT_C(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS,
                    result->Get()->Record.ShortDebugString());
                return result->Get()->Record;
            };
            const auto errorDeadline = TInstant::Now() + TDuration::Seconds(30);
            while (TInstant::Now() < errorDeadline
                && (!describe().GetState().HasError() || counters->StopCommands.load() == 0)) {
                Sleep(TDuration::MilliSeconds(100));
            }
            auto checkError = [&]() {
                const auto state = describe();
                UNIT_ASSERT_C(state.GetState().HasError(), state.ShortDebugString());
                const auto issue = state.GetState().GetError().ShortDebugString();
                UNIT_ASSERT_STRING_CONTAINS(issue, "Failed to decompress topic message");
                UNIT_ASSERT_STRING_CONTAINS(issue, Local || InternalDecompression ? "topic=/Root/topic" : "topic=topic");
                UNIT_ASSERT_STRING_CONTAINS(issue, "partition=0");
                UNIT_ASSERT_STRING_CONTAINS(issue, "offset=1");
                UNIT_ASSERT_VALUES_EQUAL(counters->DataErrors.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(counters->OtherStops.load(), 0);
                UNIT_ASSERT_VALUES_EQUAL(counters->Starts.load(), 2);
                UNIT_ASSERT_GE(counters->StopCommands.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(committedOffset(), 1);
                UNIT_ASSERT_VALUES_EQUAL(rows(), 1);
            };
            checkError();

            // The other partition must stop too, although its data is valid.
            write(std::string("must-not-be-transferred"), NYdb::NTopic::ECodec::RAW, 1);
            Sleep(TDuration::Seconds(2));
            checkError();

            const auto lookupEdge = runtime.AllocateEdgeActor();
            auto generation = [&]() {
                runtime.Send(new IEventHandle(MakeStateStorageProxyID(), lookupEdge,
                    new TEvStateStorage::TEvLookup(controllerId, 0)));
                auto info = runtime.GrabEdgeEvent<TEvStateStorage::TEvInfo>(lookupEdge);
                return info && info->Get()->Status == NKikimrProto::OK ? info->Get()->CurrentGeneration : 0;
            };
            const auto beforeRestart = generation();
            ForwardToTablet(runtime, controllerId, env.GetSender(), new TEvents::TEvPoisonPill());
            const auto rebootDeadline = TInstant::Now() + TDuration::Seconds(30);
            while (TInstant::Now() < rebootDeadline && generation() <= beforeRestart) {
                Sleep(TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT_GT(generation(), beforeRestart);
            InvalidateTabletResolverCache(runtime, controllerId);
            Sleep(TDuration::Seconds(2));
            checkError();
            Cerr << "KIKIMR-27525 local=" << Local << " internal=" << InternalDecompression
                << " codec=" << static_cast<ui32>(codec) << " data_errors=" << counters->DataErrors.load()
                << " worker_start_requests=" << counters->Starts.load()
                << " state=Error committed_offset=1 rows=1 survives_controller_restart=1" << Endl;

            scheme = session.ExecuteSchemeQuery("DROP TRANSFER `/Root/broken_transfer`;").GetValueSync();
            UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        }
    }

    struct TSetup {
        TEnv<> Env;
        TActorId Service;
        TActorId LookupEdge;
        ui64 ControllerId = 0;
        TString StreamPath;
        TWorkerId Root{0, 0, 0};

        explicit TSetup(bool mockService = true) {
            auto& runtime = Env.GetRuntime();
            LookupEdge = runtime.AllocateEdgeActor();
            runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

            if (mockService) {
                Service = runtime.AllocateEdgeActor();
                runtime.RegisterService(
                    MakeReplicationServiceId(runtime.GetNodeId(0)),
                    runtime.Register(CreateReplicationMockService(Service))
                );
            } else {
                runtime.RegisterService(
                    MakeReplicationServiceId(runtime.GetNodeId(0)),
                    runtime.Register(CreateReplicationService())
                );
            }

            auto result = Session().ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/table` (key Uint32, value Utf8, PRIMARY KEY (key));
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            // goes through the changefeed initial scan
            Upsert(1, 100);

            result = Session().ExecuteSchemeQuery(Sprintf(R"(
                CREATE ASYNC REPLICATION `replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://%s/?database=/Root"
                );
            )", Env.GetEndpoint().c_str())).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            ControllerId = Env.GetDescription("/Root/replication")
                .GetPathDescription().GetReplicationDescription().GetControllerId();

            if (mockService) {
                Handshake({});

                auto ev = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(Service, TDuration::Seconds(30));
                UNIT_ASSERT_C(ev, "root worker was not started");
                Root = TWorkerId::Parse(ev->Get()->Record.GetWorker());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(WaitReplicated(100, TDuration::Seconds(60)), 100);
            }

            const auto desc = Env.GetDescription("/Root/table");
            for (const auto& stream : desc.GetPathDescription().GetTable().GetCdcStreams()) {
                StreamPath = "/Root/table/" + stream.GetName();
            }
            UNIT_ASSERT_C(StreamPath, desc.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(Partitions().size(), 1);
        }

        NYdb::NTable::TSession Session() {
            NYdb::NTable::TTableClient client(Env.GetDriver(), NYdb::NTable::TClientSettings()
                .DiscoveryEndpoint(Env.GetEndpoint())
                .Database(Env.GetDatabase())
            );
            return client.CreateSession().GetValueSync().GetSession();
        }

        void Upsert(ui32 from, ui32 to) {
            TStringBuilder query;
            query << "UPSERT INTO `/Root/table` (key, value) VALUES ";
            for (ui32 i = from; i <= to; ++i) {
                query << (i == from ? "" : ", ") << "(" << i << "u, \"" << TString(1000, 'a') << "\"u)";
            }
            auto result = Session().ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ui64 WaitReplicated(ui64 expected, TDuration timeout) {
            ui64 count = 0;
            for (const auto deadline = TInstant::Now() + timeout; TInstant::Now() < deadline; Sleep(TDuration::MilliSeconds(500))) {
                auto result = Session().ExecuteDataQuery("SELECT COUNT(*) FROM `/Root/replica`",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                if (!result.IsSuccess()) {
                    continue;
                }
                auto parser = result.GetResultSetParser(0);
                UNIT_ASSERT(parser.TryNextRow());
                count = parser.ColumnParser(0).GetUint64();
                if (count == expected) {
                    break;
                }
            }
            Cerr << "... replicated " << count << " of " << expected << Endl;
            return count;
        }

        ui32 Generation() {
            auto& runtime = Env.GetRuntime();
            runtime.Send(new IEventHandle(MakeStateStorageProxyID(), LookupEdge, new TEvStateStorage::TEvLookup(ControllerId, 0)));
            const auto ev = runtime.GrabEdgeEvent<TEvStateStorage::TEvInfo>(LookupEdge);
            return ev && ev->Get()->Status == NKikimrProto::OK ? ev->Get()->CurrentGeneration : 0;
        }

        // RebootTablet() waits for TEvBoot via the test dispatcher, which does not work with real threads
        void RebootController() {
            auto& runtime = Env.GetRuntime();
            const auto generation = Generation();
            ForwardToTablet(runtime, ControllerId, Env.GetSender(), new TEvents::TEvPoisonPill());
            for (int i = 0; i < 300 && Generation() <= generation; ++i) {
                Sleep(TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT_GT(Generation(), generation);
            InvalidateTabletResolverCache(runtime, ControllerId);
        }

        // Takes over the real service's session as if it were the current controller
        // generation, so DataEnd goes to the returned edge instead of the controller.
        TActorId HijackServiceSession() {
            auto& runtime = Env.GetRuntime();
            const auto generation = Generation();
            UNIT_ASSERT_GT(generation, 0);

            const auto edge = runtime.AllocateEdgeActor();
            runtime.Send(new IEventHandle(MakeReplicationServiceId(runtime.GetNodeId(0)), edge,
                new TEvService::TEvHandshake(ControllerId, generation)));
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvStatus>(edge));
            return edge;
        }

        // plays the replication service side of a (re)established session
        void Handshake(const TVector<TWorkerId>& alive) {
            auto ev = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(Service, TDuration::Seconds(30));
            UNIT_ASSERT_C(ev, "no handshake from controller");

            auto status = MakeHolder<TEvService::TEvStatus>();
            for (const auto& id : alive) {
                id.Serialize(*status->Record.AddWorkers());
            }
            Env.SendAsync(ControllerId, status.Release());
        }

        google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TPersQueueGroupDescription::TPartition> Partitions() {
            const auto desc = Env.GetDescription(StreamPath + "/streamImpl");
            return desc.GetPathDescription().GetPersQueueGroup().GetPartitions();
        }

        // what topic autopartitioning does on write load
        TVector<ui64> SplitRoot() {
            auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            auto& tx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            tx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
            tx.SetWorkingDir(StreamPath);
            tx.SetAllowAccessToPrivatePaths(true);
            tx.MutableAlterPersQueueGroup()->SetName("streamImpl");
            auto& split = *tx.MutableAlterPersQueueGroup()->AddSplit();
            split.SetPartition(0);
            split.SetSplitBoundary(TString(1, '\x80'));

            auto resp = Env.Send<TEvTxUserProxy::TEvProposeTransactionStatus>(MakeTxProxyID(), ev.Release());
            const auto status = resp->Get()->Record.GetStatus();
            UNIT_ASSERT_C(status == NTxProxy::TResultStatus::ExecComplete || status == NTxProxy::TResultStatus::ExecInProgress,
                resp->Get()->Record.ShortDebugString());

            for (int i = 0; i < 60 && Partitions().size() < 3; ++i) {
                Sleep(TDuration::MilliSeconds(500));
            }

            TVector<ui64> children;
            for (const auto& p : Partitions()) {
                if (p.GetParentPartitionIds().size()) {
                    children.push_back(p.GetPartitionId());
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
            return children;
        }

        // what the service sends when the reader gets TEndPartitionSessionEvent
        void DataEnd(const TVector<ui64>& children) {
            auto ev = MakeHolder<TEvService::TEvWorkerDataEnd>();
            Root.Serialize(*ev->Record.MutableWorker());
            ev->Record.SetPartitionId(Root.WorkerId());
            for (auto id : children) {
                ev->Record.AddChildPartitionsIds(id);
            }
            Env.SendAsync(ControllerId, ev.Release());
        }

        THashSet<ui64> WaitStarted(const TVector<ui64>& children, TDuration timeout) {
            THashSet<ui64> started;
            const auto deadline = TInstant::Now() + timeout;
            while (started.size() < children.size() && TInstant::Now() < deadline) {
                auto ev = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(Service, deadline - TInstant::Now());
                if (!ev) {
                    break;
                }
                const auto id = TWorkerId::Parse(ev->Get()->Record.GetWorker());
                Cerr << "... run worker " << id << Endl;
                if (Find(children, id.WorkerId()) != children.end()) {
                    started.insert(id.WorkerId());
                }
            }
            return started;
        }
    };

    Y_UNIT_TEST(ChildrenStartOnDataEnd) {
        TSetup s;
        const auto children = s.SplitRoot();

        s.DataEnd(children);
        UNIT_ASSERT_VALUES_EQUAL(s.WaitStarted(children, TDuration::Seconds(30)).size(), 2);
    }

    // No faults: the real service reads the parent to the end, reports DataEnd,
    // and the controller starts the children. Rows written after the split
    // can only reach the replica through the children.
    Y_UNIT_TEST(EndToEndSplit) {
        TSetup s(false);
        s.SplitRoot();

        s.Upsert(101, 200);
        UNIT_ASSERT_VALUES_EQUAL(s.WaitReplicated(200, TDuration::Seconds(60)), 200);
    }

    // The controller subscribes to the interconnect session with every worker node.
    // On TEvNodeDisconnected it deletes the replication session, but a new one is
    // created only for nodes that discovery reports as new. The node is still in
    // discovery, so the session is never re-established.
    Y_UNIT_TEST(SessionRestoredAfterNodeDisconnect) {
        TSetup s;
        auto& runtime = s.Env.GetRuntime();

        s.Env.SendAsync(s.ControllerId, new TEvInterconnect::TEvNodeDisconnected(runtime.GetNodeId(0)));

        auto ev = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Service, TDuration::Seconds(60));
        UNIT_ASSERT_C(ev, "no new session with the node after a transient disconnect");
    }

    // The parent's DataEnd is sent while the controller has no session with the node
    // (here it goes to a stand-in), then the node disconnects. The controller must
    // restore the session, and the service must resend DataEnd on the new handshake.
    // TEvNodeDisconnected is injected: the controller is on the same node, so it does
    // not subscribe to the interconnect session in this test.
    Y_UNIT_TEST(EndToEndDataEndLostOnNodeDisconnect) {
        TSetup s(false);
        auto& runtime = s.Env.GetRuntime();
        const auto edge = s.HijackServiceSession();

        s.SplitRoot();
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvWorkerDataEnd>(edge));

        s.Env.SendAsync(s.ControllerId, new TEvInterconnect::TEvNodeDisconnected(runtime.GetNodeId(0)));

        s.Upsert(101, 200);
        UNIT_ASSERT_VALUES_EQUAL_C(s.WaitReplicated(200, TDuration::Seconds(60)), 200,
            "rows written after the split are lost: child partitions are never read");
    }

    // After a controller restart running workers are re-adopted from TEvStatus.
    // A re-adopted child (its command is not persisted) must be rebooted when it stops.
    Y_UNIT_TEST(AdoptedWorkerRebootedAfterStop) {
        TSetup s;
        const auto children = s.SplitRoot();

        s.DataEnd(children);
        UNIT_ASSERT_VALUES_EQUAL(s.WaitStarted(children, TDuration::Seconds(30)).size(), 2);

        s.RebootController();
        const TWorkerId child(s.Root.ReplicationId(), s.Root.TargetId(), children.at(0));
        s.Handshake({s.Root, child, TWorkerId(s.Root.ReplicationId(), s.Root.TargetId(), children.at(1))});

        s.Env.SendAsync(s.ControllerId, new TEvService::TEvWorkerStatus(child, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));

        const auto started = s.WaitStarted({children.at(0)}, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL_C(started.size(), 1, "re-adopted worker is not rebooted");
    }

    // The parent's DataEnd goes to a controller generation that is gone
    // (here: a stand-in with the same generation), then the controller restarts.
    // The service must resend DataEnd on the next handshake.
    Y_UNIT_TEST(EndToEndDataEndLost) {
        TSetup s(false);
        auto& runtime = s.Env.GetRuntime();
        const auto edge = s.HijackServiceSession();

        s.SplitRoot();
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvWorkerDataEnd>(edge));

        s.RebootController();

        s.Upsert(101, 200);
        UNIT_ASSERT_VALUES_EQUAL_C(s.WaitReplicated(200, TDuration::Seconds(60)), 200,
            "rows written after the split are lost: child partitions are never read");
    }
}

}
