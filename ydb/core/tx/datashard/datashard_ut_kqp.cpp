#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>


using namespace NKikimr;
using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace Tests;
using namespace NKqp;
using namespace NYql;
using namespace NYql::NDq;


namespace {

ui32 CalcDrops(const NDqProto::TDqExecutionStats& profile) {
    ui32 count = 0;
    for (auto& stage : profile.GetStages()) {
        for (auto& ca : stage.GetComputeActors()) {
            for (auto& task: ca.GetTasks()) {
                for (auto& inputChannels: task.GetInputChannels()) {
                    count += inputChannels.GetResentMessages();
                }
                for (auto& outputChannels: task.GetOutputChannels()) {
                    count += outputChannels.GetResentMessages();
                }
            }
        }
    }
    return count;
}

} // namespace

class KqpStabilityTests : public TTestBase {
public:
    void SetUp() override;

    void TearDown() override {
        GetDqExecutionSettingsForTests().Reset();
    }

    void BeforeTest(const char* test) {
        Cerr << "-- Before test " << test << Endl;
        AppCfg.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1);
        TTestBase::BeforeTest(test);
    }

    // disable due to https://a.yandex-team.ru/arc/trunk/arcadia/ydb/core/testlib/test_client.cpp#L448
    UNIT_TEST_SUITE(KqpStabilityTests);
//        UNIT_TEST(DropChannelDataWithUnreachable);
//        UNIT_TEST(DropChannelDataWithUnknownActor);
//        UNIT_TEST(DropChannelDataWithDisconnected);     // TEvKqpCompute::TEvChannelsInfo can be lost
//        UNIT_TEST(DropChannelDataAckWithUnreachable);
//        UNIT_TEST(DropChannelDataAckWithUnknownActor);
//        UNIT_TEST(DropChannelDataAckWithDisconnected);  // TEvKqpCompute::TEvChannelsInfo can be lost
//        UNIT_TEST(AbortOnDisconnect);
    UNIT_TEST_SUITE_END();

    void DropChannelDataWithUnreachable();
    void DropChannelDataWithUnknownActor();
    void DropChannelDataWithDisconnected();
    void DropChannelDataAckWithUnreachable();
    void DropChannelDataAckWithUnknownActor();
    void DropChannelDataAckWithDisconnected();
    void AbortOnDisconnect();

private:
    struct TDropInfo {
        TString EventName;
        ui32 EventType = 0;
        ui64 ChannelId = 0;
        ui64 SeqNo = 0;
        TMaybe<TEvents::TEvUndelivered::EReason> Reason;
        std::function<ui64(NActors::IEventHandle*)> SeqNoExtractor;
        i32 Counter = 1;
        ui32 DroppedEvents = 0;

        ui32 StartedScans = 0;
        bool ActionDone = false;

        bool Matches(TAutoPtr<NActors::IEventHandle>& event) const {
            return event->GetTypeRewrite() == EventType
                   && event->Cookie == ChannelId
                   && (!SeqNoExtractor || SeqNoExtractor(event.Get()) == SeqNo);
        }

        void Attach(NActors::TTestActorRuntime* runtime) {
            runtime->SetObserverFunc([this, runtime](TAutoPtr<NActors::IEventHandle>& event) {

                    if (event->GetTypeRewrite() == TEvDataShard::TEvProposeTransactionResult::EventType) {
                        auto status = event.Get()->Get<TEvDataShard::TEvProposeTransactionResult>()->GetStatus();
                        if (status == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE) {
                            StartedScans++;
                            if (StartedScans == 3 && !ActionDone && Counter <= 0 && !Reason) {
                                auto edge = runtime->AllocateEdgeActor(0);
                                runtime->Send(new IEventHandle(runtime->GetInterconnectProxy(0, 1), edge,
                                    new TEvInterconnect::TEvPoisonSession), 0, true);
                                ActionDone = true;
                            }
                        }
                    }

                    if (Counter > 0 && Matches(event)) {
                        Cerr << "-- DROP " << EventName << ", channel: " << ChannelId << ", seqNo: " << SeqNo << Endl;

                        ++DroppedEvents;
                        Counter--;

                        if (Reason) {
                            auto evUndelivered = new TEvents::TEvUndelivered(event->GetTypeRewrite(), *Reason);
                            auto handle = new IEventHandle(event->Sender, TActorId(), evUndelivered, 0, event->Cookie);
                            runtime->Send(handle, 0, true);
                            ActionDone = true;
                        } else {
                            if (Counter <= 0 && StartedScans == 3 && !ActionDone) {
                                auto edge = runtime->AllocateEdgeActor(0);
                                runtime->Send(new IEventHandle(runtime->GetInterconnectProxy(0, 1), edge,
                                    new TEvInterconnect::TEvPoisonSession), 0, true);
                                ActionDone = true;
                            }
                        }

                        return NActors::TTestActorRuntime::EEventAction::DROP;
                    }

                    if (Matches(event) && Counter <= 0 && !ActionDone) {
                        return NActors::TTestActorRuntime::EEventAction::DROP;
                    }

                    return NActors::TTestActorRuntime::EEventAction::PROCESS;
                });
        }
    };

    void DoRun(TDropInfo& dropInfo, TMaybe<TString> error) {
        dropInfo.Attach(Server->GetRuntime());

        TMaybe<NDqProto::TDqExecutionStats> profile;

        auto client = Server->GetRuntime()->AllocateEdgeActor();
        SendRequest(*Server->GetRuntime(), client, MakeStreamRequest(client, Query, true));

        TStringStream out;
        NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
        writer.OnBeginList();

        while (true) {
            TAutoPtr<IEventHandle> handle;
            auto replies = Server->GetRuntime()->GrabEdgeEventsRethrow<TEvKqp::TEvQueryResponse, TEvKqp::TEvAbortExecution,
                TEvKqpExecuter::TEvStreamData, TEvKqpExecuter::TEvStreamProfile>(handle);

            if (auto* ev = std::get<TEvKqp::TEvQueryResponse*>(replies)) {
                auto& response = ev->Record.GetRef();

                if (!error) {
                    UNIT_ASSERT_EQUAL_C(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS, response.Utf8DebugString());
                    UNIT_ASSERT(response.GetResponse().GetYdbResults().empty());

                    writer.OnEndList();
                    UNIT_ASSERT_STRINGS_EQUAL(ReformatYson(R"(
                        [[[1u];[1u]];
                        [[10u];[1u]];
                        [[100u];[1u]];
                        [[1000u];[1u]];
                        [[4000000001u];[1u]];
                        [[4000000011u];[1u]];
                        [[4000000101u];[1u]];
                        [[4000001001u];[1u]]]
                    )"), ReformatYson(out.Str()));

                    UNIT_ASSERT(profile.Defined());
                    UNIT_ASSERT(dropInfo.DroppedEvents > 0);
                    UNIT_ASSERT(CalcDrops(*profile) > 0);
                    //UNIT_ASSERT_C(dropInfo.DroppedEvents >= CalcDrops(*profile), dropCount << " !>= " << CalcDrops(*profile));
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::GENERIC_ERROR);
                    UNIT_ASSERT_STRINGS_EQUAL(response.GetResponse().GetQueryIssues()[0].issues()[0].message(), *error);
                }
                break;
            }

            if (auto* ev = std::get<TEvKqp::TEvAbortExecution*>(replies)) {
                UNIT_FAIL(ev->GetIssues().ToOneLineString());
            }

            if (auto* ev = std::get<TEvKqpExecuter::TEvStreamData*>(replies)) {
                NYdb::TResultSet result = ev->Record.resultset();
                auto columns = result.GetColumnsMeta();

                NYdb::TResultSetParser parser(result);
                while (parser.TryNextRow()) {
                    writer.OnListItem();
                    writer.OnBeginList();
                    for (ui32 i = 0; i < columns.size(); ++i) {
                        writer.OnListItem();
                        FormatValueYson(parser.GetValue(i), writer);
                    }
                    writer.OnEndList();
                }

                continue;
            }

            if (auto* ev = std::get<TEvKqpExecuter::TEvStreamProfile*>(replies)) {
                profile = ev->Record.profile();
                continue;
            }
        }
    }

    NKikimrConfig::TAppConfig AppCfg;
    TPortManager PortManager;
    TServer::TPtr Server;
    TString Query;
};
UNIT_TEST_SUITE_REGISTRATION(KqpStabilityTests);

void KqpStabilityTests::SetUp() {
    Cerr << "-- SetUp\n";
    auto serverSettings = TServerSettings(PortManager.GetPort(2134))
        .SetDomainName("Root")
        .SetNodeCount(2)
        .SetUseRealThreads(false)
        .SetAppConfig(AppCfg);

    Server = new Tests::TServer(serverSettings);
    auto& runtime = *Server->GetRuntime();

//    runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::KQP_WORKER, NActors::NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::RPC_REQUEST, NActors::NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::KQP_RESOURCES_MANAGER, NActors::NLog::PRI_DEBUG);

    auto sender = runtime.AllocateEdgeActor();
    InitRoot(Server, sender);

    CreateShardedTable(Server, sender, "/Root", "table-1", 2);
    CreateShardedTable(Server, sender, "/Root", "table-2", 1);

    ExecSQL(Server, sender, R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES
            (1, 1), (2, 2), (3, 3),
            (10, 1), (20, 2), (30, 3),
            (100, 1), (200, 2), (300, 3),
            (1000, 1), (2000, 2), (3000, 3),
            (4000000001u, 1), (4000000002u, 2), (4000000003u, 3),
            (4000000011u, 1), (4000000012u, 2), (4000000013u, 3),
            (4000000101u, 1), (4000000102u, 2), (4000000103u, 3),
            (4000001001u, 1), (4000001002u, 2), (4000001003u, 3)
    )");
    ExecSQL(Server, sender, R"(
        UPSERT INTO `/Root/table-2` (key, value) VALUES
          (1, 1), (5, 5), (7, 7), (11, 11)
    )");

    GetDqExecutionSettingsForTests().FlowControl.MaxOutputChunkSize = 1;
    GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1.0f;

    //  Query Plan
    //
    //  +---------------------+
    //  | Scan: /Root/table-2 |
    //  | Stage: #0           |
    //  | Task: #1            |
    //  +---------------------+
    //        | (ch: #1, map)                     (ch: #3, bcast)
    //        ↓                         +-------------------------------------+
    //  +---------------+               |                                     |
    //  | Compute Actor |---------------+                                     ↓
    //  | Stage: #2     |                    +---------------------+    +---------------------+
    //  | Task: #2      |                    | Scan: /Root/table-1 |    | Scan: /Root/table-1 |
    //  |     Union     |------------------->| Stage: #1           |    | Stage: #1           |
    //  +---------------+  (ch: #2, bcast)   | Task: #3            |    | Task: #4            |
    //                                       |       MapJoin       |    |       MapJoin       |
    //                                       +---------------------+    +---------------------+
    //                                            (ch: #4, map) |          | (ch: #5, map)
    //                                                          ↓          ↓
    //                                                       +----------------+
    //                                                       | ComputeActor   |
    //                                                       | Stage: #3      |
    //                                                       | Task: #5       |
    //                                                       |  Union + Sort  |
    //                                                       +----------------+
    //                                                                | (ch: #6, map)
    //                                                                ↓
    //                                                       +----------------+
    //                                                       | ComputeActor   |
    //                                                       | Stage: #4      |
    //                                                       | Task: #6       |
    //                                                       |     Union      |
    //                                                       +----------------+
    //                                                                | (ch: 7, map)
    //                                                                ↓
    //                                                          +------------+
    //                                                          |   Result   |
    //                                                          +------------+

    Query = R"(
            SELECT t1.key AS a, t2.key AS b
            FROM `/Root/table-1` AS t1 JOIN `/Root/table-2` AS t2 ON t1.value = t2.key
            ORDER BY a, b
        )";
}

void KqpStabilityTests::DropChannelDataWithUnreachable() {
    for (ui64 channelId = 1; channelId < 8; ++channelId) {
        ui64 maxSeqNo = channelId < 4 ? 3 : 1;
        for (ui64 seqNo = 1; seqNo <= maxSeqNo; ++seqNo) {
            TDropInfo drop;
            drop.EventName = "TEvChannelData";
            drop.EventType = NYql::NDq::TEvDqCompute::TEvChannelData::EventType;
            drop.ChannelId = channelId;
            drop.SeqNo = seqNo;
            drop.Reason = TEvents::TEvUndelivered::EReason::Disconnected;
            drop.SeqNoExtractor = [](NActors::IEventHandle* handle) {
                return handle->Get<NYql::NDq::TEvDqCompute::TEvChannelData>()->Record.GetSeqNo();
            };
            drop.Counter = 3;

            // Cerr << "-- " << __func__ << " channelId: " << channelId << ", seqNo: " << seqNo << Endl;
            DoRun(drop, Nothing());
        }
    }
}

void KqpStabilityTests::DropChannelDataWithUnknownActor() {
    for (ui64 channelId = 1; channelId < 8; ++channelId) {
        ui64 maxSeqNo = channelId < 4 ? 3 : 1;
        for (ui64 seqNo = 1; seqNo <= maxSeqNo; ++seqNo) {
            TDropInfo drop;
            drop.EventName = "TEvChannelData";
            drop.EventType = NYql::NDq::TEvDqCompute::TEvChannelData::EventType;
            drop.ChannelId = channelId;
            drop.SeqNo = seqNo;
            drop.Reason = TEvents::TEvUndelivered::EReason::ReasonActorUnknown;
            drop.SeqNoExtractor = [](NActors::IEventHandle* handle) {
                return handle->Get<NYql::NDq::TEvDqCompute::TEvChannelData>()->Record.GetSeqNo();
            };
            drop.Counter = 3;

            // Cerr << "-- " << __func__ << " channelId: " << channelId << ", seqNo: " << seqNo << Endl;
            DoRun(drop, "Internal error while executing transaction.");
        }
    }
}

void KqpStabilityTests::DropChannelDataAckWithUnreachable() {
    for (ui64 channelId = 1; channelId < 7 /* TODO: add support for EXECUTOR<->CA */; ++channelId) {
        ui64 maxSeqNo = channelId < 4 ? 3 : 1;
        for (ui64 seqNo = 1; seqNo <= maxSeqNo; ++seqNo) {
            TDropInfo drop;
            drop.EventName = "TEvChannelDataAck";
            drop.EventType = NYql::NDq::TEvDqCompute::TEvChannelDataAck::EventType;
            drop.ChannelId = channelId;
            drop.SeqNo = seqNo;
            drop.Reason = TEvents::TEvUndelivered::EReason::Disconnected;
            drop.SeqNoExtractor = [](NActors::IEventHandle* handle) {
                return handle->Get<NYql::NDq::TEvDqCompute::TEvChannelDataAck>()->Record.GetSeqNo();
            };
            drop.Counter = 3;

            Cerr << "-- " << __func__ << " channelId: " << channelId << ", seqNo: " << seqNo << Endl;
            DoRun(drop, Nothing());
        }
    }
}

void KqpStabilityTests::DropChannelDataAckWithUnknownActor() {
    for (ui64 channelId = 1; channelId < 7 /* TODO: add support for EXECUTOR<->CA */; ++channelId) {
        ui64 maxSeqNo = channelId < 4 ? 3 : 1;
        for (ui64 seqNo = 1; seqNo <= maxSeqNo; ++seqNo) {
            TDropInfo drop;
            drop.EventName = "TEvChannelDataAck";
            drop.EventType = NYql::NDq::TEvDqCompute::TEvChannelDataAck::EventType;
            drop.ChannelId = channelId;
            drop.SeqNo = seqNo;
            drop.Reason = TEvents::TEvUndelivered::EReason::ReasonActorUnknown;
            drop.SeqNoExtractor = [](NActors::IEventHandle* handle) {
                return handle->Get<NYql::NDq::TEvDqCompute::TEvChannelDataAck>()->Record.GetSeqNo();
            };
            drop.Counter = 3;

            Cerr << "-- " << __func__ << " channelId: " << channelId << ", seqNo: " << seqNo << Endl;
            DoRun(drop, "Internal error while executing transaction.");
        }
    }
}

void KqpStabilityTests::DropChannelDataWithDisconnected() {
    GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 100.0f;
    Y_DEFER {
        GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1.0f;
    };

    TDropInfo drop;
    drop.EventName = "TEvChannelData";
    drop.EventType = NYql::NDq::TEvDqCompute::TEvChannelData::EventType;
    drop.ChannelId = 1;
    drop.Counter = 3; // drop 3 messages and then kill interconnect session

    DoRun(drop, Nothing());
}

void KqpStabilityTests::DropChannelDataAckWithDisconnected() {
    GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 100.0f;
    Y_DEFER {
        GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1.0f;
    };

    TDropInfo drop;
    drop.EventName = "TEvChannelDataAck";
    drop.EventType = NYql::NDq::TEvDqCompute::TEvChannelDataAck::EventType;
    drop.ChannelId = 1;
    drop.Counter = 2; // drop 3 messages and then kill interconnect session

    DoRun(drop, Nothing());
}

void KqpStabilityTests::AbortOnDisconnect() {
    GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 100.0f;
    Y_DEFER {
        GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1.0f;
    };

    TDropInfo drop;
    drop.EventName = "TEvChannelData";
    drop.EventType = NYql::NDq::TEvDqCompute::TEvChannelData::EventType;
    drop.ChannelId = 1;
    drop.Counter = 3; // drop 3 messages and then kill interconnect session

    drop.Attach(Server->GetRuntime());

    auto client = Server->GetRuntime()->AllocateEdgeActor();
    SendRequest(*Server->GetRuntime(), client, MakeStreamRequest(client, Query, true));

    while (true) {
        TAutoPtr<IEventHandle> handle;
        auto replies = Server->GetRuntime()->GrabEdgeEventsRethrow<TEvKqp::TEvQueryResponse, TEvKqp::TEvAbortExecution,
            TEvKqpExecuter::TEvStreamData, TEvKqpExecuter::TEvStreamProfile>(handle);

        if (auto* ev = std::get<TEvKqp::TEvQueryResponse*>(replies)) {
            auto& response = ev->Record.GetRef();

            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::ABORTED);
            UNIT_ASSERT_STRINGS_EQUAL(response.GetResponse().GetQueryIssues()[0].issues()[0].message(), "Table /Root/table-1 scan failed, reason: 2");
            return;
        }

        if (auto* ev = std::get<TEvKqp::TEvAbortExecution*>(replies)) {
            UNIT_FAIL(ev->GetIssues().ToOneLineString());
        }

        if (std::get<TEvKqpExecuter::TEvStreamData*>(replies)) {
            UNIT_FAIL("unexpected");
        }

        if (std::get<TEvKqpExecuter::TEvStreamProfile*>(replies)) {
            UNIT_FAIL("unexpected");
        }
    }
}
