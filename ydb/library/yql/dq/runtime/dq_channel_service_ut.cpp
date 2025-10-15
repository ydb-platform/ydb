#include "dq_channel_service.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/runtime/dq_channel_service_impl.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <library/cpp/threading/mux_event/mux_event.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

using namespace NKikimr::NKqp;
using namespace NYql::NDq;

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(Channels20) {

    bool ReadBlocked(std::shared_ptr<IChannelBuffer>& buffer, TDataChunk& data, TDuration timeout = TDuration::Seconds(10)) {
        auto deadline = TInstant::Now() + timeout;
        while (true) {
            if (buffer->Pop(data)) {
                return true;
            }
            Sleep(TDuration::MilliSeconds(10));
            if (TInstant::Now() > deadline) {
                return false;
            }
        }
    }

    bool ReadAsyncNotified(NActors::TTestActorRuntime& runtime, NActors::TActorId receiver, std::shared_ptr<IChannelBuffer>& buffer, TDataChunk& data, TDuration timeout = TDuration::Seconds(10)) {
        if (buffer->Pop(data)) {
            return true;
        }

        auto resume = runtime.GrabEdgeEvent<TEvDqCompute::TEvResumeExecution>(receiver, timeout)->Release();
        return resume && buffer->Pop(data);
    }

    Y_UNIT_TEST(LocalChannelBackPressure) {

        TKikimrRunner kikimr(TKikimrSettings{});
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto receiver = runtime.AllocateEdgeActor();

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId()), sender, new TEvPrivate::TEvServiceLookup());
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
        auto service = serviceReply->Service;

        TChannelInfo info(1, sender, receiver);
        auto senderBuffer = service->GetLocalBuffer(info);
        auto receiverBuffer = service->GetLocalBuffer(info);

        auto aggregator = std::make_shared<TDqFillAggregator>();
        senderBuffer->SetFillAggregator(aggregator);
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        UNIT_ASSERT(receiverBuffer->IsEmpty());

        while (senderBuffer->GetFillLevel() == EDqFillLevel::NoLimit) {
            senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));
        }
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::HardLimit);

        ui64 totalCount = 0;
        ui64 deltaCount = 0;

        while (true) {
            TDataChunk data;
            if (!receiverBuffer->Pop(data)) {
                break;
            }
            totalCount++;
            if (aggregator->GetFillLevel() == EDqFillLevel::HardLimit) {
                deltaCount++;
            }
            UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
        }
        UNIT_ASSERT(receiverBuffer->IsEmpty());
        UNIT_ASSERT(deltaCount * 4 < totalCount);
        UNIT_ASSERT(deltaCount * 6 > totalCount);
    }

    Y_UNIT_TEST(LocalChannelConcurrency) {
        constexpr ui32 MESSAGE_COUNT = 100000;

        TKikimrRunner kikimr(TKikimrSettings{});
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto receiver = runtime.AllocateEdgeActor();

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId()), sender, new TEvPrivate::TEvServiceLookup());
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
        auto service = serviceReply->Service;

        TChannelInfo info(1, sender, receiver);
        auto senderBuffer = service->GetLocalBuffer(info);
        auto receiverBuffer = service->GetLocalBuffer(info);

        TMuxEvent event;
        ui32 receivedCount = 0;
        ui32 outputBlockCount = 0;
        ui32 inputBlockCount = 0;
        NPar::LocalExecutor().RunAdditionalThreads(2);
        NPar::LocalExecutor().Exec([&](int) mutable {
            bool blocked = false;
            for (ui32 i = 0; i < MESSAGE_COUNT; i++) {
                Sleep(TDuration::MicroSeconds(i / (MESSAGE_COUNT / 10)));
                while (senderBuffer->GetFillLevel() == EDqFillLevel::HardLimit) {
                    if (!blocked) {
                        blocked = true;
                        outputBlockCount++;
                    }
                    Sleep(TDuration::MicroSeconds(i / (MESSAGE_COUNT / 100)));
                }
                blocked = false;
                senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));
            }
            senderBuffer->Push(TDataChunk(true));
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);
        NPar::LocalExecutor().Exec([&](int) mutable {
            bool blocked = false;
            while (true) {
                Sleep(TDuration::MicroSeconds(10 - receivedCount / (MESSAGE_COUNT / 10)));
                if (receiverBuffer->IsEmpty()) {
                    if (!blocked) {
                        blocked = true;
                        inputBlockCount++;
                    }
                    continue;
                }
                TDataChunk data;
                UNIT_ASSERT(receiverBuffer->Pop(data));
                if (data.Finished) {
                    break;
                }
                receivedCount++;
                blocked = false;
                UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
            }
            event.Signal();
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);
        event.WaitI();
        UNIT_ASSERT_VALUES_EQUAL(receivedCount, MESSAGE_COUNT);
        Cerr << outputBlockCount << " / " << inputBlockCount << Endl;
    }

    Y_UNIT_TEST(LocalChannelEarlyFinish) {
        constexpr ui32 MESSAGE_COUNT = 1000;

        TKikimrRunner kikimr(TKikimrSettings{});
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto receiver = runtime.AllocateEdgeActor();

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId()), sender, new TEvPrivate::TEvServiceLookup());
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
        auto service = serviceReply->Service;

        TChannelInfo info(1, sender, receiver);
        auto senderBuffer = service->GetLocalBuffer(info);
        auto receiverBuffer = service->GetLocalBuffer(info);

        TMuxEvent eventR;
        TMuxEvent eventS;
        ui32 sentCount = 0;
        ui32 receivedCount = 0;
        NPar::LocalExecutor().RunAdditionalThreads(2);
        NPar::LocalExecutor().Exec([&](int) mutable {
            for (ui32 i = 0; i < MESSAGE_COUNT; i++) {
                sentCount++;
                senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));
                Sleep(TDuration::MicroSeconds(1));
                if (senderBuffer->IsEarlyFinished()) {
                    break;
                }
            }
            eventS.Signal();
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);
        NPar::LocalExecutor().Exec([&](int) mutable {
            while (true) {
                if (receiverBuffer->IsEmpty()) {
                    Sleep(TDuration::MicroSeconds(10));
                    continue;
                }
                TDataChunk data;
                UNIT_ASSERT(receiverBuffer->Pop(data));
                if (data.Finished) {
                    break;
                }
                receivedCount++;
                UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
                if (receivedCount > MESSAGE_COUNT / 4) {
                    receiverBuffer->EarlyFinish();
                    break;
                }
            }
            eventR.Signal();
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);
        eventS.WaitI();
        eventR.WaitI();
        UNIT_ASSERT(sentCount < MESSAGE_COUNT);
        Cerr << sentCount << " / " << receivedCount << Endl;
    }

    Y_UNIT_TEST(LocalChannelAsyncRead) {

        TKikimrRunner kikimr(TKikimrSettings{});
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto receiver = runtime.AllocateEdgeActor();

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId()), sender, new TEvPrivate::TEvServiceLookup());
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
        auto service = serviceReply->Service;

        TChannelInfo info(1, sender, receiver);
        auto senderBuffer = service->GetLocalBuffer(info);
        auto receiverBuffer = service->GetLocalBuffer(info);

        UNIT_ASSERT(receiverBuffer->IsEmpty());
        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));

        TDataChunk data;
        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
    }

    Y_UNIT_TEST(IcChannelTrivial) {

        TVector<NKikimrKqp::TKqpSetting> kqpSettings;
        TKikimrRunner kikimr(kqpSettings, "", KikimrDefaultUtDomainRoot, 2);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelInfo info(1, sender, receiver);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderBuffer = service->GetRemoteOutputBuffer(info);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));
        senderBuffer->Push(TDataChunk(true));

        TDataChunk data;

        UNIT_ASSERT(ReadBlocked(receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
        UNIT_ASSERT(ReadBlocked(receiverBuffer, data));
        UNIT_ASSERT(data.Finished);
    }

    Y_UNIT_TEST(IcChannelLateBinding) {

        TVector<NKikimrKqp::TKqpSetting> kqpSettings;
        TKikimrRunner kikimr(kqpSettings, "", KikimrDefaultUtDomainRoot, 2);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<TDqChannelService> service0;
        std::shared_ptr<TDqChannelService> service1;

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            service0 = serviceReply->Service;
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            service1 = serviceReply->Service;
        }

        TChannelInfo info1(1, sender, receiver);
        TChannelInfo info2(2, sender, receiver);

        auto senderBuffer1 = service0->GetRemoteOutputBuffer(info1);
        auto senderBuffer2 = service0->GetRemoteOutputBuffer(info2);
        auto receiverBuffer1 = service1->GetRemoteInputBuffer(info1);

        senderBuffer2->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));
        senderBuffer1->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));
        senderBuffer2->Push(TDataChunk(true));
        senderBuffer1->Push(TDataChunk(true));

        TDataChunk data;

        UNIT_ASSERT(ReadBlocked(receiverBuffer1, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
        UNIT_ASSERT(ReadBlocked(receiverBuffer1, data));
        UNIT_ASSERT(data.Finished);

        auto receiverBuffer2 = service1->GetRemoteInputBuffer(info2);

        UNIT_ASSERT(ReadBlocked(receiverBuffer2, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
        UNIT_ASSERT(ReadBlocked(receiverBuffer2, data));
        UNIT_ASSERT(data.Finished);
    }

    Y_UNIT_TEST(IcChannelAsyncRead) {

        TVector<NKikimrKqp::TKqpSetting> kqpSettings;
        TKikimrRunner kikimr(kqpSettings, "", KikimrDefaultUtDomainRoot, 2);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelInfo info(1, sender, receiver);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderBuffer = service->GetRemoteOutputBuffer(info);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        UNIT_ASSERT(receiverBuffer->IsEmpty());
        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));

        TDataChunk data;
        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
    }

    Y_UNIT_TEST(IcChannelEarlyFinish) {

        TVector<NKikimrKqp::TKqpSetting> kqpSettings;
        TKikimrRunner kikimr(kqpSettings, "", KikimrDefaultUtDomainRoot, 2);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelInfo info(1, sender, receiver);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderBuffer = service->GetRemoteOutputBuffer(info);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));

        TDataChunk data;
        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");

        receiverBuffer->EarlyFinish();
        auto deadline = TInstant::Now() + TDuration::Seconds(10);
        bool earlyFinished = false;
        while (TInstant::Now() <= deadline) {
            if (senderBuffer->IsEarlyFinished()) {
                earlyFinished = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(earlyFinished);
    }

    Y_UNIT_TEST(IcChannelBackPressure) {

        TKikimrSettings settings;
        settings.SetNodeCount(2);
        settings.AppConfig.MutableTableServiceConfig()->SetRemoteChannelInflightBytes(10);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelInfo info(1, sender, receiver);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderBuffer = service->GetRemoteOutputBuffer(info);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));

        auto aggregator = std::make_shared<TDqFillAggregator>();
        senderBuffer->SetFillAggregator(aggregator);
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("ByeBye"), 1));
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::HardLimit);

        TDataChunk data;

        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");

        auto deadline = TInstant::Now() + TDuration::Seconds(10);
        bool noLimit = false;
        while (TInstant::Now() <= deadline) {
            if (aggregator->GetFillLevel() == EDqFillLevel::NoLimit) {
                noLimit = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(noLimit);

        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "ByeBye");
    }

    Y_UNIT_TEST(IcChannelSessionInflight) {

        TKikimrSettings settings;
        settings.SetNodeCount(2);
        settings.AppConfig.MutableTableServiceConfig()->SetNodeSessionIcInflightBytes(8);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<TDebugNodeState> senderState;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        std::shared_ptr<TDebugNodeState> receiverState;
        TChannelInfo info(1, sender, receiver);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderState = service->CreateDebugNodeState(runtime.GetNodeId(1));
            senderBuffer = service->GetRemoteOutputBuffer(info);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverState = service->CreateDebugNodeState(runtime.GetNodeId(0));
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        auto aggregator = std::make_shared<TDqFillAggregator>();
        senderBuffer->SetFillAggregator(aggregator);

        senderState->PauseChannelAck();

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1));
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("ByeBye"), 1));
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("IamBack"), 1));
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        TDataChunk data;

        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");

        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "ByeBye");

        UNIT_ASSERT(!ReadBlocked(receiverBuffer, data));

        senderState->ResumeChannelAck();

        UNIT_ASSERT(ReadBlocked(receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "IamBack");
    }

    Y_UNIT_TEST(CaIntegrationTrivial) {

        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetEnableFastChannels(false);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        auto result = client.ExecuteQuery(R"(
            PRAGMA ydb.UseFastChannels = "true";
            SELECT 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[1]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(CaIntegrationAgg) {

        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableFastChannels(false);
        TKikimrRunner kikimr(settings);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        {
            const TString query = R"(
                CREATE TABLE `/Root/Table1` (
                    k1 Uint32 NOT NULL,
                    PRIMARY KEY (k1)
                )
                PARTITION BY HASH (k1)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Table1` (k1) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString query = R"(
                PRAGMA ydb.UseFastChannels = "true";
                SELECT SUM(k1) FROM `/Root/Table1`
            )";

            NYdb::NTable::TExecDataQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), R"([[[55u]]])");
        }
    }

    Y_UNIT_TEST(CaIntegrationIc) {

        TKikimrSettings settings;
        settings.SetNodeCount(4);
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableFastChannels(false);
        TKikimrRunner kikimr(settings);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        {
            const TString query = R"(
                CREATE TABLE `/Root/Table1` (
                    k1 Uint32 NOT NULL,
                    PRIMARY KEY (k1)
                )
                PARTITION BY HASH (k1)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Table1` (k1) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();

        auto result = client.ExecuteQuery(R"(
            PRAGMA ydb.UseFastChannels = "true";
            SELECT SUM(k1) FROM `/Root/Table1`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[[55u]]]", FormatResultSetYson(result.GetResultSet(0)));
    }
}