#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/utils/yql_panic.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <library/cpp/testing/unittest/gtest.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>

#include <thread>

namespace NYql::NDq {

const ui64 PartitionId1 = 666;
const ui64 PartitionId2 = 667;

using namespace NTestUtils;

struct TFixture : public TPqIoTestFixture {
    TFixture() {
        LocalRowDispatcherId = CaSetup->Runtime->AllocateEdgeActor();
        Coordinator1Id = CaSetup->Runtime->AllocateEdgeActor();
        Coordinator2Id = CaSetup->Runtime->AllocateEdgeActor();
        RowDispatcher1 = CaSetup->Runtime->AllocateEdgeActor();
        RowDispatcher2 = CaSetup->Runtime->AllocateEdgeActor();
    }

    void InitRdSource(
        const NYql::NPq::NProto::TDqPqTopicSource& settings,
        i64 freeSpace = 1_MB,
        ui64 partitionCount = 1)
    {
        CaSetup->Execute([&](TFakeActor& actor) {
            NPq::NProto::TDqReadTaskParams params;
            auto* partitioninigParams = params.MutablePartitioningParams();
            partitioninigParams->SetTopicPartitionsCount(partitionCount);
            partitioninigParams->SetEachTopicPartitionGroupId(PartitionId1);
            partitioninigParams->SetDqPartitionsCount(1);

            TString serializedParams;
            Y_PROTOBUF_SUPPRESS_NODISCARD params.SerializeToString(&serializedParams);

            const THashMap<TString, TString> secureParams;
            const THashMap<TString, TString> taskParams { {"pq", serializedParams} };

            NYql::NPq::NProto::TDqPqTopicSource copySettings = settings;
            TPqIoTestFixture setup;
            auto [dqSource, dqSourceAsActor] = CreateDqPqRdReadActor(
                actor.TypeEnv,
                std::move(copySettings),
                0,
                NYql::NDq::TCollectStatsLevel::None,
                "query_1",
                0,
                secureParams,
                taskParams,
                setup.Driver,
                {},
                actor.SelfId(),         // computeActorId
                LocalRowDispatcherId,
                actor.GetHolderFactory(),
                MakeIntrusive<NMonitoring::TDynamicCounters>(),
                freeSpace,
                CreateMockPqGateway({.Runtime = CaSetup->Runtime.get()})
                );

            actor.InitAsyncInput(dqSource, dqSourceAsActor);
        });
    }

    void ExpectCoordinatorChangesSubscribe() {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe>(LocalRowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    auto ExpectCoordinatorRequest(NActors::TActorId coordinatorId) {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorRequest>(coordinatorId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        return eventHolder;
    }

    void ExpectStartSession(const TMap<ui32, ui64>& expectedOffsets, NActors::TActorId rowDispatcherId, ui64 expectedGeneration = 1) {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStartSession>(rowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        TMap<ui32, ui64> offsets;
        for (auto p : eventHolder->Get()->Record.GetOffsets()) {
            offsets[p.GetPartitionId()] = p.GetOffset();
        }
        UNIT_ASSERT_EQUAL(offsets, expectedOffsets);
        UNIT_ASSERT_EQUAL(eventHolder->Cookie, expectedGeneration);
    }

    void ExpectStopSession(NActors::TActorId rowDispatcherId, ui64 expectedGeneration = 1) {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStopSession>(rowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT_EQUAL(eventHolder->Cookie, expectedGeneration);
    }

    void ExpectNoSession(NActors::TActorId rowDispatcherId, ui64 expectedGeneration = 1) {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvNoSession>(rowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Cookie == expectedGeneration);
    }

    void ExpectGetNextBatch(NActors::TActorId rowDispatcherId, ui64 partitionId = PartitionId1) {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvGetNextBatch>(rowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT_EQUAL(eventHolder->Get()->Record.GetPartitionId(), partitionId);
    }

    void MockCoordinatorChanged(NActors::TActorId coordinatorId) {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvCoordinatorChanged(coordinatorId, 0);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, LocalRowDispatcherId, event));
        });
    }

    void MockCoordinatorResult(const TMap<NActors::TActorId, ui64> result, ui64 cookie = 0) {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvCoordinatorResult();

            for (const auto& [rowDispatcherId, partitionId] : result) {
                auto* partitions = event->Record.AddPartitions();
                partitions->AddPartitionIds(partitionId);
                ActorIdToProto(rowDispatcherId, partitions->MutableActorId());
            }
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, Coordinator1Id, event, 0, cookie));
        });
    }

    void MockAck(NActors::TActorId rowDispatcherId, ui64 generation = 1, ui64 partitionId = PartitionId1) {
        CaSetup->Execute([&](TFakeActor& actor) {
            NFq::NRowDispatcherProto::TEvStartSession proto;
            proto.AddPartitionIds(partitionId);
            auto event = new NFq::TEvRowDispatcher::TEvStartSessionAck(proto);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void MockHeartbeat(NActors::TActorId rowDispatcherId, ui64 generation = 1) {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvHeartbeat();
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void MockNewDataArrived(NActors::TActorId rowDispatcherId, ui64 generation = 1, ui64 partitionId = PartitionId1) {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvNewDataArrived();
            event->Record.SetPartitionId(partitionId);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    TRope SerializeItem(TFakeActor& actor, ui64 intValue, const TString& strValue) {
        NKikimr::NMiniKQL::TType* typeMkql = actor.ProgramBuilder.NewMultiType({
            NYql::NCommon::ParseTypeFromYson(TStringBuf("[DataType; Uint64]"), actor.ProgramBuilder, Cerr),
            NYql::NCommon::ParseTypeFromYson(TStringBuf("[DataType; String]"), actor.ProgramBuilder, Cerr)
        });
        UNIT_ASSERT_C(typeMkql, "Failed to create multi type");

        NKikimr::NMiniKQL::TValuePackerTransport<true> packer(typeMkql, NKikimr::NMiniKQL::EValuePackerVersion::V0);

        TVector<NUdf::TUnboxedValue> values = {
            NUdf::TUnboxedValuePod(intValue),
            NKikimr::NMiniKQL::MakeString(strValue)
        };
        packer.AddWideItem(values.data(), 2);

        return NYql::MakeReadOnlyRope(packer.Finish());
    }

    // Supported schema (Uint64, String)
    void MockMessageBatch(ui64 offset, const std::vector<std::pair<ui64, TString>>& messages, NActors::TActorId rowDispatcherId, ui64 generation = 1, ui64 partitionId = PartitionId1) {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvMessageBatch();
            for (const auto& item : messages) {
                NFq::NRowDispatcherProto::TEvMessage message;
                message.SetPayloadId(event->AddPayload(SerializeItem(actor, item.first, item.second)));
                message.AddOffsets(offset++);
                *event->Record.AddMessages() = message;
            }
            event->Record.SetPartitionId(partitionId);
            event->Record.SetNextMessageOffset(offset);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void MockSessionError() {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvSessionError();
            event->Record.SetStatusCode(::NYql::NDqProto::StatusIds::BAD_REQUEST);
            IssueToMessage(TIssue("A problem has been detected and session has been shut down to prevent damage your life"), event->Record.AddIssues());
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, RowDispatcher1, event, 0, 1));
        });
    }

    void MockStatistics(NActors::TActorId rowDispatcherId, ui64 nextOffset, ui64 generation, ui64 partitionId) {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvStatistics();
            auto* partitionsProto = event->Record.AddPartition();
            partitionsProto->SetPartitionId(partitionId);
            partitionsProto->SetNextMessageOffset(nextOffset);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    template<typename T>
    void AssertDataWithWatermarks(
        const std::vector<std::variant<T, TInstant>>& actual,
        const std::vector<T>& expected,
        const std::vector<ui32>& watermarkBeforePositions)
    {
        auto expectedPos = 0U;
        auto watermarksBeforeIter = watermarkBeforePositions.begin();

        for (auto item : actual) {
            if (std::holds_alternative<TInstant>(item)) {
                if (watermarksBeforeIter != watermarkBeforePositions.end()) {
                    watermarksBeforeIter++;
                }
                continue;
            } else {
                UNIT_ASSERT_C(expectedPos < expected.size(), "Too many data items");
                UNIT_ASSERT_C(
                    watermarksBeforeIter == watermarkBeforePositions.end() ||
                    *watermarksBeforeIter > expectedPos,
                    "Watermark before item on position " << expectedPos << " was expected");
                UNIT_ASSERT_VALUES_EQUAL(std::get<T>(item), expected.at(expectedPos));
                expectedPos++;
            }
        }
    }

    void MockDisconnected() {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NActors::TEvInterconnect::TEvNodeDisconnected(CaSetup->Runtime->GetNodeId(0));
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, RowDispatcher1, event));
        });
    }

    void MockConnected() {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NActors::TEvInterconnect::TEvNodeConnected(CaSetup->Runtime->GetNodeId(0));
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, RowDispatcher1, event));
        });
    }

    void MockUndelivered(NActors::TActorId rowDispatcherId, ui64 generation = 0, NActors::TEvents::TEvUndelivered::EReason reason = NActors::TEvents::TEvUndelivered::ReasonActorUnknown) {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NActors::TEvents::TEvUndelivered(0, reason);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void StartSession(NYql::NPq::NProto::TDqPqTopicSource& settings, i64 freeSpace = 1_MB, ui64 partitionCount = 1) {
        InitRdSource(settings, freeSpace, partitionCount);
        SourceRead<std::pair<ui64, TString>>(UVPairParser);
        ExpectCoordinatorChangesSubscribe();
    
        MockCoordinatorChanged(Coordinator1Id);
        auto req = ExpectCoordinatorRequest(Coordinator1Id);

        MockCoordinatorResult({{RowDispatcher1, PartitionId1}}, req->Cookie);
        ExpectStartSession({}, RowDispatcher1);
        MockAck(RowDispatcher1);
    }

    void ProcessSomeMessages(ui64 offset, const std::vector<std::pair<ui64, TString>>& messages, NActors::TActorId rowDispatcherId,
        std::function<std::vector<std::pair<ui64, TString>>(const NUdf::TUnboxedValue&)> uvParser = UVPairParser, ui64 generation = 1,
        ui64 partitionId = PartitionId1, bool readedByCA = true) {
        MockNewDataArrived(rowDispatcherId, generation, partitionId);
        ExpectGetNextBatch(rowDispatcherId, partitionId);

        MockMessageBatch(offset, messages, rowDispatcherId, generation, partitionId);
        if (readedByCA) {
            auto result = SourceReadDataUntil<std::pair<ui64, TString>>(uvParser, messages.size());
            AssertDataWithWatermarks(result, messages, {});
        }
    } 

    const std::pair<ui64, TString> Message1 = {100, "value1"};
    const std::pair<ui64, TString> Message2 = {200, "value2"};
    const std::pair<ui64, TString> Message3 = {300, "value3"};
    const std::pair<ui64, TString> Message4 = {400, "value4"};

    NYql::NPq::NProto::TDqPqTopicSource Source1 = BuildPqTopicSourceSettings("topicName");

    NActors::TActorId LocalRowDispatcherId;
    NActors::TActorId Coordinator1Id;
    NActors::TActorId Coordinator2Id;
    NActors::TActorId RowDispatcher1;
    NActors::TActorId RowDispatcher2;
};

Y_UNIT_TEST_SUITE(TDqPqRdReadActorTests) {
    Y_UNIT_TEST_F(TestReadFromTopic2, TFixture) {
        StartSession(Source1);
        ProcessSomeMessages(0, {Message1, Message2}, RowDispatcher1);
    }

    Y_UNIT_TEST_F(IgnoreUndeliveredWithWrongGeneration, TFixture) {
        StartSession(Source1);
        ProcessSomeMessages(0, {Message1, Message2}, RowDispatcher1);
        MockUndelivered(RowDispatcher1, 999, NActors::TEvents::TEvUndelivered::Disconnected);
        ProcessSomeMessages(2, {Message3}, RowDispatcher1);
    }

    Y_UNIT_TEST_F(SessionError, TFixture) {
        StartSession(Source1);

        TInstant deadline = Now() + TDuration::Seconds(5);
        auto future = CaSetup->AsyncInputPromises.FatalError.GetFuture();
        MockSessionError();

        bool failured = false;
        while (Now() < deadline) {
            SourceRead<std::pair<ui64, TString>>(UVPairParser);
            if (future.HasValue()) {
                UNIT_ASSERT_STRING_CONTAINS(future.GetValue().ToOneLineString(), "damage your life");
                failured = true;
                break;
            }
        }
        UNIT_ASSERT_C(failured, "Failure timeout");
    }

    Y_UNIT_TEST_F(ReadWithFreeSpace, TFixture) {
        StartSession(Source1);

        MockNewDataArrived(RowDispatcher1);
        ExpectGetNextBatch(RowDispatcher1);

        const std::vector<std::pair<ui64, TString>> data1 = {Message1, Message2};
        MockMessageBatch(0, data1, RowDispatcher1);

        const std::vector<std::pair<ui64, TString>> data2 = {Message3, Message4};
        MockMessageBatch(2, data2, RowDispatcher1);

        auto result = SourceReadDataUntil<std::pair<ui64, TString>>(UVPairParser, 1, 1);
        std::vector<std::pair<ui64, TString>> expected{data1};
        AssertDataWithWatermarks(result, expected, {});

        auto readSize = SourceRead<std::pair<ui64, TString>>(UVPairParser, 0).size();
        UNIT_ASSERT_VALUES_EQUAL(readSize, 0);
    }

    Y_UNIT_TEST(TestSaveLoadPqRdRead) {
        TSourceState state;
 
        {
            TFixture f;
            f.StartSession(f.Source1);
            f.ProcessSomeMessages(0, {f.Message1, f.Message2}, f.RowDispatcher1);  // offsets: 0, 1

            f.SaveSourceState(CreateCheckpoint(), state);
            Cerr << "State saved" << Endl;
        }
        {
            TFixture f;
            f.InitRdSource(f.Source1);
            f.LoadSource(state);
            f.SourceRead<std::pair<ui64, TString>>(UVPairParser);
            f.ExpectCoordinatorChangesSubscribe();
    
            f.MockCoordinatorChanged(f.Coordinator1Id);
            auto req = f.ExpectCoordinatorRequest(f.Coordinator1Id);

            f.MockCoordinatorResult({{f.RowDispatcher1, PartitionId1}}, req->Cookie);
            f.ExpectStartSession({{PartitionId1, 2}}, f.RowDispatcher1);
            f.MockAck(f.RowDispatcher1);

            f.ProcessSomeMessages(2, {f.Message3}, f.RowDispatcher1);  // offsets: 2
            state.Data.clear();
            f.SaveSourceState(CreateCheckpoint(), state);
            Cerr << "State saved" << Endl;
        }
        {
            TFixture f;
            f.InitRdSource(f.Source1);
            f.LoadSource(state);
            f.SourceRead<std::pair<ui64, TString>>(UVPairParser);
            f.ExpectCoordinatorChangesSubscribe();
    
            f.MockCoordinatorChanged(f.Coordinator1Id);
            auto req = f.ExpectCoordinatorRequest(f.Coordinator1Id);

            f.MockCoordinatorResult({{f.RowDispatcher1, PartitionId1}}, req->Cookie);
            f.ExpectStartSession({{PartitionId1, 3}}, f.RowDispatcher1);
            f.MockAck(f.RowDispatcher1);

            f.ProcessSomeMessages(3, {f.Message4}, f.RowDispatcher1);  // offsets: 3
        }
    }

    Y_UNIT_TEST_F(CoordinatorChanged, TFixture) {
        StartSession(Source1);
        ProcessSomeMessages(0, {Message1, Message2}, RowDispatcher1);
        MockMessageBatch(2, {Message3}, RowDispatcher1);

        // change active Coordinator 
        MockCoordinatorChanged(Coordinator2Id);
        // continue use old sessions
        MockMessageBatch(3, {Message4}, RowDispatcher1);

        auto req = ExpectCoordinatorRequest(Coordinator2Id);

        auto result = SourceReadDataUntil<std::pair<ui64, TString>>(UVPairParser, 2);
        AssertDataWithWatermarks(result, {Message3, Message4}, {});

        MockCoordinatorResult({{RowDispatcher2, PartitionId1}}, req->Cookie);       // change distribution
        ExpectStopSession(RowDispatcher1);

        ExpectStartSession({{PartitionId1, 4}}, RowDispatcher2, 2);
        MockAck(RowDispatcher2, 2);

        ProcessSomeMessages(4, {Message4}, RowDispatcher2, UVPairParser, 2);

        MockHeartbeat(RowDispatcher1, 1);       // old generation
        ExpectNoSession(RowDispatcher1, 1);

        // change active Coordinator 
        MockCoordinatorChanged(Coordinator1Id);
        req = ExpectCoordinatorRequest(Coordinator1Id);
        MockCoordinatorResult({{RowDispatcher2, PartitionId1}}, req->Cookie);       // distribution is not changed
        ProcessSomeMessages(5, {Message2}, RowDispatcher2, UVPairParser, 2);
    }

    Y_UNIT_TEST_F(Backpressure, TFixture) {
        StartSession(Source1, 2_KB);

        std::pair<ui64, TString> message = {100500, TString(900, 'c')};
        ProcessSomeMessages(0, {message}, RowDispatcher1);

        MockNewDataArrived(RowDispatcher1);
        ExpectGetNextBatch(RowDispatcher1);
        MockMessageBatch(1, {message, message, message}, RowDispatcher1);

        MockNewDataArrived(RowDispatcher1);
        ASSERT_THROW(
            CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvGetNextBatch>(RowDispatcher1, TDuration::Seconds(0)),
            NActors::TEmptyEventQueueException);

        auto result = SourceReadDataUntil<std::pair<ui64, TString>>(UVPairParser, 3);
        AssertDataWithWatermarks(result, {message, message, message}, {});
        ExpectGetNextBatch(RowDispatcher1);

        MockMessageBatch(4, {Message1}, RowDispatcher1);
        result = SourceReadDataUntil<std::pair<ui64, TString>>(UVPairParser, 1);
        AssertDataWithWatermarks(result, {Message1}, {});
    }

    Y_UNIT_TEST_F(RowDispatcherIsRestarted2, TFixture) {
        StartSession(Source1);
        ProcessSomeMessages(0, {Message1, Message2}, RowDispatcher1);
        MockDisconnected();
        MockConnected();
        MockUndelivered(RowDispatcher1, 1);

        auto req = ExpectCoordinatorRequest(Coordinator1Id);
        MockCoordinatorResult({{RowDispatcher1, PartitionId1}}, req->Cookie);
        ExpectStartSession({{PartitionId1, 2}}, RowDispatcher1, 2);
        MockAck(RowDispatcher1, 2);

        ProcessSomeMessages(2, {Message3}, RowDispatcher1, UVPairParser, 2);
    }

    Y_UNIT_TEST_F(TwoPartitionsRowDispatcherIsRestarted, TFixture) {
        InitRdSource(Source1, 1_MB, PartitionId2 + 1);
        SourceRead<TString>(UVParser);
        ExpectCoordinatorChangesSubscribe();
        MockCoordinatorChanged(Coordinator1Id);
        auto req = ExpectCoordinatorRequest(Coordinator1Id);
        MockCoordinatorResult({{RowDispatcher1, PartitionId1}, {RowDispatcher2, PartitionId2}}, req->Cookie);
        ExpectStartSession({}, RowDispatcher1, 1);
        ExpectStartSession({}, RowDispatcher2, 2);
        MockAck(RowDispatcher1, 1, PartitionId1);
        MockAck(RowDispatcher2, 2, PartitionId2);
        
        ProcessSomeMessages(0, {Message1, Message2}, RowDispatcher1, UVPairParser, 1, PartitionId1);
        ProcessSomeMessages(0, {Message3}, RowDispatcher2, UVPairParser, 2, PartitionId2, false);     // not read by CA
        MockStatistics(RowDispatcher2, 10,  2, PartitionId2);

        // Restart node 2 (RowDispatcher2)
        MockDisconnected();
        MockConnected();
        MockUndelivered(RowDispatcher2, 2);

        // session1 is still working
        ProcessSomeMessages(2, {Message4}, RowDispatcher1, UVPairParser, 1, PartitionId1, false); 

        // Reinit session to RowDispatcher2
        auto req2 = ExpectCoordinatorRequest(Coordinator1Id);
        MockCoordinatorResult({{RowDispatcher1, PartitionId1}, {RowDispatcher2, PartitionId2}}, req2->Cookie);
        ExpectStartSession({{PartitionId2, 10}}, RowDispatcher2, 3);
        MockAck(RowDispatcher2, 3, PartitionId2);

        ProcessSomeMessages(3, {Message4}, RowDispatcher1, UVPairParser, 1, PartitionId1, false);
        ProcessSomeMessages(10, {Message4}, RowDispatcher2, UVPairParser, 3, PartitionId2, false);

        std::vector<std::pair<ui64, TString>> expected{Message3, Message4, Message4, Message4};
        auto result = SourceReadDataUntil<std::pair<ui64, TString>>(UVPairParser, expected.size());
        AssertDataWithWatermarks(result, expected, {});
    }

    Y_UNIT_TEST_F(IgnoreMessageIfNoSessions, TFixture) {
        StartSession(Source1);
        MockCoordinatorChanged(Coordinator2Id);
        MockUndelivered(RowDispatcher1, 1);
        MockSessionError();
    }

    Y_UNIT_TEST_F(MetadataFields, TFixture) {
        auto metadataUVParser = [](const NUdf::TUnboxedValue& item) -> std::vector<std::pair<ui64, TString>> {
            UNIT_ASSERT_VALUES_EQUAL(item.GetListLength(), 3);
            auto stringElement = item.GetElement(2);
            return { {item.GetElement(1).Get<ui64>(), TString(stringElement.AsStringRef())} };
        };

        auto source = BuildPqTopicSourceSettings("topicName");
        source.AddMetadataFields("_yql_sys_create_time");
        source.SetRowType("[StructType; [[_yql_sys_create_time; [DataType; Uint32]]; [dt; [DataType; Uint64]]; [value; [DataType; String]]]]");
        StartSession(source);
        ProcessSomeMessages(0, {Message1}, RowDispatcher1, metadataUVParser);  
    }

    Y_UNIT_TEST_F(IgnoreCoordinatorResultIfWrongState, TFixture) {
        StartSession(Source1);
        ProcessSomeMessages(0, {Message1, Message2}, RowDispatcher1);

        MockCoordinatorChanged(Coordinator2Id);
        auto req = ExpectCoordinatorRequest(Coordinator2Id);
        MockUndelivered(RowDispatcher1, 1);
        auto req2 = ExpectCoordinatorRequest(Coordinator2Id);

        MockCoordinatorResult({{RowDispatcher1, PartitionId1}}, req->Cookie);
        MockCoordinatorResult({{RowDispatcher1, PartitionId1}}, req2->Cookie);
        ExpectStartSession({{PartitionId1, 2}}, RowDispatcher1, 2);

        MockAck(RowDispatcher1);
    }
}
} // NYql::NDq
