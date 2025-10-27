#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>

#include <library/cpp/testing/unittest/gtest.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>
#include <util/string/join.h>

#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

namespace {

using namespace NTestUtils;

constexpr ui64 PartitionId1 = 666;
constexpr ui64 PartitionId2 = 667;

constexpr auto DefaultWatermarkPeriod = TDuration::MicroSeconds(100);
constexpr auto DefaultLateArrivalDelay = TDuration::Days(1);

const TMessage Message1 = {100, "value1"};
const TMessage Message2 = {200, "value2"};
const TMessage Message3 = {300, "value3"};
const TMessage Message4 = {400, "value4"};
const TMessage Message5 = {500, "value5"};
const TMessage Message6 = {600, "value6"};

class TFixture : public TPqIoTestFixture {
public:
    TFixture()
        : LocalRowDispatcherId(CaSetup->Runtime->AllocateEdgeActor())
        , CoordinatorId1(CaSetup->Runtime->AllocateEdgeActor())
        , CoordinatorId2(CaSetup->Runtime->AllocateEdgeActor())
        , RowDispatcherId1(CaSetup->Runtime->AllocateEdgeActor())
        , RowDispatcherId2(CaSetup->Runtime->AllocateEdgeActor())
    {}

    void InitRdSource(
        NYql::NPq::NProto::TDqPqTopicSource settings,
        i64 freeSpace = 1_MB,
        ui64 partitionCount = 1
    ) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            NPq::NProto::TDqReadTaskParams params;
            auto* partitioningParams = params.MutablePartitioningParams();
            partitioningParams->SetTopicPartitionsCount(partitionCount);
            partitioningParams->SetEachTopicPartitionGroupId(PartitionId1);
            partitioningParams->SetDqPartitionsCount(1);

            auto [dqAsyncInput, dqAsyncInputAsActor] = CreateDqPqRdReadActor(
                actor.TypeEnv,
                std::move(settings),
                0,
                NYql::NDq::TCollectStatsLevel::None,
                "query_1",
                0,
                {},
                TVector<NPq::NProto::TDqReadTaskParams>{params},
                Driver,
                {},
                actor.SelfId(),         // computeActorId
                LocalRowDispatcherId,
                actor.GetHolderFactory(),
                MakeIntrusive<NMonitoring::TDynamicCounters>(),
                freeSpace,
                CreateMockPqGateway({.Runtime = CaSetup->Runtime.get()})
            );

            actor.InitAsyncInput(dqAsyncInput, dqAsyncInputAsActor);
        });
    }

    void ExpectCoordinatorChangesSubscribe() const {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe>(LocalRowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, eventHolder.Get());
    }

    auto ExpectCoordinatorRequest(NActors::TActorId coordinatorId) const {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorRequest>(coordinatorId, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, eventHolder.Get());
        return eventHolder;
    }

    void ExpectStartSession(const TMap<ui32, ui64>& expectedOffsets, NActors::TActorId rowDispatcherId, ui64 expectedGeneration) const {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStartSession>(rowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, eventHolder.Get());
        TMap<ui32, ui64> offsets;
        for (auto p : eventHolder->Get()->Record.GetOffsets()) {
            offsets[p.GetPartitionId()] = p.GetOffset();
        }
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        UNIT_ASSERT_VALUES_EQUAL(expectedGeneration, eventHolder->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(0, eventHolder->Get()->Record.GetStartingMessageTimestampMs());
    }

    void ExpectStopSession(NActors::TActorId rowDispatcherId, ui64 expectedGeneration) const {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStopSession>(rowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, eventHolder.Get());
        UNIT_ASSERT_VALUES_EQUAL(expectedGeneration, eventHolder->Cookie);
    }

    void ExpectNoSession(NActors::TActorId rowDispatcherId, ui64 expectedGeneration) const {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvNoSession>(rowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, eventHolder.Get());
        UNIT_ASSERT_VALUES_EQUAL(expectedGeneration, eventHolder->Cookie);
    }

    void ExpectGetNextBatch(NActors::TActorId rowDispatcherId, ui64 partitionId) const {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvGetNextBatch>(rowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, eventHolder.Get());
        UNIT_ASSERT_VALUES_EQUAL(partitionId, eventHolder->Get()->Record.GetPartitionId());
    }

    void MockCoordinatorChanged(NActors::TActorId coordinatorId) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvCoordinatorChanged(coordinatorId, 0);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, LocalRowDispatcherId, event));
        });
    }

    void MockCoordinatorResult(NActors::TActorId coordinatorId, const TMap<NActors::TActorId, ui64>& result, ui64 cookie = 0) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvCoordinatorResult();

            for (const auto& [rowDispatcherId, partitionId] : result) {
                auto* partitions = event->Record.AddPartitions();
                partitions->AddPartitionIds(partitionId);
                ActorIdToProto(rowDispatcherId, partitions->MutableActorId());
            }
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, coordinatorId, event, 0, cookie));
        });
    }

    void MockAck(NActors::TActorId rowDispatcherId, ui64 generation, ui64 partitionId) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            NFq::NRowDispatcherProto::TEvStartSession proto;
            proto.AddPartitionIds(partitionId);
            auto event = new NFq::TEvRowDispatcher::TEvStartSessionAck(proto);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void MockHeartbeat(NActors::TActorId rowDispatcherId, ui64 generation) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvHeartbeat();
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void MockNewDataArrived(NActors::TActorId rowDispatcherId, ui64 generation, ui64 partitionId) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvNewDataArrived();
            event->Record.SetPartitionId(partitionId);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    TRope SerializeItem(NKikimr::NMiniKQL::TProgramBuilder& builder, const TMaybe<TMessage>& item) const {
        NKikimr::NMiniKQL::TType* typeMkql = builder.NewMultiType({
            NYql::NCommon::ParseTypeFromYson(TStringBuf("[DataType; Uint64]"), builder, Cerr),
            NYql::NCommon::ParseTypeFromYson(TStringBuf("[DataType; String]"), builder, Cerr)
        });
        UNIT_ASSERT_C(typeMkql, "Failed to create multi type");

        NKikimr::NMiniKQL::TValuePackerTransport<true> packer(typeMkql, NKikimr::NMiniKQL::EValuePackerVersion::V0);

        if (item) {
            auto values = TVector<NUdf::TUnboxedValue>{
                NUdf::TUnboxedValuePod(item->first),
                NKikimr::NMiniKQL::MakeString(item->second),
            };
            packer.AddWideItem(values.data(), values.size());
        }

        return NYql::MakeReadOnlyRope(packer.Finish());
    }

    // Supported schema (Uint64, String)
    void MockMessageBatch(
        ui64 offset,
        const std::vector<TMessage>& messages,
        NActors::TActorId rowDispatcherId,
        ui64 generation,
        ui64 partitionId
    ) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvMessageBatch();
            for (const auto& item : messages) {
                NFq::NRowDispatcherProto::TEvMessage message;
                message.SetPayloadId(event->AddPayload(SerializeItem(actor.ProgramBuilder, item)));
                message.AddOffsets(offset++);
                *event->Record.AddMessages() = message;
            }
            event->Record.SetPartitionId(partitionId);
            event->Record.SetNextMessageOffset(offset);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void MockMessageBatch(
        ui64 offset,
        const std::vector<std::pair<TMaybe<TMessage>, TMaybe<TInstant>>>& messages,
        NActors::TActorId rowDispatcherId,
        ui64 generation,
        ui64 partitionId
    ) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvMessageBatch();
            for (size_t i = 0; i < messages.size(); ++i) {
                const auto& [item, watermark] = messages[i];
                UNIT_ASSERT_C((item || watermark), i);
                NFq::NRowDispatcherProto::TEvMessage message;
                message.SetPayloadId(event->AddPayload(SerializeItem(actor.ProgramBuilder, item)));
                message.AddOffsets(offset++);
                if (watermark) {
                    message.AddWatermarksUs(watermark->MicroSeconds());
                }
                *event->Record.AddMessages() = message;
            }
            event->Record.SetPartitionId(partitionId);
            event->Record.SetNextMessageOffset(offset);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void MockSessionError(NActors::TActorId rowDispatcherId) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvSessionError();
            event->Record.SetStatusCode(::NYql::NDqProto::StatusIds::BAD_REQUEST);
            IssueToMessage(TIssue("A problem has been detected and session has been shut down to prevent damage your life"), event->Record.AddIssues());
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, 1));
        });
    }

    void MockStatistics(NActors::TActorId rowDispatcherId, ui64 nextOffset, ui64 generation, ui64 partitionId) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvStatistics();
            auto* partitionsProto = event->Record.AddPartition();
            partitionsProto->SetPartitionId(partitionId);
            partitionsProto->SetNextMessageOffset(nextOffset);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void AssertDataWithWatermarks(
        const std::vector<TWatermarkOr<TMessage>>& expected,
        const std::vector<TWatermarkOr<TMessage>>& actual
    ) const {
        UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(expected[i].index(), actual[i].index(), i);
            std::visit(TOverloaded {
                [i](const TMessage& expected, const TMessage& actual) {
                    UNIT_ASSERT_VALUES_EQUAL_C(expected, actual, i);
                },
                [i](const TMessage&, TInstant) {
                    UNIT_ASSERT_C(false, i);
                },
                [i](TInstant, const TMessage&) {
                    UNIT_ASSERT_C(false, i);
                },
                [i](TInstant expected, TInstant actual) {
                    UNIT_ASSERT_VALUES_EQUAL_C(expected, actual, i);
                },
            }, expected[i], actual[i]);
        }
    }

    void MockDisconnected(NActors::TActorId rowDispatcherId) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NActors::TEvInterconnect::TEvNodeDisconnected(CaSetup->Runtime->GetNodeId(0));
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event));
        });
    }

    void MockConnected(NActors::TActorId rowDispatcherId) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NActors::TEvInterconnect::TEvNodeConnected(CaSetup->Runtime->GetNodeId(0));
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event));
        });
    }

    void MockUndelivered(
        NActors::TActorId rowDispatcherId,
        ui64 generation = 0,
        NActors::TEvents::TEvUndelivered::EReason reason = NActors::TEvents::TEvUndelivered::ReasonActorUnknown
    ) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NActors::TEvents::TEvUndelivered(0, reason);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, rowDispatcherId, event, 0, generation));
        });
    }

    void StartSession(
        const NYql::NPq::NProto::TDqPqTopicSource& settings,
        i64 freeSpace = 1_MB,
        ui64 partitionCount = 1
    ) const {
        InitRdSource(settings, freeSpace, partitionCount);
        SourceRead<TMessage>(UVPairParser);
        ExpectCoordinatorChangesSubscribe();

        MockCoordinatorChanged(CoordinatorId1);
        auto req = ExpectCoordinatorRequest(CoordinatorId1);

        MockCoordinatorResult(CoordinatorId1, {{RowDispatcherId1, PartitionId1}}, req->Cookie);
        ExpectStartSession({}, RowDispatcherId1, 1);
        MockAck(RowDispatcherId1, 1, PartitionId1);
    }

    void WriteMessages(
        ui64 offset,
        const std::vector<TMessage>& messages,
        NActors::TActorId rowDispatcherId,
        ui64 generation,
        ui64 partitionId
    ) const {
        MockNewDataArrived(rowDispatcherId, generation, partitionId);
        ExpectGetNextBatch(rowDispatcherId, partitionId);
        MockMessageBatch(offset, messages, rowDispatcherId, generation, partitionId);
    }

    void WriteMessages(
        ui64 offset,
        const std::vector<std::pair<TMaybe<TMessage>, TMaybe<TInstant>>>& messages,
        NActors::TActorId rowDispatcherId,
        ui64 generation,
        ui64 partitionId
    ) const {
        MockNewDataArrived(rowDispatcherId, generation, partitionId);
        ExpectGetNextBatch(rowDispatcherId, partitionId);
        MockMessageBatch(offset, messages, rowDispatcherId, generation, partitionId);
    }

    void ReadMessages(const std::vector<TWatermarkOr<TMessage>>& expected, TReadValueParser<TMessage> parser = UVPairParser) const {
        auto op = [](size_t init, const TWatermarkOr<TMessage>& v) { return init + std::holds_alternative<TMessage>(v); };
        auto size = std::accumulate(expected.begin(), expected.end(), 0ull, op);
        auto actual = SourceReadDataUntil<TMessage>(parser, size);
        AssertDataWithWatermarks(expected, actual);
    }

public:
    NYql::NPq::NProto::TDqPqTopicSource Settings = BuildPqTopicSourceSettings(
        "topic",
        DefaultWatermarkPeriod,
        DefaultLateArrivalDelay,
        false
    );

    NActors::TActorId LocalRowDispatcherId;
    NActors::TActorId CoordinatorId1;
    NActors::TActorId CoordinatorId2;
    NActors::TActorId RowDispatcherId1;
    NActors::TActorId RowDispatcherId2;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqPqRdReadActorTests) {
    Y_UNIT_TEST_F(TestReadFromTopic2, TFixture) {
        StartSession(Settings);

        auto messages = std::vector{Message1, Message2};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);
        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
        };
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(IgnoreUndeliveredWithWrongGeneration, TFixture) {
        StartSession(Settings);

        auto messages = std::vector{Message1, Message2};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);
        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
        };
        ReadMessages(expected);

        MockUndelivered(RowDispatcherId1, 999, NActors::TEvents::TEvUndelivered::Disconnected);

        messages = {Message3};
        WriteMessages(2, messages, RowDispatcherId1, 1, PartitionId1);
        expected = {TWatermarkOr<TMessage>{Message3}};
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(SessionError, TFixture) {
        StartSession(Settings);

        TInstant deadline = Now() + TDuration::Seconds(5);
        auto future = CaSetup->AsyncInputPromises.FatalError.GetFuture();
        MockSessionError(RowDispatcherId1);

        bool failed = false;
        while (Now() < deadline) {
            SourceRead<TMessage>(UVPairParser);
            if (future.HasValue()) {
                UNIT_ASSERT_STRING_CONTAINS(future.GetValue().ToOneLineString(), "damage your life");
                failed = true;
                break;
            }
        }
    UNIT_ASSERT_C(failed, "Failure timeout");
    }

    Y_UNIT_TEST_F(ReadWithFreeSpace, TFixture) {
        StartSession(Settings);

        auto messages = std::vector{Message1, Message2};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);

        messages = {Message3, Message4};
        MockMessageBatch(2, messages, RowDispatcherId1, 1, PartitionId1);

        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
            TWatermarkOr<TMessage>{Message3},
            TWatermarkOr<TMessage>{Message4},
        };
        ReadMessages(expected);

        UNIT_ASSERT_VALUES_EQUAL(0, (SourceRead<TMessage>(UVPairParser, 0).size()));
    }

    Y_UNIT_TEST(TestSaveLoadPqRdRead) {
        TSourceState state;

        {
            TFixture f;
            f.StartSession(f.Settings);

            auto messages = std::vector{Message1, Message2};
            f.WriteMessages(0, messages, f.RowDispatcherId1, 1, PartitionId1);
            auto expected = std::vector{
                TWatermarkOr<TMessage>{Message1},
                TWatermarkOr<TMessage>{Message2},
            };
            f.ReadMessages(expected);

            f.SaveSourceState(CreateCheckpoint(), state);
            Cerr << "State saved" << Endl;
        }
        {
            TFixture f;
            f.InitRdSource(f.Settings);
            f.LoadSource(state);
            f.SourceRead<TMessage>(UVPairParser);
            f.ExpectCoordinatorChangesSubscribe();

            f.MockCoordinatorChanged(f.CoordinatorId1);
            auto req = f.ExpectCoordinatorRequest(f.CoordinatorId1);

            f.MockCoordinatorResult(f.CoordinatorId1, {{f.RowDispatcherId1, PartitionId1}}, req->Cookie);
            f.ExpectStartSession({{PartitionId1, 2}}, f.RowDispatcherId1, 1);
            f.MockAck(f.RowDispatcherId1, 1, PartitionId1);

            auto messages = std::vector{Message3};
            f.WriteMessages(2, messages, f.RowDispatcherId1, 1, PartitionId1);
            auto expected = std::vector{TWatermarkOr<TMessage>{Message3}};
            f.ReadMessages(expected);

            state.Data.clear();
            f.SaveSourceState(CreateCheckpoint(), state);
            Cerr << "State saved" << Endl;
        }
        {
            TFixture f;
            f.InitRdSource(f.Settings);
            f.LoadSource(state);
            f.SourceRead<TMessage>(UVPairParser);
            f.ExpectCoordinatorChangesSubscribe();

            f.MockCoordinatorChanged(f.CoordinatorId1);
            auto req = f.ExpectCoordinatorRequest(f.CoordinatorId1);

            f.MockCoordinatorResult(f.CoordinatorId1, {{f.RowDispatcherId1, PartitionId1}}, req->Cookie);
            f.ExpectStartSession({{PartitionId1, 3}}, f.RowDispatcherId1, 1);
            f.MockAck(f.RowDispatcherId1, 1, PartitionId1);

            auto messages = std::vector{Message4};
            f.WriteMessages(3, messages, f.RowDispatcherId1, 1, PartitionId1);
            auto expected = std::vector{TWatermarkOr<TMessage>{Message4}};
            f.ReadMessages(expected);
        }
    }

    Y_UNIT_TEST_F(CoordinatorChanged, TFixture) {
        StartSession(Settings);

        auto messages = std::vector{Message1, Message2};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);
        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
        };
        ReadMessages(expected);

        messages = {Message3};
        MockMessageBatch(2, messages, RowDispatcherId1, 1, PartitionId1);

        // change active Coordinator
        MockCoordinatorChanged(CoordinatorId2);
        // continue use old sessions
        messages = {Message4};
        MockMessageBatch(3, messages, RowDispatcherId1, 1, PartitionId1);

        auto req = ExpectCoordinatorRequest(CoordinatorId2);

        expected = {
            TWatermarkOr<TMessage>{Message3},
            TWatermarkOr<TMessage>{Message4},
        };
        ReadMessages(expected);

        MockCoordinatorResult(CoordinatorId1, {{RowDispatcherId2, PartitionId1}}, req->Cookie);       // change distribution
        ExpectStopSession(RowDispatcherId1, 1);

        ExpectStartSession({{PartitionId1, 4}}, RowDispatcherId2, 2);
        MockAck(RowDispatcherId2, 2, PartitionId1);

        messages = {Message4};
        WriteMessages(4, messages, RowDispatcherId2, 2, PartitionId1);
        expected = {TWatermarkOr<TMessage>{Message4}};
        ReadMessages(expected);

        MockHeartbeat(RowDispatcherId1, 1);       // old generation
        ExpectNoSession(RowDispatcherId1, 1);

        // change active Coordinator
        MockCoordinatorChanged(CoordinatorId1);
        req = ExpectCoordinatorRequest(CoordinatorId1);
        MockCoordinatorResult(CoordinatorId1, {{RowDispatcherId2, PartitionId1}}, req->Cookie);       // distribution is not changed

        messages = {Message2};
        WriteMessages(5, messages, RowDispatcherId2, 2, PartitionId1);
        expected = {TWatermarkOr<TMessage>{Message2}};
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(Backpressure, TFixture) {
        StartSession(Settings, 2_KB);
        const auto message = TMessage{100500, TString(900, 'c')};

        auto messages = std::vector{message};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);
        auto expected = std::vector{TWatermarkOr<TMessage>{message}};
        ReadMessages(expected);

        messages = {message, message, message};
        WriteMessages(1, messages, RowDispatcherId1, 1, PartitionId1);

        MockNewDataArrived(RowDispatcherId1, 1, PartitionId1);
        ASSERT_THROW(
            CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvGetNextBatch>(RowDispatcherId1, TDuration::Seconds(0)),
            NActors::TEmptyEventQueueException);

        expected = {
            TWatermarkOr<TMessage>{message},
            TWatermarkOr<TMessage>{message},
            TWatermarkOr<TMessage>{message},
        };
        ReadMessages(expected);

        ExpectGetNextBatch(RowDispatcherId1, PartitionId1);
        messages = {Message2};
        MockMessageBatch(4, messages, RowDispatcherId1, 1, PartitionId1);

        expected = {TWatermarkOr<TMessage>{Message2}};
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(RowDispatcherIsRestarted2, TFixture) {
        StartSession(Settings);

        auto messages = std::vector{Message1, Message2};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);
        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
        };
        ReadMessages(expected);

        MockDisconnected(RowDispatcherId1);
        MockConnected(RowDispatcherId1);
        MockUndelivered(RowDispatcherId1, 1);

        auto req = ExpectCoordinatorRequest(CoordinatorId1);
        MockCoordinatorResult(CoordinatorId1, {{RowDispatcherId1, PartitionId1}}, req->Cookie);
        ExpectStartSession({{PartitionId1, 2}}, RowDispatcherId1, 2);
        MockAck(RowDispatcherId1, 2, PartitionId1);

        messages = {Message3};
        WriteMessages(2, messages, RowDispatcherId1, 2, PartitionId1);
        expected = {TWatermarkOr<TMessage>{Message3}};
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(TwoPartitionsRowDispatcherIsRestarted, TFixture) {
        InitRdSource(Settings, 1_MB, PartitionId2 + 1);
        SourceRead<TMessage>(UVPairParser);
        ExpectCoordinatorChangesSubscribe();

        MockCoordinatorChanged(CoordinatorId1);
        auto req = ExpectCoordinatorRequest(CoordinatorId1);

        MockCoordinatorResult(CoordinatorId1, {{RowDispatcherId1, PartitionId1}, {RowDispatcherId2, PartitionId2}}, req->Cookie);
        ExpectStartSession({}, RowDispatcherId1, 1);
        ExpectStartSession({}, RowDispatcherId2, 2);
        MockAck(RowDispatcherId1, 1, PartitionId1);
        MockAck(RowDispatcherId2, 2, PartitionId2);

        auto messages = std::vector{Message1, Message2};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);
        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
        };
        ReadMessages(expected);

        messages = {Message3};
        WriteMessages(0, messages, RowDispatcherId2, 2, PartitionId2);     // not read by CA
        MockStatistics(RowDispatcherId2, 10,  2, PartitionId2);

        // Restart node 2 (RowDispatcher2)
        MockDisconnected(RowDispatcherId1);
        MockConnected(RowDispatcherId1);
        MockUndelivered(RowDispatcherId2, 2);

        // session1 is still working
        messages = {Message4};
        WriteMessages(2, messages, RowDispatcherId1, 1, PartitionId1);

        // Reinit session to RowDispatcher2
        auto req2 = ExpectCoordinatorRequest(CoordinatorId1);
        MockCoordinatorResult(CoordinatorId1, {{RowDispatcherId1, PartitionId1}, {RowDispatcherId2, PartitionId2}}, req2->Cookie);
        ExpectStartSession({{PartitionId2, 10}}, RowDispatcherId2, 3);
        MockAck(RowDispatcherId2, 3, PartitionId2);

        messages = {Message4};
        WriteMessages(3, messages, RowDispatcherId1, 1, PartitionId1);
        WriteMessages(10, messages, RowDispatcherId2, 3, PartitionId2);

        expected = {
            TWatermarkOr<TMessage>{Message3},
            TWatermarkOr<TMessage>{Message4},
            TWatermarkOr<TMessage>{Message4},
            TWatermarkOr<TMessage>{Message4},
        };
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(IgnoreMessageIfNoSessions, TFixture) {
        StartSession(Settings);
        MockCoordinatorChanged(CoordinatorId2);
        MockUndelivered(RowDispatcherId1, 1);
        MockSessionError(RowDispatcherId1);
    }

    Y_UNIT_TEST_F(MetadataFields, TFixture) {
        TReadValueParser<TMessage> metadataUVParser = [](const NUdf::TUnboxedValue& item) -> std::vector<TMessage> {
            UNIT_ASSERT_VALUES_EQUAL(item.GetListLength(), 3);
            auto stringElement = item.GetElement(2);
            return { {item.GetElement(1).Get<ui64>(), TString(stringElement.AsStringRef())} };
        };

        auto source = Settings;
        source.AddMetadataFields("_yql_sys_create_time");
        source.SetRowType("[StructType; [[_yql_sys_create_time; [DataType; Uint32]]; [dt; [DataType; Uint64]]; [value; [DataType; String]]]]");
        StartSession(source);

        auto messages = std::vector{Message1};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);
        auto expected = std::vector{TWatermarkOr<TMessage>{Message1}};
        ReadMessages(expected, metadataUVParser);
    }

    Y_UNIT_TEST_F(IgnoreCoordinatorResultIfWrongState, TFixture) {
        StartSession(Settings);

        auto messages = std::vector{Message1, Message2};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);
        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
        };
        ReadMessages(expected);

        MockCoordinatorChanged(CoordinatorId2);
        auto req = ExpectCoordinatorRequest(CoordinatorId2);
        MockUndelivered(RowDispatcherId1, 1);
        auto req2 = ExpectCoordinatorRequest(CoordinatorId2);

        MockCoordinatorResult(CoordinatorId1, {{RowDispatcherId1, PartitionId1}}, req->Cookie);
        MockCoordinatorResult(CoordinatorId1, {{RowDispatcherId1, PartitionId1}}, req2->Cookie);
        ExpectStartSession({{PartitionId1, 2}}, RowDispatcherId1, 2);

        MockAck(RowDispatcherId1, 1, PartitionId1);
    }

    Y_UNIT_TEST_F(TestReadFromTopicFirstWatermark, TFixture) {
        StartSession(Settings);

        auto messages = std::vector<std::pair<TMaybe<TMessage>, TMaybe<TInstant>>>{
            {Message1, Nothing()},
            {Message2, TInstant::MicroSeconds(100)},
            {Message3, Nothing()},
            {Message4, Nothing()},
            {Message5, TInstant::MicroSeconds(200)},
            {Message6, TInstant::MicroSeconds(300)},
        };
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);

        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(100)},
        };
        ReadMessages(expected);

        expected = {
            TWatermarkOr<TMessage>{Message3},
            TWatermarkOr<TMessage>{Message4},
            TWatermarkOr<TMessage>{Message5},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(200)},
        };
        ReadMessages(expected);

        expected = {
            TWatermarkOr<TMessage>{Message6},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(300)},
        };
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(TestReadFromTopicWatermarks1, TFixture) {
        StartSession(Settings);

        auto messages = std::vector<std::pair<TMaybe<TMessage>, TMaybe<TInstant>>>{{Message1, Nothing()}};
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);

        messages = {
            {Message2, TInstant::MicroSeconds(100)},
            {Message3, Nothing()},
        };
        WriteMessages(1, messages, RowDispatcherId1, 1, PartitionId1);

        messages = {
            {Message4, Nothing()},
            {Message5, TInstant::MicroSeconds(200)},
            {Message6, TInstant::MicroSeconds(300)},
        };
        WriteMessages(3, messages, RowDispatcherId1, 1, PartitionId1);

        auto expected = std::vector{
            TWatermarkOr<TMessage>{Message1},
            TWatermarkOr<TMessage>{Message2},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(100)},
            TWatermarkOr<TMessage>{Message3},
            TWatermarkOr<TMessage>{Message4},
            TWatermarkOr<TMessage>{Message5},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(200)},
            TWatermarkOr<TMessage>{Message6},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(300)},
        };
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(TestWatermarksWhere, TFixture) {
        StartSession(Settings);

        auto messages = std::vector<std::pair<TMaybe<TMessage>, TMaybe<TInstant>>>{
            {Nothing(), TInstant::MicroSeconds(100)},
            {Message2, TInstant::MicroSeconds(200)},
            {Nothing(), TInstant::MicroSeconds(300)},
            {Nothing(), TInstant::MicroSeconds(400)},
            {Message5, TInstant::MicroSeconds(500)},
            {Message6, TInstant::MicroSeconds(600)},
        };
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);

        auto expected = std::vector{
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(100)},
            TWatermarkOr<TMessage>{Message2},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(200)},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(300)},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(400)},
            TWatermarkOr<TMessage>{Message5},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(500)},
            TWatermarkOr<TMessage>{Message6},
            TWatermarkOr<TMessage>{TInstant::MicroSeconds(600)},
        };
        ReadMessages(expected);
    }

    Y_UNIT_TEST_F(TestWatermarksWhereFalse, TFixture) {
        StartSession(Settings);

        auto messages = std::vector<std::pair<TMaybe<TMessage>, TMaybe<TInstant>>>{
            {Nothing(), TInstant::MicroSeconds(100)},
            {Nothing(), TInstant::MicroSeconds(200)},
        };
        WriteMessages(0, messages, RowDispatcherId1, 1, PartitionId1);

        auto expected = std::vector{TWatermarkOr<TMessage>{TInstant::MicroSeconds(100)}};
        ReadMessages(expected);

        expected = {TWatermarkOr<TMessage>{TInstant::MicroSeconds(200)}};
        ReadMessages(expected);
    }

    Y_UNIT_TEST(WatermarkCheckpointWithItemsInReadyBuffer) {
        TSourceState state;

        {
            TFixture f;
            f.StartSession(f.Settings);

            auto messages = std::vector<std::pair<TMaybe<TMessage>, TMaybe<TInstant>>>{
                {Message1, TInstant::MicroSeconds(100)},
                {Message2, TInstant::MicroSeconds(200)},
            };
            f.WriteMessages(0, messages, f.RowDispatcherId1, 1, PartitionId1);

            auto expected = std::vector{
                TWatermarkOr<TMessage>{Message1},
                TWatermarkOr<TMessage>{TInstant::MicroSeconds(100)},
            };
            f.ReadMessages(expected);

            messages = {
                {Message3, TInstant::MicroSeconds(300)},
                {Message4, TInstant::MicroSeconds(400)},
            };
            f.WriteMessages(2, messages, f.RowDispatcherId1, 1, PartitionId1);

            // read only watermark (1-st batch), items '3', '4' will stay in ready buffer inside Source actor
            expected = {
                TWatermarkOr<TMessage>{Message2},
                TWatermarkOr<TMessage>{TInstant::MicroSeconds(200)},
            };
            f.ReadMessages(expected);

            f.SaveSourceState(CreateCheckpoint(), state);
            Cerr << "State saved" << Endl;
        }
        {
            TFixture f;
            f.InitRdSource(f.Settings);
            f.LoadSource(state);
            f.SourceRead<TMessage>(UVPairParser);
            f.ExpectCoordinatorChangesSubscribe();

            f.MockCoordinatorChanged(f.CoordinatorId1);
            auto req = f.ExpectCoordinatorRequest(f.CoordinatorId1);

            f.MockCoordinatorResult(f.CoordinatorId1, {{f.RowDispatcherId1, PartitionId1}}, req->Cookie);
            f.ExpectStartSession({{PartitionId1, 2}}, f.RowDispatcherId1, 1);
            f.MockAck(f.RowDispatcherId1, 1, PartitionId1);

            // Since items '3', '4' weren't returned from source actor, they should be reread
            auto messages = std::vector<std::pair<TMaybe<TMessage>, TMaybe<TInstant>>>{
                {Message3, TInstant::MicroSeconds(300)},
                {Message4, TInstant::MicroSeconds(400)},
            };
            f.WriteMessages(2, messages, f.RowDispatcherId1, 1, PartitionId1);
            auto expected = std::vector{
                TWatermarkOr<TMessage>{Message3},
                TWatermarkOr<TMessage>{TInstant::MicroSeconds(300)},
                TWatermarkOr<TMessage>{Message4},
                TWatermarkOr<TMessage>{TInstant::MicroSeconds(400)},
            };
            f.ReadMessages(expected);
        }
    }
}

} // namespace NYql::NDq
