#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <thread>

namespace NYql::NDq {

struct TFixture : public TPqIoTestFixture {


    void ExpectCoordinatorChangesSubscribe() {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe>(LocalRowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectCoordinatorRequest() {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorRequest>(Coordinator1Id, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectStartSession() {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStartSession>(RemoteRowDispatcher, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectGetNextBatch() {
        auto eventHolder = CaSetup->Runtime->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvGetNextBatch>(RemoteRowDispatcher, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void MockCoordinatorChanged() {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvCoordinatorChanged(Coordinator1Id);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, LocalRowDispatcherId, event));
        });
    }

    void MockCoordinatorResult() {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvCoordinatorResult();
            auto* partitions = event->Record.AddPartitions();
            partitions->AddPartitionId(0);
            ActorIdToProto(RemoteRowDispatcher, partitions->MutableActorId());
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, Coordinator1Id, event));
        });
    }

    void MockAck() {
        CaSetup->Execute([&](TFakeActor& actor) {
            NFq::NRowDispatcherProto::TEvStartSession proto;
            proto.SetPartitionId(0);
            auto event = new NFq::TEvRowDispatcher::TEvStartSessionAck(proto);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, RemoteRowDispatcher, event));
        });
    }

    void MockNewDataArrived() {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvNewDataArrived();
            event->Record.SetPartitionId(0);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, RemoteRowDispatcher, event));
        });
    }

    void MockMessageBatch(ui64 offset, const std::vector<TString>& jsons) {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvMessageBatch();
            for (const auto& json :jsons) {
                NFq::NRowDispatcherProto::TEvMessage message;
                message.SetJson(json);
                message.SetOffset(offset++);
                event->Record.AddMessages()->CopyFrom(message);
            }
            event->Record.SetPartitionId(0);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, RemoteRowDispatcher, event));
        });
    }

    void MockSessionError() {
        CaSetup->Execute([&](TFakeActor& actor) {
            auto event = new NFq::TEvRowDispatcher::TEvSessionError();
            event->Record.SetMessage("A problem has been detected and session has been shut down to prevent damage your life");
            event->Record.SetPartitionId(0);
            CaSetup->Runtime->Send(new NActors::IEventHandle(*actor.DqAsyncInputActorId, RemoteRowDispatcher, event));
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
                UNIT_ASSERT_EQUAL(std::get<T>(item), expected.at(expectedPos));
                expectedPos++;
            }
        }
    }

    void StartSession() {
        InitRdSource(BuildPqTopicSourceSettings("topicName"));
        SourceRead<TString>(UVParser);
        ExpectCoordinatorChangesSubscribe();
    
        MockCoordinatorChanged();
        ExpectCoordinatorRequest();

        MockCoordinatorResult();
        ExpectStartSession();
        MockAck();
    }

    const TString Json1 = "{\"dt\":100,\"value\":\"value1\"}";
    const TString Json2 = "{\"dt\":200,\"value\":\"value2\"}";
    const TString Json3 = "{\"dt\":300,\"value\":\"value3\"}";
    const TString Json4 = "{\"dt\":400,\"value\":\"value4\"}";
};

Y_UNIT_TEST_SUITE(TDqPqRdReadActorTest) {
    Y_UNIT_TEST_F(TestReadFromTopic2, TFixture) {
        StartSession();

        MockNewDataArrived();
        ExpectGetNextBatch();

        const std::vector<TString> data = {Json1, Json2};
        MockMessageBatch(0, data);

        auto result = SourceReadDataUntil<TString>(UVParser, 2);
        AssertDataWithWatermarks(result, data, {});
    }

    Y_UNIT_TEST_F(SessionError, TFixture) {
        StartSession();

        TInstant deadline = Now() + TDuration::Seconds(5);
        auto future = CaSetup->AsyncInputPromises.FatalError.GetFuture();
        MockSessionError();

      //  UNIT_ASSERT(future.HasValue());

        bool failured = false;
        while (Now() < deadline) {
            SourceRead<TString>(UVParser);
            if (future.HasValue()) {
              //  UNIT_ASSERT_STRING_CONTAINS(future.GetValue().ToOneLineString(), "Read session to topic \"NonExistentTopic\" was closed");
                failured = true;
                break;
            }
        }
        UNIT_ASSERT_C(failured, "Failure timeout");
    }

    Y_UNIT_TEST_F(ReadWithFreeSpace, TFixture) {
        StartSession();

        MockNewDataArrived();
        ExpectGetNextBatch();

        const std::vector<TString> data1 = {Json1, Json2};
        MockMessageBatch(0, data1);

        const std::vector<TString> data2 = {Json3, Json4};
        MockMessageBatch(0, data2);

        auto result = SourceReadDataUntil<TString>(UVParser, 1, 1);
        std::vector<TString> expected{data1};
        AssertDataWithWatermarks(result, expected, {});
    }
}
} // NYql::NDq
