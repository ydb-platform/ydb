#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

namespace {

constexpr auto DefaultWatermarkPeriod = TDuration::MilliSeconds(100);
constexpr auto DefaultLateArrivalDelay = TDuration::MilliSeconds(10);

const TString Message0 = "value0";
const TString Message1 = "value1";
const TString Message2 = "value2";
const TString Message3 = "value3";
const TString Message4 = "value4";
const TString Message5 = "value5";

class TFixture : public TPqIoTestFixture {
public:
    void InitSource(NYql::NPq::NProto::TDqPqTopicSource&& settings) const {
        CaSetup->Execute([&](TFakeActor& actor) {
            NPq::NProto::TDqReadTaskParams params;
            auto* partitioningParams = params.MutablePartitioningParams();
            partitioningParams->SetTopicPartitionsCount(1);
            partitioningParams->SetEachTopicPartitionGroupId(0);
            partitioningParams->SetDqPartitionsCount(1);

            TPqGatewayServices pqServices(
                Driver,
                nullptr,
                nullptr,
                std::make_shared<TPqGatewayConfig>(),
                nullptr
            );

            i64 freeSpace = 1_MB;

            auto [dqAsyncInput, dqAsyncInputAsActor] = CreateDqPqReadActor(
                std::move(settings),
                0,
                NYql::NDq::TCollectStatsLevel::None,
                "query_1",
                0,
                {},
                {params},
                Driver,
                nullptr,
                actor.SelfId(),
                actor.GetHolderFactory(),
                MakeIntrusive<NMonitoring::TDynamicCounters>(),
                CreatePqNativeGateway(std::move(pqServices)),
                1,
                freeSpace
            );

            actor.InitAsyncInput(dqAsyncInput, dqAsyncInputAsActor);
        });
    }

    void InitSource(const TString& topic) const {
        InitSource(BuildPqTopicSourceSettings(topic));
    }

    template<typename T>
    void PQRead(
        const std::vector<TWatermarkOr<T>>& expected,
        TReadValueParser<T> parser = UVParser,
        i64 eachReadFreeSpace = 1'000
    ) const {
        auto op = [](size_t init, const TWatermarkOr<T>& v) { return init + std::holds_alternative<T>(v); };
        auto size = std::accumulate(expected.begin(), expected.end(), 0ull, op);
        auto actual = SourceReadDataUntil<TString>(parser, size, eachReadFreeSpace);
        AssertDataWithWatermarks(expected, actual);
    }

    // We can't be sure that no extra watermarks were generated (we can't control LB receipt write time).
    // So, we will check only if there is at least one watermark before each specified position.
    template<typename T>
    void AssertDataWithWatermarks(
        const std::vector<TWatermarkOr<T>>& expected,
        const std::vector<TWatermarkOr<T>>& actual
    ) const {
        size_t expected_i = 0, actual_i = 0;
        for (; expected_i < expected.size() && actual_i < actual.size(); ++expected_i, ++actual_i) {
            std::visit(TOverloaded{
                [&expected_i, &actual_i](const T& expected, const T& actual) {
                    UNIT_ASSERT_VALUES_EQUAL_C(expected, actual, "expected_i = " << expected_i << ", actual_i = " << actual_i);
                },
                [&expected_i](const T&, const TInstant&) {
                    --expected_i;
                },
                [&actual_i](const TInstant&, const T&) {
                    --actual_i;
                },
                [](TInstant, TInstant) {
                },
            }, expected[expected_i], actual[actual_i]);
        }
        for (; expected_i < expected.size(); ++expected_i) {
            std::visit(TOverloaded{
                [expected_i](const T&) {
                    UNIT_FAIL("expected_i = " << expected_i);
                },
                [](TInstant) {
                },
            }, expected[expected_i]);
        }
        for (; actual_i < actual.size(); ++actual_i) {
            std::visit(TOverloaded{
                [actual_i](const T&) {
                    UNIT_FAIL("actual_i = " << actual_i);
                },
                [](TInstant) {
                },
            }, actual[actual_i]);
        }
    }

    void WaitForNextWatermark() {
        // We can't control write time in LB, so just sleep for watermarkPeriod to ensure the next written data
        // will obtain write_time which will move watermark forward.
        Sleep(DefaultLateArrivalDelay);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqPqReadActorTest) {
    Y_UNIT_TEST_F(TestReadFromTopic, TFixture) {
        const TString topicName = "ReadFromTopic";
        PQCreateStream(topicName);
        InitSource(topicName);

        const std::vector<TString> data = { "1", "2", "3", "4" };
        auto messages = std::vector{Message0, Message1, Message2, Message3};
        PQWrite(messages, topicName);

        auto expected = std::vector{
            TWatermarkOr<TString>{Message0},
            TWatermarkOr<TString>{Message1},
            TWatermarkOr<TString>{Message2},
            TWatermarkOr<TString>{Message3},
        };
        PQRead(expected);
    }

    Y_UNIT_TEST_F(TestReadFromTopicFromNow, TFixture) {
        const TString topicName = "ReadFromTopicFromNow";
        PQCreateStream(topicName);

        auto oldMessages = std::vector{Message0};
        PQWrite(oldMessages, topicName);

        auto settings = BuildPqTopicSourceSettings(topicName);
        settings.mutable_disposition()->mutable_fresh()->CopyFrom(google::protobuf::Empty{});
        InitSource(std::move(settings));

        auto messages = std::vector{Message1, Message2, Message3, Message4};
        PQWrite(messages, topicName);

        auto expected = std::vector{
            TWatermarkOr<TString>{Message1},
            TWatermarkOr<TString>{Message2},
            TWatermarkOr<TString>{Message3},
            TWatermarkOr<TString>{Message4},
        };
        PQRead(expected);
    }

    Y_UNIT_TEST_F(ReadWithFreeSpace, TFixture) {
        const TString topicName = "ReadWithFreeSpace";
        PQCreateStream(topicName);
        InitSource(topicName);

        auto messages = std::vector{Message0, Message1, Message2};
        PQWrite(messages, topicName);

        auto expected = std::vector{TWatermarkOr<TString>{Message0}};
        PQRead<TString>(expected, UVParser, 1);

        UNIT_ASSERT_VALUES_EQUAL(0, SourceRead<TString>(UVParser, 0).size());
        UNIT_ASSERT_VALUES_EQUAL(0, SourceRead<TString>(UVParser, -1).size());
    }

    Y_UNIT_TEST_F(ReadNonExistentTopic, TFixture) {
        const TString topicName = "NonExistentTopic";
        InitSource(topicName);

        TInstant deadline = Now() + TDuration::Seconds(5);
        auto future = CaSetup->AsyncInputPromises.FatalError.GetFuture();
        bool failed = false;
        while (Now() < deadline) {
            SourceRead<TString>(UVParser);
            if (future.HasValue()) {
                auto message = future.GetValue().ToOneLineString();
                UNIT_ASSERT_STRING_CONTAINS(message, "Error: ");
                UNIT_ASSERT_STRING_CONTAINS(message, " \"NonExistentTopic\" ");
                failed = true;
                break;
            }
        }
        UNIT_ASSERT_C(failed, "Failure timeout");
    }

    Y_UNIT_TEST(TestSaveLoadPqRead) {
        TSourceState state;
        const TString topicName = "SaveLoadPqRead";
        PQCreateStream(topicName);

        {
            TFixture setup1;
            setup1.InitSource(topicName);

            auto messages = std::vector{Message0};
            PQWrite(messages, topicName);

            auto expected = std::vector{TWatermarkOr<TString>{Message0}};
            setup1.PQRead(expected);

            auto checkpoint = CreateCheckpoint();
            setup1.SaveSourceState(checkpoint, state);
            Cerr << "State saved" << Endl;
        }

        TSourceState state2;
        {
            TFixture setup2;
            setup2.InitSource(topicName);

            auto messages = std::vector{Message1};
            PQWrite(messages, topicName);

            setup2.LoadSource(state);
            Cerr << "State loaded" << Endl;
            auto expected = std::vector{TWatermarkOr<TString>{Message1}};
            setup2.PQRead(expected);

            auto checkpoint = CreateCheckpoint();
            setup2.SaveSourceState(checkpoint, state2);

            PQWrite({Message2}, topicName);

            // Wait for events to be written to topic
            Sleep(TDuration::Seconds(10));
        }

        TSourceState state3;
        {
            TFixture setup3;
            setup3.InitSource(topicName);
            setup3.LoadSource(state2);

            auto expected = std::vector{TWatermarkOr<TString>{Message2}};
            setup3.PQRead(expected);

            // pq session is still alive

            PQWrite({Message3}, topicName);

            // Wait for events to be written to topic
            Sleep(TDuration::Seconds(10));

            auto checkpoint = CreateCheckpoint();
            setup3.SaveSourceState(checkpoint, state3);
        }

        // Load the first state and check it.
        {
            TFixture setup4;
            setup4.InitSource(topicName);
            setup4.LoadSource(state);

            auto expected = std::vector{
                TWatermarkOr<TString>{Message1},
                TWatermarkOr<TString>{Message2},
                TWatermarkOr<TString>{Message3},
            };
            setup4.PQRead(expected);
        }

        // Load graphState2 and check it (offsets were saved).
        {
            TFixture setup5;
            setup5.InitSource(topicName);
            setup5.LoadSource(state2);

            auto expected = std::vector{
                TWatermarkOr<TString>{Message2},
                TWatermarkOr<TString>{Message3},
            };
            setup5.PQRead(expected);
        }

        // Load graphState3 and check it (other offsets).
        {
            TFixture setup6;
            setup6.InitSource(topicName);
            setup6.LoadSource(state3);

            auto expected = std::vector{TWatermarkOr<TString>{Message3}};
            setup6.PQRead(expected);
        }
    }

    Y_UNIT_TEST(LoadCorruptedState) {
        TSourceState state;
        const TString topicName = "Invalid"; // We wouldn't read from this topic.
        auto checkpoint = CreateCheckpoint();

        {
            TFixture setup1;
            setup1.InitSource(topicName);
            setup1.SaveSourceState(checkpoint, state);
        }

        // Corrupt state.
        TString corruptedBlob = state.Data.front().Blob;
        corruptedBlob.append('a');
        state.Data.front().Blob = corruptedBlob;

        {
            TFixture setup2;
            setup2.InitSource(topicName);
            UNIT_ASSERT_EXCEPTION_CONTAINS(setup2.LoadSource(state), yexception, "Serialized state is corrupted");
        }
    }

    Y_UNIT_TEST(TestLoadFromSeveralStates) {
        const TString topicName = "LoadFromSeveralStates";
        PQCreateStream(topicName);

        TSourceState state2;
        {
            TFixture setup;
            setup.InitSource(topicName);

            auto messages = std::vector{Message0};
            PQWrite(messages, topicName);

            auto expected = std::vector{TWatermarkOr<TString>{Message0}};
            setup.PQRead(expected);

            TSourceState state1;
            auto checkpoint1 = CreateCheckpoint();
            setup.SaveSourceState(checkpoint1, state1);
            Cerr << "State saved" << Endl;

            messages = {Message1};
            PQWrite(messages, topicName);

            expected = {TWatermarkOr<TString>{Message1}};
            setup.PQRead(expected);

            auto checkpoint2 = CreateCheckpoint();
            setup.SaveSourceState(checkpoint2, state2);
            Cerr << "State 2 saved" << Endl;

            // Add state1 to state2
            state2.Data.push_back(state1.Data.front());
        }

        TFixture setup2;
        setup2.InitSource(topicName);
        setup2.LoadSource(state2); // Loads min offset

        auto messages = std::vector{Message2};
        PQWrite(messages, topicName);

        auto expected = std::vector{
            TWatermarkOr<TString>{Message1},
            TWatermarkOr<TString>{Message2},
        };
        setup2.PQRead(expected);
    }

    Y_UNIT_TEST_F(TestReadFromTopicFirstWatermark, TFixture) {
        const TString topicName = "ReadFromTopicFirstWatermark";
        PQCreateStream(topicName);

        auto settings = BuildPqTopicSourceSettings(topicName, DefaultWatermarkPeriod);
        InitSource(std::move(settings));

        auto messages = std::vector{Message0, Message1, Message2, Message3};
        PQWrite(messages, topicName);

        auto expected = std::vector{
            TWatermarkOr<TString>{Message0},
            TWatermarkOr<TString>{TInstant::Zero()},
            TWatermarkOr<TString>{Message1},
            TWatermarkOr<TString>{Message2},
            TWatermarkOr<TString>{Message3},
        };
        PQRead(expected);
    }

    Y_UNIT_TEST_F(TestReadFromTopicWatermarks1, TFixture) {
        const TString topicName = "ReadFromTopicWatermarks1";
        PQCreateStream(topicName);

        auto settings = BuildPqTopicSourceSettings(topicName, DefaultWatermarkPeriod, DefaultLateArrivalDelay);
        InitSource(std::move(settings));

        auto messages = std::vector{Message0, Message1};
        PQWrite(messages, topicName);

        WaitForNextWatermark();
        messages = {Message2, Message3, Message4};
        PQWrite(messages, topicName);

        WaitForNextWatermark();
        messages = {Message5};
        PQWrite(messages, topicName);

        auto expected = std::vector{
            TWatermarkOr<TString>{Message0},
            TWatermarkOr<TString>{TInstant::Zero()},
            TWatermarkOr<TString>{Message1},
            TWatermarkOr<TString>{Message2},
            TWatermarkOr<TString>{TInstant::Zero()},
            TWatermarkOr<TString>{Message3},
            TWatermarkOr<TString>{Message4},
            TWatermarkOr<TString>{Message5},
            TWatermarkOr<TString>{TInstant::Zero()},
        };
        PQRead(expected);
    }

    Y_UNIT_TEST(WatermarkCheckpointWithItemsInReadyBuffer) {
        const TString topicName = "WatermarkCheckpointWithItemsInReadyBuffer";
        PQCreateStream(topicName);
        TSourceState state;

        {
            TFixture setup;
            auto settings = BuildPqTopicSourceSettings(topicName, DefaultWatermarkPeriod);
            setup.InitSource(std::move(settings));

            auto messages = std::vector{Message0, Message1};
            PQWrite(messages, topicName);

            auto expected = std::vector{
                TWatermarkOr<TString>{Message0},
                TWatermarkOr<TString>{TInstant::Zero()},
            };
            setup.PQRead(expected);

            setup.WaitForNextWatermark();
            messages = {Message2, Message3};
            PQWrite(messages, topicName);

            // read only watermark (1-st batch), items '3', '4' will stay in ready buffer inside Source actor
            expected = {TWatermarkOr<TString>{Message1}};
            setup.PQRead(expected);

            auto checkpoint = CreateCheckpoint();
            setup.SaveSourceState(checkpoint, state);
            Cerr << "State saved" << Endl;
        }
        {
            TFixture setup;
            auto settings = BuildPqTopicSourceSettings(topicName, DefaultWatermarkPeriod);
            setup.InitSource(std::move(settings));
            setup.LoadSource(state);

            // Since items '3', '4' weren't returned from source actor, they should be reread
            auto expected = std::vector{
                TWatermarkOr<TString>{Message2},
                TWatermarkOr<TString>{TInstant::Zero()},
                TWatermarkOr<TString>{Message3},
            };
            setup.PQRead(expected);
        }
    }
}

} // namespace NYql::NDq
