#include "ut_helpers.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NYql::NDq {

namespace {

// We can't be sure that no extra watermarks were generated (we can't control LB receipt write time).
// So, we will check only if there is at least one watermark before each specified position.
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

constexpr auto defaultWatermarkPeriod = TDuration::MilliSeconds(100);
constexpr auto defaultLateArrivalDelay = TDuration::MilliSeconds(1);

void WaitForNextWatermark(TDuration lateArrivalDelayMs = defaultLateArrivalDelay) {
    // We can't control write time in LB, so just sleep for watermarkPeriod to ensure the next written data
    // will obtain write_time which will move watermark forward.
    Sleep(lateArrivalDelayMs);
}

}

Y_UNIT_TEST_SUITE(TDqPqReadActorTest) {
    Y_UNIT_TEST_F(TestReadFromTopic, TPqIoTestFixture) {
        const TString topicName = "ReadFromTopic";
        PQCreateStream(topicName);
        InitSource(topicName);

        const std::vector<TString> data = { "1", "2", "3", "4" };
        PQWrite(data, topicName);

        auto result = SourceReadDataUntil<TString>(UVParser, 4);
        AssertDataWithWatermarks(result, data, {});
    }

    Y_UNIT_TEST_F(TestReadFromTopicFromNow, TPqIoTestFixture) {
        const TString topicName = "ReadFromTopicFromNow";
        PQCreateStream(topicName);

        const std::vector<TString> oldData = { "-4", "-3", "-2", "-1", "0" };
        PQWrite(oldData, topicName);

        InitSource(topicName);

        const std::vector<TString> data = { "1", "2", "3", "4" };
        PQWrite(data, topicName);

        auto result = SourceReadDataUntil<TString>(UVParser, 4);
        AssertDataWithWatermarks(result, data, {});
    }

    Y_UNIT_TEST_F(ReadWithFreeSpace, TPqIoTestFixture) {
        const TString topicName = "ReadWithFreeSpace";
        PQCreateStream(topicName);
        InitSource(topicName);

        PQWrite({"data1", "data2", "data3"}, topicName);

        {
            auto result = SourceReadDataUntil<TString>(UVParser, 1, 1);
            std::vector<TString> expected {"data1"};
            AssertDataWithWatermarks(result, expected, {});
        }

        UNIT_ASSERT_EQUAL(SourceRead<TString>(UVParser, 0).size(), 0);
        UNIT_ASSERT_EQUAL(SourceRead<TString>(UVParser, -1).size(), 0);
    }

    Y_UNIT_TEST_F(ReadNonExistentTopic, TPqIoTestFixture) {
        const TString topicName = "NonExistentTopic";
        InitSource(topicName);

        TInstant deadline = Now() + TDuration::Seconds(5);
        auto future = CaSetup->AsyncInputPromises.FatalError.GetFuture();
        bool failured = false;
        while (Now() < deadline) {
            SourceRead<TString>(UVParser);
            if (future.HasValue()) {
                UNIT_ASSERT_STRING_CONTAINS(future.GetValue().ToOneLineString(), "Read session to topic \"NonExistentTopic\" was closed");
                failured = true;
                break;
            }
        }
        UNIT_ASSERT_C(failured, "Failure timeout");
    }

    Y_UNIT_TEST(TestSaveLoadPqRead) {
        TSourceState state;
        const TString topicName = "SaveLoadPqRead";
        PQCreateStream(topicName);

        {
            TPqIoTestFixture setup1;
            setup1.InitSource(topicName);

            std::vector<TString> data {"data"};
            PQWrite(data, topicName);

            auto result = setup1.SourceReadDataUntil<TString>(UVParser, 1);
            AssertDataWithWatermarks(result, data, {});

            auto checkpoint = CreateCheckpoint();
            setup1.SaveSourceState(checkpoint, state);
            Cerr << "State saved" << Endl;
        }

        TSourceState state2;
        {
            TPqIoTestFixture setup2;
            setup2.InitSource(topicName);

            std::vector<TString> data {"data"};
            PQWrite({"data"}, topicName);

            setup2.LoadSource(state);
            Cerr << "State loaded" << Endl;
            auto result = setup2.SourceReadDataUntil<TString>(UVParser, 1);
            AssertDataWithWatermarks(result, data, {});

            auto checkpoint = CreateCheckpoint();
            setup2.SaveSourceState(checkpoint, state2);

            PQWrite({"futherData"}, topicName);
        }

        TSourceState state3;
        {
            TPqIoTestFixture setup3;
            setup3.InitSource(topicName);
            setup3.LoadSource(state2);

            auto result = setup3.SourceReadDataUntil<TString>(UVParser, 1);
            std::vector<TString> data {"futherData"};
            AssertDataWithWatermarks(result, data, {});

            // pq session is steel alive

            PQWrite({"yetAnotherData"}, topicName);

            auto checkpoint = CreateCheckpoint();
            setup3.SaveSourceState(checkpoint, state3);
        }

        // Load the first state and check it.
        {
            TPqIoTestFixture setup4;
            setup4.InitSource(topicName);
            setup4.LoadSource(state);

            auto result = setup4.SourceReadUntil<TString>(UVParser, 3);
            std::vector<TString> data {"data", "futherData", "yetAnotherData"};
            AssertDataWithWatermarks(result, data, {});
        }

        // Load graphState2 and check it (offsets were saved).
        {
            TPqIoTestFixture setup5;
            setup5.InitSource(topicName);
            setup5.LoadSource(state2);

            auto result = setup5.SourceReadDataUntil<TString>(UVParser, 2);
            std::vector<TString> data {"futherData", "yetAnotherData"};
            AssertDataWithWatermarks(result, data, {});
        }

        // Load graphState3 and check it (other offsets).
        {
            TPqIoTestFixture setup6;
            setup6.InitSource(topicName);
            setup6.LoadSource(state3);

            auto result = setup6.SourceReadDataUntil<TString>(UVParser, 1);
            std::vector<TString> data {"yetAnotherData"};
            AssertDataWithWatermarks(result, data, {});
        }
    }

    Y_UNIT_TEST(LoadCorruptedState) {
        TSourceState state;
        const TString topicName = "Invalid"; // We wouldn't read from this topic.
        auto checkpoint = CreateCheckpoint();

        {
            TPqIoTestFixture setup1;
            setup1.InitSource(topicName);
            setup1.SaveSourceState(checkpoint, state);
        }

        // Corrupt state.
        TString corruptedBlob = state.Data.front().Blob;
        corruptedBlob.append('a');
        state.Data.front().Blob = corruptedBlob;

        {
            TPqIoTestFixture setup2;
            setup2.InitSource(topicName);
            UNIT_ASSERT_EXCEPTION_CONTAINS(setup2.LoadSource(state), yexception, "Serialized state is corrupted");
        }
    }

    Y_UNIT_TEST(TestLoadFromSeveralStates) {
        const TString topicName = "LoadFromSeveralStates";
        PQCreateStream(topicName);

        TSourceState state2;
        {
            TPqIoTestFixture setup;
            setup.InitSource(topicName);

            std::vector<TString> data {"data"};
            PQWrite(data, topicName);

            auto result1 = setup.SourceReadDataUntil<TString>(UVParser, 1);
            AssertDataWithWatermarks(result1, data, {});

            TSourceState state1;
            auto checkpoint1 = CreateCheckpoint();
            setup.SaveSourceState(checkpoint1, state1);
            Cerr << "State saved" << Endl;

            std::vector<TString> data2 {"data2"};
            PQWrite(data2, topicName);

            auto result2 = setup.SourceReadDataUntil<TString>(UVParser, 1);
            AssertDataWithWatermarks(result2, data2, {});

            auto checkpoint2 = CreateCheckpoint();
            setup.SaveSourceState(checkpoint2, state2);
            Cerr << "State 2 saved" << Endl;

            // Add state1 to state2
            state2.Data.push_back(state1.Data.front());
        }

        TPqIoTestFixture setup2;
        setup2.InitSource(topicName);
        setup2.LoadSource(state2); // Loads min offset

        std::vector<TString> data3 {"data3"};
        PQWrite(data3, topicName);

        auto result = setup2.SourceReadUntil<TString>(UVParser, 2);
        std::vector<TString> dataResult {"data2", "data3"};
        AssertDataWithWatermarks(result, dataResult, {});
    }

    Y_UNIT_TEST_F(TestReadFromTopicFirstWatermark, TPqIoTestFixture) {
        const TString topicName = "ReadFromTopicFirstWatermark";
        PQCreateStream(topicName);

        auto settings = BuildPqTopicSourceSettings(topicName, defaultWatermarkPeriod);
        InitSource(std::move(settings));

        const std::vector<TString> data = { "1", "2", "3", "4" };
        PQWrite(data, topicName);

        auto result = SourceReadDataUntil<TString>(UVParser, 4);
        AssertDataWithWatermarks(result, { "1", "2", "3", "4" }, {0});
    }

    Y_UNIT_TEST_F(TestReadFromTopicWatermarks1, TPqIoTestFixture) {
        const TString topicName = "ReadFromTopicWatermarks1";
        PQCreateStream(topicName);

        auto settings = BuildPqTopicSourceSettings(topicName, defaultWatermarkPeriod, defaultLateArrivalDelay);
        InitSource(std::move(settings));

        const std::vector<TString> data1 = { "1", "2" };
        PQWrite(data1, topicName);

        WaitForNextWatermark();
        const std::vector<TString> data2 = { "3", "4", "5" };
        PQWrite(data2, topicName);

        WaitForNextWatermark();
        const std::vector<TString> data3 = { "6" };
        PQWrite(data3, topicName);

        auto result = SourceReadDataUntil<TString>(UVParser, 6);
        AssertDataWithWatermarks(result, {"1", "2", "3", "4", "5", "6"}, {0, 2, 5});
    }

    Y_UNIT_TEST(WatermarkCheckpointWithItemsInReadyBuffer) {
        const TString topicName = "WatermarkCheckpointWithItemsInReadyBuffer";
        PQCreateStream(topicName);
        TSourceState state;

        {
            TPqIoTestFixture setup;
            auto settings = BuildPqTopicSourceSettings(topicName, defaultWatermarkPeriod);
            setup.InitSource(std::move(settings));

            std::vector<TString> data1 {"1", "2"};
            PQWrite(data1, topicName);

            auto result = setup.SourceReadDataUntil<TString>(UVParser, 2);
            AssertDataWithWatermarks(result, data1, {0});

            WaitForNextWatermark();
            std::vector<TString> data2 {"3", "4"};
            PQWrite(data2, topicName);

            // read only watermark (1-st batch), items '3', '4' will stay in ready buffer inside Source actor
            result = setup.SourceReadUntil<TString>(UVParser, 1);
            AssertDataWithWatermarks(result, {}, {0});

            auto checkpoint = CreateCheckpoint();
            setup.SaveSourceState(checkpoint, state);
            Cerr << "State saved" << Endl;
        }

        {
            TPqIoTestFixture setup;
            auto settings = BuildPqTopicSourceSettings(topicName, defaultWatermarkPeriod);
            setup.InitSource(std::move(settings));
            setup.LoadSource(state);

            auto result = setup.SourceReadDataUntil<TString>(UVParser, 2);
            // Since items '3', '4' weren't returned from source actor, they should be reread
            AssertDataWithWatermarks(result, {"3", "4"}, {});
        }
    }
}
} // NYql::NDq
