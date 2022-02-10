#include "ut_helpers.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NYql::NDq {

Y_UNIT_TEST_SUITE(TDqPqReadActorTest) {
    Y_UNIT_TEST_F(TestReadFromTopic, TPqIoTestFixture) { 
        const TString topicName = "ReadFromTopic";
        InitSource(topicName); 

        const std::vector<TString> data = { "1", "2", "3", "4" };
        PQWrite(data, topicName);

        auto result = SourceReadUntil<TString>(UVParser, 4); 
        UNIT_ASSERT_EQUAL(result, data);
    }

    Y_UNIT_TEST_F(ReadWithFreeSpace, TPqIoTestFixture) { 
        const TString topicName = "ReadWithFreeSpace";
        InitSource(topicName); 

        PQWrite({"data1", "data2", "data3"}, topicName);

        {
            auto result = SourceReadUntil<TString>(UVParser, 1, 1); 
            std::vector<TString> expected {"data1"};
            UNIT_ASSERT_EQUAL(result, expected);
        }

        UNIT_ASSERT_EQUAL(SourceRead<TString>(UVParser, 0).size(), 0); 
        UNIT_ASSERT_EQUAL(SourceRead<TString>(UVParser, -1).size(), 0); 
    }

    Y_UNIT_TEST_F(ReadNonExistentTopic, TPqIoTestFixture) { 
        const TString topicName = "NonExistentTopic";
        InitSource(topicName); 

        while (true) {
            try {
                SourceRead<TString>(UVParser); 
            } catch (yexception& e) {
                UNIT_ASSERT_STRING_CONTAINS(e.what(), "Read session to topic \"NonExistentTopic\" was closed");
                break;
            }

            sleep(1);
        }
    }

    Y_UNIT_TEST(TestSaveLoadPqRead) {
        NDqProto::TSourceState state; 
        const TString topicName = "SaveLoadPqRead";

        {
            TPqIoTestFixture setup1; 
            setup1.InitSource(topicName); 

            std::vector<TString> data {"data"};
            PQWrite(data, topicName);

            auto result = setup1.SourceReadUntil<TString>(UVParser, 1);
            UNIT_ASSERT_EQUAL(result, data);

            auto checkpoint = CreateCheckpoint();
            setup1.SaveSourceState(checkpoint, state); 
            Cerr << "State saved" << Endl;
        }

        NDqProto::TSourceState state2; 
        {
            TPqIoTestFixture setup2; 
            setup2.InitSource(topicName); 

            std::vector<TString> data {"data"};
            PQWrite({"data"}, topicName);

            setup2.LoadSource(state);
            Cerr << "State loaded" << Endl;
            auto result = setup2.SourceReadUntil<TString>(UVParser, 1);
            UNIT_ASSERT_EQUAL(result, data);

            auto checkpoint = CreateCheckpoint();
            setup2.SaveSourceState(checkpoint, state2); 

            PQWrite({"futherData"}, topicName);
        }

        NDqProto::TSourceState state3; 
        {
            TPqIoTestFixture setup3; 
            setup3.InitSource(topicName); 
            setup3.LoadSource(state2);

            auto result = setup3.SourceReadUntil<TString>(UVParser, 1);
            std::vector<TString> data {"futherData"};
            UNIT_ASSERT_EQUAL(result, data);

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
            UNIT_ASSERT_EQUAL(result, data);
        }

        // Load graphState2 and check it (offsets were saved).
        {
            TPqIoTestFixture setup5; 
            setup5.InitSource(topicName); 
            setup5.LoadSource(state2);

            auto result = setup5.SourceReadUntil<TString>(UVParser, 2);
            std::vector<TString> data {"futherData", "yetAnotherData"};
            UNIT_ASSERT_EQUAL(result, data);
        }

        // Load graphState3 and check it (other offsets).
        {
            TPqIoTestFixture setup6; 
            setup6.InitSource(topicName); 
            setup6.LoadSource(state3);

            auto result = setup6.SourceReadUntil<TString>(UVParser, 1);
            std::vector<TString> data {"yetAnotherData"};
            UNIT_ASSERT_EQUAL(result, data);
        }
    }

    Y_UNIT_TEST(LoadCorruptedState) {
        NDqProto::TSourceState state; 
        const TString topicName = "Invalid"; // We wouldn't read from this topic.
        auto checkpoint = CreateCheckpoint();

        {
            TPqIoTestFixture setup1; 
            setup1.InitSource(topicName); 
            setup1.SaveSourceState(checkpoint, state); 
        }

        // Corrupt state.
        TString corruptedBlob = state.GetData(0).GetStateData().GetBlob(); 
        corruptedBlob.append('a'); 
        state.MutableData(0)->MutableStateData()->SetBlob(corruptedBlob); 

        {
            TPqIoTestFixture setup2; 
            setup2.InitSource(topicName); 
            UNIT_ASSERT_EXCEPTION_CONTAINS(setup2.LoadSource(state), yexception, "Serialized state is corrupted");
        }
    }
 
    Y_UNIT_TEST(TestLoadFromSeveralStates) { 
        const TString topicName = "LoadFromSeveralStates"; 
 
        NDqProto::TSourceState state2; 
        { 
            TPqIoTestFixture setup; 
            setup.InitSource(topicName); 
 
            std::vector<TString> data {"data"}; 
            PQWrite(data, topicName); 
 
            auto result1 = setup.SourceReadUntil<TString>(UVParser, 1); 
            UNIT_ASSERT_EQUAL(result1, data); 
 
            NDqProto::TSourceState state1; 
            auto checkpoint1 = CreateCheckpoint(); 
            setup.SaveSourceState(checkpoint1, state1); 
            Cerr << "State saved" << Endl; 
 
            std::vector<TString> data2 {"data2"}; 
            PQWrite(data2, topicName); 
 
            auto result2 = setup.SourceReadUntil<TString>(UVParser, 1); 
            UNIT_ASSERT_EQUAL(result2, data2); 
 
            auto checkpoint2 = CreateCheckpoint(); 
            setup.SaveSourceState(checkpoint2, state2); 
            Cerr << "State 2 saved" << Endl; 
 
            // Add state1 to state2 
            *state2.AddData() = state1.GetData(0); 
        } 
 
        TPqIoTestFixture setup2; 
        setup2.InitSource(topicName); 
        setup2.LoadSource(state2); // Loads min offset 
 
        std::vector<TString> data3 {"data3"}; 
        PQWrite(data3, topicName); 
 
        auto result = setup2.SourceReadUntil<TString>(UVParser, 2); 
        std::vector<TString> dataResult {"data2", "data3"}; 
        UNIT_ASSERT_EQUAL(result, dataResult); 
    } 
}
} // namespace NKikimr::NMiniKQL
