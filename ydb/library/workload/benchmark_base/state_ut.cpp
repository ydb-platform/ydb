#include "state.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/thread.h>

Y_UNIT_TEST_SUITE(DataGeneratorState) {
    Y_UNIT_TEST(PortionProcessing) {
        TFsPath path;
        NYdbWorkload::TGeneratorStateProcessor proccessor(path);
        proccessor.AddPortion("test1", 0, 10);
        UNIT_ASSERT(!proccessor.GetState().contains("test1"));
        proccessor.FinishPortions();
        UNIT_ASSERT_EQUAL(proccessor.GetState().at("test1").Position, 10);
        TThread t([&proccessor](){
            proccessor.AddPortion("test1", 10, 10);
            proccessor.AddPortion("test1", 50, 10);
            proccessor.FinishPortions();
        });
        t.Start();
        t.Join();
        UNIT_ASSERT_EQUAL(proccessor.GetState().at("test1").Position, 20);
        proccessor.AddPortion("test1", 20, 30);
        proccessor.FinishPortions();
        UNIT_ASSERT_EQUAL(proccessor.GetState().at("test1").Position, 60);
    };

    Y_UNIT_TEST(SaveLoad) {
        TFsPath path("test_state.json");
        path.DeleteIfExists();

        NYdbWorkload::TGeneratorStateProcessor proccessor1(path);
        proccessor1.AddPortion("test1", 0, 10);
        proccessor1.AddPortion("test2", 0, 15);
        proccessor1.AddPortion("test3", 0, 17);
        proccessor1.FinishPortions();
        UNIT_ASSERT_EQUAL(proccessor1.GetState().at("test1").Position, 10);
        UNIT_ASSERT_EQUAL(proccessor1.GetState().at("test2").Position, 15);
        UNIT_ASSERT_EQUAL(proccessor1.GetState().at("test3").Position, 17);

        NYdbWorkload::TGeneratorStateProcessor proccessor2(path);
        UNIT_ASSERT_EQUAL(proccessor2.GetState().at("test1").Position, 10);
        UNIT_ASSERT_EQUAL(proccessor2.GetState().at("test2").Position, 15);
        UNIT_ASSERT_EQUAL(proccessor2.GetState().at("test3").Position, 17);
    }
};