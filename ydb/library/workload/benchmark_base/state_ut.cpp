#include "state.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/thread.h>

Y_UNIT_TEST_SUITE(DataGeneratorState) {
    Y_UNIT_TEST(PortionProcessing) {
        TFsPath path;
        NYdbWorkload::TGeneratorStateProcessor proccessor(path, true);
        UNIT_ASSERT(!proccessor.GetState().contains("test1"));
        proccessor.FinishPortion("test1", 0, 10);
        UNIT_ASSERT_EQUAL(proccessor.GetState().at("test1").Position, 10);
        TThread t([&proccessor](){
            proccessor.FinishPortion("test1", 10, 10);
            proccessor.FinishPortion("test1", 50, 10);
        });
        t.Start();
        t.Join();
        UNIT_ASSERT_EQUAL(proccessor.GetState().at("test1").Position, 20);
        proccessor.FinishPortion("test1", 20, 30);
        UNIT_ASSERT_EQUAL(proccessor.GetState().at("test1").Position, 60);
    };

    Y_UNIT_TEST(SaveLoad) {
        TFsPath path("test_state.json");
        path.DeleteIfExists();

        NYdbWorkload::TGeneratorStateProcessor proccessor1(path, true);
        proccessor1.FinishPortion("test1", 0, 10);
        proccessor1.FinishPortion("test2", 0, 15);
        proccessor1.FinishPortion("test3", 0, 17);
        UNIT_ASSERT_EQUAL(proccessor1.GetState().at("test1").Position, 10);
        UNIT_ASSERT_EQUAL(proccessor1.GetState().at("test2").Position, 15);
        UNIT_ASSERT_EQUAL(proccessor1.GetState().at("test3").Position, 17);

        NYdbWorkload::TGeneratorStateProcessor proccessor2(path, false);
        UNIT_ASSERT_EQUAL(proccessor2.GetState().at("test1").Position, 10);
        UNIT_ASSERT_EQUAL(proccessor2.GetState().at("test2").Position, 15);
        UNIT_ASSERT_EQUAL(proccessor2.GetState().at("test3").Position, 17);
    }
};