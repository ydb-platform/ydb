#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <ydb/core/persqueue/public/inflite_limiter.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TInFlightMemoryControllerTest) {

    Y_UNIT_TEST(TestDefaultConstructor) {
        TInFlightMemoryController controller;
        
        UNIT_ASSERT_VALUES_EQUAL(controller.LayoutUnitSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(controller.Add(100, 1000));
        UNIT_ASSERT(controller.Remove(100));
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestConstructorWithLimit) {
        TInFlightMemoryController controller(10240);
        
        UNIT_ASSERT_VALUES_EQUAL(controller.LayoutUnitSize, 10);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestConstructorWithSmallLimit) {
        TInFlightMemoryController controller(100);
        
        UNIT_ASSERT_VALUES_EQUAL(controller.LayoutUnitSize, 1);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
    }

    Y_UNIT_TEST(TestConstructorWithZeroLimit) {
        TInFlightMemoryController controller(0);
        
        UNIT_ASSERT_VALUES_EQUAL(controller.LayoutUnitSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(controller.Add(100, 1000));
        UNIT_ASSERT(controller.Remove(100));
    }

    Y_UNIT_TEST(TestAddBasic) {
        TInFlightMemoryController controller(10240);
        
        UNIT_ASSERT(controller.Add(100, 50));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 50);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 5);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(controller.Add(200, 30));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 80);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestAddMultipleUnits) {
        TInFlightMemoryController controller(10240);
        
        UNIT_ASSERT(controller.Add(100, 50));
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 5);
        
        UNIT_ASSERT(controller.Add(200, 50));
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 100);
    }

    Y_UNIT_TEST(TestAddReachesLimit) {
        TInFlightMemoryController controller(10240);
        
        ui64 offset = 100;
        ui64 sizePerAdd = 100;
        ui64 totalAdded = 0;
        
        while (totalAdded + sizePerAdd <= controller.LayoutUnitSize * controller.MAX_LAYOUT_COUNT) {
            bool canAdd = controller.Add(offset, sizePerAdd);
            totalAdded += sizePerAdd;
            offset += 100;
            
            if (totalAdded >= controller.LayoutUnitSize * controller.MAX_LAYOUT_COUNT) {
                UNIT_ASSERT(!canAdd);
                UNIT_ASSERT(controller.IsMemoryLimitReached());
                break;
            } else {
                UNIT_ASSERT(canAdd);
                UNIT_ASSERT(!controller.IsMemoryLimitReached());
            }
        }
    }

    Y_UNIT_TEST(TestAddExactLimit) {
        TInFlightMemoryController controller(10240);
        
        ui64 maxSize = controller.LayoutUnitSize * controller.MAX_LAYOUT_COUNT;
        
        UNIT_ASSERT(controller.Add(100, maxSize - 1));
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(!controller.Add(200, 1));
        UNIT_ASSERT(controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestRemoveBasic) {
        TInFlightMemoryController controller(10240);
        
        controller.Add(100, 50);
        controller.Add(200, 30);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 80);
        
        UNIT_ASSERT(controller.Remove(150));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 30);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestRemoveAtExactOffset) {
        TInFlightMemoryController controller(10240);
        
        controller.Add(100, 50);
        controller.Add(200, 30);
        
        UNIT_ASSERT(controller.Remove(200));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
    }

    Y_UNIT_TEST(TestRemoveAfterLimit) {
        TInFlightMemoryController controller(10240);
        
        ui64 maxSize = controller.LayoutUnitSize * controller.MAX_LAYOUT_COUNT;
        
        controller.Add(100, maxSize);
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(controller.Remove(150));
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestRemoveAll) {
        TInFlightMemoryController controller(10240);
        
        controller.Add(100, 50);
        controller.Add(200, 30);
        
        UNIT_ASSERT(controller.Remove(100));
        UNIT_ASSERT(controller.Remove(200));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestIsMemoryLimitReached) {
        TInFlightMemoryController controller(10240);
        
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        ui64 maxSize = controller.LayoutUnitSize * controller.MAX_LAYOUT_COUNT;
        controller.Add(100, maxSize);
        
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        
        controller.Remove(150);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestSequentialAddRemove) {
        TInFlightMemoryController controller(10240);
        
        for (ui64 i = 0; i < 10; ++i) {
            UNIT_ASSERT(controller.Add(i * 100, 100));
            UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, (i + 1) * 100);
        }
        
        for (ui64 i = 0; i < 10; ++i) {
            UNIT_ASSERT(controller.Remove(i * 100 + 50));
            UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, (9 - i) * 100);
        }
        
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
    }

    Y_UNIT_TEST(TestOutOfOrderRemove) {
        TInFlightMemoryController controller(10240);
        
        controller.Add(100, 50);
        controller.Add(200, 30);
        controller.Add(300, 40);
        
        UNIT_ASSERT(controller.Remove(250));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 40);
        
        UNIT_ASSERT(controller.Remove(350));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
    }

    Y_UNIT_TEST(TestSmallMaxAllowedSize) {
        TInFlightMemoryController controller(100);
        
        UNIT_ASSERT(controller.LayoutUnitSize == 1);
        UNIT_ASSERT(!controller.Add(0, 1000));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 1000);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 1000);
        UNIT_ASSERT(controller.IsMemoryLimitReached());

        UNIT_ASSERT(controller.Remove(0));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestLargeMaxAllowedSize) {
        TInFlightMemoryController controller(10_MB);
        
        UNIT_ASSERT(controller.LayoutUnitSize > 0);
        UNIT_ASSERT(controller.Add(100, 1_MB));
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestLayoutUnitCalculation) {
        TInFlightMemoryController controller1(1024);
        UNIT_ASSERT_VALUES_EQUAL(controller1.LayoutUnitSize, 1);
        
        TInFlightMemoryController controller2(1025);
        UNIT_ASSERT_VALUES_EQUAL(controller2.LayoutUnitSize, 1);
        
        TInFlightMemoryController controller3(2048);
        UNIT_ASSERT_VALUES_EQUAL(controller3.LayoutUnitSize, 2);
    }

    Y_UNIT_TEST(TestAddWithZeroSize) {
        TInFlightMemoryController controller(10240);
        
        UNIT_ASSERT(controller.Add(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
    }

    Y_UNIT_TEST(TestAddMessagesInOneUnit) {
        TInFlightMemoryController controller(102400);
        
        controller.Add(1, 50);
        controller.Add(2, 50);

        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 100);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 1);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout[0], 2);
    }

    Y_UNIT_TEST(TestAddWithLargeSize) {
        TInFlightMemoryController controller(10240);
        
        controller.Add(1, 1000001);
        controller.Remove(1);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 0);
    }

    Y_UNIT_TEST(TestAddManyOffsets) {
        TInFlightMemoryController controller(102400);
        
        for (ui64 i = 0; i < 1024; ++i) {
            controller.Add(i, 101);
        }
    
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        UNIT_ASSERT(controller.Layout.size() == 1035);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 103424);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout[0], 0);
        
        for (ui64 i = 0; i < 1024; ++i) {
            controller.Remove(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestAddManyOffsets2) {
        TInFlightMemoryController controller(102400);
        
        for (ui64 i = 0; i < 1024; ++i) {
            controller.Add(i, 99);
        }
    
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        UNIT_ASSERT(controller.Layout.size() == 1014);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 101376);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout[0], 1);
        
        for (ui64 i = 0; i < 1024; ++i) {
            controller.Remove(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }
}

}
