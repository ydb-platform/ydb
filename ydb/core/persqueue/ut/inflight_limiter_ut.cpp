#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <ydb/core/persqueue/public/inflight_limiter.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TInFlightControllerTest) {
    Y_UNIT_TEST(TestConstructorWithLimit) {
        TInFlightController controller(10240);
        
        UNIT_ASSERT_VALUES_EQUAL(controller.LayoutUnitSize, 10);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestConstructorWithSmallLimit) {
        TInFlightController controller(100);
        
        UNIT_ASSERT_VALUES_EQUAL(controller.LayoutUnitSize, 1);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
    }

    Y_UNIT_TEST(TestConstructorWithZeroLimit) {
        TInFlightController controller(0);
        
        UNIT_ASSERT_VALUES_EQUAL(controller.LayoutUnitSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(controller.Add(100, 1000));
        UNIT_ASSERT(controller.Remove(101));
    }

    Y_UNIT_TEST(TestAddBasic) {
        TInFlightController controller(10240);
        
        UNIT_ASSERT(controller.Add(100, 50));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 50);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 5);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(controller.Add(200, 30));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 80);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestAddMultipleUnits) {
        TInFlightController controller(10240);
        
        UNIT_ASSERT(controller.Add(100, 50));
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 5);
        
        UNIT_ASSERT(controller.Add(200, 50));
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 100);
    }

    Y_UNIT_TEST(TestAddReachesLimit) {
        TInFlightController controller(10240);
        
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
        TInFlightController controller(10240);
        
        ui64 maxSize = controller.LayoutUnitSize * controller.MAX_LAYOUT_COUNT;
        
        UNIT_ASSERT(controller.Add(100, maxSize - 1));
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(!controller.Add(200, 1));
        UNIT_ASSERT(controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestRemoveBasic) {
        TInFlightController controller(10240);
        
        controller.Add(100, 50);
        controller.Add(200, 30);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 80);
        
        UNIT_ASSERT(controller.Remove(150));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 30);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestRemoveAtExactOffset) {
        TInFlightController controller(10240);
        
        controller.Add(100, 50);
        controller.Add(200, 30);
        
        UNIT_ASSERT(controller.Remove(201));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
    }

    Y_UNIT_TEST(TestRemoveAfterLimit) {
        TInFlightController controller(10240);
        
        ui64 maxSize = controller.LayoutUnitSize * controller.MAX_LAYOUT_COUNT;
        
        controller.Add(100, maxSize);
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        
        UNIT_ASSERT(controller.Remove(150));
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestRemoveAll) {
        TInFlightController controller(10240);
        
        controller.Add(100, 50);
        controller.Add(200, 30);
        
        UNIT_ASSERT(controller.Remove(101));
        UNIT_ASSERT(controller.Remove(201));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestIsMemoryLimitReached) {
        TInFlightController controller(10240);
        
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        
        ui64 maxSize = controller.LayoutUnitSize * controller.MAX_LAYOUT_COUNT;
        controller.Add(100, maxSize);
        
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        
        controller.Remove(150);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestSequentialAddRemove) {
        TInFlightController controller(10240);
        
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
        TInFlightController controller(10240);
        
        controller.Add(100, 50);
        controller.Add(200, 30);
        controller.Add(300, 40);
        
        UNIT_ASSERT(controller.Remove(250));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 40);
        
        UNIT_ASSERT(controller.Remove(350));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
    }

    Y_UNIT_TEST(TestSmallMaxAllowedSize) {
        TInFlightController controller(100);
        
        UNIT_ASSERT(controller.LayoutUnitSize == 1);
        UNIT_ASSERT(!controller.Add(0, 1000));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 1000);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 1000);
        UNIT_ASSERT(controller.IsMemoryLimitReached());

        UNIT_ASSERT(controller.Remove(1));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestLargeMaxAllowedSize) {
        TInFlightController controller(10_MB);
        
        UNIT_ASSERT(controller.LayoutUnitSize > 0);
        UNIT_ASSERT(controller.Add(100, 1_MB));
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestLayoutUnitCalculation) {
        TInFlightController controller1(1024);
        UNIT_ASSERT_VALUES_EQUAL(controller1.LayoutUnitSize, 1);
        
        TInFlightController controller2(1025);
        UNIT_ASSERT_VALUES_EQUAL(controller2.LayoutUnitSize, 1);
        
        TInFlightController controller3(2048);
        UNIT_ASSERT_VALUES_EQUAL(controller3.LayoutUnitSize, 2);
    }

    Y_UNIT_TEST(TestAddWithZeroSize) {
        TInFlightController controller(10240);
        
        UNIT_ASSERT(controller.Add(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
    }

    Y_UNIT_TEST(TestAddMessagesInOneUnit) {
        TInFlightController controller(102400);
        
        controller.Add(1, 50);
        controller.Add(2, 50);

        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 100);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 1);
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout[0], 2);
    }

    Y_UNIT_TEST(TestAddWithLargeSize) {
        TInFlightController controller(10240);
        
        controller.Add(1, 1000001);
        controller.Remove(2);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 0);
    }

    Y_UNIT_TEST(TestAddManyOffsets) {
        TInFlightController controller(102400);
        
        for (ui64 i = 0; i < 1024; ++i) {
            controller.Add(i, 101);
        }
    
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 103424);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout[0], 0);
        
        for (ui64 i = 1; i <= 1024; ++i) {
            controller.Remove(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestAddManyOffsets2) {
        TInFlightController controller(102400);
        
        for (ui64 i = 0; i < 1024; ++i) {
            controller.Add(i, 99);
        }
    
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
        UNIT_ASSERT(controller.Layout.size() == 1014);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 101376);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout[0], 1);
        
        for (ui64 i = 1; i <= 1024; ++i) {
            controller.Remove(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestAddManyOffsets3) {
        TInFlightController controller(102400);
        
        for (ui64 i = 0; i < 1024; ++i) {
            controller.Add(i, 100);
        }
    
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        UNIT_ASSERT(controller.Layout.size() == 1024);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 102400);
        for (ui64 i = 0; i < 1024; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(controller.Layout[i], i);
        }
        
        for (ui64 i = 1; i <= 1024; ++i) {
            controller.Remove(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());
    }

    Y_UNIT_TEST(TestAddBigMessageAndSmallMessagesAfterIt) {
        TInFlightController controller(1000);
        
        controller.Add(1, 1000001);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 1000001);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 1024);

        controller.Add(2, 1);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 1000002);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.back(), 2);

        controller.Add(3, 1);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 1000003);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.back(), 3);
    
        controller.Remove(4);
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(controller.Layout.size(), 0);
    }

    Y_UNIT_TEST(SlidingWindowTest) {
        TInFlightController controller(10240);

        for (ui64 i = 0; i < 10; ++i) {
            controller.Add(i, 1024);
        }
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        Sleep(TDuration::Seconds(1));
        UNIT_ASSERT(controller.GetLimitReachedDuration() >= TDuration::Seconds(1));
        
        for (ui64 i = 0; i < 10; ++i) {
            controller.Remove(i+1);
        }
        UNIT_ASSERT_VALUES_EQUAL(controller.TotalSize, 0);
        UNIT_ASSERT(controller.Layout.empty());
        UNIT_ASSERT(!controller.IsMemoryLimitReached());

        for (ui64 i = 10; i < 20; ++i) {
            controller.Add(i, 1024);
        }
        UNIT_ASSERT(controller.IsMemoryLimitReached());
        Sleep(TDuration::Seconds(1));
        UNIT_ASSERT(controller.GetLimitReachedDuration() >= TDuration::Seconds(2));
    }
}

}
