#include "top.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

using namespace NKikimr;

#define STR Cnull

Y_UNIT_TEST_SUITE(TopTest) {

    Y_UNIT_TEST(Test1) {
        TTop<int, 3> top;

        top.Push(15);
        top.Push(2);
        top.Push(1);
        top.Push(5);
        top.Push(6);
        top.Push(9);
        top.Push(10);
        top.Push(3);
        top.Push(4);

        std::vector<int> res {9, 15, 10};
        UNIT_ASSERT_VALUES_EQUAL(top.GetContainer(), res);

        top.Output(STR);
        STR << "\n";
    }

    Y_UNIT_TEST(Test2) {
        TTop<int, 4> top;

        top.Push(2);
        top.Push(1);
        top.Push(5);
        top.Push(6);
        top.Push(15);
        top.Push(10);
        top.Push(10);
        top.Push(3);
        top.Push(4);
        top.Push(17);
        top.Push(16);

        std::vector<int> res {10, 16, 15, 17};
        UNIT_ASSERT_VALUES_EQUAL(top.GetContainer(), res);

        top.Output(STR);
        STR << "\n";
    }
}

