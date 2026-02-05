#include "numerical_maybe.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

/**
 * Unit tests, which verify the behavior of the TNumericalMaybe class.
 */
Y_UNIT_TEST_SUITE(TNumericalMaybeTest) {
    /**
     * Verify that the TNumericalMaybe class can be created for all numerical types.
     */
    Y_UNIT_TEST(Types) {
        UNIT_ASSERT((!TNumericalMaybe<char, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<unsigned char, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<short, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<unsigned short, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<int, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<unsigned int, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<long, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<unsigned long, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<long long, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<unsigned long long, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<i8, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<ui8, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<i16, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<ui16, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<i32, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<ui32, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<i64, 0>().Defined()));
        UNIT_ASSERT((!TNumericalMaybe<ui64, 0>().Defined()));
    }

    /**
     * Verify that the constructor for the TNumericalMaybe class work correctly.
     */
    Y_UNIT_TEST(Constructor) {
        // TEST 1: An unset value created through the default constructor
        {
            const auto value = TNumericalMaybe<int, 0>();
            UNIT_ASSERT(!value.Defined());
        }

        // TEST 2: An unset value created through the explicit constructor
        {
            const auto value = TNumericalMaybe<int, 0>(0);
            UNIT_ASSERT(!value.Defined());
        }

        // TEST 3: An unset value created through the copy constructor
        {
            const auto value = TNumericalMaybe<int, 0>(TNumericalMaybe<int, 0>());
            UNIT_ASSERT(!value.Defined());
        }

        // TEST 4: A set value created through the explicit constructor
        {
            const auto value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT(value.Defined());
            UNIT_ASSERT_VALUES_EQUAL(value.Get(), 1);
        }

        // TEST 5: A set value created through the copy constructor
        {
            const auto value = TNumericalMaybe<int, 0>(TNumericalMaybe<int, 0>(1));
            UNIT_ASSERT(value.Defined());
            UNIT_ASSERT_VALUES_EQUAL(value.Get(), 1);
        }
    }

    /**
     * Verify that the assignment operator for the TNumericalMaybe class work correctly,
     * when the left side does not contain a value ("unset").
     */
    Y_UNIT_TEST(AssignmentUnsetValue) {
        // TEST 1: Assigning the value (not an object), which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            value = 0;
            UNIT_ASSERT(!value.Defined());
        }

        // TEST 2: Assigning the value (not an object), which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            value = 1;
            UNIT_ASSERT(value.Defined());
            UNIT_ASSERT_VALUES_EQUAL(value.Get(), 1);
        }

        // TEST 3: Assigning another object, which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            value = TNumericalMaybe<int, 0>(0);
            UNIT_ASSERT(!value.Defined());
        }

        // TEST 4: Assigning another object, which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT(value.Defined());
            UNIT_ASSERT_VALUES_EQUAL(value.Get(), 1);
        }
    }

    /**
     * Verify that the assignment operator for the TNumericalMaybe class work correctly,
     * when the left side contains a real value (not "unset").
     */
    Y_UNIT_TEST(AssignmentSetValue) {
        // TEST 1: Assigning the value (not an object), which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>(100);
            value = 0;
            UNIT_ASSERT(!value.Defined());
        }

        // TEST 2: Assigning the value (not an object), which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>(100);
            value = 1;
            UNIT_ASSERT(value.Defined());
            UNIT_ASSERT_VALUES_EQUAL(value.Get(), 1);
        }

        // TEST 3: Assigning another object, which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>(100);
            value = TNumericalMaybe<int, 0>(0);
            UNIT_ASSERT(!value.Defined());
        }

        // TEST 4: Assigning another object, which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>(100);
            value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT(value.Defined());
            UNIT_ASSERT_VALUES_EQUAL(value.Get(), 1);
        }
    }

    /**
     * Verify that the comparison operator for the TNumericalMaybe class work correctly,
     * when the left side does not contain a value ("unset").
     */
    Y_UNIT_TEST(ComparisonUnsetValue) {
        // TEST 1: Comparing to the value (not an object), which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            UNIT_ASSERT_VALUES_EQUAL(value == 0, true);
            UNIT_ASSERT_VALUES_EQUAL(value != 0, false);
        }

        // TEST 2: Comparing to the value (not an object), which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            UNIT_ASSERT_VALUES_EQUAL(value == 1, false);
            UNIT_ASSERT_VALUES_EQUAL(value != 1, true);
        }

        // TEST 3: Comparing to another object, which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            UNIT_ASSERT_VALUES_EQUAL((value == TNumericalMaybe<int, 0>(0)), true);
            UNIT_ASSERT_VALUES_EQUAL((value != TNumericalMaybe<int, 0>(0)), false);
        }

        // TEST 4: Comparing to another object, which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            UNIT_ASSERT_VALUES_EQUAL((value == TNumericalMaybe<int, 0>(1)), false);
            UNIT_ASSERT_VALUES_EQUAL((value != TNumericalMaybe<int, 0>(1)), true);
        }
    }

    /**
     * Verify that the comparison operator for the TNumericalMaybe class work correctly,
     * when the left side contains a real value (not "unset").
     */
    Y_UNIT_TEST(ComparisonSetValue) {
        // TEST 1: Comparing to the value (not an object), which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT_VALUES_EQUAL(value == 0, false);
            UNIT_ASSERT_VALUES_EQUAL(value != 0, true);
        }

        // TEST 2: Comparing to the value (not an object) -- equal, which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT_VALUES_EQUAL(value == 1, true);
            UNIT_ASSERT_VALUES_EQUAL(value != 1, false);
        }

        // TEST 3: Comparing to the value (not an object) -- not equal, which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT_VALUES_EQUAL(value == 100, false);
            UNIT_ASSERT_VALUES_EQUAL(value != 100, true);
        }

        // TEST 4: Comparing to another object, which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT_VALUES_EQUAL((value == TNumericalMaybe<int, 0>(0)), false);
            UNIT_ASSERT_VALUES_EQUAL((value != TNumericalMaybe<int, 0>(0)), true);
        }

        // TEST 5: Comparing to another object -- equal, which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT_VALUES_EQUAL((value == TNumericalMaybe<int, 0>(1)), true);
            UNIT_ASSERT_VALUES_EQUAL((value != TNumericalMaybe<int, 0>(1)), false);
        }

        // TEST 6: Comparing to another object -- not equal, which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>(1);
            UNIT_ASSERT_VALUES_EQUAL((value == TNumericalMaybe<int, 0>(100)), false);
            UNIT_ASSERT_VALUES_EQUAL((value != TNumericalMaybe<int, 0>(100)), true);
        }
    }

    /**
     * Verify that the Clear() function for the TNumericalMaybe class work correctly.
     */
    Y_UNIT_TEST(Clear) {
        // TEST 1: Calling Clear() on the object, which is "unset"
        {
            auto value = TNumericalMaybe<int, 0>();
            value.Clear();
            UNIT_ASSERT(!value.Defined());
        }

        // TEST 2: Calling Clear() on the object, which is not "unset"
        {
            auto value = TNumericalMaybe<int, 0>(1);
            value.Clear();
            UNIT_ASSERT(!value.Defined());
        }
    }

    /**
     * Verify that the TNumericalMaybe class supports stream output.
     */
    Y_UNIT_TEST(StreamOutput) {
        // TEST 1: Stream output for the value, which is "unset"
        {
            TString s;
            TStringOutput output(s);
            output << TNumericalMaybe<int, 0>();
            UNIT_ASSERT_EQUAL(s, "(undefined)");
        }

        // TEST 2: Stream output for the value, which is not "unset"
        {
            TString s;
            TStringOutput output(s);
            output << TNumericalMaybe<int, 0>(100);
            UNIT_ASSERT_EQUAL(s, "100");
        }
    }
}

} // namespace NKikimr
