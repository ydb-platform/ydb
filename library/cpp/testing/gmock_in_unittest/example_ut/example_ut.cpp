#include <library/cpp/testing/gmock_in_unittest/gmock.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

// Set this variable to true if you want to see failures
/////////////////////////////////////////////////////////
static const bool fail = false;
/////////////////////////////////////////////////////////

class ITestIface {
public:
    virtual ~ITestIface() {
    }

    virtual void Func1() = 0;

    virtual int Func2(const TString&) const = 0;
};

class TTestMock: public ITestIface {
public:
    MOCK_METHOD(void, Func1, (), (override));
    MOCK_METHOD(int, Func2, (const TString&), (const, override));
};

using namespace testing;

Y_UNIT_TEST_SUITE(TExampleGMockTest) {
    Y_UNIT_TEST(TSimpleTest) {
        TTestMock mock;
        EXPECT_CALL(mock, Func1())
            .Times(AtLeast(1));

        if (!fail) {
            mock.Func1();
        }
    }

    Y_UNIT_TEST(TNonExpectedCallTest) {
        TTestMock mock;
        EXPECT_CALL(mock, Func1())
            .Times(AtMost(1));
        mock.Func1();
        if (fail) {
            mock.Func1();
        }
    }

    Y_UNIT_TEST(TReturnValuesTest) {
        TTestMock mock;
        EXPECT_CALL(mock, Func2(TString("1")))
            .WillOnce(Return(1))
            .WillRepeatedly(Return(42));

        EXPECT_CALL(mock, Func2(TString("hello")))
            .WillOnce(Return(-1));

        UNIT_ASSERT_VALUES_EQUAL(mock.Func2("hello"), -1);

        UNIT_ASSERT_VALUES_EQUAL(mock.Func2("1"), 1);
        UNIT_ASSERT_VALUES_EQUAL(mock.Func2("1"), 42);
        UNIT_ASSERT_VALUES_EQUAL(mock.Func2("1"), 42);
        UNIT_ASSERT_VALUES_EQUAL(mock.Func2("1"), 42);
        UNIT_ASSERT_VALUES_EQUAL(mock.Func2("1"), 42);

        if (fail) {
            UNIT_ASSERT_VALUES_EQUAL(mock.Func2("hello"), -1); // expected to return -1 only once
        }
    }

    Y_UNIT_TEST(TStrictCallSequenceTest) {
        TTestMock mock;
        {
            InSequence seq;
            EXPECT_CALL(mock, Func1())
                .Times(1);
            EXPECT_CALL(mock, Func2(_))
                .Times(2)
                .WillOnce(Return(1))
                .WillOnce(Return(2));
            EXPECT_CALL(mock, Func1());
        }
        mock.Func1();
        UNIT_ASSERT_VALUES_EQUAL(mock.Func2("sample"), 1);
        if (fail) {
            mock.Func1();
        }
        UNIT_ASSERT_VALUES_EQUAL(mock.Func2(""), 2);
        if (!fail) {
            mock.Func1();
        }
    }

    Y_UNIT_TEST(TUninterestingMethodIsFailureTest) {
        StrictMock<TTestMock> mock;
        EXPECT_CALL(mock, Func1())
            .Times(1);
        mock.Func1();
        if (fail) {
            mock.Func1();
        }
    }
}
