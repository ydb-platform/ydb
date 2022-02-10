#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

namespace {
    class IMock {
    public:
        virtual void M1(const TStringBuf&) = 0;
        virtual void M2(TStringBuf) = 0;
        virtual void M3(const TString&) = 0;
        virtual void M4(TString) = 0;
    };

    class TSampleMock : IMock {
    public:
        MOCK_METHOD(void, M1, (const TStringBuf&));
        MOCK_METHOD(void, M2, (TStringBuf));
        MOCK_METHOD(void, M3, (const TString&));
        MOCK_METHOD(void, M4, (TString));
    };
}


TEST(MatchersSpecializations, String) {
    TSampleMock mock;

    TStringBuf simpleStringBuf = "SimpleStringBuf";
    const TStringBuf constSimpleStringBuf = "ConstSimpleStringBuf";

    TString simpleString = "SimpleString";
    const TString constSimpleString = "ConstSimpleString";

    EXPECT_CALL(mock, M1("ConstSimpleStringBuf")).Times(1);
    EXPECT_CALL(mock, M2("SimpleStringBuf")).Times(1);
    EXPECT_CALL(mock, M3("ConstSimpleString")).Times(1);
    EXPECT_CALL(mock, M4("SimpleString")).Times(1);

    mock.M1(constSimpleStringBuf);
    mock.M2(simpleStringBuf);
    mock.M3(constSimpleString);
    mock.M4(simpleString);
}

template <typename T, typename M>
std::pair<bool, std::string> Match(T&& t, M&& m) {
    testing::StringMatchResultListener listener;
    auto matches = testing::SafeMatcherCast<T>(std::forward<M>(m)).MatchAndExplain(std::forward<T>(t), &listener);
    return {matches, listener.str()};
}

TEST(Matchers, Throws) {
    auto matcher = testing::Throws<std::runtime_error>();

    {
        std::stringstream ss;
        testing::SafeMatcherCast<void(*)()>(matcher).DescribeTo(&ss);
        auto explanation = ss.str();

        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::runtime_error("error message"); }, matcher);
        EXPECT_TRUE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::logic_error("error message"); }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::logic_error"));
        EXPECT_THAT(explanation, testing::HasSubstr("\"error message\""));
    }

    {
        auto [matched, explanation] = Match([]() { throw 10; }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("throws an exception of an unknown type"));
    }

    {
        auto [matched, explanation] = Match([]() { (void)0; }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("does not throw any exception"));
    }
}

TEST(Matchers, ThrowsMessage) {
    auto matcher = testing::ThrowsMessage<std::runtime_error>(testing::HasSubstr("error message"));

    {
        std::stringstream ss;
        testing::SafeMatcherCast<void(*)()>(matcher).DescribeTo(&ss);
        auto explanation = ss.str();

        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
        EXPECT_THAT(explanation, testing::HasSubstr("\"error message\""));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::runtime_error("error message"); }, matcher);
        EXPECT_TRUE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::runtime_error("message error"); }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::logic_error("error message"); }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::logic_error"));
        EXPECT_THAT(explanation, testing::HasSubstr("\"error message\""));
    }

    {
        auto [matched, explanation] = Match([]() { throw 10; }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("throws an exception of an unknown type"));
    }

    {
        auto [matched, explanation] = Match([]() { (void)0; }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("does not throw any exception"));
    }
}

TEST(Matchers, ThrowsMessageHasSubstr) {
    auto matcher = testing::ThrowsMessage<std::runtime_error>(testing::HasSubstr("error message"));

    {
        std::stringstream ss;
        testing::SafeMatcherCast<void(*)()>(matcher).DescribeTo(&ss);
        auto explanation = ss.str();

        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
        EXPECT_THAT(explanation, testing::HasSubstr("\"error message\""));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::runtime_error("error message"); }, matcher);
        EXPECT_TRUE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::runtime_error("message error"); }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::logic_error("error message"); }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::logic_error"));
        EXPECT_THAT(explanation, testing::HasSubstr("\"error message\""));
    }

    {
        auto [matched, explanation] = Match([]() { throw 10; }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("throws an exception of an unknown type"));
    }

    {
        auto [matched, explanation] = Match([]() { (void)0; }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("does not throw any exception"));
    }
}

TEST(Matchers, ThrowsCondition) {
    auto matcher = testing::Throws<std::runtime_error>(
        testing::Property(&std::exception::what, testing::HasSubstr("error message")));

    {
        std::stringstream ss;
        testing::SafeMatcherCast<void(*)()>(matcher).DescribeTo(&ss);
        auto explanation = ss.str();

        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
        EXPECT_THAT(explanation, testing::HasSubstr("\"error message\""));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::runtime_error("error message"); }, matcher);
        EXPECT_TRUE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::runtime_error("message error"); }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::runtime_error"));
        EXPECT_THAT(explanation, testing::HasSubstr("\"message error\""));
    }

    {
        auto [matched, explanation] = Match([]() { throw std::logic_error("error message"); }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("std::logic_error"));
        EXPECT_THAT(explanation, testing::HasSubstr("\"error message\""));
    }

    {
        auto [matched, explanation] = Match([]() { throw 10; }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("throws an exception of an unknown type"));
    }

    {
        auto [matched, explanation] = Match([]() { (void)0; }, matcher);
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, testing::HasSubstr("does not throw any exception"));
    }
}

template <typename T>
std::string GtestPrint(T&& v) {
    std::stringstream ss;
    testing::internal::UniversalPrint(std::forward<T>(v), &ss);
    return ss.str();
}

struct TThrowsOnMove {
    TThrowsOnMove() = default;
    TThrowsOnMove(TThrowsOnMove&&) {
        ythrow yexception() << "move failed";
    }
};

TEST(PrettyPrinters, String) {
    EXPECT_EQ(GtestPrint(TString("hello world")), "\"hello world\"");
    EXPECT_EQ(GtestPrint(TStringBuf("hello world")), "\"hello world\"");
}

TEST(PrettyPrinters, Maybe) {
    EXPECT_EQ(GtestPrint(TMaybe<TString>("hello world")), "\"hello world\"");
    EXPECT_EQ(GtestPrint(TMaybe<TString>()), "nothing");
    EXPECT_EQ(GtestPrint(Nothing()), "nothing");
}

struct T1 {
    int x;
};

void PrintTo(T1 value, std::ostream* stream) {
    *stream << "T1{" << value.x << "}";
}

struct T2 {
    int x;
};

Y_DECLARE_OUT_SPEC(inline, T2, stream, value) {
    stream << "T2{" << value.x << "}";
}

Y_GTEST_ARCADIA_PRINTER(T2)

TEST(PrettyPrinters, Custom) {
    EXPECT_EQ(GtestPrint(T1{10}), "T1{10}");
}

TEST(PrettyPrinters, CustomArcadia) {
    EXPECT_EQ(GtestPrint(T2{10}), "T2{10}");
}

TEST(Exceptions, ExpectThrow) {
    EXPECT_THROW(ythrow yexception() << "msg", yexception);
}

TEST(Exceptions, ExpectThrowStructuredBindings) {
    auto [a, b] = std::make_pair("a", "b");
    EXPECT_THROW(throw yexception() << a << "-" << b, yexception);
}

TEST(Exceptions, ExpectThrowSkipInThrowTest) {
    // this test should be skipped, not failed
    EXPECT_THROW(GTEST_SKIP(), yexception);
}

TEST(Exceptions, AssertThrow) {
    ASSERT_THROW(ythrow yexception() << "msg", yexception);
}

TEST(Exceptions, AssertThrowStructuredBindings) {
    auto [a, b] = std::make_pair("a", "b");
    ASSERT_THROW(throw yexception() << a << "-" << b, yexception);
}

TEST(Exceptions, AssertThrowSkipInThrowTest) {
    // this test should be skipped, not failed
    ASSERT_THROW(GTEST_SKIP(), yexception);
}

TEST(Exceptions, ExpectThrowMessageHasSubstr) {
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(ythrow yexception() << "msg", yexception, "msg");
}

TEST(Exceptions, ExpectThrowMessageHasSubstrStructuredBindings) {
    auto [a, b] = std::make_pair("a", "b");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(throw yexception() << a << "-" << b, yexception, "-");
}

TEST(Exceptions, ExpectThrowMessageHasSubstrSkipInThrowTest) {
    // this test should be skipped, not failed
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(GTEST_SKIP(), yexception, "-");
}

TEST(Exceptions, AssertThrowMessageHasSubstr) {
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(ythrow yexception() << "msg", yexception, "msg");
}

TEST(Exceptions, AssertThrowMessageHasSubstrStructuredBindings) {
    auto [a, b] = std::make_pair("a", "b");
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(throw yexception() << a << "-" << b, yexception, "-");
}

TEST(Exceptions, AssertThrowMessageHasSubstrSkipInThrowTest) {
    // this test should be skipped, not failed
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(GTEST_SKIP(), yexception, "-");
}

TEST(Exceptions, ExpectNoThrow) {
    EXPECT_NO_THROW((void)0);
}

TEST(Exceptions, AssertNoThrow) {
    ASSERT_NO_THROW((void)0);
}

TEST(Exceptions, ExpectAnyThrow) {
    EXPECT_ANY_THROW(ythrow yexception() << "msg");
}

TEST(Exceptions, AssertAnyThrow) {
    ASSERT_ANY_THROW(ythrow yexception() << "msg");
}
