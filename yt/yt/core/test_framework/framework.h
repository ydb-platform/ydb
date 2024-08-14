#pragma once

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/misc/preprocessor.h>

// Include Google Test and Google Mock headers.
#define GTEST_DONT_DEFINE_FAIL 1

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A tiny helper function to generate random file names.
TString GenerateRandomFileName(const char* prefix);

////////////////////////////////////////////////////////////////////////////////

// NB. EXPECT_THROW_* are macros not functions so when failure occurres
// gtest framework points to source code of test not the source code
// of EXPECT_THROW_* function.
#define EXPECT_THROW_THAT(expr, matcher) \
    do { \
        try { \
            expr; \
            ADD_FAILURE() << "Expected exception to be thrown"; \
        } catch (const std::exception& ex) { \
            EXPECT_THAT(ex.what(), matcher); \
        } \
    } while (0)

#define EXPECT_THROW_WITH_SUBSTRING(expr, exceptionSubstring) \
    EXPECT_THROW_THAT(expr, testing::HasSubstr(exceptionSubstring))

#define EXPECT_THROW_WITH_ERROR_CODE(expr, code) \
    do { \
        try { \
            expr; \
            ADD_FAILURE() << "Expected exception to be thrown"; \
        } catch (const TErrorException& ex) { \
            EXPECT_TRUE(ex.Error().FindMatching(code).has_value()); \
        } \
    } while (0)

////////////////////////////////////////////////////////////////////////////////

struct TWaitForPredicateOptions
{
    int IterationCount = 300;
    TDuration Period = TDuration::MilliSeconds(100);
    bool IgnoreExceptions = false;
    TString Message = "<no-message>";
};

void WaitForPredicate(
    std::function<bool()> predicate,
    TWaitForPredicateOptions options);

void WaitForPredicate(
    std::function<bool()> predicate,
    const TString& message);

void WaitForPredicate(
    std::function<bool()> predicate,
    int iterationCount = 300,
    TDuration period = TDuration::MilliSeconds(100));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

#define MOCK_RPC_SERVICE_METHOD(ns, method) \
    DEFINE_RPC_SERVICE_METHOD_THUNK(ns, method) \
    MOCK_METHOD(void, method, (TReq##method*, TRsp##method*, TCtx##method##Ptr))

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPredicateMatcher
    : public ::testing::MatcherInterface<T>
{
public:
    TPredicateMatcher(
        std::function<bool(T)> predicate,
        const char* description)
        : Predicate_(predicate)
        , Description_(description)
    { }

    bool MatchAndExplain(T value, ::testing::MatchResultListener* /*listener*/) const override
    {
        return Predicate_(value);
    }

    void DescribeTo(std::ostream* os) const override
    {
        *os << Description_;
    }

private:
    std::function<bool(T)> Predicate_;
    const char* Description_;

};

template <class T>
::testing::Matcher<T> MakePredicateMatcher(
    std::function<bool(T)> predicate,
    const char* description)
{
    return ::testing::MakeMatcher(new TPredicateMatcher<T>(predicate, description));
}

#define MAKE_PREDICATE_MATCHER(type, arg, capture, predicate) \
    MakePredicateMatcher<type>( \
        PP_DEPAREN(capture) (type arg) { return (predicate); }, \
        #predicate)

////////////////////////////////////////////////////////////////////////////////

#define RPC_MOCK_CALL(mock, method) \
    method( \
        ::testing::_, \
        ::testing::_, \
        ::testing::_)

#define RPC_MOCK_CALL_WITH_PREDICATE(mock, method, capture, predicate) \
    method( \
        MAKE_PREDICATE_MATCHER(mock::TReq##method*, request, capture, predicate), \
        ::testing::_, \
        ::testing::_)

#define EXPECT_CALL_WITH_MESSAGE_IMPL(obj, call, message) \
    ((obj).gmock_##call).InternalExpectedAt(__FILE__, __LINE__, #obj, message)

#define EXPECT_CALL_WITH_MESSAGE(obj, call, message) \
    EXPECT_CALL_WITH_MESSAGE_IMPL(obj, call, message)

#define EXPECT_RPC_CALL(mock, method) \
    EXPECT_CALL_WITH_MESSAGE( \
        mock, \
        RPC_MOCK_CALL(mock, method), \
        #method)

#define EXPECT_RPC_CALL_WITH_PREDICATE(mock, method, capture, predicate) \
    EXPECT_CALL_WITH_MESSAGE( \
        mock, \
        RPC_MOCK_CALL_WITH_PREDICATE(mock, method, capture, predicate), \
        #method "(" #predicate ")")

////////////////////////////////////////////////////////////////////////////////

#define ON_CALL_WITH_MESSAGE_IMPL(obj, call, message) \
    ((obj).gmock_##call).InternalDefaultActionSetAt(__FILE__, __LINE__, #obj, message)

#define ON_CALL_WITH_MESSAGE(obj, call, message) \
    ON_CALL_WITH_MESSAGE_IMPL(obj, call, message)

#define ON_RPC_CALL(mock, method) \
    ON_CALL_WITH_MESSAGE( \
        mock, \
        RPC_MOCK_CALL(mock, method), \
        #method)

////////////////////////////////////////////////////////////////////////////////

#define HANDLE_RPC_CALL(mockType, method, capture, body) \
    ::testing::Invoke(PP_DEPAREN(capture) ( \
        [[maybe_unused]] mockType::TReq##method* request, \
        [[maybe_unused]] mockType::TRsp##method* response, \
        [[maybe_unused]] mockType::TCtx##method##Ptr context) \
    body)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

namespace testing {

////////////////////////////////////////////////////////////////////////////////

void RunAndTrackFiber(NYT::TClosure closure);

// Wraps tests in an extra fiber and awaits termination. Adapted from `gtest.h`.
#define TEST_W_(test_case_name, test_name, parent_class, parent_id)\
class GTEST_TEST_CLASS_NAME_(test_case_name, test_name) : public parent_class {\
public:\
  GTEST_TEST_CLASS_NAME_(test_case_name, test_name)() {}\
  GTEST_TEST_CLASS_NAME_(test_case_name, test_name)(const GTEST_TEST_CLASS_NAME_(test_case_name, test_name)&) = delete;\
  GTEST_TEST_CLASS_NAME_(test_case_name, test_name)& operator= (const GTEST_TEST_CLASS_NAME_(test_case_name, test_name)&) = delete;\
private:\
  virtual void TestBody();\
  void TestInnerBody();\
  static ::testing::TestInfo* const test_info_ GTEST_ATTRIBUTE_UNUSED_;\
};\
\
::testing::TestInfo* const GTEST_TEST_CLASS_NAME_(test_case_name, test_name)\
  ::test_info_ =\
    ::testing::internal::MakeAndRegisterTestInfo(\
        #test_case_name, #test_name, nullptr, nullptr, \
        ::testing::internal::CodeLocation(__FILE__, __LINE__), \
        (parent_id), \
        parent_class::SetUpTestCase, \
        parent_class::TearDownTestCase, \
        new ::testing::internal::TestFactoryImpl<\
            GTEST_TEST_CLASS_NAME_(test_case_name, test_name)>);\
void GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestBody() {\
  ::testing::RunAndTrackFiber(BIND(\
    &GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestInnerBody,\
    this));\
}\
void GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestInnerBody()
#define TEST_W(test_fixture, test_name)\
  TEST_W_(test_fixture, test_name, test_fixture, \
    ::testing::internal::GetTypeId<test_fixture>())

////////////////////////////////////////////////////////////////////////////////

} // namespace testing

#define FRAMEWORK_INL_H_
#include "framework-inl.h"
#undef FRAMEWORK_INL_H_
