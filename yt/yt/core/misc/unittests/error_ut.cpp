#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/stream/str.h>
#include <util/string/join.h>
#include <util/string/split.h>

namespace NYT {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TErrorTest, SerializationDepthLimit)
{
    constexpr int Depth = 1000;
    auto error = TError(TErrorCode(Depth), "error");
    for (int i = Depth - 1; i >= 0; --i) {
        error = TError(TErrorCode(i), "error") << std::move(error);
    }

    // Use intermediate conversion to test YSON parser depth limit simultaneously.
    auto errorYson = ConvertToYsonString(error);
    auto errorNode = ConvertTo<IMapNodePtr>(errorYson);

    for (int i = 0; i < ErrorSerializationDepthLimit - 1; ++i) {
        ASSERT_EQ(errorNode->GetChildValueOrThrow<i64>("code"), i);
        ASSERT_EQ(errorNode->GetChildValueOrThrow<TString>("message"), "error");
        ASSERT_FALSE(errorNode->GetChildOrThrow("attributes")->AsMap()->FindChild("original_error_depth"));
        auto innerErrors = errorNode->GetChildOrThrow("inner_errors")->AsList()->GetChildren();
        ASSERT_EQ(innerErrors.size(), 1u);
        errorNode = innerErrors[0]->AsMap();
    }
    auto innerErrors = errorNode->GetChildOrThrow("inner_errors")->AsList();
    const auto& children = innerErrors->GetChildren();
    ASSERT_EQ(std::ssize(children), Depth - ErrorSerializationDepthLimit + 1);
    for (int i = 0; i < std::ssize(children); ++i) {
        auto child = children[i]->AsMap();
        ASSERT_EQ(child->GetChildValueOrThrow<i64>("code"), i + ErrorSerializationDepthLimit);
        ASSERT_EQ(child->GetChildValueOrThrow<TString>("message"), "error");
        auto originalErrorDepth = child->GetChildOrThrow("attributes")->AsMap()->FindChild("original_error_depth");
        if (i > 0) {
            ASSERT_TRUE(originalErrorDepth);
            ASSERT_EQ(originalErrorDepth->GetValue<i64>(), i + ErrorSerializationDepthLimit);
        } else {
            ASSERT_FALSE(originalErrorDepth);
        }
    }
}

TEST(TErrorTest, ErrorSkeletonStubImplementation)
{
    TError error("foo");
    EXPECT_THROW(error.GetSkeleton(), std::exception);
}

TEST(TErrorTest, FormatCtor)
{
    EXPECT_EQ("Some error %v", TError("Some error %v").GetMessage());
    EXPECT_EQ("Some error hello", TError("Some error %v", "hello").GetMessage());
}

TEST(TErrorTest, TruncateSimple)
{
    auto error = TError("Some error")
        << TErrorAttribute("my_attr", "Attr value");
    auto truncatedError = error.Truncate();
    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());
    EXPECT_EQ(error.GetPid(), truncatedError.GetPid());
    EXPECT_EQ(error.GetTid(), truncatedError.GetTid());
    EXPECT_EQ(error.GetSpanId(), truncatedError.GetSpanId());
    EXPECT_EQ(error.GetDatetime(), truncatedError.GetDatetime());
    EXPECT_EQ(error.Attributes().Get<TString>("my_attr"), truncatedError.Attributes().Get<TString>("my_attr"));
}

TEST(TErrorTest, TruncateLarge)
{
    auto error = TError("Some long long error");
    error.MutableAttributes()->Set("my_attr", "Some long long attr");

    auto truncatedError = error.Truncate(/*maxInnerErrorCount*/ 2, /*stringLimit*/ 10);
    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ("Some long ...<message truncated>", truncatedError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<TString>("my_attr"));
}

TEST(TErrorTest, YTExceptionToError)
{
    try {
        throw TSimpleException("message");
    } catch (const std::exception& ex) {
        TError error(ex);
        EXPECT_EQ(NYT::EErrorCode::Generic, error.GetCode());
        EXPECT_EQ("message", error.GetMessage());
    }
}

TEST(TErrorTest, CompositeYTExceptionToError)
{
    try {
        try {
            throw TSimpleException("inner message");
        } catch (const std::exception& ex) {
            throw TCompositeException(ex, "outer message");
        }
    } catch (const std::exception& ex) {
        TError outerError(ex);
        EXPECT_EQ(NYT::EErrorCode::Generic, outerError.GetCode());
        EXPECT_EQ("outer message", outerError.GetMessage());
        EXPECT_EQ(1, std::ssize(outerError.InnerErrors()));
        const auto& innerError = outerError.InnerErrors()[0];
        EXPECT_EQ(NYT::EErrorCode::Generic, innerError.GetCode());
        EXPECT_EQ("inner message", innerError.GetMessage());
    }
}

TEST(TErrorTest, ErrorSanitizer)
{
    auto checkSantizied = [&] (const TError& error) {
        EXPECT_FALSE(error.HasOriginAttributes());
        EXPECT_FALSE(error.HasTracingAttributes());

        EXPECT_EQ("", error.GetHost());
        EXPECT_EQ(0, error.GetPid());
        EXPECT_EQ(NThreading::InvalidThreadId, error.GetTid());
        EXPECT_EQ(NConcurrency::InvalidFiberId, error.GetFid());
        EXPECT_EQ(NTracing::InvalidTraceId, error.GetTraceId());
        EXPECT_EQ(NTracing::InvalidSpanId, error.GetSpanId());
    };

    auto checkNotSanitized = [&] (const TError& error) {
        EXPECT_TRUE(error.HasOriginAttributes());

        EXPECT_FALSE(error.GetHost() == "");
        EXPECT_FALSE(error.GetPid() == 0);

        auto now = TInstant::Now();
        EXPECT_GE(error.GetDatetime() + TDuration::Minutes(1), now);
    };

    auto error1 = TError("error1");
    checkNotSanitized(error1);

    {
        auto instant1 = TInstant::Days(123);
        TErrorSanitizerGuard guard1(instant1);

        auto error2 = TError("error2");
        checkSantizied(error2);
        EXPECT_EQ(instant1, error2.GetDatetime());

        {
            auto instant2 = TInstant::Days(234);
            TErrorSanitizerGuard guard2(instant2);

            auto error3 = TError("error3");
            checkSantizied(error3);
            EXPECT_EQ(instant2, error3.GetDatetime());
        }

        auto error4 = TError("error4");
        checkSantizied(error4);
        EXPECT_EQ(instant1, error4.GetDatetime());
    }

    auto error5 = TError("error5");
    checkNotSanitized(error5);
}

TEST(TErrorTest, SimpleLoadAfterSave)
{
    TStringStream stream;

    TStreamSaveContext saveContext(&stream);
    TError savedError("error");
    savedError.Save(saveContext);

    TStreamLoadContext loadContext(&stream);
    TError loadedError;
    loadedError.Load(loadContext);

    EXPECT_EQ(ToString(savedError), ToString(loadedError));
}

TEST(TErrorTest, AttributeSerialization)
{
    auto getWeededText = [](const TError& err) {
        std::vector<TString> lines;
        for (const auto& line : StringSplitter(ToString(err)).Split('\n')) {
            if (!line.Contains("origin") && !line.Contains("datetime")) {
                lines.push_back(TString{line});
            }
        }
        return JoinSeq("\n", lines);
    };

    EXPECT_EQ(getWeededText(TError("E1") << TErrorAttribute("A1", "V1")), TString(
        "E1\n"
        "    A1              V1\n"));
    EXPECT_EQ(getWeededText(TError("E1") << TErrorAttribute("A1", "L1\nL2\nL3")), TString(
        "E1\n"
        "    A1\n"
        "        L1\n"
        "        L2\n"
        "        L3\n"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
