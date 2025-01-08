#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/attributes.h>
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

TEST(TErrorTest, DoNotDuplicateOriginalErrorDepth)
{
    constexpr int Depth = 20;
    ASSERT_GE(Depth, ErrorSerializationDepthLimit);

    auto error = TError(TErrorCode(Depth), "error");
    for (int i = Depth; i >= 2; --i) {
        error = TError(TErrorCode(i), "error") << std::move(error);
    }

    auto errorYson = ConvertToYsonString(error);
    error = ConvertTo<TError>(errorYson);

    // Due to reserialization, error already contains "original_error_depth" attribute.
    // It should not be duplicated after the next serialization.
    error = TError(TErrorCode(1), "error") << std::move(error);

    // Use intermediate conversion to test YSON parser depth limit simultaneously.
    errorYson = ConvertToYsonString(error);
    auto errorNode = ConvertTo<IMapNodePtr>(errorYson);

    error = ConvertTo<TError>(errorYson);
    for (int index = 0; index < ErrorSerializationDepthLimit - 1; ++index) {
        ASSERT_FALSE(error.Attributes().Contains("original_error_depth"));
        error = error.InnerErrors()[0];
    }

    const auto& children = error.InnerErrors();
    ASSERT_EQ(std::ssize(children), Depth - ErrorSerializationDepthLimit + 1);
    for (int index = 0; index < std::ssize(children); ++index) {
        const auto& child = children[index];
        if (index == 0) {
            ASSERT_FALSE(child.Attributes().Contains("original_error_depth"));
        } else {
            ASSERT_TRUE(child.Attributes().Contains("original_error_depth"));
            if (index == 1) {
                ASSERT_EQ(child.Attributes().Get<i64>("original_error_depth"), index + ErrorSerializationDepthLimit);
            } else {
                ASSERT_EQ(child.Attributes().Get<i64>("original_error_depth"), index + ErrorSerializationDepthLimit - 1);
            }
        }
    }
}

TEST(TErrorTest, ErrorSanitizer)
{
    auto checkSantizied = [&] (const TError& error) {
        EXPECT_FALSE(error.HasOriginAttributes());
        EXPECT_FALSE(HasTracingAttributes(error));

        EXPECT_EQ("<host-override>", GetHost(error));
        EXPECT_EQ(0, error.GetPid());
        EXPECT_EQ(NThreading::InvalidThreadId, error.GetTid());
        EXPECT_EQ(NConcurrency::InvalidFiberId, GetFid(error));
        EXPECT_EQ(NTracing::InvalidTraceId, GetTraceId(error));
        EXPECT_EQ(NTracing::InvalidSpanId, GetSpanId(error));
    };

    auto checkNotSanitized = [&] (const TError& error) {
        EXPECT_TRUE(error.HasOriginAttributes());

        EXPECT_FALSE(GetHost(error) == "<host-override>");
        EXPECT_FALSE(error.GetPid() == 0);

        auto now = TInstant::Now();
        EXPECT_GE(error.GetDatetime() + TDuration::Minutes(1), now);
    };

    auto error1 = TError("error1");
    checkNotSanitized(error1);

    {
        auto instant1 = TInstant::Days(123);
        TErrorSanitizerGuard guard1(
            instant1,
            /*localHostNameOverride*/ TSharedRef::FromString("<host-override>"));

        auto error2 = TError("error2");
        checkSantizied(error2);
        EXPECT_EQ(instant1, error2.GetDatetime());

        {
            auto instant2 = TInstant::Days(234);
            TErrorSanitizerGuard guard2(
                instant2,
                /*localHostNameOverride*/
                TSharedRef::FromString("<host-override>"));

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
    TErrorSerializer::Save(saveContext, savedError);

    TStreamLoadContext loadContext(&stream);
    TError loadedError;
    TErrorSerializer::Load(loadContext, loadedError);

    EXPECT_EQ(ToString(savedError), ToString(loadedError));
}

TEST(TErrorTest, TraceContext)
{
    TError error;

    NTracing::TTraceId traceId;
    NTracing::TSpanId spanId;

    // NB(arkady-e1ppa): Make sure that tracing data can be decoded
    // even after traceContext was destroyed.
    {
        auto traceContext = NTracing::GetOrCreateTraceContext("Tester");
        traceId = traceContext->GetTraceId();
        spanId = traceContext->GetSpanId();

        auto guard = NTracing::TCurrentTraceContextGuard(traceContext);

        error = TError("Capture test");
    }

    EXPECT_EQ(GetTraceId(error), traceId);
    EXPECT_EQ(GetSpanId(error), spanId);
}

TEST(TErrorTest, NativeHostName)
{
    auto hostName = "TestHost";
    NNet::WriteLocalHostName(hostName);

    auto error = TError("NativeHostTest");

    EXPECT_TRUE(HasHost(error));
    EXPECT_EQ(GetHost(error), TStringBuf(hostName));
}

TEST(TErrorTest, NativeFiberId)
{
    auto actionQueue = New<NConcurrency::TActionQueue>();

    NConcurrency::WaitFor(BIND([] {
        auto fiberId = NConcurrency::GetCurrentFiberId();

        auto error = TError("TestNativeFiberId");

        EXPECT_EQ(GetFid(error), fiberId);
    }).AsyncVia(actionQueue->GetInvoker()).Run())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
