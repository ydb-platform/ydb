#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/error_helpers.h>

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

class TAdlException
    : public std::exception
{
public:
    static int ResetCallCount()
    {
        return std::exchange(OverloadCallCount, 0);
    }

    const char* what() const noexcept override
    {
        return "Adl exception";
    }

    // Simulate overload from TAdlException::operator <<
    template <class TLikeThis, class TArg>
        requires std::derived_from<std::decay_t<TLikeThis>, TAdlException>
    friend TLikeThis&& operator << (TLikeThis&& ex, const TArg& /*other*/)
    {
        ++OverloadCallCount;
        return std::forward<TLikeThis>(ex);
    }

private:
    static inline int OverloadCallCount = 0;
};

class TAdlArgument
{
public:
    static int ResetCallCount()
    {
        return std::exchange(OverloadCallCount, 0);
    }

    // Simulate overload TAdlArgument::operator <<
    friend TError operator << (TError&& error, const TAdlArgument& /*other*/)
    {
        static const TErrorAttribute Attr("attr", "attr_value");
        ++OverloadCallCount;
        return std::move(error) << Attr;
    }

    friend TError operator << (const TError& error, const TAdlArgument& /*other*/)
    {
        static const TErrorAttribute Attr("attr", "attr_value");
        ++OverloadCallCount;
        return error << Attr;
    }

private:
    static inline int OverloadCallCount = 0;
};

class TWidget
{
public:
    TWidget()
    {
        DefaultConstructorCalls++;
    };

    TWidget(const TWidget&)
    {
        CopyConstructorCalls++;
    }
    TWidget& operator = (const TWidget&) = delete;

    TWidget(TWidget&&)
    {
        MoveConstructorCalls++;
    }
    TWidget& operator = (TWidget&&) = delete;

    static int ResetDefaultCount()
    {
        return std::exchange(DefaultConstructorCalls, 0);
    }

    static int ResetCopyCount()
    {
        return std::exchange(CopyConstructorCalls, 0);
    }

    static int ResetMoveCount()
    {
        return std::exchange(MoveConstructorCalls, 0);
    }

private:
    static inline int DefaultConstructorCalls = 0;
    static inline int CopyConstructorCalls = 0;
    static inline int MoveConstructorCalls = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOverloadTest, bool LeftOperandHasUserDefinedOverload = false>
void IterateTestOverEveryRightOperand(TOverloadTest& tester)
{
    {
        TErrorAttribute attribute("attr", "attr_value");
        const auto& attributeRef = attribute;
        tester(attributeRef);
    }

    {
        std::vector<TErrorAttribute> attributeVector{{"attr1", "attr_value"}, {"attr2", "attr_value"}};
        const auto& attributeVectorRef = attributeVector;
        tester(attributeVectorRef);
    }

    {
        TError error("Error");

        const auto& errorRef = error;
        tester(errorRef);

        auto errorCopy = error;
        tester(std::move(errorCopy));

        if constexpr (!LeftOperandHasUserDefinedOverload) {
            EXPECT_TRUE(errorCopy.IsOK());
        }
    }

    {
        std::vector<TError> vectorError{TError("Error"), TError("Error")};

        const auto& vectorErrorRef = vectorError;
        tester(vectorErrorRef);

        auto vectorErrorCopy = vectorError;
        tester(std::move(vectorErrorCopy));

        if constexpr (!LeftOperandHasUserDefinedOverload) {
            for (const auto& errorCopy : vectorErrorCopy) {
                EXPECT_TRUE(errorCopy.IsOK());
            }
        }
    }

    {
        TError error("Error");

        const auto& attributeDictionaryRef = error.Attributes();
        tester(attributeDictionaryRef);
    }

    {
        try {
            THROW_ERROR TError("Test error");
        } catch(const NYT::TErrorException& ex) {
            const auto& exRef = ex;
            tester(exRef);

            auto exCopy = ex;
            tester(std::move(exCopy));
        }
    }

    {
        TErrorOr<int> err(std::exception{});

        const auto& errRef = err;
        tester(errRef);

        auto errCopy = err;
        tester(std::move(errCopy));

        if constexpr (!LeftOperandHasUserDefinedOverload) {
            EXPECT_TRUE(errCopy.IsOK());
        }
    }

    {
        TAdlArgument adlArg;

        const TAdlArgument& adlArgRef = adlArg;
        tester(adlArgRef);

        if constexpr (!LeftOperandHasUserDefinedOverload) {
            EXPECT_EQ(TAdlArgument::ResetCallCount(), 1);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TErrorTest, BitshiftOverloadsExplicitLeftOperand)
{
    // TError&& overload.
    auto moveTester = [] (auto&& arg) {
        TError error = TError("Test error");
        TError moved = std::move(error) << std::forward<decltype(arg)>(arg);
        EXPECT_TRUE(error.IsOK());
        EXPECT_EQ(moved.GetMessage(), "Test error");
    };
    IterateTestOverEveryRightOperand(moveTester);

    // const TError& overloads.
    auto copyTester = [] (auto&& arg) {
        TError error = TError("Test error");
        TError copy = error << std::forward<decltype(arg)>(arg);
        EXPECT_EQ(error.GetMessage(), copy.GetMessage());
    };
    IterateTestOverEveryRightOperand(copyTester);

    // Test that TError pr value binds correctly and the call itself is unambiguous.
    auto prvalueTester = [] (auto&& arg) {
        TError error = TError("Test error") << std::forward<decltype(arg)>(arg);
        EXPECT_EQ(error.GetMessage(), "Test error");
    };
    IterateTestOverEveryRightOperand(prvalueTester);
}

TEST(TErrorTest, BitshiftOverloadsImplicitLeftOperand)
{
    // We want to be able to write THROW_ERROR ex
    auto throwErrorTester1 = [] (auto&& arg) {
        try {
            try {
                THROW_ERROR TError("Test error");
            } catch(const NYT::TErrorException& ex) {
                THROW_ERROR ex << std::forward<decltype(arg)>(arg);
            }
        } catch(const NYT::TErrorException& ex) {
            TError error = ex;
            EXPECT_EQ(error.GetMessage(), "Test error");
        }
    };
    IterateTestOverEveryRightOperand(throwErrorTester1);

    // We also want to be able to write THROW_ERROR TError(smth) without compiler errors
    auto throwErrorTester2 = [] (auto&& arg) {
        try {
            try {
                THROW_ERROR TError("Test error");
            } catch(const NYT::TErrorException& ex) {
                THROW_ERROR TError(ex) << std::forward<decltype(arg)>(arg);
            }
        } catch(const NYT::TErrorException& ex) {
            TError error = ex;
            EXPECT_EQ(error.GetMessage(), "Test error");
        }
    };
    IterateTestOverEveryRightOperand(throwErrorTester2);

    // Left operand ADL finds the user-defined overload over NYT one.
    // In this case AdlException should find templated function
    // specialization with perfect match for args over conversions.
    auto adlResolutionTester = [] (auto&& arg) {
        TAdlException ex;
        auto result = ex << std::forward<decltype(arg)>(arg);
        static_assert(std::same_as<TAdlException, std::decay_t<decltype(result)>>);
        EXPECT_EQ(TAdlException::ResetCallCount(), 1);
    };
    IterateTestOverEveryRightOperand<
        decltype(adlResolutionTester),
        /*LeftOperandHasUserDefinedOverload*/ true>(adlResolutionTester);

    // Make sure no ambiguous calls.
    auto genericErrorOrTester = [] (auto&& arg) {
        TErrorOr<int> err(std::exception{});
        TError error = err << std::forward<decltype(arg)>(arg);
        EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Generic);
    };
    IterateTestOverEveryRightOperand(genericErrorOrTester);
}

TEST(TErrorTest, Wrap)
{
    TError error("Error");

    auto wrapped = error.Wrap("Wrapped error");
    EXPECT_EQ(wrapped.GetCode(), NYT::EErrorCode::Generic);
    EXPECT_EQ(wrapped.GetMessage(), "Wrapped error");
    EXPECT_EQ(wrapped.InnerErrors().size(), 1u);
    EXPECT_EQ(wrapped.InnerErrors()[0], error);

    auto triviallyWrapped = error.Wrap();
    EXPECT_EQ(triviallyWrapped, error);
}

TEST(TErrorTest, WrapRValue)
{
    TError error("Error");

    // TError errorCopy = error;
    // auto wrapped = std::move(errorCopy).Wrap("Wrapped error");
    // EXPECT_TRUE(errorCopy.IsOK());
    // EXPECT_EQ(wrapped.GetCode(), NYT::EErrorCode::Generic);
    // EXPECT_EQ(wrapped.GetMessage(), "Wrapped error");
    // EXPECT_EQ(wrapped.InnerErrors().size(), 1u);
    // EXPECT_EQ(wrapped.InnerErrors()[0], error);

    // TError anotherErrorCopy = error;
    // auto trviallyWrapped = std::move(anotherErrorCopy).Wrap();
    // EXPECT_TRUE(anotherErrorCopy.IsOK());
    // EXPECT_EQ(trviallyWrapped, error);
}

TEST(TErrorTest, ThrowErrorExceptionIfFailedMacroJustWorks)
{
    TError error;

    EXPECT_NO_THROW(THROW_ERROR_EXCEPTION_IF_FAILED(error, "Outer error"));

    error = TError("Real error");

    TError errorCopy = error;

    try {
        THROW_ERROR_EXCEPTION_IF_FAILED(errorCopy, "Outer error");
    } catch (const std::exception& ex) {
        TError outerError(ex);

        EXPECT_TRUE(errorCopy.IsOK());
        EXPECT_EQ(outerError.GetMessage(), "Outer error");
        EXPECT_EQ(outerError.InnerErrors().size(), 1u);
        EXPECT_EQ(outerError.InnerErrors()[0], error);
    }
}

TEST(TErrorTest, ThrowErrorExceptionIfFailedMacroExpression)
{
    try {
        THROW_ERROR_EXCEPTION_IF_FAILED(
            TError("Inner error")
                << TErrorAttribute("attr", "attr_value"),
            "Outer error");
    } catch (const std::exception& ex) {
        TError outerError(ex);

        EXPECT_EQ(outerError.GetMessage(), "Outer error");
        EXPECT_EQ(outerError.InnerErrors().size(), 1u);
        EXPECT_EQ(outerError.InnerErrors()[0].GetMessage(), "Inner error");
        EXPECT_EQ(outerError.InnerErrors()[0].Attributes().Get<TString>("attr"), "attr_value");
    }
}

TEST(TErrorTest, ThrowErrorExceptionIfFailedMacroDontStealValue)
{
    TErrorOr<TWidget> widget = TWidget();
    EXPECT_TRUE(widget.IsOK());
    EXPECT_EQ(TWidget::ResetDefaultCount(), 1);
    EXPECT_EQ(TWidget::ResetCopyCount(), 0);
    EXPECT_EQ(TWidget::ResetMoveCount(), 1);

    EXPECT_NO_THROW(THROW_ERROR_EXCEPTION_IF_FAILED(widget));
    EXPECT_TRUE(widget.IsOK());
    EXPECT_NO_THROW(widget.ValueOrThrow());
    EXPECT_EQ(TWidget::ResetDefaultCount(), 0);
    EXPECT_EQ(TWidget::ResetCopyCount(), 0);
    EXPECT_EQ(TWidget::ResetMoveCount(), 0);
}

TEST(TErrorTest, ThrowErrorExceptionIfFailedMacroDontDupeCalls)
{
    EXPECT_NO_THROW(THROW_ERROR_EXCEPTION_IF_FAILED(TErrorOr<TWidget>(TWidget())));
    EXPECT_EQ(TWidget::ResetDefaultCount(), 1);
    EXPECT_EQ(TWidget::ResetCopyCount(), 0);
    EXPECT_EQ(TWidget::ResetMoveCount(), 1);
}

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

TEST(TErrorTest, ErrorSkeletonStubImplementation)
{
    TError error("foo");
    EXPECT_THROW(error.GetSkeleton(), std::exception);
}

TEST(TErrorTest, FormatCtor)
{
    // EXPECT_EQ("Some error %v", TError("Some error %v").GetMessage()); // No longer compiles due to static analysis.
    EXPECT_EQ("Some error hello", TError("Some error %v", "hello").GetMessage());
}

TEST(TErrorTest, FindRecursive)
{
    auto inner = TError("Inner")
        << TErrorAttribute("inner_attr", 42);
    auto error = TError("Error")
        << inner
        << TErrorAttribute("attr", 8);

    auto attr = FindAttribute<int>(error, "attr");
    EXPECT_TRUE(attr);
    EXPECT_EQ(*attr, 8);

    EXPECT_FALSE(FindAttribute<int>(error, "inner_attr"));

    auto innerAttr = FindAttributeRecursive<int>(error, "inner_attr");
    EXPECT_TRUE(innerAttr);
    EXPECT_EQ(*innerAttr, 42);
}

TEST(TErrorTest, TruncateSimple)
{
    auto error = TError("Some error")
        << TErrorAttribute("my_attr", "Attr value")
        << TError("Inner error");
    auto truncatedError = error.Truncate();
    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());
    EXPECT_EQ(error.GetPid(), truncatedError.GetPid());
    EXPECT_EQ(error.GetTid(), truncatedError.GetTid());
    EXPECT_EQ(GetSpanId(error), GetSpanId(truncatedError));
    EXPECT_EQ(error.GetDatetime(), truncatedError.GetDatetime());
    EXPECT_EQ(error.Attributes().Get<TString>("my_attr"), truncatedError.Attributes().Get<TString>("my_attr"));
    EXPECT_EQ(error.InnerErrors().size(), truncatedError.InnerErrors().size());
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), truncatedError.InnerErrors()[0].GetMessage());
}

TEST(TErrorTest, TruncateLarge)
{
    auto error = TError("Some long long error")
        << TError("First inner error")
        << TError("Second inner error")
        << TError("Third inner error")
        << TError("Fourth inner error");
    error.MutableAttributes()->Set("my_attr", "Some long long attr");

    auto truncatedError = error.Truncate(/*maxInnerErrorCount*/ 3, /*stringLimit*/ 10);
    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ("Some long ...<message truncated>", truncatedError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<TString>("my_attr"));
    EXPECT_EQ(truncatedError.InnerErrors().size(), 3u);

    EXPECT_EQ("First inne...<message truncated>", truncatedError.InnerErrors()[0].GetMessage());
    EXPECT_EQ("Second inn...<message truncated>", truncatedError.InnerErrors()[1].GetMessage());
    EXPECT_EQ("Fourth inn...<message truncated>", truncatedError.InnerErrors()[2].GetMessage());
}

TEST(TErrorTest, TruncateSimpleRValue)
{
    auto error = TError("Some error")
        << TErrorAttribute("my_attr", "Attr value")
        << TError("Inner error");
    auto errorCopy = error;
    auto truncatedError = std::move(errorCopy).Truncate();
    EXPECT_TRUE(errorCopy.IsOK());

    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());
    EXPECT_EQ(error.GetPid(), truncatedError.GetPid());
    EXPECT_EQ(error.GetTid(), truncatedError.GetTid());
    EXPECT_EQ(GetSpanId(error), GetSpanId(truncatedError));
    EXPECT_EQ(error.GetDatetime(), truncatedError.GetDatetime());
    EXPECT_EQ(error.Attributes().Get<TString>("my_attr"), truncatedError.Attributes().Get<TString>("my_attr"));
    EXPECT_EQ(error.InnerErrors().size(), truncatedError.InnerErrors().size());
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), truncatedError.InnerErrors()[0].GetMessage());
}

TEST(TErrorTest, TruncateLargeRValue)
{
    auto error = TError("Some long long error")
        << TError("First inner error")
        << TError("Second inner error")
        << TError("Third inner error")
        << TError("Fourth inner error");
    error.MutableAttributes()->Set("my_attr", "Some long long attr");

    auto errorCopy = error;
    auto truncatedError = std::move(errorCopy).Truncate(/*maxInnerErrorCount*/ 3, /*stringLimit*/ 10);
    EXPECT_TRUE(errorCopy.IsOK());

    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ("Some long ...<message truncated>", truncatedError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<TString>("my_attr"));
    EXPECT_EQ(truncatedError.InnerErrors().size(), 3u);

    EXPECT_EQ("First inne...<message truncated>", truncatedError.InnerErrors()[0].GetMessage());
    EXPECT_EQ("Second inn...<message truncated>", truncatedError.InnerErrors()[1].GetMessage());
    EXPECT_EQ("Fourth inn...<message truncated>", truncatedError.InnerErrors()[2].GetMessage());
}

TEST(TErrorTest, TruncateConsistentOverloads)
{
    auto error = TError("Some long long error")
        << TError("First inner error")
        << TError("Second inner error")
        << TError("Third inner error")
        << TError("Fourth inner error");
    error.MutableAttributes()->Set("my_attr", "Some long long attr");

    auto errorCopy = error;
    auto truncatedRValueError = std::move(errorCopy).Truncate(/*maxInnerErrorCount*/ 3, /*stringLimit*/ 10);

    auto trunactedLValueError = error.Truncate(/*maxInnerErrorCount*/ 3, /*stringLimit*/ 10);

    EXPECT_EQ(truncatedRValueError, trunactedLValueError);
}

TEST(TErrorTest, TruncateWhitelist)
{
    auto error = TError("Some error");
    error.MutableAttributes()->Set("attr1", "Some long long attr");
    error.MutableAttributes()->Set("attr2", "Some long long attr");

    THashSet<TStringBuf> myWhitelist = {"attr2"};

    auto truncatedError = error.Truncate(2, 10, myWhitelist);

    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());

    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<TString>("attr1"));
    EXPECT_EQ("Some long long attr", truncatedError.Attributes().Get<TString>("attr2"));
}

TEST(TErrorTest, TruncateWhitelistRValue)
{
    auto error = TError("Some error");
    error.MutableAttributes()->Set("attr1", "Some long long attr");
    error.MutableAttributes()->Set("attr2", "Some long long attr");

    THashSet<TStringBuf> myWhitelist = {"attr2"};

    auto errorCopy = error;
    auto truncatedError = std::move(errorCopy).Truncate(2, 10, myWhitelist);
    EXPECT_TRUE(errorCopy.IsOK());

    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());

    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<TString>("attr1"));
    EXPECT_EQ("Some long long attr", truncatedError.Attributes().Get<TString>("attr2"));
}

TEST(TErrorTest, TruncateWhitelistInnerErrors)
{
    auto innerError = TError("Inner error");
    innerError.MutableAttributes()->Set("attr1", "Some long long attr");
    innerError.MutableAttributes()->Set("attr2", "Some long long attr");

    auto error = TError("Error") << innerError;

    THashSet<TStringBuf> myWhitelist = {"attr2"};

    auto truncatedError = error.Truncate(2, 20, myWhitelist);
    EXPECT_EQ(truncatedError.InnerErrors().size(), 1u);

    auto truncatedInnerError = truncatedError.InnerErrors()[0];
    EXPECT_EQ(truncatedInnerError.GetCode(), innerError.GetCode());
    EXPECT_EQ(truncatedInnerError.GetMessage(), innerError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedInnerError.Attributes().Get<TString>("attr1"));
    EXPECT_EQ("Some long long attr", truncatedInnerError.Attributes().Get<TString>("attr2"));
}

TEST(TErrorTest, TruncateWhitelistInnerErrorsRValue)
{
    auto innerError = TError("Inner error");
    innerError.MutableAttributes()->Set("attr1", "Some long long attr");
    innerError.MutableAttributes()->Set("attr2", "Some long long attr");

    auto error = TError("Error") << innerError;

    THashSet<TStringBuf> myWhitelist = {"attr2"};

    auto errorCopy = error;
    auto truncatedError = std::move(errorCopy).Truncate(2, 20, myWhitelist);
    EXPECT_TRUE(errorCopy.IsOK());
    EXPECT_EQ(truncatedError.InnerErrors().size(), 1u);

    auto truncatedInnerError = truncatedError.InnerErrors()[0];
    EXPECT_EQ(truncatedInnerError.GetCode(), innerError.GetCode());
    EXPECT_EQ(truncatedInnerError.GetMessage(), innerError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedInnerError.Attributes().Get<TString>("attr1"));
    EXPECT_EQ("Some long long attr", truncatedInnerError.Attributes().Get<TString>("attr2"));
}

TEST(TErrorTest, TruncateWhitelistSaveInnerError)
{
    auto genericInner = TError("GenericInner");
    auto whitelistedInner = TError("Inner")
        << TErrorAttribute("whitelisted_key", 42);

    auto error = TError("Error")
        << (genericInner << TErrorAttribute("foo", "bar"))
        << whitelistedInner
        << genericInner;

    error = std::move(error).Truncate(1, 20, {
        "whitelisted_key"
    });
    EXPECT_TRUE(!error.IsOK());
    EXPECT_EQ(error.InnerErrors().size(), 2u);
    EXPECT_EQ(error.InnerErrors()[0], whitelistedInner);
    EXPECT_EQ(error.InnerErrors()[1], genericInner);

    EXPECT_TRUE(FindAttributeRecursive<int>(error, "whitelisted_key"));
    EXPECT_FALSE(FindAttributeRecursive<int>(error, "foo"));
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
            throw TSimpleException(ex, "outer message");
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

TEST(TErrorTest, YTExceptionWithAttributesToError)
{
    try {
        throw TSimpleException("message")
            << TExceptionAttribute{"Int64 value", static_cast<i64>(42)}
            << TExceptionAttribute{"double value", 7.77}
            << TExceptionAttribute{"bool value", false}
            << TExceptionAttribute{"String value", "FooBar"};
    } catch (const std::exception& ex) {
        TError error(ex);
        EXPECT_EQ(NYT::EErrorCode::Generic, error.GetCode());
        EXPECT_EQ("message", error.GetMessage());

        auto i64value = error.Attributes().Find<i64>("Int64 value");
        EXPECT_TRUE(i64value);
        EXPECT_EQ(*i64value, static_cast<i64>(42));

        auto doubleValue = error.Attributes().Find<double>("double value");
        EXPECT_TRUE(doubleValue);
        EXPECT_EQ(*doubleValue, 7.77);

        auto boolValue = error.Attributes().Find<bool>("bool value");
        EXPECT_TRUE(boolValue);
        EXPECT_EQ(*boolValue, false);

        auto stringValue = error.Attributes().Find<TString>("String value");
        EXPECT_TRUE(stringValue);
        EXPECT_EQ(*stringValue, "FooBar");
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

TEST(TErrorTest, AttributeSerialization)
{
    auto getWeededText = [] (const TError& err) {
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

TEST(TErrorTest, MacroStaticAnalysis)
{
    auto swallow = [] (auto expr) {
        try {
            expr();
        } catch (...) {
        }
    };

    swallow([] {
        THROW_ERROR_EXCEPTION("Foo");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION("Hello, %v", "World");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION(NYT::EErrorCode::Generic, "Foo");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION(NYT::EErrorCode::Generic, "Foo%v", "Bar");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION(NYT::EErrorCode::Generic, "Foo%v%v", "Bar", "Baz");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION_IF_FAILED(TError{}, "Foo");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION_IF_FAILED(TError{}, "Foo%v", "Bar");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION_IF_FAILED(TError{}, "Foo%v%v", "Bar", "Baz");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION_IF_FAILED(TError{}, NYT::EErrorCode::Generic, "Foo%v", "Bar");
    });
    swallow([] {
        THROW_ERROR_EXCEPTION_IF_FAILED(TError{}, NYT::EErrorCode::Generic, "Foo%v%v", "Bar", "Baz");
    });
}

TEST(TErrorTest, WrapStaticAnalysis)
{
    TError error;
    Y_UNUSED(error.Wrap());
    Y_UNUSED(error.Wrap(std::exception{}));
    Y_UNUSED(error.Wrap("Hello"));
    Y_UNUSED(error.Wrap("Hello, %v", "World"));
    Y_UNUSED(error.Wrap(TRuntimeFormat{"Hello, %v"}));
}

// NB(arkady-e1ppa): Uncomment these occasionally to see
// that static analysis is still working.
TEST(TErrorTest, MacroStaticAnalysisBrokenFormat)
{
    // auto swallow = [] (auto expr) {
    //     try {
    //         expr();
    //     } catch (...) {
    //     }
    // };

    // swallow([] {
    //     THROW_ERROR_EXCEPTION("Hello, %v");
    // });
    // swallow([] {
    //     THROW_ERROR_EXCEPTION(TErrorCode{}, "Foo%v");
    // });
    // swallow([] {
    //     THROW_ERROR_EXCEPTION_IF_FAILED(TError{}, "Foo%v");
    // });
    // swallow([] {
    //     THROW_ERROR_EXCEPTION_IF_FAILED(TError{}, TErrorCode{}, "Foo%v");
    // });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
