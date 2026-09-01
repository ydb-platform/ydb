#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/error/error_helpers.h>

#include <util/generic/noncopyable.h>
#include <util/stream/str.h>
#include <util/string/join.h>
#include <util/string/split.h>

#include <array>
#include <list>

namespace NYT {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

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

    TWidget(TWidget&&) noexcept
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

// legacy class without move constructor
class TCopyOnly
{
public:
    explicit TCopyOnly(int val = 0)
        : Value_(val)
    { }

    TCopyOnly(const TCopyOnly&) = default;
    TCopyOnly(TCopyOnly&&) = delete;
    TCopyOnly& operator=(const TCopyOnly&) = default;
    TCopyOnly& operator=(TCopyOnly&&) = delete;

    int Value() const
    {
        return Value_;
    }

private:
    int Value_;
};


////////////////////////////////////////////////////////////////////////////////

template <class T>
void SetErrorAttribute(TError* error, const std::string& key, const T& value)
{
    error->Add(key, value);
}

////////////////////////////////////////////////////////////////////////////////

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

    TError errorCopy = error;
    auto wrapped = std::move(errorCopy).Wrap("Wrapped error");
    EXPECT_TRUE(errorCopy.IsOK());
    EXPECT_EQ(wrapped.GetCode(), NYT::EErrorCode::Generic);
    EXPECT_EQ(wrapped.GetMessage(), "Wrapped error");
    EXPECT_EQ(wrapped.InnerErrors().size(), 1u);
    EXPECT_EQ(wrapped.InnerErrors()[0], error);

    TError anotherErrorCopy = error;
    auto triviallyWrapped = std::move(anotherErrorCopy).Wrap();
    EXPECT_TRUE(anotherErrorCopy.IsOK());
    EXPECT_EQ(triviallyWrapped, error);
}

TEST(TErrorTest, WithAttributes)
{
    const auto base = TError("Error").With("base", 1);

    auto error = base
        .With("added", 2)
        .With(TErrorAttribute("direct", 3));

    EXPECT_EQ(base.Attributes().Get<int>("base"), 1);
    EXPECT_FALSE(base.Attributes().Contains("added"));
    EXPECT_EQ(error.Attributes().Get<int>("base"), 1);
    EXPECT_EQ(error.Attributes().Get<int>("added"), 2);
    EXPECT_EQ(error.Attributes().Get<int>("direct"), 3);
}

TEST(TErrorTest, WithAttributeRange)
{
    std::array attributes{
        TErrorAttribute("first", 1),
        TErrorAttribute("second", 2),
    };

    auto error = TError("Error").With(attributes);

    EXPECT_EQ(error.Attributes().Get<int>("first"), 1);
    EXPECT_EQ(error.Attributes().Get<int>("second"), 2);
}

TEST(TErrorTest, WithInnerErrors)
{
    auto innerError = TError("Inner error");
    const auto base = TError("Outer error");

    auto error = base
        .With(innerError)
        .With(TError("Moved inner error"));

    EXPECT_TRUE(base.InnerErrors().empty());
    ASSERT_EQ(error.InnerErrors().size(), 2u);
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), "Inner error");
    EXPECT_EQ(error.InnerErrors()[1].GetMessage(), "Moved inner error");
}

TEST(TErrorTest, WithInnerErrorRange)
{
    std::list innerErrors{
        TError("First inner error"),
        TError("Second inner error"),
    };

    auto error = TError("Outer error").With(std::move(innerErrors));

    ASSERT_EQ(error.InnerErrors().size(), 2u);
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), "First inner error");
    EXPECT_EQ(error.InnerErrors()[1].GetMessage(), "Second inner error");
    EXPECT_TRUE(innerErrors.front().IsOK());
    EXPECT_TRUE(innerErrors.back().IsOK());
}

TEST(TErrorTest, WithAttributeDictionary)
{
    auto source = TError("Source").With("first", 1).With("second", 2);

    auto error = TError("Error").With(source.Attributes());

    EXPECT_EQ(error.Attributes().Get<int>("first"), 1);
    EXPECT_EQ(error.Attributes().Get<int>("second"), 2);
}

TEST(TErrorTest, AddAttributeDictionary)
{
    auto source = TError("Source").With("first", 1).With("second", 2);

    auto error = TError("Error");
    error.Add(source.Attributes());

    EXPECT_EQ(error.Attributes().Get<int>("first"), 1);
    EXPECT_EQ(error.Attributes().Get<int>("second"), 2);
}

TEST(TErrorTest, WithIf)
{
    auto innerError = TError("Inner error");

    auto attached = TError("Error")
        .WithIf(true, "key", 1)
        .WithIf(true, innerError);

    EXPECT_EQ(attached.Attributes().Get<int>("key"), 1);
    ASSERT_EQ(attached.InnerErrors().size(), 1u);
    EXPECT_EQ(attached.InnerErrors()[0].GetMessage(), "Inner error");

    auto skipped = TError("Error")
        .WithIf(false, "key", 1)
        .WithIf(false, innerError);

    EXPECT_FALSE(skipped.Attributes().Contains("key"));
    EXPECT_TRUE(skipped.InnerErrors().empty());
}

TEST(TErrorTest, WithIfGuardsOKInnerError)
{
    TError okError;
    auto error = TError("Outer error").WithIf(!okError.IsOK(), okError);

    EXPECT_TRUE(error.InnerErrors().empty());
}

TEST(TErrorTest, AddAttributes)
{
    auto error = TError("Error");
    error
        .Add("added", 1)
        .Add(TErrorAttribute("direct", 2));

    EXPECT_EQ(error.Attributes().Get<int>("added"), 1);
    EXPECT_EQ(error.Attributes().Get<int>("direct"), 2);
}

TEST(TErrorTest, AddAttributeRange)
{
    std::array attributes{
        TErrorAttribute("first", 1),
        TErrorAttribute("second", 2),
    };

    auto error = TError("Error");
    error.Add(attributes);

    EXPECT_EQ(error.Attributes().Get<int>("first"), 1);
    EXPECT_EQ(error.Attributes().Get<int>("second"), 2);
}

TEST(TErrorTest, AddInnerErrors)
{
    auto innerError = TError("Inner error");
    TErrorOr<int> innerErrorOr(TError("Inner error or"));

    auto error = TError("Outer error");
    error
        .Add(innerError)
        .Add(TError("Moved inner error"))
        .Add(std::move(innerErrorOr));

    ASSERT_EQ(error.InnerErrors().size(), 3u);
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), "Inner error");
    EXPECT_EQ(error.InnerErrors()[1].GetMessage(), "Moved inner error");
    EXPECT_EQ(error.InnerErrors()[2].GetMessage(), "Inner error or");
}

TEST(TErrorTest, AddInnerErrorRange)
{
    std::list innerErrors{
        TError("First inner error"),
        TError("Second inner error"),
    };

    auto error = TError("Outer error");
    error.Add(std::move(innerErrors));

    ASSERT_EQ(error.InnerErrors().size(), 2u);
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), "First inner error");
    EXPECT_EQ(error.InnerErrors()[1].GetMessage(), "Second inner error");
    EXPECT_TRUE(innerErrors.front().IsOK());
    EXPECT_TRUE(innerErrors.back().IsOK());
}

TEST(TErrorTest, AddOverwritesAttribute)
{
    auto error = TError("Error").With("key", 1);
    error.Add("key", 2);

    EXPECT_EQ(error.Attributes().Get<int>("key"), 2);
}

TEST(TErrorTest, AddDropsOKInnerError)
{
    auto error = TError("Outer error");
    error
        .Add(TError())
        .Add(TError("Inner error"))
        .Add(TError());

    ASSERT_EQ(error.InnerErrors().size(), 1u);
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), "Inner error");
}

TEST(TErrorTest, AddDropsOKInnerErrorRange)
{
    std::vector innerErrors{TError(), TError("Inner error"), TError()};

    auto error = TError("Outer error");
    error.Add(innerErrors);

    ASSERT_EQ(error.InnerErrors().size(), 1u);
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), "Inner error");
}

TEST(TErrorTest, WithDropsOKInnerError)
{
    auto error = TError("Outer error").With(TError());

    EXPECT_TRUE(error.InnerErrors().empty());
}

TEST(TErrorTest, WithDropsOKInnerErrorRange)
{
    std::vector innerErrors{TError(), TError("Inner error"), TError()};

    auto error = TError("Outer error").With(innerErrors);

    ASSERT_EQ(error.InnerErrors().size(), 1u);
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), "Inner error");
}

TEST(TErrorTest, WrapOKError)
{
    TError error;

    auto wrapped = error.Wrap("Wrapped OK error");
    EXPECT_EQ(wrapped.GetCode(), NYT::EErrorCode::Generic);
    EXPECT_EQ(wrapped.GetMessage(), "Wrapped OK error");
    EXPECT_EQ(wrapped.InnerErrors().size(), 0u);
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
                .With("attr", "attr_value"),
            "Outer error");
    } catch (const std::exception& ex) {
        TError outerError(ex);

        EXPECT_EQ(outerError.GetMessage(), "Outer error");
        EXPECT_EQ(outerError.InnerErrors().size(), 1u);
        EXPECT_EQ(outerError.InnerErrors()[0].GetMessage(), "Inner error");
        EXPECT_EQ(outerError.InnerErrors()[0].Attributes().Get<std::string>("attr"), "attr_value");
    }
}

TEST(TErrorTest, ThrowErrorExceptionIfMacroAttributes)
{
    try {
        THROW_ERROR_EXCEPTION_IF(true, "Condition holds")
            .With("attr", "attr_value");
        ADD_FAILURE() << "Expected the macro to throw.";
    } catch (const std::exception& ex) {
        TError error(ex);
        EXPECT_EQ(error.GetMessage(), "Condition holds");
        EXPECT_EQ(error.Attributes().Get<std::string>("attr"), "attr_value");
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

TEST(TErrorTest, ExceptionCtor)
{
    {
        auto error = TError(std::runtime_error("Some error"));
        EXPECT_EQ(error.GetMessage(), "Some error");
        EXPECT_EQ(error.Attributes().Get<std::string>("exception_type"), "std::runtime_error");
    }
    EXPECT_EQ(TError(std::runtime_error("Some bad char sequences: %v %Qv {}")).GetMessage(),
        "Some bad char sequences: %v %Qv {}");

    EXPECT_EQ(TError(TSimpleException("Some error")).GetMessage(),
        "Some error");
    EXPECT_EQ(TError(TSimpleException("Some bad char sequences: %v %d {}")).GetMessage(),
        "Some bad char sequences: %v %d {}");
}

TEST(TErrorTest, FindRecursive)
{
    auto inner = TError("Inner")
        .With("inner_attr", 42);
    auto error = TError("Error")
        .With(inner)
        .With("attr", 8);

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
        .With("my_attr", "Attr value")
        .With(TError("Inner error"));
    auto truncatedError = error.Truncate();
    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());
    EXPECT_EQ(error.GetPid(), truncatedError.GetPid());
    EXPECT_EQ(error.GetTid(), truncatedError.GetTid());
    EXPECT_EQ(error.GetDatetime(), truncatedError.GetDatetime());
    EXPECT_EQ(error.Attributes().Get<std::string>("my_attr"), truncatedError.Attributes().Get<std::string>("my_attr"));
    EXPECT_EQ(error.InnerErrors().size(), truncatedError.InnerErrors().size());
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), truncatedError.InnerErrors()[0].GetMessage());
}

TEST(TErrorTest, TruncateLarge)
{
    auto error = TError("Some long long error")
        .With(TError("First inner error"))
        .With(TError("Second inner error"))
        .With(TError("Third inner error"))
        .With(TError("Fourth inner error"));
    SetErrorAttribute(&error, "my_attr", "Some long long attr");

    auto truncatedError = error.Truncate(/*maxInnerErrorCount*/ 3, /*stringLimit*/ 10);
    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ("Some long ...<message truncated>", truncatedError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<std::string>("my_attr"));
    EXPECT_EQ(truncatedError.InnerErrors().size(), 3u);

    EXPECT_EQ("First inne...<message truncated>", truncatedError.InnerErrors()[0].GetMessage());
    EXPECT_EQ("Second inn...<message truncated>", truncatedError.InnerErrors()[1].GetMessage());
    EXPECT_EQ("Fourth inn...<message truncated>", truncatedError.InnerErrors()[2].GetMessage());
}

TEST(TErrorTest, TruncateSimpleRValue)
{
    auto error = TError("Some error")
        .With("my_attr", "Attr value")
        .With(TError("Inner error"));
    auto errorCopy = error;
    auto truncatedError = std::move(errorCopy).Truncate();
    EXPECT_TRUE(errorCopy.IsOK());

    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());
    EXPECT_EQ(error.GetPid(), truncatedError.GetPid());
    EXPECT_EQ(error.GetTid(), truncatedError.GetTid());
    EXPECT_EQ(error.GetDatetime(), truncatedError.GetDatetime());
    EXPECT_EQ(error.Attributes().Get<std::string>("my_attr"), truncatedError.Attributes().Get<std::string>("my_attr"));
    EXPECT_EQ(error.InnerErrors().size(), truncatedError.InnerErrors().size());
    EXPECT_EQ(error.InnerErrors()[0].GetMessage(), truncatedError.InnerErrors()[0].GetMessage());
}

TEST(TErrorTest, TruncateLargeRValue)
{
    auto error = TError("Some long long error")
        .With(TError("First inner error"))
        .With(TError("Second inner error"))
        .With(TError("Third inner error"))
        .With(TError("Fourth inner error"));
    SetErrorAttribute(&error, "my_attr", "Some long long attr");

    auto errorCopy = error;
    auto truncatedError = std::move(errorCopy).Truncate(/*maxInnerErrorCount*/ 3, /*stringLimit*/ 10);
    EXPECT_TRUE(errorCopy.IsOK());

    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ("Some long ...<message truncated>", truncatedError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<std::string>("my_attr"));
    EXPECT_EQ(truncatedError.InnerErrors().size(), 3u);

    EXPECT_EQ("First inne...<message truncated>", truncatedError.InnerErrors()[0].GetMessage());
    EXPECT_EQ("Second inn...<message truncated>", truncatedError.InnerErrors()[1].GetMessage());
    EXPECT_EQ("Fourth inn...<message truncated>", truncatedError.InnerErrors()[2].GetMessage());
}

TEST(TErrorTest, TruncateConsistentOverloads)
{
    auto error = TError("Some long long error")
        .With(TError("First inner error"))
        .With(TError("Second inner error"))
        .With(TError("Third inner error"))
        .With(TError("Fourth inner error"));
    SetErrorAttribute(&error, "my_attr", "Some long long attr");

    auto errorCopy = error;
    auto truncatedRValueError = std::move(errorCopy).Truncate(/*maxInnerErrorCount*/ 3, /*stringLimit*/ 10);

    auto trunactedLValueError = error.Truncate(/*maxInnerErrorCount*/ 3, /*stringLimit*/ 10);

    EXPECT_EQ(truncatedRValueError, trunactedLValueError);
}

TEST(TErrorTest, TruncateWhitelist)
{
    auto error = TError("Some error");
    SetErrorAttribute(&error, "attr1", "Some long long attr");
    SetErrorAttribute(&error, "attr2", "Some long long attr");

    THashSet<TStringBuf> myWhitelist = {"attr2"};

    auto truncatedError = error.Truncate(2, 10, myWhitelist);

    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());

    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<std::string>("attr1"));
    EXPECT_EQ("Some long long attr", truncatedError.Attributes().Get<std::string>("attr2"));
}

TEST(TErrorTest, TruncateWhitelistRValue)
{
    auto error = TError("Some error");
    SetErrorAttribute(&error, "attr1", "Some long long attr");
    SetErrorAttribute(&error, "attr2", "Some long long attr");

    THashSet<TStringBuf> myWhitelist = {"attr2"};

    auto errorCopy = error;
    auto truncatedError = std::move(errorCopy).Truncate(2, 10, myWhitelist);
    EXPECT_TRUE(errorCopy.IsOK());

    EXPECT_EQ(error.GetCode(), truncatedError.GetCode());
    EXPECT_EQ(error.GetMessage(), truncatedError.GetMessage());

    EXPECT_EQ("...<attribute truncated>...", truncatedError.Attributes().Get<std::string>("attr1"));
    EXPECT_EQ("Some long long attr", truncatedError.Attributes().Get<std::string>("attr2"));
}

TEST(TErrorTest, TruncateWhitelistInnerErrors)
{
    auto innerError = TError("Inner error");
    SetErrorAttribute(&innerError, "attr1", "Some long long attr");
    SetErrorAttribute(&innerError, "attr2", "Some long long attr");

    auto error = TError("Error").With(innerError);

    THashSet<TStringBuf> myWhitelist = {"attr2"};

    auto truncatedError = error.Truncate(2, 15, myWhitelist);
    EXPECT_EQ(truncatedError.InnerErrors().size(), 1u);

    auto truncatedInnerError = truncatedError.InnerErrors()[0];
    EXPECT_EQ(truncatedInnerError.GetCode(), innerError.GetCode());
    EXPECT_EQ(truncatedInnerError.GetMessage(), innerError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedInnerError.Attributes().Get<std::string>("attr1"));
    EXPECT_EQ("Some long long attr", truncatedInnerError.Attributes().Get<std::string>("attr2"));
}

TEST(TErrorTest, TruncateWhitelistInnerErrorsRValue)
{
    auto innerError = TError("Inner error");
    SetErrorAttribute(&innerError, "attr1", "Some long long attr");
    SetErrorAttribute(&innerError, "attr2", "Some long long attr");

    auto error = TError("Error").With(innerError);

    THashSet<TStringBuf> myWhitelist = {"attr2"};

    auto errorCopy = error;
    auto truncatedError = std::move(errorCopy).Truncate(2, 15, myWhitelist);
    EXPECT_TRUE(errorCopy.IsOK());
    EXPECT_EQ(truncatedError.InnerErrors().size(), 1u);

    auto truncatedInnerError = truncatedError.InnerErrors()[0];
    EXPECT_EQ(truncatedInnerError.GetCode(), innerError.GetCode());
    EXPECT_EQ(truncatedInnerError.GetMessage(), innerError.GetMessage());
    EXPECT_EQ("...<attribute truncated>...", truncatedInnerError.Attributes().Get<std::string>("attr1"));
    EXPECT_EQ("Some long long attr", truncatedInnerError.Attributes().Get<std::string>("attr2"));
}

TEST(TErrorTest, TruncateWhitelistSaveInnerError)
{
    auto genericInner = TError("GenericInner");
    auto whitelistedInner = TError("Inner")
        .With("whitelisted_key", 42);

    auto error = TError("Error")
        .With(genericInner.With("foo", "bar"))
        .With(whitelistedInner)
        .With(genericInner);

    error = std::move(error).Truncate(1, 20, {
        "whitelisted_key"
    });
    EXPECT_TRUE(!error.IsOK());
    EXPECT_EQ(error.InnerErrors().size(), 2u);
    EXPECT_EQ(error.InnerErrors()[0], whitelistedInner);
    EXPECT_EQ(error.InnerErrors()[1], genericInner);

    // TODO(arkady-e1ppa): error_helpers???
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

        auto stringValue = error.Attributes().Find<std::string>("String value");
        EXPECT_TRUE(stringValue);
        EXPECT_EQ(*stringValue, "FooBar");
    }
}

TEST(TErrorTest, AttributeSerialization)
{
    auto getWeededText = [] (const TError& err) {
        std::vector<std::string> lines;
        for (const auto& line : StringSplitter(ToString(err)).Split('\n')) {
            if (!line.Contains("origin") && !line.Contains("datetime")) {
                lines.push_back(std::string{line});
            }
        }
        return JoinSeq("\n", lines);
    };

    EXPECT_EQ(getWeededText(TError("E1").With("A1", "V1")), std::string(
        "E1\n"
        "    A1              V1\n"));
    EXPECT_EQ(getWeededText(TError("E1").With("A1", "L1\nL2\nL3")), std::string(
        "E1\n"
        "    A1\n"
        "        L1\n"
        "        L2\n"
        "        L3\n"));
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

TEST(TErrorTest, Enrichers)
{
    static auto getAttribute = [] (const TError& error) {
        return error.Attributes().Get<TString>("test_attribute", "");
    };

    {
        static thread_local bool testEnricherEnabled = false;
        testEnricherEnabled = true;

        TError::RegisterEnricher([](TError* error) {
            if (testEnricherEnabled) {
                error->Add("test_attribute", getAttribute(*error) + "X");
            }
        });

        // Not from exception.
        EXPECT_EQ(getAttribute(TError("E")), "X");
        EXPECT_EQ(getAttribute(TError(NYT::EErrorCode::Generic, "E")), "X");

        // std::exception.
        EXPECT_EQ(getAttribute(TError(std::runtime_error("E"))), "X");

        // Copying.
        EXPECT_EQ(getAttribute(TError(TError(std::runtime_error("E")))), "X");
        EXPECT_EQ(getAttribute(TError(TErrorException() <<= TError(std::runtime_error("E")))), "X");

        testEnricherEnabled = false;
    }

    {
        static thread_local bool testFromExceptionEnricherEnabled = false;
        testFromExceptionEnricherEnabled = true;

        TError::RegisterFromExceptionEnricher([](TError* error, const std::exception&) {
            if (testFromExceptionEnricherEnabled) {
                error->Add("test_attribute", getAttribute(*error) + "X");
            }
        });

        // Not from exception.
        EXPECT_EQ(getAttribute(TError("E")), "");
        EXPECT_EQ(getAttribute(TError(NYT::EErrorCode::Generic, "E")), "");

        // From exception.
        EXPECT_EQ(getAttribute(TError(std::runtime_error("E"))), "X");
        EXPECT_EQ(getAttribute(TError(TError(std::runtime_error("E")))), "X");

        // From exception twice.
        EXPECT_EQ(getAttribute(TError(TErrorException() <<= TError(std::runtime_error("E")))), "XX");

        testFromExceptionEnricherEnabled = false;
    }
}

TEST(TErrorTest, ValueOrCrashSimple)
{
    TErrorOr<int> result = 42;
    EXPECT_EQ(result.ValueOrCrash(), 42);

    result.ValueOrCrash() = 67;
    EXPECT_EQ(result.ValueOrCrash(), 67);
}

TEST(TErrorTest, ValueOrCrashHappyPath)
{
    TErrorOr<TWidget> result;
    EXPECT_EQ(result.ValueOrCrash().ResetDefaultCount(), 1);

    {
        const auto& resultRef = result;
        auto value = resultRef.ValueOrCrash();
        EXPECT_EQ(value.ResetCopyCount(), 1);
    }

    {
        auto value = std::move(result).ValueOrCrash();
        EXPECT_EQ(value.ResetMoveCount(), 1);
    }
}

TEST(TErrorTest, ValueOrCrashDeath)
{
    TErrorOr<TWidget> result = TError("death");

    EXPECT_DEATH({ result.ValueOrCrash(); }, "YT_VERIFY");

    {
        const auto& resultRef = result;
        EXPECT_DEATH({ resultRef.ValueOrCrash(); }, "YT_VERIFY");
    }

    {
        EXPECT_DEATH({ std::move(result).ValueOrCrash(); }, "YT_VERIFY");
    }
}

TEST(TErrorOrConstructionTraitsTest, ValueTraits)
{
    using TMoveOnlyError = TErrorOr<TMoveOnly>;
    static_assert(!std::is_copy_constructible_v<TMoveOnly>);
    static_assert(std::is_move_constructible_v<TMoveOnlyError>);
    static_assert(!std::is_copy_constructible_v<TMoveOnlyError>);
    static_assert(std::is_move_assignable_v<TMoveOnlyError>);
    static_assert(!std::is_copy_assignable_v<TMoveOnlyError>);
}

TEST(TErrorOrConstructionTraitsTest, FromError)
{
    {
        TError error1("string: copy from error");
        TErrorOr<std::string> errorOr1(error1);
        EXPECT_FALSE(errorOr1.IsOK());
        EXPECT_EQ(errorOr1.GetMessage(), "string: copy from error");
        EXPECT_FALSE(error1.IsOK());

        TError error2("string: move from error");
        TErrorOr<std::string> errorOr2(std::move(error2));
        EXPECT_FALSE(errorOr2.IsOK());
        EXPECT_EQ(errorOr2.GetMessage(), "string: move from error");
        EXPECT_TRUE(error2.IsOK());
    }
    {
        TError error("unique_ptr: move from error");
        TErrorOr<std::unique_ptr<int>> errorOr(std::move(error));
        EXPECT_FALSE(errorOr.IsOK());
        EXPECT_EQ(errorOr.GetMessage(), "unique_ptr: move from error");
        EXPECT_TRUE(error.IsOK());

        static_assert(std::is_constructible_v<TErrorOr<std::unique_ptr<int>>, const TError&>);
        static_assert(std::is_constructible_v<TErrorOr<std::unique_ptr<int>>, TError&&>);
    }
    {
        TError error("copy_only: copy from error");
        TErrorOr<TCopyOnly> errorOr(error);
        EXPECT_FALSE(errorOr.IsOK());
        EXPECT_EQ(errorOr.GetMessage(), "copy_only: copy from error");
        EXPECT_FALSE(error.IsOK());

        static_assert(std::is_constructible_v<TErrorOr<TCopyOnly>, const TError&>);
        static_assert(std::is_constructible_v<TErrorOr<TCopyOnly>, TError&&>);
    }
}

TEST(TErrorOrConstructionTraitsTest, FromValue)
{
    {
        std::string str = "value copy";
        TErrorOr<std::string> errorOr1(str);
        EXPECT_TRUE(errorOr1.IsOK());
        EXPECT_EQ(errorOr1.Value(), "value copy");
        EXPECT_EQ(str, "value copy");

        TErrorOr<std::string> errorOr2(std::string("value move"));
        EXPECT_TRUE(errorOr2.IsOK());
        EXPECT_EQ(errorOr2.Value(), "value move");
    }
    {
        auto ptr = std::make_unique<int>(42);
        TErrorOr<std::unique_ptr<int>> errorOr(std::move(ptr));
        EXPECT_TRUE(errorOr.IsOK());
        EXPECT_EQ(*errorOr.Value(), 42);
        EXPECT_EQ(ptr, nullptr);
    }

    static_assert(std::is_constructible_v<TErrorOr<std::unique_ptr<int>>, std::unique_ptr<int>&&>);
    static_assert(!std::is_constructible_v<TErrorOr<std::unique_ptr<int>>, const std::unique_ptr<int>&>);

    {
        TCopyOnly obj(123);
        TErrorOr<TCopyOnly> errorOr(obj);
        EXPECT_TRUE(errorOr.IsOK());
        EXPECT_EQ(errorOr.Value().Value(), 123);
        EXPECT_EQ(obj.Value(), 123);
    }

    static_assert(std::is_constructible_v<TErrorOr<TCopyOnly>, const TCopyOnly&>);
    static_assert(std::is_constructible_v<TErrorOr<TCopyOnly>, TCopyOnly&&>);
}

TEST(TErrorOrConstructionTraitsTest, FromSameSpecialization)
{
    {
        TErrorOr<std::string> source1(TError("string: error copy"));
        TErrorOr<std::string> dest1(source1);
        EXPECT_FALSE(dest1.IsOK());
        EXPECT_EQ(dest1.GetMessage(), "string: error copy");
        EXPECT_FALSE(source1.IsOK());

        TErrorOr<std::string> source2(TError("string: error move"));
        TErrorOr<std::string> dest2(std::move(source2));
        EXPECT_FALSE(dest2.IsOK());
        EXPECT_EQ(dest2.GetMessage(), "string: error move");
    }
    {
        TErrorOr<std::string> source1(std::string("value copy"));
        TErrorOr<std::string> dest1(source1);
        EXPECT_TRUE(dest1.IsOK());
        EXPECT_EQ(dest1.Value(), "value copy");
        EXPECT_EQ(source1.Value(), "value copy");

        TErrorOr<std::string> source2(std::string("value move"));
        TErrorOr<std::string> dest2(std::move(source2));
        EXPECT_TRUE(dest2.IsOK());
        EXPECT_EQ(dest2.Value(), "value move");
    }
    {
        TErrorOr<std::unique_ptr<int>> source(TError("unique_ptr: error"));
        TErrorOr<std::unique_ptr<int>> dest(std::move(source));
        EXPECT_FALSE(dest.IsOK());
        EXPECT_EQ(dest.GetMessage(), "unique_ptr: error");
    }
    {
        TErrorOr<std::unique_ptr<int>> source(std::make_unique<int>(42));
        TErrorOr<std::unique_ptr<int>> dest(std::move(source));
        EXPECT_TRUE(dest.IsOK());
        EXPECT_EQ(*dest.Value(), 42);
    }
    {
        TErrorOr<TCopyOnly> source(TError("copy_only: error"));
        TErrorOr<TCopyOnly> dest(source);
        EXPECT_FALSE(dest.IsOK());
        EXPECT_EQ(dest.GetMessage(), "copy_only: error");
        EXPECT_FALSE(source.IsOK());
    }
    {
        TErrorOr<TCopyOnly> source(TCopyOnly(123));
        TErrorOr<TCopyOnly> dest(source);
        EXPECT_TRUE(dest.IsOK());
        EXPECT_EQ(dest.Value().Value(), 123);
        EXPECT_EQ(source.Value().Value(), 123);
    }

    static_assert(!std::is_copy_constructible_v<TErrorOr<std::unique_ptr<int>>>);
    static_assert(std::is_move_constructible_v<TErrorOr<std::unique_ptr<int>>>);
    static_assert(std::is_copy_constructible_v<TErrorOr<TCopyOnly>>);
    static_assert(std::is_move_constructible_v<TErrorOr<TCopyOnly>>);
    static_assert(std::is_copy_constructible_v<TErrorOr<std::string>>);
    static_assert(std::is_move_constructible_v<TErrorOr<std::string>>);
}

TEST(TErrorOrConstructionTraitsTest, FromDifferentSpecialization)
{
    {
        TErrorOr<int> sourceInt(TError("int→long: error copy"));
        TErrorOr<long> destLong1(sourceInt);
        EXPECT_FALSE(destLong1.IsOK());
        EXPECT_EQ(destLong1.GetMessage(), "int→long: error copy");
        EXPECT_FALSE(sourceInt.IsOK());

        TErrorOr<int> sourceInt2(TError("int→long: error move"));
        TErrorOr<long> destLong2(std::move(sourceInt2));
        EXPECT_FALSE(destLong2.IsOK());
        EXPECT_EQ(destLong2.GetMessage(), "int→long: error move");
    }
    {
        TErrorOr<int> sourceInt(42);
        TErrorOr<long> destLong1(sourceInt);
        EXPECT_TRUE(destLong1.IsOK());
        EXPECT_EQ(destLong1.Value(), 42L);
        EXPECT_EQ(sourceInt.Value(), 42);

        TErrorOr<int> sourceInt2(84);
        TErrorOr<long> destLong2(std::move(sourceInt2));
        EXPECT_TRUE(destLong2.IsOK());
        EXPECT_EQ(destLong2.Value(), 84L);
    }
    {
        TErrorOr<const char*> sourceStr(TError("cstr→string: error"));
        TErrorOr<std::string> destStr(sourceStr);
        EXPECT_FALSE(destStr.IsOK());
        EXPECT_EQ(destStr.GetMessage(), "cstr→string: error");
        EXPECT_FALSE(sourceStr.IsOK());
    }
    {
        const char* cstr = "value copy";
        TErrorOr<const char*> sourceStr(cstr);
        TErrorOr<std::string> destStr(sourceStr);
        EXPECT_TRUE(destStr.IsOK());
        EXPECT_EQ(destStr.Value(), "value copy");
        EXPECT_TRUE(sourceStr.IsOK());
    }

    static_assert(!std::is_constructible_v<TErrorOr<std::string>, const TErrorOr<int*>&>);
    static_assert(!std::is_constructible_v<TErrorOr<std::string>, TErrorOr<int*>&&>);
    static_assert(!std::is_constructible_v<TErrorOr<std::unique_ptr<int>>, const TErrorOr<int>&>);
    static_assert(!std::is_constructible_v<TErrorOr<std::unique_ptr<int>>, TErrorOr<int>&&>);
    static_assert(!std::is_constructible_v<TErrorOr<TCopyOnly>, const TErrorOr<std::string>&>);
    static_assert(!std::is_constructible_v<TErrorOr<TCopyOnly>, TErrorOr<std::string>&&>);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
