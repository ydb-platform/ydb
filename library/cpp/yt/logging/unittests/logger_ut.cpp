#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/logging/tagged_payload.h>
#include <library/cpp/yt/logging/structured_payload.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/yt/yson_string/string.h>

#include <atomic>
#include <string>
#include <utility>
#include <vector>

namespace NYT::NLogging {
namespace {

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

//! A log manager that captures enqueued events for inspection in tests.
class TMockLogManager
    : public ILogManager
{
public:
    explicit TMockLogManager(ELogLevel minLevel = ELogLevel::Minimum)
    {
        Category_.MinPlainTextLevel.store(minLevel);
    }

    void RegisterStaticAnchor(TLoggingAnchor* anchor, ::TSourceLocation /*sourceLocation*/, TStringBuf /*message*/) override
    {
        anchor->Registered.store(true);
    }

    void UpdateAnchor(TLoggingAnchor* anchor) override
    {
        anchor->CurrentVersion.store(ActualVersion_.load());
    }

    void Enqueue(TLogEvent&& event) override
    {
        Events_.push_back(std::move(event));
    }

    const TLoggingCategory* GetCategory(TStringBuf /*categoryName*/) override
    {
        return &Category_;
    }

    void UpdateCategory(TLoggingCategory* category) override
    {
        category->CurrentVersion.store(ActualVersion_.load());
    }

    bool GetAbortOnAlert() const override
    {
        return false;
    }

    const std::vector<TLogEvent>& GetEvents() const
    {
        return Events_;
    }

private:
    std::vector<TLogEvent> Events_;
    std::atomic<int> ActualVersion_ = 1;
    TLoggingCategory Category_ = {
        .Name = "Test",
        .MinPlainTextLevel = ELogLevel::Minimum,
        .CurrentVersion = 0,
        .ActualVersion = &ActualVersion_,
    };
};

struct TDecodedEvent
{
    std::string Message;
    std::vector<std::pair<std::string, std::string>> Tags;
};

TDecodedEvent DecodeEvent(const TLogEvent& event)
{
    TTaggedPayloadReader reader(std::get<TTaggedLogEventPayload>(event.Payload));
    TDecodedEvent result;
    result.Message = reader.ReadMessage();
    while (auto tag = reader.TryReadTag()) {
        result.Tags.emplace_back(tag->Key, tag->Value);
    }
    return result;
}

TDecodedEvent DecodeSingleEvent(const TMockLogManager& manager)
{
    EXPECT_EQ(manager.GetEvents().size(), 1u);
    return DecodeEvent(manager.GetEvents()[0]);
}

NYson::TYsonString MakeMapFragment(TStringBuf yson)
{
    return NYson::TYsonString(yson, NYson::EYsonType::MapFragment);
}

TStringBuf GetStructuredYson(const TLogEvent& event)
{
    return GetYsonFromStructuredPayload(std::get<TStructuredLogEventPayload>(event.Payload)).AsStringBuf();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLoggerTest, NullByDefault)
{
    {
        TLogger logger;
        EXPECT_FALSE(logger);
        EXPECT_FALSE(logger.IsLevelEnabled(ELogLevel::Fatal));
    }
    {
        TLogger logger{"Category"};
        EXPECT_FALSE(logger);
        EXPECT_FALSE(logger.IsLevelEnabled(ELogLevel::Fatal));
    }
}

TEST(TLoggerTest, CopyOfNullLogger)
{
    TLogger nullLogger{/*logManager*/ nullptr, "Category"};
    ASSERT_FALSE(nullLogger);

    auto logger = nullLogger.WithMinLevel(ELogLevel::Debug);

    EXPECT_FALSE(logger);
    EXPECT_FALSE(logger.IsLevelEnabled(ELogLevel::Fatal));
}

TEST(TLoggerTest, LogAlertAndThrowMessage)
{
    try {
        TLogger Logger;
        YT_LOG_ALERT_AND_THROW("Alert message (Arg1: %v, Arg2: %v)",
            1,
            2);
        EXPECT_TRUE(false);
    } catch (const TErrorException& ex) {
        const auto& error = ex.Error();
        EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Fatal);
        EXPECT_EQ(error.GetMessage(), "Malformed request or incorrect state detected");
        EXPECT_EQ(error.Attributes().Get<std::string>("message"), "Alert message (Arg1: 1, Arg2: 2)");
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTaggedApiTest, MessageOnly)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");
    YT_TLOG_INFO("Message");

    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Message");
    EXPECT_TRUE(decoded.Tags.empty());
}

TEST(TTaggedApiTest, Tags)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");
    YT_TLOG_INFO("Message")
        .With("Arg1", 123)
        .With("Arg2", "test");

    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Message");
    ASSERT_EQ(decoded.Tags.size(), 2u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Arg1"), std::string("123")));
    EXPECT_EQ(decoded.Tags[1], std::pair(std::string("Arg2"), std::string("test")));
}

TEST(TTaggedApiTest, DynamicAnchor)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    TLoggingAnchor anchor;
    anchor.AnchorMessage = "Owner-supplied anchor name";

    YT_TLOG_EVENT_WITH_DYNAMIC_ANCHOR(Logger, ELogLevel::Info, &anchor, "Message")
        .With("Arg1", 123);

    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Message");
    ASSERT_EQ(decoded.Tags.size(), 1u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Arg1"), std::string("123")));

    EXPECT_EQ(manager.GetEvents()[0].Anchor, &anchor);
    EXPECT_EQ(anchor.AnchorMessage, "Owner-supplied anchor name");
    EXPECT_FALSE(anchor.Registered.load());
}

TEST(TTaggedApiTest, DynamicAnchorRuntimeMessage)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    TLoggingAnchor anchor;
    for (int index = 0; index < 2; ++index) {
        auto message = Format("Message %v", index);
        YT_TLOG_EVENT_WITH_DYNAMIC_ANCHOR(Logger, ELogLevel::Info, &anchor, message);
    }

    ASSERT_EQ(manager.GetEvents().size(), 2u);
    EXPECT_EQ(DecodeEvent(manager.GetEvents()[0]).Message, "Message 0");
    EXPECT_EQ(DecodeEvent(manager.GetEvents()[1]).Message, "Message 1");
    EXPECT_TRUE(anchor.AnchorMessage.empty());
}

TEST(TTaggedApiTest, DynamicAnchorLevelOverrideSuppresses)
{
    TMockLogManager manager(/*minLevel*/ ELogLevel::Warning);
    TLogger Logger(&manager, "Test");

    TLoggingAnchor anchor;
    anchor.LevelOverride.store(ELogLevel::Debug);

    int evaluated = 0;
    auto evaluate = [&] {
        ++evaluated;
        return 123;
    };
    YT_TLOG_EVENT_WITH_DYNAMIC_ANCHOR(Logger, ELogLevel::Warning, &anchor, "Message")
        .With("Arg1", evaluate());

    EXPECT_EQ(evaluated, 0);
    EXPECT_TRUE(manager.GetEvents().empty());
}

TEST(TTaggedApiTest, DynamicAnchorLevelOverridePromotes)
{
    TMockLogManager manager(/*minLevel*/ ELogLevel::Warning);
    TLogger Logger(&manager, "Test");

    TLoggingAnchor anchor;
    anchor.LevelOverride.store(ELogLevel::Warning);

    YT_TLOG_EVENT_WITH_DYNAMIC_ANCHOR(Logger, ELogLevel::Debug, &anchor, "Message");

    ASSERT_EQ(manager.GetEvents().size(), 1u);
    EXPECT_EQ(manager.GetEvents()[0].Level, ELogLevel::Warning);
}

TEST(TTaggedApiTest, DynamicAnchorIsUpdatedWhenStale)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    TLoggingAnchor anchor;
    ASSERT_FALSE(Logger.IsAnchorUpToDate(anchor));

    YT_TLOG_EVENT_WITH_DYNAMIC_ANCHOR(Logger, ELogLevel::Info, &anchor, "Message");

    EXPECT_TRUE(Logger.IsAnchorUpToDate(anchor));
    EXPECT_EQ(manager.GetEvents().size(), 1u);
}

TEST(TTaggedApiTest, WithFormat)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");
    YT_TLOG_INFO("Message")
        .WithFormat("Method", "%v.%v", "MyService", "MyMethod")
        .With("Arg1", 123);

    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Message");
    ASSERT_EQ(decoded.Tags.size(), 2u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Method"), std::string("MyService.MyMethod")));
    EXPECT_EQ(decoded.Tags[1], std::pair(std::string("Arg1"), std::string("123")));
}

TEST(TTaggedApiTest, WithIf)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");
    YT_TLOG_INFO("Message")
        .WithIf(true, "Kept", 1)
        .WithIf(false, "Dropped", 2)
        .With("After", 3);

    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Message");
    ASSERT_EQ(decoded.Tags.size(), 2u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Kept"), std::string("1")));
    EXPECT_EQ(decoded.Tags[1], std::pair(std::string("After"), std::string("3")));
}

TEST(TTaggedApiTest, WithFormatIf)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");
    YT_TLOG_INFO("Message")
        .WithFormatIf(true, "Kept", "%v.%v", "MyService", "MyMethod")
        .WithFormatIf(false, "Dropped", "%v.%v", "Other", "Method")
        .With("After", 3);

    auto decoded = DecodeSingleEvent(manager);
    ASSERT_EQ(decoded.Tags.size(), 2u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Kept"), std::string("MyService.MyMethod")));
    EXPECT_EQ(decoded.Tags[1], std::pair(std::string("After"), std::string("3")));
}

TEST(TTaggedApiTest, TagList)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    auto tags = TLoggingTagList()
        .With("Address", "localhost:1234")
        .With("ConnectionId", 42)
        .WithFormat("Flags", "%x", 256)
        .WithFormat("Method", "%v.%v", "MyService", "MyMethod");

    YT_TLOG_INFO("Message")
        .With("Before", 1)
        .With(tags)
        .With("After", 2);

    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Message");
    ASSERT_EQ(decoded.Tags.size(), 6u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Before"), std::string("1")));
    EXPECT_EQ(decoded.Tags[1], std::pair(std::string("Address"), std::string("localhost:1234")));
    EXPECT_EQ(decoded.Tags[2], std::pair(std::string("ConnectionId"), std::string("42")));
    EXPECT_EQ(decoded.Tags[3], std::pair(std::string("Flags"), std::string("100")));
    EXPECT_EQ(decoded.Tags[4], std::pair(std::string("Method"), std::string("MyService.MyMethod")));
    EXPECT_EQ(decoded.Tags[5], std::pair(std::string("After"), std::string("2")));
}

TEST(TTaggedApiTest, EmptyTagList)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    TLoggingTagList tags;
    EXPECT_TRUE(tags.IsEmpty());

    YT_TLOG_INFO("Message")
        .With(tags);

    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Message");
    EXPECT_TRUE(decoded.Tags.empty());
}

TEST(TTaggedApiTest, TagListReusedAcrossEvents)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    TLoggingTagList tags;
    tags.Add("Shared", "yes");
    EXPECT_FALSE(tags.IsEmpty());

    YT_TLOG_INFO("First").With(tags);
    YT_TLOG_INFO("Second").With(tags);

    ASSERT_EQ(manager.GetEvents().size(), 2u);
    for (const auto& event : manager.GetEvents()) {
        auto decoded = DecodeEvent(event);
        ASSERT_EQ(decoded.Tags.size(), 1u);
        EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Shared"), std::string("yes")));
    }
}

TEST(TTaggedApiTest, TagListBeforeWellKnownTag)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    TLoggingTagList tags;
    tags.Add("Shared", "yes");
    auto error = TError("boom");

    YT_TLOG_INFO("Message")
        .With(tags)
        .With(error);

    ASSERT_EQ(manager.GetEvents().size(), 1u);
    TTaggedPayloadReader reader(std::get<TTaggedLogEventPayload>(manager.GetEvents()[0].Payload));
    EXPECT_EQ(reader.ReadMessage(), "Message");

    auto shared = reader.TryReadTag();
    ASSERT_TRUE(shared);
    EXPECT_EQ(shared->Key, "Shared");
    EXPECT_FALSE(shared->IsWellKnown);

    auto errorTag = reader.TryReadTag();
    ASSERT_TRUE(errorTag);
    EXPECT_EQ(errorTag->Key, "Error");
    EXPECT_TRUE(errorTag->IsWellKnown);

    EXPECT_FALSE(reader.TryReadTag());
}

TEST(TTaggedApiTest, WithFormatDisabledDoesNotEvaluateArgs)
{
    TMockLogManager manager(/*minLevel*/ ELogLevel::Warning);
    TLogger Logger(&manager, "Test");

    int evaluated = 0;
    auto evaluate = [&] {
        ++evaluated;
        return 123;
    };
    YT_TLOG_INFO("Message")
        .WithFormat("Arg1", "%v-%v", evaluate(), evaluate());

    EXPECT_EQ(evaluated, 0);
    EXPECT_TRUE(manager.GetEvents().empty());
}

TEST(TTaggedApiTest, LoggerTagsStayStructured)
{
    TMockLogManager manager;
    auto Logger = TLogger(&manager, "Test")
        .WithTag("LoggingTag", 555);
    YT_TLOG_INFO("Message")
        .With("Arg1", 123);

    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Message");
    ASSERT_EQ(decoded.Tags.size(), 2u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("LoggingTag"), std::string("555")));
    EXPECT_EQ(decoded.Tags[1], std::pair(std::string("Arg1"), std::string("123")));
}

TEST(TTaggedApiTest, DisabledDoesNotEvaluateTags)
{
    TMockLogManager manager(/*minLevel*/ ELogLevel::Warning);
    TLogger Logger(&manager, "Test");

    int evaluated = 0;
    auto evaluate = [&] {
        ++evaluated;
        return 123;
    };
    YT_TLOG_INFO("Message")
        .With("Arg1", evaluate());

    EXPECT_EQ(evaluated, 0);
    EXPECT_TRUE(manager.GetEvents().empty());
}

TEST(TTaggedApiTest, WellKnownErrorTag)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");
    auto error = TError("boom");
    YT_TLOG_INFO("Message")
        .With("Tag", 1)
        .With(error);

    ASSERT_EQ(manager.GetEvents().size(), 1u);
    TTaggedPayloadReader reader(std::get<TTaggedLogEventPayload>(manager.GetEvents()[0].Payload));
    EXPECT_EQ(reader.ReadMessage(), "Message");

    auto regular = reader.TryReadTag();
    ASSERT_TRUE(regular);
    EXPECT_EQ(regular->Key, "Tag");
    EXPECT_EQ(regular->Value, "1");
    EXPECT_FALSE(regular->IsWellKnown);

    // |.With(error)| attaches the error under the well-known "Error" key (resolved via
    // TWellKnownLoggingTagTraits), with the formatted error as the value.
    auto errorTag = reader.TryReadTag();
    ASSERT_TRUE(errorTag);
    EXPECT_EQ(errorTag->Key, "Error");
    EXPECT_TRUE(errorTag->IsWellKnown);
    EXPECT_NE(errorTag->Value.find("boom"), TStringBuf::npos);

    EXPECT_FALSE(reader.TryReadTag());
}

TEST(TTaggedApiTest, WellKnownExceptionTag)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    try {
        THROW_ERROR_EXCEPTION("boom");
    } catch (const std::exception& ex) {
        YT_TLOG_INFO("Message")
            .With(ex);
    }

    ASSERT_EQ(manager.GetEvents().size(), 1u);
    TTaggedPayloadReader reader(std::get<TTaggedLogEventPayload>(manager.GetEvents()[0].Payload));
    EXPECT_EQ(reader.ReadMessage(), "Message");

    auto errorTag = reader.TryReadTag();
    ASSERT_TRUE(errorTag);
    EXPECT_EQ(errorTag->Key, "Error");
    EXPECT_TRUE(errorTag->IsWellKnown);
    EXPECT_NE(errorTag->Value.find("boom"), TStringBuf::npos);

    EXPECT_FALSE(reader.TryReadTag());
}

// The single-pass formatter assumes well-known tags come last, so the fluent API must
// forbid a keyed tag after a well-known one (e.g. |.With(error).With("Key", 1)|) at
// compile time.
template <class TGuard>
concept CAllowsKeyedTagAfterWellKnown = requires (TGuard guard, TError error) {
    guard
        .With(error)
        .With("Key", 1);
};

// A further well-known tag after a well-known one stays allowed.
template <class TGuard>
concept CAllowsWellKnownTagAfterWellKnown = requires (TGuard guard, TError error) {
    guard
        .With(error)
        .With(error);
};

static_assert(!CAllowsKeyedTagAfterWellKnown<TTaggedLoggingGuard>);
static_assert(CAllowsWellKnownTagAfterWellKnown<TTaggedLoggingGuard>);

TEST(TTaggedApiTest, AlertAndThrowWithTags)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    try {
        YT_TLOG_ALERT_AND_THROW("Alert message")
            .With("Arg1", 1)
            .With("Arg2", 2);
        EXPECT_TRUE(false);
    } catch (const TErrorException& ex) {
        const auto& error = ex.Error();
        EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Fatal);
        EXPECT_EQ(error.GetMessage(), "Malformed request or incorrect state detected");
        EXPECT_EQ(error.Attributes().Get<std::string>("message"), "Alert message (Arg1: 1, Arg2: 2)");
    }

    // The alert is also logged, carrying the structured tags.
    auto decoded = DecodeSingleEvent(manager);
    EXPECT_EQ(decoded.Message, "Alert message");
    ASSERT_EQ(decoded.Tags.size(), 2u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Arg1"), std::string("1")));
    EXPECT_EQ(decoded.Tags[1], std::pair(std::string("Arg2"), std::string("2")));
}

TEST(TTaggedApiTest, AlertAndThrowWithoutTags)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    try {
        YT_TLOG_ALERT_AND_THROW("Alert message");
        EXPECT_TRUE(false);
    } catch (const TErrorException& ex) {
        EXPECT_EQ(ex.Error().Attributes().Get<std::string>("message"), "Alert message");
    }
}

TEST(TTaggedApiTest, AlertAndThrowDisabledStillThrows)
{
    TMockLogManager manager(/*minLevel*/ ELogLevel::Fatal);
    TLogger Logger(&manager, "Test");

    // The level is disabled, so nothing is logged, but the exception (with its message)
    // is still raised.
    try {
        YT_TLOG_ALERT_AND_THROW("Alert message")
            .With("Arg1", 1);
        EXPECT_TRUE(false);
    } catch (const TErrorException& ex) {
        EXPECT_EQ(ex.Error().Attributes().Get<std::string>("message"), "Alert message (Arg1: 1)");
    }
    EXPECT_TRUE(manager.GetEvents().empty());
}

//! Compiles only if |YT_TLOG_FATAL| is noreturn: no |return| follows it.
int LogFatal(const TLogger& Logger)
{
    YT_TLOG_FATAL("Fatal message")
        .With("Arg1", 1);
}

TEST(TTaggedApiDeathTest, Fatal)
{
    EXPECT_DEATH({
        TMockLogManager manager;
        TLogger Logger(&manager, "Test");
        LogFatal(Logger);
    }, "Fatal message \\(Arg1: 1\\)");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStructuredApiTest, StructuredEvent)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    LogStructuredEvent(Logger, MakeMapFragment("\"key\"=\"value\""), ELogLevel::Info);

    ASSERT_EQ(manager.GetEvents().size(), 1u);
    const auto& event = manager.GetEvents()[0];
    EXPECT_TRUE(std::holds_alternative<TStructuredLogEventPayload>(event.Payload));
    EXPECT_EQ(event.Family, ELogFamily::Structured);
    EXPECT_EQ(event.Level, ELogLevel::Info);
    // The payload is the raw YSON map fragment.
    EXPECT_EQ(GetStructuredYson(event), "\"key\"=\"value\"");
}

TEST(TStructuredApiTest, PreservesLevel)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    LogStructuredEvent(Logger, MakeMapFragment("\"a\"=1;\"b\"=2"), ELogLevel::Warning);

    ASSERT_EQ(manager.GetEvents().size(), 1u);
    EXPECT_EQ(manager.GetEvents()[0].Level, ELogLevel::Warning);
    EXPECT_EQ(GetStructuredYson(manager.GetEvents()[0]), "\"a\"=1;\"b\"=2");
}

TEST(TStructuredApiTest, EmptyFragment)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    LogStructuredEvent(Logger, MakeMapFragment(""), ELogLevel::Info);

    ASSERT_EQ(manager.GetEvents().size(), 1u);
    EXPECT_EQ(GetStructuredYson(manager.GetEvents()[0]), "");
}

TEST(TStructuredApiTest, MultipleEvents)
{
    TMockLogManager manager;
    TLogger Logger(&manager, "Test");

    LogStructuredEvent(Logger, MakeMapFragment("\"i\"=0"), ELogLevel::Debug);
    LogStructuredEvent(Logger, MakeMapFragment("\"i\"=1"), ELogLevel::Info);

    ASSERT_EQ(manager.GetEvents().size(), 2u);
    EXPECT_EQ(manager.GetEvents()[0].Level, ELogLevel::Debug);
    EXPECT_EQ(GetStructuredYson(manager.GetEvents()[0]), "\"i\"=0");
    EXPECT_EQ(manager.GetEvents()[1].Level, ELogLevel::Info);
    EXPECT_EQ(GetStructuredYson(manager.GetEvents()[1]), "\"i\"=1");
}

TEST(TTaggedApiTest, TraceTagsSpliced)
{
    auto logger = TLogger("Test").WithTag("LoggerTag", 1);

    auto traceTags = TLoggingTagList().With("TraceContextTag", 1);
    TLoggingContext loggingContext{};
    loggingContext.TraceLoggingTags = AsView(traceTags.GetPayload());

    TTaggedPayloadWriter writer;
    writer.BeginMessage()->AppendString("Message"_sb);
    writer.EndMessage();
    NDetail::AppendContextualTags(&writer, loggingContext, logger);
    auto payload = writer.Finish();

    // Trace tags are ordinary keyed records, spliced after the logger's own.
    TTaggedPayloadReader reader(payload);
    EXPECT_EQ(reader.ReadMessage(), "Message");
    auto loggerTag = reader.TryReadTag();
    ASSERT_TRUE(loggerTag);
    EXPECT_EQ(loggerTag->Key, "LoggerTag");
    auto traceTag = reader.TryReadTag();
    ASSERT_TRUE(traceTag);
    EXPECT_EQ(traceTag->Key, "TraceContextTag");
    EXPECT_EQ(traceTag->Value, "1");
    EXPECT_FALSE(reader.TryReadTag());

    EXPECT_EQ(
        FormatTaggedPayload(payload),
        std::string(GetMessageFromTaggedPayload(
            NDetail::BuildLogMessage(loggingContext, logger, "Message").Payload)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
