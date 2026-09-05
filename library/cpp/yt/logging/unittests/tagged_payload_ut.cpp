#include "helpers.h"

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/tag.h>
#include <library/cpp/yt/logging/tagged_payload.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/raw_formatter.h>
#include <library/cpp/yt/string/string_builder.h>

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace NYT::NLogging {
namespace {

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

struct TDecodedPayload
{
    std::string Message;
    std::vector<std::pair<std::string, std::string>> Tags;
};

TTaggedLogEventPayload Encode(TStringBuf message, const std::vector<std::pair<TStringBuf, TStringBuf>>& tags = {})
{
    TTaggedPayloadWriter writer;
    WriteMessage(&writer, message);
    for (auto [key, value] : tags) {
        WriteTag(&writer, key, value);
    }
    return writer.Finish();
}

TDecodedPayload Decode(const TTaggedLogEventPayload& payload)
{
    TTaggedPayloadReader reader(payload);
    TDecodedPayload result;
    result.Message = reader.ReadMessage();
    while (auto tag = reader.TryReadTag()) {
        result.Tags.emplace_back(tag->Key, tag->Value);
    }
    return result;
}

TEST(TTaggedPayloadTest, MessageOnly)
{
    auto decoded = Decode(Encode("Hello"));
    EXPECT_EQ(decoded.Message, "Hello");
    EXPECT_TRUE(decoded.Tags.empty());
}

TEST(TTaggedPayloadTest, EmptyMessage)
{
    auto decoded = Decode(Encode(""));
    EXPECT_EQ(decoded.Message, "");
    EXPECT_TRUE(decoded.Tags.empty());
}

TEST(TTaggedPayloadTest, OneTag)
{
    auto decoded = Decode(Encode("Message", {{"Key", "Value"}}));
    EXPECT_EQ(decoded.Message, "Message");
    ASSERT_EQ(decoded.Tags.size(), 1u);
    EXPECT_EQ(decoded.Tags[0].first, "Key");
    EXPECT_EQ(decoded.Tags[0].second, "Value");
}

TEST(TTaggedPayloadTest, ManyTags)
{
    auto decoded = Decode(Encode("Message", {{"Arg1", "123"}, {"Arg2", "test"}, {"Arg3", ""}}));
    EXPECT_EQ(decoded.Message, "Message");
    ASSERT_EQ(decoded.Tags.size(), 3u);
    EXPECT_EQ(decoded.Tags[0], std::pair(std::string("Arg1"), std::string("123")));
    EXPECT_EQ(decoded.Tags[1], std::pair(std::string("Arg2"), std::string("test")));
    EXPECT_EQ(decoded.Tags[2], std::pair(std::string("Arg3"), std::string("")));
}

TEST(TTaggedPayloadTest, BinarySafeValues)
{
    // Keys/values may carry arbitrary bytes, including embedded NULs and delimiters.
    std::string message("a\0b ()", 6);
    std::string key("k\0:", 3);
    std::string value("v\0, ", 4);

    auto decoded = Decode(Encode(message, {{key, value}}));
    EXPECT_EQ(decoded.Message, message);
    ASSERT_EQ(decoded.Tags.size(), 1u);
    EXPECT_EQ(decoded.Tags[0].first, key);
    EXPECT_EQ(decoded.Tags[0].second, value);
}

TEST(TTaggedPayloadTest, ReaderViewsPointIntoPayload)
{
    auto payload = Encode("Message", {{"Key", "Value"}});

    auto pointsInto = [&] (TStringBuf view) {
        return view.data() >= payload.Underlying().Begin() && view.data() + view.size() <= payload.Underlying().End();
    };

    TTaggedPayloadReader reader(payload);
    EXPECT_TRUE(pointsInto(reader.ReadMessage()));
    auto tag = reader.TryReadTag();
    ASSERT_TRUE(tag.has_value());
    EXPECT_TRUE(pointsInto(tag->Key));
    EXPECT_TRUE(pointsInto(tag->Value));
}

TEST(TTaggedPayloadTest, WellKnownTag)
{
    TTaggedPayloadWriter writer;
    WriteMessage(&writer, "Message");
    WriteTag(&writer, "Key", "Value");
    WriteWellKnownTag(&writer, "Error", "boom");
    auto payload = writer.Finish();

    TTaggedPayloadReader reader(payload);
    EXPECT_EQ(reader.ReadMessage(), "Message");

    auto regular = reader.TryReadTag();
    ASSERT_TRUE(regular);
    EXPECT_EQ(regular->Key, "Key");
    EXPECT_EQ(regular->Value, "Value");
    EXPECT_FALSE(regular->IsWellKnown);

    auto wellKnown = reader.TryReadTag();
    ASSERT_TRUE(wellKnown);
    EXPECT_EQ(wellKnown->Key, "Error");
    EXPECT_EQ(wellKnown->Value, "boom");
    EXPECT_TRUE(wellKnown->IsWellKnown);

    EXPECT_FALSE(reader.TryReadTag());
}

TEST(TTaggedPayloadTest, FormatWellKnownTagTrailing)
{
    TTaggedPayloadWriter writer;
    WriteMessage(&writer, "Message");
    WriteTag(&writer, "Key", "Value");
    WriteWellKnownTag(&writer, "Error", "boom");
    // Regular tags stay inline; the well-known tag is appended after the |(...)| group.
    EXPECT_EQ(FormatTaggedPayload(writer.Finish()), "Message (Key: Value)\nboom");
}

TEST(TTaggedPayloadTest, FormatIntoFormatter)
{
    TRawFormatter<256> formatter;
    FormatTaggedPayload(&formatter, Encode("Message", {{"Key", "Value"}}));
    EXPECT_EQ(formatter.GetBuffer(), "Message (Key: Value)");
}

TEST(TTaggedPayloadTest, FormatIntoFullFormatter)
{
    // A full buffer clips the tail, closing paren included.
    TRawFormatter<12> formatter;
    FormatTaggedPayload(&formatter, Encode("Message", {{"Key", "Value"}}));
    EXPECT_EQ(formatter.GetBuffer(), "Message (Key");
}

TEST(TTaggedPayloadTest, FormatWellKnownTagIntoFormatter)
{
    TTaggedPayloadWriter writer;
    WriteMessage(&writer, "Message");
    WriteTag(&writer, "Key", "Value");
    WriteWellKnownTag(&writer, "Error", "boom");

    TRawFormatter<256> formatter;
    FormatTaggedPayload(&formatter, writer.Finish());
    EXPECT_EQ(formatter.GetBuffer(), "Message (Key: Value)\nboom");
}

////////////////////////////////////////////////////////////////////////////////

using TTags = std::vector<std::pair<std::string, std::string>>;

TTags ReadTags(const TLoggingTagListPayload& tags)
{
    TTags result;
    TTaggedPayloadReader reader(AsView(tags));
    while (auto tag = reader.TryReadTag()) {
        result.emplace_back(tag->Key, tag->Value);
    }
    return result;
}

void AppendStringTag(TLoggingTagListPayload* tags, TStringBuf key, TStringBuf value)
{
    TTaggedPayloadWriter::AppendTag(tags, key, [&] (TStringBuilderBase* builder) {
        builder->AppendString(value);
    });
}

TEST(TTaggedPayloadTest, AppendTag)
{
    auto check = [] (TStringBuf key, const auto& value, TStringBuf expected) {
        TLoggingTagListPayload tags;
        TTaggedPayloadWriter::AppendTag(&tags, key, [&] (TStringBuilderBase* builder) {
            FormatValue(builder, value, "v"_sb);
        });
        EXPECT_EQ(ReadTags(tags), (TTags{{std::string(key), std::string(expected)}}));
    };

    // Preallocates room for the digits and advances by fewer, leaving slack to trim.
    check("Count", 42, "42");
    check("Empty", TStringBuf(""), "");
    // Past TStringBuilderBase::MinBufferLength, so the payload grows mid-value.
    check("Long", std::string(4096, 'x'), std::string(4096, 'x'));
}

TEST(TTaggedPayloadTest, AppendTagAppendsAfterExistingTags)
{
    TLoggingTagListPayload tags;
    AppendStringTag(&tags, "First", "1");
    TTaggedPayloadWriter::AppendTag(&tags, "Second", [&] (TStringBuilderBase* builder) {
        FormatValue(builder, std::string(4096, 'y'), "v"_sb);
    });
    AppendStringTag(&tags, "Third", "3");

    EXPECT_EQ(ReadTags(tags), (TTags{{"First", "1"}, {"Second", std::string(4096, 'y')}, {"Third", "3"}}));
}

TEST(TTaggedPayloadTest, AppendTagRestoresTagsOnThrow)
{
    TLoggingTagListPayload tags;
    AppendStringTag(&tags, "Kept", "yes");
    auto before = tags.Underlying();

    auto throwAfter = [&] (int byteCount) {
        EXPECT_THROW(
            TTaggedPayloadWriter::AppendTag(&tags, "Doomed", [&] (TStringBuilderBase* builder) {
                builder->AppendString(std::string(byteCount, 'x'));
                throw std::runtime_error("boom");
            }),
            std::runtime_error);
        EXPECT_EQ(tags.Underlying(), before);
    };

    throwAfter(0);
    throwAfter(4096);
}

TEST(TTaggedPayloadTest, AppendTagWithReset)
{
    TLoggingTagListPayload tags;
    TTaggedPayloadWriter::AppendTag(&tags, "Rewritten", [] (TStringBuilderBase* builder) {
        builder->AppendString(std::string(4096, 'x'));
        builder->Reset();
        builder->AppendString("final");
    });
    TTaggedPayloadWriter::AppendTag(&tags, "Emptied", [] (TStringBuilderBase* builder) {
        builder->AppendString(std::string(4096, 'x'));
        builder->Reset();
    });

    EXPECT_EQ(ReadTags(tags), (TTags{{"Rewritten", "final"}, {"Emptied", ""}}));
}

TEST(TLoggingTagListTest, Add)
{
    TLoggingTagList tags;
    tags.Add("Count", 42);
    tags.AddFormat("Range", "%v-%v", 1, 9);

    EXPECT_EQ(ReadTags(tags.GetPayload()), (TTags{{"Count", "42"}, {"Range", "1-9"}}));
}

TEST(TLoggingTagListBuilderTest, AppendsToTarget)
{
    TLoggingTagList tags;
    TLoggingTagListBuilder(&tags).With("Key", 1);
    EXPECT_EQ(ToString(tags), "Key: 1");
}

TEST(TLoggingTagListBuilderTest, ChainKeepsOrder)
{
    TLoggingTagList tags;
    TLoggingTagListBuilder(&tags)
        .With("First", 1)
        .WithFormat("Second", "%.2f", 1.5)
        .With("Third", "value");
    EXPECT_EQ(ToString(tags), "First: 1, Second: 1.50, Third: value");
}

TEST(TLoggingTagListBuilderTest, SkipsTagOnFalseCondition)
{
    TLoggingTagList tags;
    TLoggingTagListBuilder(&tags)
        .WithIf(false, "Skipped", 1)
        .WithIf(true, "Kept", 2)
        .WithFormatIf(false, "SkippedFormat", "%x", 255)
        .WithFormatIf(true, "KeptFormat", "%x", 255);
    EXPECT_EQ(ToString(tags), "Kept: 2, KeptFormat: ff");
}

TEST(TLoggingTagListBuilderTest, SplicesList)
{
    auto spliced = TLoggingTagList()
        .With("Inner", 1)
        .With("Other", 2);

    TLoggingTagList tags;
    TLoggingTagListBuilder(&tags)
        .With("Outer", 0)
        .With(spliced);
    EXPECT_EQ(ToString(tags), "Outer: 0, Inner: 1, Other: 2");
}

TEST(TLoggingTagListBuilderTest, AccumulatesAcrossBuilders)
{
    TLoggingTagList tags;
    TLoggingTagListBuilder(&tags).With("First", 1);
    TLoggingTagListBuilder(&tags).With("Second", 2);
    EXPECT_EQ(ToString(tags), "First: 1, Second: 2");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
