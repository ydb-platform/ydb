#include "helpers.h"

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/tagged_payload.h>

#include <string>
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
