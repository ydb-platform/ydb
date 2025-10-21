#include "utils/trace.h"

#include <gtest/gtest.h>

namespace NYdb::inline Dev::NTopic::NTests {

TEST(Trace, SkipSpaces) {
    ASSERT_EQ(SkipSpaces(""), "");
    ASSERT_EQ(SkipSpaces("   "), "");
    ASSERT_EQ(SkipSpaces("  a"), "a");
    ASSERT_EQ(SkipSpaces(" a "), "a ");
}

TEST(Trace, NextToken) {
    ASSERT_EQ(NextToken(""), "");
    ASSERT_EQ(NextToken("   "), "");
    ASSERT_EQ(NextToken("a"), "a");
    ASSERT_EQ(NextToken("  a"), "a");
    ASSERT_EQ(NextToken(" a "), "a");
    std::string_view b("a=1");
    ASSERT_EQ(NextToken(b, '='), "a");
    ASSERT_EQ(b, "1");
}

TEST(Trace, TTraceEvent) {
    ASSERT_THROW(TTraceEvent::FromString(""), TTraceException);
    std::string const eventName("init");
    {
        std::string s(eventName);
        auto ev = TTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_TRUE(ev.KeyValues.empty());
    }
    {
        std::string s(eventName +  " a");
        auto ev = TTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 1u);
        ASSERT_TRUE(ev.KeyValues.at("a").empty());
    }
    {
        std::string s(eventName + " a b");
        auto ev = TTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 2u);
        ASSERT_TRUE(ev.KeyValues.at("a").empty());
        ASSERT_TRUE(ev.KeyValues.at("b").empty());
    }
    {   
        std::string s(eventName + " =");
        auto ev = TTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 1u);
        ASSERT_TRUE(ev.KeyValues.at("").empty());
    }
    {
        std::string s(eventName + " a=1 b");
        auto ev = TTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 2u);
        ASSERT_EQ(ev.KeyValues.at("a"), "1");
        ASSERT_TRUE(ev.KeyValues.at("b").empty());
    }
    {
        std::string s(eventName + " a b=2");
        auto ev = TTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 2u);
        ASSERT_TRUE(ev.KeyValues.at("a").empty());
        ASSERT_EQ(ev.KeyValues.at("b"), "2");
    }
    {
        std::string s(eventName + " a=1 b=2");
        auto ev = TTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 2u);
        ASSERT_EQ(ev.KeyValues.at("a"), "1");
        ASSERT_EQ(ev.KeyValues.at("b"), "2");
    }
    {
        TExpectedTraceEvent expected = {eventName, {{"a", {"1"}}}, {"d"}};
        ASSERT_FALSE(expected.Matches({"", {}}));
        ASSERT_FALSE(expected.Matches({eventName, {}}));
        ASSERT_FALSE(expected.Matches({eventName, {{"a", ""}}}));
        ASSERT_FALSE(expected.Matches({eventName, {{"a", "0"}}}));
        ASSERT_FALSE(expected.Matches({eventName, {{"c", "1"}}}));
        ASSERT_TRUE(expected.Matches({eventName, {{"a", "1"}}}));
        ASSERT_TRUE(expected.Matches({eventName, {{"a", "1"}, {"b", "2"}}}));
        ASSERT_FALSE(expected.Matches({eventName, {{"a", "1"}, {"d", "4"}}}));  // The "d" should NOT appear in the event.
    }
}

TEST(Trace, TExpectedTraceEvent) {
    ASSERT_THROW(TExpectedTraceEvent::FromString(""), TTraceException);
    std::string const eventName("init");
    {
        std::string s(eventName);
        auto ev = TExpectedTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_TRUE(ev.KeyValues.empty());
    }
    {
        std::string s(eventName +  " a");
        auto ev = TExpectedTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 1u);
        ASSERT_TRUE(ev.KeyValues.at("a").empty());
    }
    {
        std::string s(eventName + " a b");
        auto ev = TExpectedTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 2u);
        ASSERT_TRUE(ev.KeyValues.at("a").empty());
        ASSERT_TRUE(ev.KeyValues.at("b").empty());
    }
    {
        std::string s(eventName + " =");
        auto ev = TExpectedTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 1u);
        ASSERT_TRUE(ev.KeyValues.at("").empty());
    }
    {
        std::string s(eventName + " a=1 b");
        auto ev = TExpectedTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 2u);
        ASSERT_EQ(ev.KeyValues.at("a"), "1");
        ASSERT_TRUE(ev.KeyValues.at("b").empty());
    }
    {
        std::string s(eventName + " a b=2");
        auto ev = TExpectedTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 2u);
        ASSERT_TRUE(ev.KeyValues.at("a").empty());
        ASSERT_EQ(ev.KeyValues.at("b"), "2");
    }
    {
        std::string s(eventName + " a=1 b=2");
        auto ev = TExpectedTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_EQ(ev.KeyValues.size(), 2u);
        ASSERT_EQ(ev.KeyValues.at("a"), "1");
        ASSERT_EQ(ev.KeyValues.at("b"), "2");
    }
    {
        std::string s(eventName + " !a");
        auto ev = TExpectedTraceEvent::FromString(s);
        ASSERT_EQ(ev.Event, eventName);
        ASSERT_TRUE(ev.KeyValues.empty());
        ASSERT_EQ(ev.DeniedKeys.size(), 1u);
        ASSERT_EQ(ev.DeniedKeys[0], "a");
    }
}

TEST(Trace, TExpectedTrace) {
    TExpectedTrace expected{"A", "B"};
    std::vector<TTraceEvent> events{{"X", {}}, {"A", {}}, {"X", {}}, {"B", {}}, {"X", {}}};
    ASSERT_TRUE(expected.Matches(events));
    expected = {"A", "B", "C"};
    ASSERT_FALSE(expected.Matches(events));
}

}
