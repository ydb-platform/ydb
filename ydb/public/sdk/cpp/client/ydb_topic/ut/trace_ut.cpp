#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/trace.h>

namespace NYdb::NTopic::NTests {

    Y_UNIT_TEST_SUITE(Trace) {

        Y_UNIT_TEST(SkipSpaces) {
            UNIT_ASSERT_STRINGS_EQUAL(SkipSpaces(""), "");
            UNIT_ASSERT_STRINGS_EQUAL(SkipSpaces("   "), "");
            UNIT_ASSERT_STRINGS_EQUAL(SkipSpaces("  a"), "a");
            UNIT_ASSERT_STRINGS_EQUAL(SkipSpaces(" a "), "a ");
        }

        Y_UNIT_TEST(NextToken) {
            UNIT_ASSERT_STRINGS_EQUAL(NextToken(""), "");
            UNIT_ASSERT_STRINGS_EQUAL(NextToken("   "), "");
            UNIT_ASSERT_STRINGS_EQUAL(NextToken("a"), "a");
            UNIT_ASSERT_STRINGS_EQUAL(NextToken("  a"), "a");
            UNIT_ASSERT_STRINGS_EQUAL(NextToken(" a "), "a");
            TStringBuf b("a=1");
            UNIT_ASSERT_STRINGS_EQUAL(NextToken(b, '='), "a");
            UNIT_ASSERT_STRINGS_EQUAL(b, "1");
        }

        Y_UNIT_TEST(TTraceEvent) {
            UNIT_ASSERT_TEST_FAILS(TTraceEvent::FromString(""));
            TString const eventName("init");
            {
                TString s(eventName);
                auto ev = TTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT(ev.KeyValues.empty());
            }
            {
                TString s(eventName +  " a");
                auto ev = TTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 1);
                UNIT_ASSERT(ev.KeyValues.at("a").empty());
            }
            {
                TString s(eventName + " a b");
                auto ev = TTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 2);
                UNIT_ASSERT(ev.KeyValues.at("a").empty());
                UNIT_ASSERT(ev.KeyValues.at("b").empty());
            }
            {
                TString s(eventName + " =");
                auto ev = TTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 1);
                UNIT_ASSERT(ev.KeyValues.at("").empty());
            }
            {
                TString s(eventName + " a=1 b");
                auto ev = TTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 2);
                UNIT_ASSERT_STRINGS_EQUAL(ev.KeyValues.at("a"), "1");
                UNIT_ASSERT(ev.KeyValues.at("b").empty());
            }
            {
                TString s(eventName + " a b=2");
                auto ev = TTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 2);
                UNIT_ASSERT(ev.KeyValues.at("a").empty());
                UNIT_ASSERT_STRINGS_EQUAL(ev.KeyValues.at("b"), "2");
            }
            {
                TString s(eventName + " a=1 b=2");
                auto ev = TTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 2);
                UNIT_ASSERT_STRINGS_EQUAL(ev.KeyValues.at("a"), "1");
                UNIT_ASSERT_STRINGS_EQUAL(ev.KeyValues.at("b"), "2");
            }
            {
                TExpectedTraceEvent expected = {eventName, {{"a", {"1"}}}, {"d"}};
                UNIT_ASSERT(!expected.Matches({"", {}}));
                UNIT_ASSERT(!expected.Matches({eventName, {}}));
                UNIT_ASSERT(!expected.Matches({eventName, {{"a", ""}}}));
                UNIT_ASSERT(!expected.Matches({eventName, {{"a", "0"}}}));
                UNIT_ASSERT(!expected.Matches({eventName, {{"c", "1"}}}));
                UNIT_ASSERT(expected.Matches({eventName, {{"a", "1"}}}));
                UNIT_ASSERT(expected.Matches({eventName, {{"a", "1"}, {"b", "2"}}}));
                UNIT_ASSERT(!expected.Matches({eventName, {{"a", "1"}, {"d", "4"}}}));  // The "d" should NOT appear in the event.
            }
        }

        Y_UNIT_TEST(TExpectedTraceEvent) {
            UNIT_ASSERT_TEST_FAILS(TExpectedTraceEvent::FromString(""));
            TString const eventName("init");
            {
                TString s(eventName);
                auto ev = TExpectedTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT(ev.KeyValues.empty());
            }
            {
                TString s(eventName +  " a");
                auto ev = TExpectedTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 1);
                UNIT_ASSERT(ev.KeyValues.at("a").empty());
            }
            {
                TString s(eventName + " a b");
                auto ev = TExpectedTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 2);
                UNIT_ASSERT(ev.KeyValues.at("a").empty());
                UNIT_ASSERT(ev.KeyValues.at("b").empty());
            }
            {
                TString s(eventName + " =");
                auto ev = TExpectedTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 1);
                UNIT_ASSERT(ev.KeyValues.at("").empty());
            }
            {
                TString s(eventName + " a=1 b");
                auto ev = TExpectedTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 2);
                UNIT_ASSERT_STRINGS_EQUAL(ev.KeyValues.at("a"), "1");
                UNIT_ASSERT(ev.KeyValues.at("b").empty());
            }
            {
                TString s(eventName + " a b=2");
                auto ev = TExpectedTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 2);
                UNIT_ASSERT(ev.KeyValues.at("a").empty());
                UNIT_ASSERT_STRINGS_EQUAL(ev.KeyValues.at("b"), "2");
            }
            {
                TString s(eventName + " a=1 b=2");
                auto ev = TExpectedTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT_EQUAL(ev.KeyValues.size(), 2);
                UNIT_ASSERT_STRINGS_EQUAL(ev.KeyValues.at("a"), "1");
                UNIT_ASSERT_STRINGS_EQUAL(ev.KeyValues.at("b"), "2");
            }
            {
                TString s(eventName + " !a");
                auto ev = TExpectedTraceEvent::FromString(s);
                UNIT_ASSERT_STRINGS_EQUAL(ev.Event, eventName);
                UNIT_ASSERT(ev.KeyValues.empty());
                UNIT_ASSERT_EQUAL(ev.DeniedKeys.size(), 1);
                UNIT_ASSERT_STRINGS_EQUAL(ev.DeniedKeys[0], "a");
            }
        }

        Y_UNIT_TEST(TExpectedTrace) {
            TExpectedTrace expected{"A", "B"};
            TVector<TTraceEvent> events{{"X", {}}, {"A", {}}, {"X", {}}, {"B", {}}, {"X", {}}};
            UNIT_ASSERT(expected.Matches(events));
            expected = {"A", "B", "C"};
            UNIT_ASSERT(!expected.Matches(events));
        }
    }
}
