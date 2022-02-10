#include "location.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TResolveRedirectTests) {
    Y_UNIT_TEST(Absolute) {
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com", "http://redir-example.com/sub"), "http://redir-example.com/sub");
    }
    Y_UNIT_TEST(AbsWithFragment) {
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com", "http://redir-example.com/sub#Hello"), "http://redir-example.com/sub#Hello");
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com/#Hello", "http://redir-example.com/sub"), "http://redir-example.com/sub#Hello");
    }
    Y_UNIT_TEST(Rel) {
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com", "/sub"), "http://example.com/sub");
    }
    Y_UNIT_TEST(RelWithFragment) {
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com", "/sub#Hello"), "http://example.com/sub#Hello");
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com/#Hello", "/sub"), "http://example.com/sub#Hello");
    }
    Y_UNIT_TEST(WrongLocation) {
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com", ""), "");
    }
    Y_UNIT_TEST(WrongBase) {
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("", "http://example.com"), "");
    }
    Y_UNIT_TEST(HashBangIsNothingSpecial) {
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com", "http://redir-example.com/sub#!Hello"), "http://redir-example.com/sub#!Hello");
        UNIT_ASSERT_EQUAL(
            NUri::ResolveRedirectLocation("http://example.com/#!Hello", "http://redir-example.com/sub"), "http://redir-example.com/sub#!Hello");
    }
}
