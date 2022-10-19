/*
 * uriparser - RFC 3986 URI parsing library
 *
 * Copyright (C) 2007, Weijia Song <songweijia@gmail.com>
 * Copyright (C) 2007, Sebastian Pipping <sebastian@pipping.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

#include <uriparser/Uri.h>
#include <uriparser/UriIp4.h>
#include <gtest/gtest.h>
#include <memory>
#include <cstdio>
#include <cstdlib>
#include <cwchar>

using namespace std;



extern "C" {
UriBool uri_TESTING_ONLY_ParseIpSixA(const char * text);
UriBool uri_TESTING_ONLY_ParseIpFourA(const char * text);
int uriCompareRangeA(const UriTextRangeA * a, const UriTextRangeA * b);
}



#define URI_TEST_IP_FOUR_FAIL(x) ASSERT_TRUE(URI_FALSE == uri_TESTING_ONLY_ParseIpFourA(x))
#define URI_TEST_IP_FOUR_PASS(x) ASSERT_TRUE(URI_TRUE == uri_TESTING_ONLY_ParseIpFourA(x))

// Note the closing brackets! TODO
#define URI_TEST_IP_SIX_FAIL(x) ASSERT_TRUE(URI_FALSE == uri_TESTING_ONLY_ParseIpSixA(x "]"))
#define URI_TEST_IP_SIX_PASS(x) ASSERT_TRUE(URI_TRUE == uri_TESTING_ONLY_ParseIpSixA(x "]"))

#define URI_EXPECT_BETWEEN(candidate, first, afterLast)  \
	EXPECT_TRUE((candidate >= first) && (candidate <= afterLast))

#define URI_EXPECT_OUTSIDE(candidate, first, afterLast)  \
	EXPECT_TRUE((candidate < first) || (candidate > afterLast))

#define URI_EXPECT_RANGE_BETWEEN(range, uriFirst, uriAfterLast)  \
	URI_EXPECT_BETWEEN(range.first, uriFirst, uriAfterLast);  \
	URI_EXPECT_BETWEEN(range.afterLast, uriFirst, uriAfterLast)

#define URI_EXPECT_RANGE_OUTSIDE(range, uriFirst, uriAfterLast)  \
	URI_EXPECT_OUTSIDE(range.first, uriFirst, uriAfterLast);  \
	URI_EXPECT_OUTSIDE(range.afterLast, uriFirst, uriAfterLast)

#define URI_EXPECT_RANGE_EMPTY(range)  \
	EXPECT_TRUE((range.first != NULL)  \
			&& (range.afterLast != NULL)  \
			&& (range.first == range.afterLast))

namespace {
	bool testDistinctionHelper(const char * uriText, bool expectedHostSet,
			bool expectedAbsPath, bool expectedEmptyTailSegment) {
		UriParserStateA state;
		UriUriA uri;
		state.uri = &uri;

		int res = uriParseUriA(&state, uriText);
		if (res != URI_SUCCESS) {
			uriFreeUriMembersA(&uri);
			return false;
		}

		if (expectedHostSet != (uri.hostText.first != NULL)) {
			uriFreeUriMembersA(&uri);
			return false;
		}

		if (expectedAbsPath != (uri.absolutePath == URI_TRUE)) {
			uriFreeUriMembersA(&uri);
			return false;
		}

		if (expectedEmptyTailSegment != ((uri.pathTail != NULL)
				&& (uri.pathTail->text.first == uri.pathTail->text.afterLast))) {
			uriFreeUriMembersA(&uri);
			return false;
		}

		uriFreeUriMembersA(&uri);
		return true;
	}
}  // namespace


TEST(UriSuite, TestDistinction) {
		/*
============================================================================
Rule                                | Example | hostSet | absPath | emptySeg
------------------------------------|---------|---------|---------|---------
1) URI = scheme ":" hier-part ...   |         |         |         |
   1) "//" authority path-abempty   | "s://"  | true    |   false |   false
                                    | "s:///" | true    |   false | true
                                    | "s://a" | true    |   false |   false
                                    | "s://a/"| true    |   false | true
   2) path-absolute                 | "s:/"   |   false | true    |   false
   3) path-rootless                 | "s:a"   |   false |   false |   false
                                    | "s:a/"  |   false |   false | true
   4) path-empty                    | "s:"    |   false |   false |   false
------------------------------------|---------|---------|---------|---------
2) relative-ref = relative-part ... |         |         |         |
   1) "//" authority path-abempty   | "//"    | true    |   false |   false
                                    | "///"   | true    |   false | true
   2) path-absolute                 | "/"     |   false | true    |   false
   3) path-noscheme                 | "a"     |   false |   false |   false
                                    | "a/"    |   false |   false | true
   4) path-empty                    | ""      |   false |   false |   false
============================================================================
		*/
		ASSERT_TRUE(testDistinctionHelper("s://", true, false, false));
		ASSERT_TRUE(testDistinctionHelper("s:///", true, false, true));
		ASSERT_TRUE(testDistinctionHelper("s://a", true, false, false));
		ASSERT_TRUE(testDistinctionHelper("s://a/", true, false, true));
		ASSERT_TRUE(testDistinctionHelper("s:/", false, true, false));
		ASSERT_TRUE(testDistinctionHelper("s:a", false, false, false));
		ASSERT_TRUE(testDistinctionHelper("s:a/", false, false, true));
		ASSERT_TRUE(testDistinctionHelper("s:", false, false, false));

		ASSERT_TRUE(testDistinctionHelper("//", true, false, false));
		ASSERT_TRUE(testDistinctionHelper("///", true, false, true));
		ASSERT_TRUE(testDistinctionHelper("/", false, true, false));
		ASSERT_TRUE(testDistinctionHelper("a", false, false, false));
		ASSERT_TRUE(testDistinctionHelper("a/", false, false, true));
		ASSERT_TRUE(testDistinctionHelper("", false, false, false));
}

TEST(UriSuite, TestIpFour) {
		URI_TEST_IP_FOUR_FAIL("01.0.0.0");
		URI_TEST_IP_FOUR_FAIL("001.0.0.0");
		URI_TEST_IP_FOUR_FAIL("00.0.0.0");
		URI_TEST_IP_FOUR_FAIL("000.0.0.0");
		URI_TEST_IP_FOUR_FAIL("256.0.0.0");
		URI_TEST_IP_FOUR_FAIL("300.0.0.0");
		URI_TEST_IP_FOUR_FAIL("1111.0.0.0");
		URI_TEST_IP_FOUR_FAIL("-1.0.0.0");
		URI_TEST_IP_FOUR_FAIL("0.0.0");
		URI_TEST_IP_FOUR_FAIL("0.0.0.");
		URI_TEST_IP_FOUR_FAIL("0.0.0.0.");
		URI_TEST_IP_FOUR_FAIL("0.0.0.0.0");
		URI_TEST_IP_FOUR_FAIL("0.0..0");
		URI_TEST_IP_FOUR_FAIL(".0.0.0");

		URI_TEST_IP_FOUR_PASS("255.0.0.0");
		URI_TEST_IP_FOUR_PASS("0.0.0.0");
		URI_TEST_IP_FOUR_PASS("1.0.0.0");
		URI_TEST_IP_FOUR_PASS("2.0.0.0");
		URI_TEST_IP_FOUR_PASS("3.0.0.0");
		URI_TEST_IP_FOUR_PASS("30.0.0.0");
}

TEST(UriSuite, TestIpSixPass) {
		// Quad length
		URI_TEST_IP_SIX_PASS("abcd::");

		URI_TEST_IP_SIX_PASS("abcd::1");
		URI_TEST_IP_SIX_PASS("abcd::12");
		URI_TEST_IP_SIX_PASS("abcd::123");
		URI_TEST_IP_SIX_PASS("abcd::1234");

		// Full length
		URI_TEST_IP_SIX_PASS("2001:0db8:0100:f101:0210:a4ff:fee3:9566"); // lower hex
		URI_TEST_IP_SIX_PASS("2001:0DB8:0100:F101:0210:A4FF:FEE3:9566"); // Upper hex
		URI_TEST_IP_SIX_PASS("2001:db8:100:f101:210:a4ff:fee3:9566");
		URI_TEST_IP_SIX_PASS("2001:0db8:100:f101:0:0:0:1");
		URI_TEST_IP_SIX_PASS("1:2:3:4:5:6:255.255.255.255");

		// Legal IPv4
		URI_TEST_IP_SIX_PASS("::1.2.3.4");
		URI_TEST_IP_SIX_PASS("3:4::5:1.2.3.4");
		URI_TEST_IP_SIX_PASS("::ffff:1.2.3.4");
		URI_TEST_IP_SIX_PASS("::0.0.0.0"); // Min IPv4
		URI_TEST_IP_SIX_PASS("::255.255.255.255"); // Max IPv4

		// Zipper position
		URI_TEST_IP_SIX_PASS("::1:2:3:4:5:6:7");
		URI_TEST_IP_SIX_PASS("1::1:2:3:4:5:6");
		URI_TEST_IP_SIX_PASS("1:2::1:2:3:4:5");
		URI_TEST_IP_SIX_PASS("1:2:3::1:2:3:4");
		URI_TEST_IP_SIX_PASS("1:2:3:4::1:2:3");
		URI_TEST_IP_SIX_PASS("1:2:3:4:5::1:2");
		URI_TEST_IP_SIX_PASS("1:2:3:4:5:6::1");
		URI_TEST_IP_SIX_PASS("1:2:3:4:5:6:7::");

		// Zipper length
		URI_TEST_IP_SIX_PASS("1:1:1::1:1:1:1");
		URI_TEST_IP_SIX_PASS("1:1:1::1:1:1");
		URI_TEST_IP_SIX_PASS("1:1:1::1:1");
		URI_TEST_IP_SIX_PASS("1:1::1:1");
		URI_TEST_IP_SIX_PASS("1:1::1");
		URI_TEST_IP_SIX_PASS("1::1");
		URI_TEST_IP_SIX_PASS("::1"); // == localhost
		URI_TEST_IP_SIX_PASS("::"); // == all addresses

		// A few more variations
		URI_TEST_IP_SIX_PASS("21ff:abcd::1");
		URI_TEST_IP_SIX_PASS("2001:db8:100:f101::1");
		URI_TEST_IP_SIX_PASS("a:b:c::12:1");
		URI_TEST_IP_SIX_PASS("a:b::0:1:2:3");

		// Issue #146: These are not leading zeros.
		URI_TEST_IP_SIX_PASS("::100.1.1.1");
		URI_TEST_IP_SIX_PASS("::1.100.1.1");
		URI_TEST_IP_SIX_PASS("::1.1.100.1");
		URI_TEST_IP_SIX_PASS("::1.1.1.100");
		URI_TEST_IP_SIX_PASS("::100.100.100.100");
		URI_TEST_IP_SIX_PASS("::10.1.1.1");
		URI_TEST_IP_SIX_PASS("::1.10.1.1");
		URI_TEST_IP_SIX_PASS("::1.1.10.1");
		URI_TEST_IP_SIX_PASS("::1.1.1.10");
		URI_TEST_IP_SIX_PASS("::10.10.10.10");
}

TEST(UriSuite, TestIpSixFail) {
		// 5 char quad
		URI_TEST_IP_SIX_FAIL("::12345");

		// Two zippers
		URI_TEST_IP_SIX_FAIL("abcd::abcd::abcd");

		// Triple-colon zipper
		URI_TEST_IP_SIX_FAIL(":::1234");
		URI_TEST_IP_SIX_FAIL("1234:::1234:1234");
		URI_TEST_IP_SIX_FAIL("1234:1234:::1234");
		URI_TEST_IP_SIX_FAIL("1234:::");

		// No quads, just IPv4
		URI_TEST_IP_SIX_FAIL("1.2.3.4");
		URI_TEST_IP_SIX_FAIL("0001.0002.0003.0004");

		// Five quads
		URI_TEST_IP_SIX_FAIL("0000:0000:0000:0000:0000:1.2.3.4");

		// Seven quads
		URI_TEST_IP_SIX_FAIL("0:0:0:0:0:0:0");
		URI_TEST_IP_SIX_FAIL("0:0:0:0:0:0:0:");
		URI_TEST_IP_SIX_FAIL("0:0:0:0:0:0:0:1.2.3.4");

		// Nine quads (or more)
		URI_TEST_IP_SIX_FAIL("1:2:3:4:5:6:7:8:9");
		URI_TEST_IP_SIX_FAIL("::2:3:4:5:6:7:8:9");
		URI_TEST_IP_SIX_FAIL("1:2:3:4::6:7:8:9");
		URI_TEST_IP_SIX_FAIL("1:2:3:4:5:6:7:8::");

		// Invalid IPv4 part
		URI_TEST_IP_SIX_FAIL("::ffff:001.02.03.004"); // Leading zeros
		URI_TEST_IP_SIX_FAIL("::ffff:1.2.3.1111"); // Four char octet
		URI_TEST_IP_SIX_FAIL("::ffff:1.2.3.256"); // > 255
		URI_TEST_IP_SIX_FAIL("::ffff:311.2.3.4"); // > 155
		URI_TEST_IP_SIX_FAIL("::ffff:1.2.3:4"); // Not a dot
		URI_TEST_IP_SIX_FAIL("::ffff:1.2.3"); // Missing octet
		URI_TEST_IP_SIX_FAIL("::ffff:1.2.3."); // Missing octet
		URI_TEST_IP_SIX_FAIL("::ffff:1.2.3a.4"); // Hex in octet
		URI_TEST_IP_SIX_FAIL("::ffff:1.2.3.4:123"); // Crap input

		// Nonhex
		URI_TEST_IP_SIX_FAIL("g:0:0:0:0:0:0");

		// Issue #146: Zipper between the 7th and 8th quads.
		URI_TEST_IP_SIX_FAIL("0:0:0:0:0:0:0::1");

		// Issue #146: Leading or trailing ":".
		URI_TEST_IP_SIX_FAIL(":1::1");
		URI_TEST_IP_SIX_FAIL("1::1:");
		URI_TEST_IP_SIX_FAIL(":1::1:");
		URI_TEST_IP_SIX_FAIL(":0:0:0:0:0:0:0:0");
		URI_TEST_IP_SIX_FAIL("0:0:0:0:0:0:0:0:");
		URI_TEST_IP_SIX_FAIL(":0:0:0:0:0:0:0:0:");

		// Issue #146: Zipper between six quads and IPv4 address.
		URI_TEST_IP_SIX_FAIL("1:1:1:1:1:1::1.1.1.1");
}

TEST(UriSuite, TestIpFuture) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		// Issue #146: The leading "v" of IPvFuture is case-insensitive.
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "//[vF.addr]"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "//[VF.addr]"));
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestIpSixOverread) {
		UriUriA uri;
		const char * errorPos;

		// NOTE: This string is designed to not have a terminator
		char uriText[2 + 3 + 2 + 1 + 1];
		memcpy(uriText, "//[::44.1", sizeof(uriText));

		EXPECT_EQ(uriParseSingleUriExA(&uri, uriText,
				uriText + sizeof(uriText), &errorPos), URI_ERROR_SYNTAX);
		EXPECT_EQ(errorPos, uriText + sizeof(uriText));
}

TEST(UriSuite, TestUri) {
		UriParserStateA stateA;
		UriParserStateW stateW;
		UriUriA uriA;
		UriUriW uriW;

		stateA.uri = &uriA;
		stateW.uri = &uriW;

		// On/off for each
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "//user:pass@[::1]:80/segment/index.html?query#frag"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://[::1]:80/segment/index.html?query#frag"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://user:pass@[::1]/segment/index.html?query#frag"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://user:pass@[::1]:80?query#frag"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://user:pass@[::1]:80/segment/index.html#frag"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://user:pass@[::1]:80/segment/index.html?query"));
		uriFreeUriMembersA(&uriA);

		// Schema, port, one segment
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "ftp://host:21/gnu/"));
		uriFreeUriMembersA(&uriA);

		// Relative
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "one/two/three"));
		ASSERT_TRUE(!uriA.absolutePath);
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "/one/two/three"));
		ASSERT_TRUE(uriA.absolutePath);
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "//user:pass@localhost/one/two/three"));
		uriFreeUriMembersA(&uriA);

		// Both narrow and wide string version
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://www.example.com/"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriW(&stateW, L"http://www.example.com/"));
		uriFreeUriMembersW(&uriW);

		// Real life examples
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://sourceforge.net/projects/uriparser/"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://sourceforge.net/project/platformdownload.php?group_id=182840"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "mailto:test@example.com"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "../../"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "/"));
		ASSERT_TRUE(uriA.absolutePath);
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, ""));
		ASSERT_TRUE(!uriA.absolutePath);
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "file:///bin/bash"));
		uriFreeUriMembersA(&uriA);

		// Percent encoding
		ASSERT_TRUE(0 == uriParseUriA(&stateA, "http://www.example.com/name%20with%20spaces/"));
		uriFreeUriMembersA(&uriA);
		ASSERT_TRUE(0 != uriParseUriA(&stateA, "http://www.example.com/name with spaces/"));
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriComponents) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0              15 01  0      7  01
		const char * const input = "http" "://" "sourceforge.net" "/" "project" "/"
		//		 0                   20 01  0              15
				"platformdownload.php" "?" "group_id=182840";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.scheme.first == input);
		ASSERT_TRUE(uriA.scheme.afterLast == input + 4);
		ASSERT_TRUE(uriA.userInfo.first == NULL);
		ASSERT_TRUE(uriA.userInfo.afterLast == NULL);
		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 15);
		ASSERT_TRUE(uriA.hostData.ipFuture.first == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.afterLast == NULL);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);

		ASSERT_TRUE(uriA.pathHead->text.first == input + 4 + 3 + 15 + 1);
		ASSERT_TRUE(uriA.pathHead->text.afterLast == input + 4 + 3 + 15 + 1 + 7);
		ASSERT_TRUE(uriA.pathHead->next->text.first == input + 4 + 3 + 15 + 1 + 7 + 1);
		ASSERT_TRUE(uriA.pathHead->next->text.afterLast == input + 4 + 3 + 15 + 1 + 7 + 1 + 20);
		ASSERT_TRUE(uriA.pathHead->next->next == NULL);
		ASSERT_TRUE(uriA.pathTail == uriA.pathHead->next);

		ASSERT_TRUE(uriA.query.first == input + 4 + 3 + 15 + 1 + 7 + 1 + 20 + 1);
		ASSERT_TRUE(uriA.query.afterLast == input + 4 + 3 + 15 + 1 + 7 + 1 + 20 + 1 + 15);
		ASSERT_TRUE(uriA.fragment.first == NULL);
		ASSERT_TRUE(uriA.fragment.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriComponentsBug20070701) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          01  01  01
		const char * const input = "a" ":" "b";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.scheme.first == input);
		ASSERT_TRUE(uriA.scheme.afterLast == input + 1);
		ASSERT_TRUE(uriA.userInfo.first == NULL);
		ASSERT_TRUE(uriA.userInfo.afterLast == NULL);
		ASSERT_TRUE(uriA.hostText.first == NULL);
		ASSERT_TRUE(uriA.hostText.afterLast == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.first == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.afterLast == NULL);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);

		ASSERT_TRUE(uriA.pathHead->text.first == input + 1 + 1);
		ASSERT_TRUE(uriA.pathHead->text.afterLast == input + 1 + 1 + 1);
		ASSERT_TRUE(uriA.pathHead->next == NULL);
		ASSERT_TRUE(uriA.pathTail == uriA.pathHead);

		ASSERT_TRUE(uriA.query.first == NULL);
		ASSERT_TRUE(uriA.query.afterLast == NULL);
		ASSERT_TRUE(uriA.fragment.first == NULL);
		ASSERT_TRUE(uriA.fragment.afterLast == NULL);

		ASSERT_TRUE(!uriA.absolutePath);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort1) {
		// User info with ":", no port
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0      7  01  0        9
		const char * const input = "http" "://" "abc:def" "@" "localhost";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.userInfo.first == input + 4 + 3);
		ASSERT_TRUE(uriA.userInfo.afterLast == input + 4 + 3 + 7);
		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3 + 7 + 1);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 7 + 1 + 9);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort2) {
		// User info with ":", with port
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0      7  01  0        9
		const char * const input = "http" "://" "abc:def" "@" "localhost"
		//		01   0  3
				":" "123";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.userInfo.first == input + 4 + 3);
		ASSERT_TRUE(uriA.userInfo.afterLast == input + 4 + 3 + 7);
		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3 + 7 + 1);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 7 + 1 + 9);
		ASSERT_TRUE(uriA.portText.first == input + 4 + 3 + 7 + 1 + 9 + 1);
		ASSERT_TRUE(uriA.portText.afterLast == input + 4 + 3 + 7 + 1 + 9 + 1 + 3);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort22Bug1948038) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		int res;

		res = uriParseUriA(&stateA, "http://user:21@host/");
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(!memcmp(uriA.userInfo.first, "user:21", 7 * sizeof(char)));
		ASSERT_TRUE(uriA.userInfo.afterLast - uriA.userInfo.first == 7);
		ASSERT_TRUE(!memcmp(uriA.hostText.first, "host", 4 * sizeof(char)));
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 4);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);

		res = uriParseUriA(&stateA, "http://user:1234@192.168.0.1:1234/foo.com");
		ASSERT_TRUE(URI_SUCCESS == res);
		uriFreeUriMembersA(&uriA);

		res = uriParseUriA(&stateA, "http://moo:21@moo:21@moo/");
		ASSERT_TRUE(URI_ERROR_SYNTAX == res);
		uriFreeUriMembersA(&uriA);

		res = uriParseUriA(&stateA, "http://moo:21@moo:21@moo:21/");
		ASSERT_TRUE(URI_ERROR_SYNTAX == res);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort23Bug3510198One) {
		// User info with ":", with port, with escaped chars in password
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		int res;
		//                           0   4  0  3  0         10 01  0   4  01
		res = uriParseUriA(&stateA, "http" "://" "user:%2F21" "@" "host" "/");
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(!memcmp(uriA.userInfo.first, "user:%2F21", 10 * sizeof(char)));
		ASSERT_TRUE(uriA.userInfo.afterLast - uriA.userInfo.first == 10);
		ASSERT_TRUE(!memcmp(uriA.hostText.first, "host", 4 * sizeof(char)));
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 4);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort23Bug3510198Two) {
		// User info with ":", with port, with escaped chars in user name and password
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		int res;
		//                           0   4  0  3  0            13 01  0   4  01
		res = uriParseUriA(&stateA, "http" "://" "%2Fuser:%2F21" "@" "host" "/");
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(!memcmp(uriA.userInfo.first, "%2Fuser:%2F21", 13 * sizeof(char)));
		ASSERT_TRUE(uriA.userInfo.afterLast - uriA.userInfo.first == 13);
		ASSERT_TRUE(!memcmp(uriA.hostText.first, "host", 4 * sizeof(char)));
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 4);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort23Bug3510198Three) {
		// User info with ":", with port, with escaped chars in password
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		int res;
		//                           0   4  0  3  0               16 01  0   4  01
		res = uriParseUriA(&stateA, "http" "://" "user:!$&'()*+,;=" "@" "host" "/");
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(!memcmp(uriA.userInfo.first, "user:!$&'()*+,;=", 16 * sizeof(char)));
		ASSERT_TRUE(uriA.userInfo.afterLast - uriA.userInfo.first == 16);
		ASSERT_TRUE(!memcmp(uriA.hostText.first, "host", 4 * sizeof(char)));
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 4);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort23Bug3510198Four) {
		// User info with ":", with port, with escaped chars in user name and password
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		int res;
		//                           0   4  0  3  0                   20 01  0   4  01
		res = uriParseUriA(&stateA, "http" "://" "!$&'()*+,;=:password" "@" "host" "/");
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(!memcmp(uriA.userInfo.first, "!$&'()*+,;=:password", 20 * sizeof(char)));
		ASSERT_TRUE(uriA.userInfo.afterLast - uriA.userInfo.first == 20);
		ASSERT_TRUE(!memcmp(uriA.hostText.first, "host", 4 * sizeof(char)));
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 4);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort23Bug3510198RelatedOne) {
		// Empty user info
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		int res;
		//                           0   4  0  3  01  0   4  01
		res = uriParseUriA(&stateA, "http" "://" "@" "host" "/");
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(uriA.userInfo.afterLast != NULL);
		ASSERT_TRUE(uriA.userInfo.first != NULL);
		ASSERT_TRUE(uriA.userInfo.afterLast - uriA.userInfo.first == 0);
		ASSERT_TRUE(!memcmp(uriA.hostText.first, "host", 4 * sizeof(char)));
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 4);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort23Bug3510198RelatedOneTwo) {
		// Empty user info
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		int res;
		//                           0   4  0  3  0      7  01
		res = uriParseUriA(&stateA, "http" "://" "%2Fhost" "/");
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(uriA.userInfo.afterLast == NULL);
		ASSERT_TRUE(uriA.userInfo.first == NULL);
		ASSERT_TRUE(!memcmp(uriA.hostText.first, "%2Fhost", 7 * sizeof(char)));
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 7);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort23Bug3510198RelatedTwo) {
		// Several colons in userinfo
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;

		int res;
		//                           0   4  0  3  0 2  01  0   4  01
		res = uriParseUriA(&stateA, "http" "://" "::" "@" "host" "/");
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(!memcmp(uriA.userInfo.first, "::", 2 * sizeof(char)));
		ASSERT_TRUE(uriA.userInfo.afterLast - uriA.userInfo.first == 2);
		ASSERT_TRUE(!memcmp(uriA.hostText.first, "host", 4 * sizeof(char)));
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 4);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort3) {
		// User info without ":", no port
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0      7  01  0        9
		const char * const input = "http" "://" "abcdefg" "@" "localhost";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.userInfo.first == input + 4 + 3);
		ASSERT_TRUE(uriA.userInfo.afterLast == input + 4 + 3 + 7);
		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3 + 7 + 1);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 7 + 1 + 9);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort4) {
		// User info without ":", with port
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0      7  01  0        9
		const char * const input = "http" "://" "abcdefg" "@" "localhost"
		//		01   0  3
				":" "123";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.userInfo.first == input + 4 + 3);
		ASSERT_TRUE(uriA.userInfo.afterLast == input + 4 + 3 + 7);
		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3 + 7 + 1);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 7 + 1 + 9);
		ASSERT_TRUE(uriA.portText.first == input + 4 + 3 + 7 + 1 + 9 + 1);
		ASSERT_TRUE(uriA.portText.afterLast == input + 4 + 3 + 7 + 1 + 9 + 1 + 3);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort5) {
		// No user info, no port
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0        9
		const char * const input = "http" "://" "localhost";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.userInfo.first == NULL);
		ASSERT_TRUE(uriA.userInfo.afterLast == NULL);
		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 9);
		ASSERT_TRUE(uriA.portText.first == NULL);
		ASSERT_TRUE(uriA.portText.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriUserInfoHostPort6) {
		// No user info, with port
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0        9  01  0  3
		const char * const input = "http" "://" "localhost" ":" "123";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.userInfo.first == NULL);
		ASSERT_TRUE(uriA.userInfo.afterLast == NULL);
		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 9);
		ASSERT_TRUE(uriA.portText.first == input + 4 + 3 + 9 + 1);
		ASSERT_TRUE(uriA.portText.afterLast == input + 4 + 3 + 9 + 1 + 3);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriHostRegname) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0          11
		const char * const input = "http" "://" "example.com";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 11);
		ASSERT_TRUE(uriA.hostData.ip4 == NULL);
		ASSERT_TRUE(uriA.hostData.ip6 == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.first == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriHostIpFour1) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0      7  01  0 2
		const char * const input = "http" "://" "1.2.3.4" ":" "80";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 7);
		ASSERT_TRUE(uriA.hostData.ip4 != NULL);
		ASSERT_TRUE(uriA.hostData.ip6 == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.first == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriHostIpFour2) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  0      7
		const char * const input = "http" "://" "1.2.3.4";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 7);
		ASSERT_TRUE(uriA.hostData.ip4 != NULL);
		ASSERT_TRUE(uriA.hostData.ip6 == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.first == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriHostIpSix1) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  01  45  01  0 2
		const char * const input = "http" "://" "[::1]" ":" "80";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3 + 1);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 4);
		ASSERT_TRUE(uriA.hostData.ip4 == NULL);
		ASSERT_TRUE(uriA.hostData.ip6 != NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.first == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriHostIpSix2) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  01  45
		const char * const input = "http" "://" "[::1]";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.hostText.first == input + 4 + 3 + 1);
		ASSERT_TRUE(uriA.hostText.afterLast == input + 4 + 3 + 4);
		ASSERT_TRUE(uriA.hostData.ip4 == NULL);
		ASSERT_TRUE(uriA.hostData.ip6 != NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.first == NULL);
		ASSERT_TRUE(uriA.hostData.ipFuture.afterLast == NULL);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriHostEmpty) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0   4  0  3  01  0  3
		const char * const input = "http" "://" ":" "123";
		const int res = uriParseUriA(&stateA, input);
		ASSERT_TRUE(URI_SUCCESS == res);
		ASSERT_TRUE(uriA.userInfo.first == NULL);
		ASSERT_TRUE(uriA.userInfo.afterLast == NULL);
		ASSERT_TRUE(uriA.hostText.first != NULL);
		ASSERT_TRUE(uriA.hostText.afterLast != NULL);
		ASSERT_TRUE(uriA.hostText.afterLast - uriA.hostText.first == 0);
		ASSERT_TRUE(uriA.portText.first == input + 4 + 3 + 1);
		ASSERT_TRUE(uriA.portText.afterLast == input + 4 + 3 + 1 + 3);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestUriHostIpFuture) {
		// TODO
}

namespace {
	bool testEscapingHelper(const wchar_t * in, const wchar_t * expectedOut,
			bool spaceToPlus = false, bool normalizeBreaks = false) {
		wchar_t * const buffer = new wchar_t[(normalizeBreaks ? 6 : 3)
				* wcslen(in) + 1];
		if (uriEscapeW(in, buffer, spaceToPlus, normalizeBreaks)
			!= buffer + wcslen(expectedOut)) {
			delete [] buffer;
			return false;
		}

		const bool equal = !wcscmp(buffer, expectedOut);
		delete [] buffer;
		return equal;
	}
}  // namespace

TEST(UriSuite, TestEscaping) {
		const bool SPACE_TO_PLUS = true;
		const bool SPACE_TO_PERCENT = false;
		const bool KEEP_UNMODIFIED = false;
		const bool NORMALIZE = true;

		// '+' to ' '
		ASSERT_TRUE(testEscapingHelper(L"abc def", L"abc+def", SPACE_TO_PLUS));
		ASSERT_TRUE(testEscapingHelper(L"abc def", L"abc%20def", SPACE_TO_PERCENT));

		// Percent encoding
		ASSERT_TRUE(testEscapingHelper(L"\x00", L"\0"));
		ASSERT_TRUE(testEscapingHelper(L"\x01", L"%01"));
		ASSERT_TRUE(testEscapingHelper(L"\xff", L"%FF"));

		// Linebreak normalization
		ASSERT_TRUE(testEscapingHelper(L"\x0d", L"%0D%0A", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"g\x0d", L"g%0D%0A", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"\x0dg", L"%0D%0Ag", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"\x0d", L"%0D", SPACE_TO_PLUS, KEEP_UNMODIFIED));
		ASSERT_TRUE(testEscapingHelper(L"g\x0d", L"g%0D", SPACE_TO_PLUS, KEEP_UNMODIFIED));
		ASSERT_TRUE(testEscapingHelper(L"\x0dg", L"%0Dg", SPACE_TO_PLUS, KEEP_UNMODIFIED));

		ASSERT_TRUE(testEscapingHelper(L"\x0a", L"%0D%0A", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"g\x0a", L"g%0D%0A", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"\x0ag", L"%0D%0Ag", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"\x0a", L"%0A", SPACE_TO_PLUS, KEEP_UNMODIFIED));
		ASSERT_TRUE(testEscapingHelper(L"g\x0a", L"g%0A", SPACE_TO_PLUS, KEEP_UNMODIFIED));
		ASSERT_TRUE(testEscapingHelper(L"\x0ag", L"%0Ag", SPACE_TO_PLUS, KEEP_UNMODIFIED));

		ASSERT_TRUE(testEscapingHelper(L"\x0d\x0a", L"%0D%0A", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"g\x0d\x0a", L"g%0D%0A", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"\x0d\x0ag", L"%0D%0Ag", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"\x0d\x0a", L"%0D%0A", SPACE_TO_PLUS, KEEP_UNMODIFIED));
		ASSERT_TRUE(testEscapingHelper(L"g\x0d\x0a", L"g%0D%0A", SPACE_TO_PLUS, KEEP_UNMODIFIED));
		ASSERT_TRUE(testEscapingHelper(L"\x0d\x0ag", L"%0D%0Ag", SPACE_TO_PLUS, KEEP_UNMODIFIED));

		ASSERT_TRUE(testEscapingHelper(L"\x0a\x0d", L"%0D%0A%0D%0A", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"g\x0a\x0d", L"g%0D%0A%0D%0A", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"\x0a\x0dg", L"%0D%0A%0D%0Ag", SPACE_TO_PLUS, NORMALIZE));
		ASSERT_TRUE(testEscapingHelper(L"\x0a\x0d", L"%0A%0D", SPACE_TO_PLUS, KEEP_UNMODIFIED));
		ASSERT_TRUE(testEscapingHelper(L"g\x0a\x0d", L"g%0A%0D", SPACE_TO_PLUS, KEEP_UNMODIFIED));
		ASSERT_TRUE(testEscapingHelper(L"\x0a\x0dg", L"%0A%0Dg", SPACE_TO_PLUS, KEEP_UNMODIFIED));
}

namespace {
	bool testUnescapingHelper(const wchar_t * input, const wchar_t * output,
			bool plusToSpace = false, UriBreakConversion breakConversion = URI_BR_DONT_TOUCH) {
		wchar_t * working = new wchar_t[URI_STRLEN(input) + 1];
		wcscpy(working, input);
		const wchar_t * newTermZero = uriUnescapeInPlaceExW(working,
				plusToSpace ? URI_TRUE : URI_FALSE, breakConversion);
		const bool success = ((newTermZero == working + wcslen(output))
				&& !wcscmp(working, output));
		delete[] working;
		return success;
	}
}  // namespace

TEST(UriSuite, TestUnescaping) {
		const bool PLUS_TO_SPACE = true;
		const bool PLUS_DONT_TOUCH = false;


		// Proper
		ASSERT_TRUE(testUnescapingHelper(L"abc%20%41BC", L"abc ABC"));
		ASSERT_TRUE(testUnescapingHelper(L"%20", L" "));

		// Incomplete
		ASSERT_TRUE(testUnescapingHelper(L"%0", L"%0"));

		// Nonhex
		ASSERT_TRUE(testUnescapingHelper(L"%0g", L"%0g"));
		ASSERT_TRUE(testUnescapingHelper(L"%G0", L"%G0"));

		// No double decoding
		ASSERT_TRUE(testUnescapingHelper(L"%2520", L"%20"));

		// Decoding of '+'
		ASSERT_TRUE(testUnescapingHelper(L"abc+def", L"abc+def", PLUS_DONT_TOUCH));
		ASSERT_TRUE(testUnescapingHelper(L"abc+def", L"abc def", PLUS_TO_SPACE));

		// Line break conversion
		ASSERT_TRUE(testUnescapingHelper(L"%0d", L"\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0d", L"\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0d", L"\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0d", L"\x0d", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));

		ASSERT_TRUE(testUnescapingHelper(L"%0d%0d", L"\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0d", L"\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0d", L"\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0d", L"\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));


		ASSERT_TRUE(testUnescapingHelper(L"%0a", L"\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0a", L"\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0a", L"\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0a", L"\x0a", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));

		ASSERT_TRUE(testUnescapingHelper(L"%0a%0a", L"\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0a", L"\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0a", L"\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0a", L"\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));


		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a", L"\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a", L"\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a", L"\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a", L"\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));

		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0a", L"\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0a", L"\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0a", L"\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0a", L"\x0d\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));

		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0d", L"\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0d", L"\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0d", L"\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0d", L"\x0d\x0a\x0d", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));

		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0d%0a", L"\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0d%0a", L"\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0d%0a", L"\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0d%0a%0d%0a", L"\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));


		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d", L"\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d", L"\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d", L"\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d", L"\x0a\x0d", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));

		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0a", L"\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0a", L"\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0a", L"\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0a", L"\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));

		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0d", L"\x0a\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0d", L"\x0d\x0a\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0d", L"\x0d\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0d", L"\x0a\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));

		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0a%0d", L"\x0a\x0a\x0a", PLUS_DONT_TOUCH, URI_BR_TO_UNIX));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0a%0d", L"\x0d\x0a\x0d\x0a\x0d\x0a", PLUS_DONT_TOUCH, URI_BR_TO_WINDOWS));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0a%0d", L"\x0d\x0d\x0d", PLUS_DONT_TOUCH, URI_BR_TO_MAC));
		ASSERT_TRUE(testUnescapingHelper(L"%0a%0d%0a%0d", L"\x0a\x0d\x0a\x0d", PLUS_DONT_TOUCH, URI_BR_DONT_TOUCH));
}

namespace {
	bool testAddBaseHelper(const wchar_t * base, const wchar_t * rel, const wchar_t * expectedResult, bool backward_compatibility = false) {
		UriParserStateW stateW;

		// Base
		UriUriW baseUri;
		stateW.uri = &baseUri;
		int res = uriParseUriW(&stateW, base);
		if (res != 0) {
			uriFreeUriMembersW(&baseUri);
			return false;
		}

		// Rel
		UriUriW relUri;
		stateW.uri = &relUri;
		res = uriParseUriW(&stateW, rel);
		if (res != 0) {
			uriFreeUriMembersW(&baseUri);
			uriFreeUriMembersW(&relUri);
			return false;
		}

		// Expected result
		UriUriW expectedUri;
		stateW.uri = &expectedUri;
		res = uriParseUriW(&stateW, expectedResult);
		if (res != 0) {
			uriFreeUriMembersW(&baseUri);
			uriFreeUriMembersW(&relUri);
			uriFreeUriMembersW(&expectedUri);
			return false;
		}

		// Transform
		UriUriW transformedUri;
		if (backward_compatibility) {
			res = uriAddBaseUriExW(&transformedUri, &relUri, &baseUri, URI_RESOLVE_IDENTICAL_SCHEME_COMPAT);
		} else {
			res = uriAddBaseUriW(&transformedUri, &relUri, &baseUri);
		}

		if (res != 0) {
			uriFreeUriMembersW(&baseUri);
			uriFreeUriMembersW(&relUri);
			uriFreeUriMembersW(&expectedUri);
			uriFreeUriMembersW(&transformedUri);
			return false;
		}

		const bool equal = (URI_TRUE == uriEqualsUriW(&transformedUri, &expectedUri));
		if (!equal) {
			wchar_t transformedUriText[1024 * 8];
			wchar_t expectedUriText[1024 * 8];
			uriToStringW(transformedUriText, &transformedUri, 1024 * 8, NULL);
			uriToStringW(expectedUriText, &expectedUri, 1024 * 8, NULL);
#ifdef HAVE_WPRINTF
			wprintf(L"\n\n\nExpected: \"%s\"\nReceived: \"%s\"\n\n\n", expectedUriText, transformedUriText);
#endif
		}

		uriFreeUriMembersW(&baseUri);
		uriFreeUriMembersW(&relUri);
		uriFreeUriMembersW(&expectedUri);
		uriFreeUriMembersW(&transformedUri);
		return equal;
	}
}  // namespace

TEST(UriSuite, TestTrailingSlash) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		//                          0  3  01
		const char * const input = "abc" "/";
		ASSERT_TRUE(0 == uriParseUriA(&stateA, input));

		ASSERT_TRUE(uriA.pathHead->text.first == input);
		ASSERT_TRUE(uriA.pathHead->text.afterLast == input + 3);
		ASSERT_TRUE(uriA.pathHead->next->text.first == uriA.pathHead->next->text.afterLast);
		ASSERT_TRUE(uriA.pathHead->next->next == NULL);
		ASSERT_TRUE(uriA.pathTail == uriA.pathHead->next);
		uriFreeUriMembersA(&uriA);
}

TEST(UriSuite, TestAddBase) {
		// 5.4.1. Normal Examples
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g:h", L"g:h"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g", L"http://a/b/c/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"./g", L"http://a/b/c/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g/", L"http://a/b/c/g/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"/g", L"http://a/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"//g", L"http://g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"?y", L"http://a/b/c/d;p?y"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g?y", L"http://a/b/c/g?y"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"#s", L"http://a/b/c/d;p?q#s"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g#s", L"http://a/b/c/g#s"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g?y#s", L"http://a/b/c/g?y#s"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L";x", L"http://a/b/c/;x"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g;x", L"http://a/b/c/g;x"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g;x?y#s", L"http://a/b/c/g;x?y#s"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"", L"http://a/b/c/d;p?q"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L".", L"http://a/b/c/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"./", L"http://a/b/c/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"..", L"http://a/b/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../", L"http://a/b/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../g", L"http://a/b/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../..", L"http://a/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../../", L"http://a/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../../g", L"http://a/g"));

		// 5.4.2. Abnormal Examples
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../../../g", L"http://a/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../../../../g", L"http://a/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"/./g", L"http://a/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"/../g", L"http://a/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g.", L"http://a/b/c/g."));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L".g", L"http://a/b/c/.g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g..", L"http://a/b/c/g.."));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"..g", L"http://a/b/c/..g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"./../g", L"http://a/b/g"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"./g/.", L"http://a/b/c/g/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g/./h", L"http://a/b/c/g/h"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g/../h", L"http://a/b/c/h"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g;x=1/./y", L"http://a/b/c/g;x=1/y"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g;x=1/../y", L"http://a/b/c/y"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g?y/./x", L"http://a/b/c/g?y/./x"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g?y/../x", L"http://a/b/c/g?y/../x"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g#s/./x", L"http://a/b/c/g#s/./x"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"g#s/../x", L"http://a/b/c/g#s/../x"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"http:g", L"http:g"));

		// Backward compatibility (feature request #4, RFC3986 5.4.2)
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"http:g", L"http:g", false));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"http:g", L"http://a/b/c/g", true));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"http:g?q#f", L"http://a/b/c/g?q#f", true));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"other:g?q#f", L"other:g?q#f", true));

		// Bug related to absolutePath flag set despite presence of host
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"/", L"http://a/"));
		ASSERT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"/g/", L"http://a/g/"));

		// GitHub issue #92
		EXPECT_TRUE(testAddBaseHelper(L"http://a/b/c/../d;p?q", L"../..", L"http://a/"));
		EXPECT_TRUE(testAddBaseHelper(L"http://a/b/c/../d;p?q", L"../../", L"http://a/"));

		EXPECT_TRUE(testAddBaseHelper(L"http://a/b/../c/d;p?q", L"../..", L"http://a/"));
		EXPECT_TRUE(testAddBaseHelper(L"http://a/b/../c/d;p?q", L"../../", L"http://a/"));

		EXPECT_TRUE(testAddBaseHelper(L"http://a/../b/c/d;p?q", L"../..", L"http://a/"));
		EXPECT_TRUE(testAddBaseHelper(L"http://a/../b/c/d;p?q", L"../../", L"http://a/"));

		EXPECT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../../..", L"http://a/"));
		EXPECT_TRUE(testAddBaseHelper(L"http://a/b/c/d;p?q", L"../../../", L"http://a/"));
}

namespace {
	bool testToStringHelper(const wchar_t * text) {
		// Parse
		UriParserStateW state;
		UriUriW uri;
		state.uri = &uri;
		int res = uriParseUriW(&state, text);
		if (res != 0) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		// Back to string, _huge_ limit
		wchar_t shouldbeTheSame[1024 * 8];
		res = uriToStringW(shouldbeTheSame, &uri, 1024 * 8, NULL);
		if (res != 0) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		// Compare
		bool equals = (0 == wcscmp(shouldbeTheSame, text));
		if (!equals) {
#ifdef HAVE_WPRINTF
			wprintf(L"\n\n\nExpected: \"%s\"\nReceived: \"%s\"\n\n\n", text, shouldbeTheSame);
#endif
		}

		// Back to string, _exact_ limit
		const int len = static_cast<int>(wcslen(text));
		int charsWritten;
		res = uriToStringW(shouldbeTheSame, &uri, len + 1, &charsWritten);
		if ((res != 0) || (charsWritten != len + 1)) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		// Back to string, _too small_ limit
		res = uriToStringW(shouldbeTheSame, &uri, len, &charsWritten);
		if ((res == 0) || (charsWritten >= len + 1)) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		uriFreeUriMembersW(&uri);
		return equals;
	}
}  // namespace

TEST(UriSuite, TestToString) {
		// Scheme
		ASSERT_TRUE(testToStringHelper(L"ftp://localhost/"));
		// UserInfo
		ASSERT_TRUE(testToStringHelper(L"http://user:pass@localhost/"));
		// IPv4
		ASSERT_TRUE(testToStringHelper(L"http://123.0.1.255/"));
		// IPv6
		ASSERT_TRUE(testToStringHelper(L"http://[abcd:abcd:abcd:abcd:abcd:abcd:abcd:abcd]/"));
		// IPvFuture
		ASSERT_TRUE(testToStringHelper(L"http://[vA.123456]/"));
		// Port
		ASSERT_TRUE(testToStringHelper(L"http://example.com:123/"));
		// Path
		ASSERT_TRUE(testToStringHelper(L"http://example.com"));
		ASSERT_TRUE(testToStringHelper(L"http://example.com/"));
		ASSERT_TRUE(testToStringHelper(L"http://example.com/abc/"));
		ASSERT_TRUE(testToStringHelper(L"http://example.com/abc/def"));
		ASSERT_TRUE(testToStringHelper(L"http://example.com/abc/def/"));
		ASSERT_TRUE(testToStringHelper(L"http://example.com//"));
		ASSERT_TRUE(testToStringHelper(L"http://example.com/./.."));
		// Query
		ASSERT_TRUE(testToStringHelper(L"http://example.com/?abc"));
		// Fragment
		ASSERT_TRUE(testToStringHelper(L"http://example.com/#abc"));
		ASSERT_TRUE(testToStringHelper(L"http://example.com/?def#abc"));

		// Relative
		ASSERT_TRUE(testToStringHelper(L"a"));
		ASSERT_TRUE(testToStringHelper(L"a/"));
		ASSERT_TRUE(testToStringHelper(L"/a"));
		ASSERT_TRUE(testToStringHelper(L"/a/"));
		ASSERT_TRUE(testToStringHelper(L"abc"));
		ASSERT_TRUE(testToStringHelper(L"abc/"));
		ASSERT_TRUE(testToStringHelper(L"/abc"));
		ASSERT_TRUE(testToStringHelper(L"/abc/"));
		ASSERT_TRUE(testToStringHelper(L"a/def"));
		ASSERT_TRUE(testToStringHelper(L"a/def/"));
		ASSERT_TRUE(testToStringHelper(L"/a/def"));
		ASSERT_TRUE(testToStringHelper(L"/a/def/"));
		ASSERT_TRUE(testToStringHelper(L"abc/def"));
		ASSERT_TRUE(testToStringHelper(L"abc/def/"));
		ASSERT_TRUE(testToStringHelper(L"/abc/def"));
		ASSERT_TRUE(testToStringHelper(L"/abc/def/"));
		ASSERT_TRUE(testToStringHelper(L"/"));
		ASSERT_TRUE(testToStringHelper(L"//a/"));
		ASSERT_TRUE(testToStringHelper(L"."));
		ASSERT_TRUE(testToStringHelper(L"./"));
		ASSERT_TRUE(testToStringHelper(L"/."));
		ASSERT_TRUE(testToStringHelper(L"/./"));
		ASSERT_TRUE(testToStringHelper(L""));
		ASSERT_TRUE(testToStringHelper(L"./abc/def"));
		ASSERT_TRUE(testToStringHelper(L"?query"));
		ASSERT_TRUE(testToStringHelper(L"#fragment"));
		ASSERT_TRUE(testToStringHelper(L"?query#fragment"));

		// Tests for bugs from the past
		ASSERT_TRUE(testToStringHelper(L"f:/.//g"));
}

TEST(UriSuite, TestToStringBug1950126) {
		UriParserStateW state;
		UriUriW uriOne;
		UriUriW uriTwo;
		const wchar_t * const uriOneString = L"http://e.com/";
		const wchar_t * const uriTwoString = L"http://e.com";
		state.uri = &uriOne;
		ASSERT_TRUE(URI_SUCCESS == uriParseUriW(&state, uriOneString));
		state.uri = &uriTwo;
		ASSERT_TRUE(URI_SUCCESS == uriParseUriW(&state, uriTwoString));
		ASSERT_TRUE(URI_FALSE == uriEqualsUriW(&uriOne, &uriTwo));
		uriFreeUriMembersW(&uriOne);
		uriFreeUriMembersW(&uriTwo);

		ASSERT_TRUE(testToStringHelper(uriOneString));
		ASSERT_TRUE(testToStringHelper(uriTwoString));
}

namespace {
	bool testToStringCharsRequiredHelper(const wchar_t * text) {
		// Parse
		UriParserStateW state;
		UriUriW uri;
		state.uri = &uri;
		int res = uriParseUriW(&state, text);
		if (res != 0) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		// Required space?
		int charsRequired;
		if (uriToStringCharsRequiredW(&uri, &charsRequired) != 0) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		EXPECT_EQ(charsRequired, wcslen(text));

		// Minimum
		wchar_t * buffer = new wchar_t[charsRequired + 1];
		if (uriToStringW(buffer, &uri, charsRequired + 1, NULL) != 0) {
			uriFreeUriMembersW(&uri);
			delete [] buffer;
			return false;
		}

		// One less than minimum
		if (uriToStringW(buffer, &uri, charsRequired, NULL) == 0) {
			uriFreeUriMembersW(&uri);
			delete [] buffer;
			return false;
		}

		uriFreeUriMembersW(&uri);
		delete [] buffer;
		return true;
	}
}  // namespace

TEST(UriSuite, TestToStringCharsRequired) {
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://1.1.1.1/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://12.1.1.1/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://123.1.1.1/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://1.12.1.1/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://1.123.1.1/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://1.1.12.1/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://1.1.123.1/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://1.1.1.12/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://1.1.1.123/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://www.example.com/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://www.example.com:80/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://user:pass@www.example.com/"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://www.example.com/index.html"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://www.example.com/?abc"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://www.example.com/#def"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"http://www.example.com/?abc#def"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"/test"));
		EXPECT_TRUE(testToStringCharsRequiredHelper(L"test"));
}

namespace {
	bool testNormalizeMaskHelper(const wchar_t * uriText, unsigned int expectedMask) {
		UriParserStateW state;
		UriUriW uri;
		state.uri = &uri;
		int res = uriParseUriW(&state, uriText);
		if (res != 0) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		const unsigned int maskBefore = uriNormalizeSyntaxMaskRequiredW(&uri);
		if (maskBefore != expectedMask) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		res = uriNormalizeSyntaxW(&uri);
		if (res != 0) {
			uriFreeUriMembersW(&uri);
			return false;
		}

		const unsigned int maskAfter = uriNormalizeSyntaxMaskRequiredW(&uri);
		uriFreeUriMembersW(&uri);

		// Second call should be no problem
		uriFreeUriMembersW(&uri);

		return (maskAfter == URI_NORMALIZED);
	}
}  // namespace

TEST(UriSuite, TestNormalizeSyntaxMaskRequired) {
		ASSERT_TRUE(testNormalizeMaskHelper(L"http://localhost/", URI_NORMALIZED));
		ASSERT_TRUE(testNormalizeMaskHelper(L"httP://localhost/", URI_NORMALIZE_SCHEME));
		ASSERT_TRUE(testNormalizeMaskHelper(L"http://%0d@localhost/", URI_NORMALIZE_USER_INFO));
		ASSERT_TRUE(testNormalizeMaskHelper(L"http://localhosT/", URI_NORMALIZE_HOST));
		ASSERT_TRUE(testNormalizeMaskHelper(L"http://localhost/./abc", URI_NORMALIZE_PATH));
		ASSERT_TRUE(testNormalizeMaskHelper(L"http://localhost/?AB%43", URI_NORMALIZE_QUERY));
		ASSERT_TRUE(testNormalizeMaskHelper(L"http://localhost/#AB%43", URI_NORMALIZE_FRAGMENT));
}

namespace {
	bool testNormalizeSyntaxHelper(const wchar_t * uriText, const wchar_t * expectedNormalized,
			unsigned int mask = static_cast<unsigned int>(-1)) {
		UriParserStateW stateW;
		int res;

		UriUriW testUri;
		stateW.uri = &testUri;
		res = uriParseUriW(&stateW, uriText);
		if (res != 0) {
			uriFreeUriMembersW(&testUri);
			return false;
		}

		// Expected result
		UriUriW expectedUri;
		stateW.uri = &expectedUri;
		res = uriParseUriW(&stateW, expectedNormalized);
		if (res != 0) {
			uriFreeUriMembersW(&testUri);
			uriFreeUriMembersW(&expectedUri);
			return false;
		}

		// First run
		res = uriNormalizeSyntaxExW(&testUri, mask);
		if (res != 0) {
			uriFreeUriMembersW(&testUri);
			uriFreeUriMembersW(&expectedUri);
			return false;
		}

		bool equalAfter = (URI_TRUE == uriEqualsUriW(&testUri, &expectedUri));

		// Second run
		res = uriNormalizeSyntaxExW(&testUri, mask);
		if (res != 0) {
			uriFreeUriMembersW(&testUri);
			uriFreeUriMembersW(&expectedUri);
			return false;
		}

		equalAfter = equalAfter
				&& (URI_TRUE == uriEqualsUriW(&testUri, &expectedUri));

		uriFreeUriMembersW(&testUri);
		uriFreeUriMembersW(&expectedUri);
		return equalAfter;
	}
}  // namespace

TEST(UriSuite, TestNormalizeSyntax) {
		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"eXAMPLE://a/./b/../b/%63/%7bfoo%7d",
				L"example://a/b/c/%7Bfoo%7D"));

		// Testcase by Adrian Manrique
		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"http://examp%4Ce.com/",
				L"http://example.com/"));

		// Testcase by Adrian Manrique
		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"http://example.com/a/b/%2E%2E/",
				L"http://example.com/a/"));

		// Reported by Adrian Manrique
		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"http://user:pass@SOMEHOST.COM:123",
				L"http://user:pass@somehost.com:123"));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"HTTP://a:b@HOST:123/./1/2/../%41?abc#def",
				L"http://a:b@host:123/1/A?abc#def"));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"../../abc",
				L"../../abc"));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"../../abc/..",
				L"../../"));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"../../abc/../def",
				L"../../def"));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"abc/..",
				L""));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"abc/../",
				L""));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"../../abc/./def",
				L"../../abc/def"));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"./def",
				L"def"));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"def/.",
				L"def/"));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"./abc:def",
				L"./abc:def"));
}

TEST(UriSuite, TestNormalizeSyntaxComponents) {
		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"HTTP://%41@EXAMPLE.ORG/../a?%41#%41",
				L"http://%41@EXAMPLE.ORG/../a?%41#%41",
				URI_NORMALIZE_SCHEME));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"HTTP://%41@EXAMPLE.ORG/../a?%41#%41",
				L"HTTP://A@EXAMPLE.ORG/../a?%41#%41",
				URI_NORMALIZE_USER_INFO));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"HTTP://%41@EXAMPLE.ORG/../a?%41#%41",
				L"HTTP://%41@example.org/../a?%41#%41",
				URI_NORMALIZE_HOST));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"HTTP://%41@EXAMPLE.ORG/../a?%41#%41",
				L"HTTP://%41@EXAMPLE.ORG/a?%41#%41",
				URI_NORMALIZE_PATH));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"HTTP://%41@EXAMPLE.ORG/../a?%41#%41",
				L"HTTP://%41@EXAMPLE.ORG/../a?A#%41",
				URI_NORMALIZE_QUERY));

		ASSERT_TRUE(testNormalizeSyntaxHelper(
				L"HTTP://%41@EXAMPLE.ORG/../a?%41#%41",
				L"HTTP://%41@EXAMPLE.ORG/../a?%41#A",
				URI_NORMALIZE_FRAGMENT));
}

TEST(UriSuite, TestNormalizeSyntaxPath) {
	// These are from GitHub issue #92
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"http://a/b/c/../../..",
			L"http://a/",
			URI_NORMALIZE_PATH));
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"http://a/b/../c/../..",
			L"http://a/",
			URI_NORMALIZE_PATH));
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"http://a/b/c/../../..",
			L"http://a/",
			URI_NORMALIZE_PATH));

	// .. and these are related
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"http://a/..",
			L"http://a/",
			URI_NORMALIZE_PATH));
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"/..",
			L"/",
			URI_NORMALIZE_PATH));
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"http://a/..///",
			L"http://a///",
			URI_NORMALIZE_PATH));
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"http://a/..///..",
			L"http://a//",
			URI_NORMALIZE_PATH));
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"a/b/c/../../..",
			L"",
			URI_NORMALIZE_PATH));
	EXPECT_TRUE(testNormalizeSyntaxHelper(
			L"a/b/../../c/..",
			L"",
			URI_NORMALIZE_PATH));
}

TEST(UriSuite, TestNormalizeCrashBug20080224) {
		UriParserStateW stateW;
		int res;
		UriUriW testUri;
		stateW.uri = &testUri;

		res = uriParseUriW(&stateW, L"http://example.org/abc//../def");
		ASSERT_TRUE(res == 0);

		// First call will make us owner of copied memory
		res = uriNormalizeSyntaxExW(&testUri, URI_NORMALIZE_SCHEME);
		ASSERT_TRUE(res == 0);
		res = uriNormalizeSyntaxExW(&testUri, URI_NORMALIZE_HOST);
		ASSERT_TRUE(res == 0);

		// Frees empty path segment -> crash
		res = uriNormalizeSyntaxW(&testUri);
		ASSERT_TRUE(res == 0);

		uriFreeUriMembersW(&testUri);
}

namespace {
	void testFilenameUriConversionHelper(const wchar_t * filename,
			const wchar_t * uriString, bool forUnix,
			const wchar_t * expectedUriString = NULL) {
		const int prefixLen = forUnix ? 7 : 8;
		if (! expectedUriString) {
			expectedUriString = uriString;
		}

		// Filename to URI string
		const size_t uriBufferLen = prefixLen + 3 * wcslen(filename) + 1;
		wchar_t * uriBuffer = new wchar_t[uriBufferLen];
		if (forUnix) {
			uriUnixFilenameToUriStringW(filename, uriBuffer);
		} else {
			uriWindowsFilenameToUriStringW(filename, uriBuffer);
		}
#ifdef HAVE_WPRINTF
		// wprintf(L"1 [%s][%s]\n", uriBuffer, expectedUriString);
#endif
		ASSERT_TRUE(!wcscmp(uriBuffer, expectedUriString));
		delete [] uriBuffer;

		// URI string to filename
		const size_t filenameBufferLen = wcslen(uriString) + 1;
		wchar_t * filenameBuffer = new wchar_t[filenameBufferLen];
		if (forUnix) {
			uriUriStringToUnixFilenameW(uriString, filenameBuffer);
		} else {
			uriUriStringToWindowsFilenameW(uriString, filenameBuffer);
		}
#ifdef HAVE_WPRINTF
		// wprintf(L"2 [%s][%s]\n", filenameBuffer, filename);
#endif
		ASSERT_TRUE(!wcscmp(filenameBuffer, filename));
		delete [] filenameBuffer;
	}
}  // namespace

TEST(UriSuite, TestFilenameUriConversion) {
		const bool FOR_UNIX = true;
		const bool FOR_WINDOWS = false;
		testFilenameUriConversionHelper(L"/bin/bash", L"file:///bin/bash", FOR_UNIX);
		testFilenameUriConversionHelper(L"/bin/bash", L"file:/bin/bash", FOR_UNIX, L"file:///bin/bash");
		testFilenameUriConversionHelper(L"./configure", L"./configure", FOR_UNIX);

		testFilenameUriConversionHelper(L"E:\\Documents and Settings", L"file:///E:/Documents%20and%20Settings", FOR_WINDOWS);
		testFilenameUriConversionHelper(L"c:\\path\\to\\file.txt", L"file:c:/path/to/file.txt", FOR_WINDOWS, L"file:///c:/path/to/file.txt");

		testFilenameUriConversionHelper(L".\\Readme.txt", L"./Readme.txt", FOR_WINDOWS);

		testFilenameUriConversionHelper(L"index.htm", L"index.htm", FOR_WINDOWS);
		testFilenameUriConversionHelper(L"index.htm", L"index.htm", FOR_UNIX);

		testFilenameUriConversionHelper(L"abc def", L"abc%20def", FOR_WINDOWS);
		testFilenameUriConversionHelper(L"abc def", L"abc%20def", FOR_UNIX);

		testFilenameUriConversionHelper(L"\\\\Server01\\user\\docs\\Letter.txt", L"file://Server01/user/docs/Letter.txt", FOR_WINDOWS);
}

TEST(UriSuite, TestCrashFreeUriMembersBug20080116) {
		// Testcase by Adrian Manrique
		UriParserStateA state;
		UriUriA uri;
		state.uri = &uri;
		uriParseUriA(&state, "http://test/?");
		uriNormalizeSyntaxA(&uri);
		uriFreeUriMembersA(&uri);

		ASSERT_TRUE(true);
}

namespace {
	void helperTestQueryString(char const * uriString, int pairsExpected);
}

TEST(UriSuite, TestCrashReport2418192) {
		// Testcase by Harvey Vrsalovic
		helperTestQueryString("http://svcs.cnn.com/weather/wrapper.jsp?&csiID=csi1", 1);
}

TEST(UriSuite, TestPervertedQueryString) {
		helperTestQueryString("http://example.org/?&&=&&&=&&&&==&===&====", 5);
}

TEST(UriSuite, TestQueryStringEndingInEqualSignNonBug32) {
		const char * queryString = "firstname=sdsd&lastname=";

		UriQueryListA * queryList = NULL;
		int itemCount = 0;
		const int res = uriDissectQueryMallocA(&queryList, &itemCount,
				queryString, queryString + strlen(queryString));

		ASSERT_TRUE(res == URI_SUCCESS);
		ASSERT_TRUE(itemCount == 2);
		ASSERT_TRUE(queryList != NULL);
		ASSERT_TRUE(strcmp(queryList->key, "firstname") == 0);
		ASSERT_TRUE(strcmp(queryList->value, "sdsd") == 0);
		ASSERT_TRUE(strcmp(queryList->next->key, "lastname") == 0);
		ASSERT_TRUE(strcmp(queryList->next->value, "") == 0);
		ASSERT_TRUE(queryList->next->next == NULL);

		uriFreeQueryListA(queryList);
}

namespace {
	void helperTestQueryString(char const * uriString, int pairsExpected) {
		UriParserStateA state;
		UriUriA uri;
		state.uri = &uri;
		int res = uriParseUriA(&state, uriString);
		ASSERT_TRUE(res == URI_SUCCESS);

		UriQueryListA * queryList = NULL;
		int itemCount = 0;

		res = uriDissectQueryMallocA(&queryList, &itemCount,
				uri.query.first, uri.query.afterLast);
		ASSERT_TRUE(res == URI_SUCCESS);
		ASSERT_TRUE(queryList != NULL);
		ASSERT_TRUE(itemCount == pairsExpected);
		uriFreeQueryListA(queryList);
		uriFreeUriMembersA(&uri);
	}
}  // namespace

TEST(UriSuite, TestCrashMakeOwnerBug20080207) {
		// Testcase by Adrian Manrique
		UriParserStateA state;
		UriUriA sourceUri;
		state.uri = &sourceUri;
		const char * const sourceUriString = "http://user:pass@somehost.com:80/";
		if (uriParseUriA(&state, sourceUriString) != 0) {
			ASSERT_TRUE(false);
		}
		if (uriNormalizeSyntaxA(&sourceUri) != 0) {
			ASSERT_TRUE(false);
		}
		uriFreeUriMembersA(&sourceUri);
		ASSERT_TRUE(true);
}

namespace {
	void testQueryListHelper(const wchar_t * input, int expectedItemCount) {
		int res;

		UriBool spacePlusConversion = URI_TRUE;
		UriBool normalizeBreaks = URI_FALSE;
		UriBreakConversion breakConversion = URI_BR_DONT_TOUCH;

		int itemCount;
		UriQueryListW * queryList;
		res = uriDissectQueryMallocExW(&queryList, &itemCount,
				input, input + wcslen(input), spacePlusConversion, breakConversion);
		ASSERT_TRUE(res == URI_SUCCESS);
		ASSERT_TRUE(itemCount == expectedItemCount);
		ASSERT_TRUE((queryList == NULL) == (expectedItemCount == 0));

		if (expectedItemCount != 0) {
			// First
			int charsRequired;
			res = uriComposeQueryCharsRequiredExW(queryList, &charsRequired, spacePlusConversion,
					normalizeBreaks);
			ASSERT_TRUE(res == URI_SUCCESS);
			ASSERT_TRUE(charsRequired >= (int)wcslen(input));

			wchar_t * recomposed = new wchar_t[charsRequired + 1];
			int charsWritten;
			res = uriComposeQueryExW(recomposed, queryList, charsRequired + 1,
					&charsWritten, spacePlusConversion, normalizeBreaks);
			ASSERT_TRUE(res == URI_SUCCESS);
			ASSERT_TRUE(charsWritten <= charsRequired);
			ASSERT_TRUE(charsWritten == (int)wcslen(input) + 1);
			ASSERT_TRUE(!wcscmp(input, recomposed));
			delete [] recomposed;

			recomposed = NULL;
			res = uriComposeQueryMallocW(&recomposed, queryList);
			ASSERT_TRUE(res == URI_SUCCESS);
			ASSERT_TRUE(recomposed != NULL);
			ASSERT_TRUE(charsWritten == (int)wcslen(input) + 1);
			ASSERT_TRUE(!wcscmp(input, recomposed));
			free(recomposed);
		}

		uriFreeQueryListW(queryList);
	}
}  // namespace

TEST(UriSuite, QueryList) {
		testQueryListHelper(L"one=ONE&two=TWO", 2);
		testQueryListHelper(L"one=ONE&two=&three=THREE", 3);
		testQueryListHelper(L"one=ONE&two&three=THREE", 3);
		testQueryListHelper(L"one=ONE", 1);
		testQueryListHelper(L"one", 1);
		testQueryListHelper(L"", 0);
}

namespace {
	void testQueryListPairHelper(const char * pair, const char * unescapedKey,
			const char * unescapedValue, const char * fixed = NULL) {
		int res;
		UriQueryListA * queryList;
		int itemCount;

		res = uriDissectQueryMallocA(&queryList, &itemCount, pair, pair + strlen(pair));
		ASSERT_TRUE(res == URI_SUCCESS);
		ASSERT_TRUE(queryList != NULL);
		ASSERT_TRUE(itemCount == 1);
		ASSERT_TRUE(!strcmp(queryList->key, unescapedKey));
		ASSERT_TRUE(!strcmp(queryList->value, unescapedValue));

		char * recomposed;
		res = uriComposeQueryMallocA(&recomposed, queryList);
		ASSERT_TRUE(res == URI_SUCCESS);
		ASSERT_TRUE(recomposed != NULL);
		ASSERT_TRUE(!strcmp(recomposed, (fixed != NULL) ? fixed : pair));
		free(recomposed);
		uriFreeQueryListA(queryList);
	}
}  // namespace

TEST(UriSuite, TestQueryListPair) {
		testQueryListPairHelper("one+two+%26+three=%2B", "one two & three", "+");
		testQueryListPairHelper("one=two=three", "one", "two=three", "one=two%3Dthree");
		testQueryListPairHelper("one=two=three=four", "one", "two=three=four", "one=two%3Dthree%3Dfour");
}

TEST(UriSuite, TestQueryDissectionBug3590761) {
		int res;
		UriQueryListA * queryList;
		int itemCount;
		const char * const pair = "q=hello&x=&y=";

		res = uriDissectQueryMallocA(&queryList, &itemCount, pair, pair + strlen(pair));
		ASSERT_TRUE(res == URI_SUCCESS);
		ASSERT_TRUE(queryList != NULL);
		ASSERT_TRUE(itemCount == 3);

		ASSERT_TRUE(!strcmp(queryList->key, "q"));
		ASSERT_TRUE(!strcmp(queryList->value, "hello"));

		ASSERT_TRUE(!strcmp(queryList->next->key, "x"));
		ASSERT_TRUE(!strcmp(queryList->next->value, ""));

		ASSERT_TRUE(!strcmp(queryList->next->next->key, "y"));
		ASSERT_TRUE(!strcmp(queryList->next->next->value, ""));

		ASSERT_TRUE(! queryList->next->next->next);

		uriFreeQueryListA(queryList);
}

TEST(UriSuite, TestQueryCompositionMathCalc) {
		UriQueryListA second = { /*.key =*/ "k2", /*.value =*/ "v2", /*.next =*/ NULL };
		UriQueryListA first = { /*.key =*/ "k1", /*.value =*/ "v1", /*.next =*/ &second };

		int charsRequired;
		ASSERT_TRUE(uriComposeQueryCharsRequiredA(&first, &charsRequired)
				== URI_SUCCESS);

		const int FACTOR = 6;  /* due to escaping with normalizeBreaks */
		ASSERT_TRUE((unsigned)charsRequired ==
			FACTOR * strlen(first.key) + 1 + FACTOR * strlen(first.value)
			+ 1
			+ FACTOR * strlen(second.key) + 1 + FACTOR * strlen(second.value)
		);
}

TEST(UriSuite, TestQueryCompositionMathWriteGoogleAutofuzz113244572) {
		UriQueryListA second = { /*.key =*/ "\x11", /*.value =*/ NULL, /*.next =*/ NULL };
		UriQueryListA first = { /*.key =*/ "\x01", /*.value =*/ "\x02", /*.next =*/ &second };

		const UriBool spaceToPlus = URI_TRUE;
		const UriBool normalizeBreaks = URI_FALSE;  /* for factor 3 but 6 */

		const int charsRequired = (3 + 1 + 3) + 1 + (3);

		{
			// Minimum space to hold everything fine
			const char * const expected = "%01=%02" "&" "%11";
			char dest[charsRequired + 1];
			int charsWritten;
			ASSERT_TRUE(uriComposeQueryExA(dest, &first, sizeof(dest),
					&charsWritten, spaceToPlus, normalizeBreaks)
				== URI_SUCCESS);
			ASSERT_TRUE(! strcmp(dest, expected));
			ASSERT_TRUE(charsWritten == strlen(expected) + 1);
		}

		{
			// Previous math failed to take ampersand into account
			char dest[charsRequired + 1 - 1];
			int charsWritten;
			ASSERT_TRUE(uriComposeQueryExA(dest, &first, sizeof(dest),
					&charsWritten, spaceToPlus, normalizeBreaks)
				== URI_ERROR_OUTPUT_TOO_LARGE);
		}
}

TEST(UriSuite, TestFreeCrashBug20080827) {
		char const * const sourceUri = "abc";
		char const * const baseUri = "http://www.example.org/";

		int res;
		UriParserStateA state;
		UriUriA absoluteDest;
		UriUriA relativeSource;
		UriUriA absoluteBase;

		state.uri = &relativeSource;
		res = uriParseUriA(&state, sourceUri);
		ASSERT_TRUE(res == URI_SUCCESS);

		state.uri = &absoluteBase;
		res = uriParseUriA(&state, baseUri);
		ASSERT_TRUE(res == URI_SUCCESS);

		res = uriRemoveBaseUriA(&absoluteDest, &relativeSource, &absoluteBase, URI_FALSE);
		ASSERT_TRUE(res == URI_ERROR_REMOVEBASE_REL_SOURCE);

		uriFreeUriMembersA(&relativeSource);
		uriFreeUriMembersA(&absoluteBase);
		uriFreeUriMembersA(&absoluteDest); // Crashed here
}

TEST(UriSuite, TestInvalidInputBug16) {
		UriParserStateA stateA;
		UriUriA uriA;
		stateA.uri = &uriA;
		const char * const input = "A>B";

		const int res = uriParseUriA(&stateA, input);

		ASSERT_TRUE(res == URI_ERROR_SYNTAX);
		ASSERT_TRUE(stateA.errorPos == input + 1);
		ASSERT_TRUE(stateA.errorCode == URI_ERROR_SYNTAX);  /* failed previously */

		uriFreeUriMembersA(&uriA);
}

namespace {
	void testEqualsHelper(const char * uri_to_test) {
		UriParserStateA state;
		UriUriA uriOne;
		UriUriA uriTwo;
		state.uri = &uriOne;
		ASSERT_TRUE(URI_SUCCESS == uriParseUriA(&state, uri_to_test));
		state.uri = &uriTwo;
		ASSERT_TRUE(URI_SUCCESS == uriParseUriA(&state, uri_to_test));
		ASSERT_TRUE(URI_TRUE == uriEqualsUriA(&uriOne, &uriTwo));
		uriFreeUriMembersA(&uriOne);
		uriFreeUriMembersA(&uriTwo);
	}
}  // namespace

TEST(UriSuite, TestEquals) {
		testEqualsHelper("http://host");
		testEqualsHelper("http://host:123");
		testEqualsHelper("http://foo:bar@host:123");
		testEqualsHelper("http://foo:bar@host:123/");
		testEqualsHelper("http://foo:bar@host:123/path");
		testEqualsHelper("http://foo:bar@host:123/path?query");
		testEqualsHelper("http://foo:bar@host:123/path?query#fragment");

		testEqualsHelper("path");
		testEqualsHelper("/path");
		testEqualsHelper("/path/");
		testEqualsHelper("//path/");
		testEqualsHelper("//host");
		testEqualsHelper("//host:123");
}

TEST(UriSuite, TestHostTextTerminationIssue15) {
		UriParserStateA state;
		UriUriA uri;
		state.uri = &uri;

		// Empty host and port
		const char * const emptyHostWithPortUri = "//:123";
		ASSERT_TRUE(URI_SUCCESS == uriParseUriA(&state, emptyHostWithPortUri));
		ASSERT_TRUE(uri.hostText.first == emptyHostWithPortUri + strlen("//"));
		ASSERT_TRUE(uri.hostText.afterLast == uri.hostText.first + 0);
		ASSERT_TRUE(uri.portText.first == emptyHostWithPortUri
															+ strlen("//:"));
		ASSERT_TRUE(uri.portText.afterLast == uri.portText.first
															+ strlen("123"));
		uriFreeUriMembersA(&uri);

		// Non-empty host and port
		const char * const hostWithPortUri = "//h:123";
		ASSERT_TRUE(URI_SUCCESS == uriParseUriA(&state, hostWithPortUri));
		ASSERT_TRUE(uri.hostText.first == hostWithPortUri + strlen("//"));
		ASSERT_TRUE(uri.hostText.afterLast == uri.hostText.first
															+ strlen("h"));
		ASSERT_TRUE(uri.portText.first == hostWithPortUri + strlen("//h:"));
		ASSERT_TRUE(uri.portText.afterLast == uri.portText.first
															+ strlen("123"));
		uriFreeUriMembersA(&uri);

		// Empty host, empty user info
		const char * const emptyHostEmptyUserInfoUri = "//@";
		ASSERT_TRUE(URI_SUCCESS == uriParseUriA(&state,
												emptyHostEmptyUserInfoUri));
		ASSERT_TRUE(uri.userInfo.first == emptyHostEmptyUserInfoUri
															+ strlen("//"));
		ASSERT_TRUE(uri.userInfo.afterLast == uri.userInfo.first + 0);
		ASSERT_TRUE(uri.hostText.first == emptyHostEmptyUserInfoUri
															+ strlen("//@"));
		ASSERT_TRUE(uri.hostText.afterLast == uri.hostText.first + 0);
		uriFreeUriMembersA(&uri);

		// Non-empty host, empty user info
		const char * const hostEmptyUserInfoUri = "//@h";
		ASSERT_TRUE(URI_SUCCESS == uriParseUriA(&state, hostEmptyUserInfoUri));
		ASSERT_TRUE(uri.userInfo.first == hostEmptyUserInfoUri + strlen("//"));
		ASSERT_TRUE(uri.userInfo.afterLast == uri.userInfo.first + 0);
		ASSERT_TRUE(uri.hostText.first == hostEmptyUserInfoUri
															+ strlen("//@"));
		ASSERT_TRUE(uri.hostText.afterLast == uri.hostText.first
															+ strlen("h"));
		uriFreeUriMembersA(&uri);

		// Empty host, non-empty user info
		const char * const emptyHostWithUserInfoUri = "//:@";
		ASSERT_TRUE(URI_SUCCESS == uriParseUriA(&state,
												emptyHostWithUserInfoUri));
		ASSERT_TRUE(uri.userInfo.first == emptyHostWithUserInfoUri
															+ strlen("//"));
		ASSERT_TRUE(uri.userInfo.afterLast == uri.userInfo.first + 1);
		ASSERT_TRUE(uri.hostText.first == emptyHostWithUserInfoUri
															+ strlen("//:@"));
		ASSERT_TRUE(uri.hostText.afterLast == uri.hostText.first + 0);
		uriFreeUriMembersA(&uri);

		// Exact case from issue #15
		const char * const issue15Uri = "//:%aa@";
		ASSERT_TRUE(URI_SUCCESS == uriParseUriA(&state, issue15Uri));
		ASSERT_TRUE(uri.userInfo.first == issue15Uri + strlen("//"));
		ASSERT_TRUE(uri.userInfo.afterLast == uri.userInfo.first
															+ strlen(":%aa"));
		ASSERT_TRUE(uri.hostText.first == issue15Uri + strlen("//:%aa@"));
		ASSERT_TRUE(uri.hostText.afterLast == uri.hostText.first + 0);
		uriFreeUriMembersA(&uri);
}

namespace {
	void testCompareRangeHelper(const char * a, const char * b, int expected, bool avoidNullRange = true) {
		UriTextRangeA ra;
		UriTextRangeA rb;

		if (a) {
			ra.first = a;
			ra.afterLast = a + strlen(a);
		} else {
			ra.first = NULL;
			ra.afterLast = NULL;
		}

		if (b) {
			rb.first = b;
			rb.afterLast = b + strlen(b);
		} else {
			rb.first = NULL;
			rb.afterLast = NULL;
		}

		const int received = uriCompareRangeA(
				((a == NULL) && avoidNullRange) ? NULL : &ra,
				((b == NULL) && avoidNullRange) ? NULL : &rb);
		if (received != expected) {
			printf("Comparing <%s> to <%s> yields %d, expected %d.\n",
					a, b, received, expected);
		}
		ASSERT_TRUE(received == expected);
	}
}  // namespace

TEST(UriSuite, TestRangeComparison) {
		testCompareRangeHelper("", "", 0);
		testCompareRangeHelper("a", "", 1);
		testCompareRangeHelper("", "a", -1);

		testCompareRangeHelper("a", "a", 0);
		testCompareRangeHelper("a", "b", -1);
		testCompareRangeHelper("b", "a", 1);

		testCompareRangeHelper("a", "aa", -1);
		testCompareRangeHelper("aa", "a", 1);

		// Fixed with 0.8.1:
		testCompareRangeHelper(NULL, "a", -1);
		testCompareRangeHelper("a", NULL, 1);
		testCompareRangeHelper(NULL, NULL, 0);

		// Fixed with 0.8.3
		const bool KEEP_NULL_RANGE = false;
		const bool AVOID_NULL_RANGE = true;
		testCompareRangeHelper(NULL, "", -1, AVOID_NULL_RANGE);
		testCompareRangeHelper(NULL, "", -1, KEEP_NULL_RANGE);
		testCompareRangeHelper("", NULL, 1, AVOID_NULL_RANGE);
		testCompareRangeHelper("", NULL, 1, KEEP_NULL_RANGE);
}

namespace {
	void testRemoveBaseUriHelper(const char * expected,
								const char * absSourceStr,
								const char * absBaseStr) {
		UriParserStateA state;
		UriUriA absSource;
		UriUriA absBase;
		UriUriA dest;

		state.uri = &absSource;
		ASSERT_TRUE(uriParseUriA(&state, absSourceStr) == URI_SUCCESS);

		state.uri = &absBase;
		ASSERT_TRUE(uriParseUriA(&state, absBaseStr) == URI_SUCCESS);

		ASSERT_TRUE(uriRemoveBaseUriA(&dest, &absSource, &absBase, URI_FALSE)
				== URI_SUCCESS);

		int size = 0;
		ASSERT_TRUE(uriToStringCharsRequiredA(&dest, &size) == URI_SUCCESS);
		char * const buffer = (char *)malloc(size + 1);
		ASSERT_TRUE(buffer);
		ASSERT_TRUE(uriToStringA(buffer, &dest, size + 1, &size)
															== URI_SUCCESS);
		if (strcmp(buffer, expected)) {
			printf("Expected \"%s\" but got \"%s\"\n", expected, buffer);
			ASSERT_TRUE(0);
		}
		free(buffer);

		uriFreeUriMembersA(&absSource);
		uriFreeUriMembersA(&absBase);
		uriFreeUriMembersA(&dest);
	}
}  // namespace

TEST(UriSuite, TestRangeComparisonRemoveBaseUriIssue19) {
		// scheme
		testRemoveBaseUriHelper("scheme://host/source",
								"scheme://host/source",
								"schemelonger://host/base");
		testRemoveBaseUriHelper("schemelonger://host/source",
								"schemelonger://host/source",
								"scheme://host/base");

		// hostText
		testRemoveBaseUriHelper("//host/source",
								"http://host/source",
								"http://hostlonger/base");
		testRemoveBaseUriHelper("//hostlonger/source",
								"http://hostlonger/source",
								"http://host/base");

		// hostData.ipFuture
		testRemoveBaseUriHelper("//[v7.host]/source",
								"http://[v7.host]/source",
								"http://[v7.hostlonger]/base");
		testRemoveBaseUriHelper("//[v7.hostlonger]/source",
								"http://[v7.hostlonger]/source",
								"http://host/base");

		// path
		testRemoveBaseUriHelper("path1",
								"http://host/path1",
								"http://host/path111");
		testRemoveBaseUriHelper("../path1/path2",
								"http://host/path1/path2",
								"http://host/path111/path222");
		testRemoveBaseUriHelper("path111",
								"http://host/path111",
								"http://host/path1");
		testRemoveBaseUriHelper("../path111/path222",
								"http://host/path111/path222",
								"http://host/path1/path2");

		// Exact issue #19
		testRemoveBaseUriHelper("//example/x/abc",
								"http://example/x/abc",
								"http://example2/x/y/z");
}

TEST(ErrorPosSuite, TestErrorPosIPvFuture) {
	UriUriA uri;
	const char * errorPos;

	const char * const uriText = "http://[vA.123456";  // missing "]"
	EXPECT_EQ(uriParseSingleUriA(&uri, uriText, &errorPos),
				URI_ERROR_SYNTAX);
	EXPECT_EQ(errorPos, uriText + strlen(uriText));
}

TEST(UriParseSingleSuite, Success) {
	UriUriA uri;

	EXPECT_EQ(uriParseSingleUriA(&uri, "file:///home/user/song.mp3", NULL),
			URI_SUCCESS);

	uriFreeUriMembersA(&uri);
}

TEST(UriParseSingleSuite, ErrorSyntaxParseErrorSetsErrorPos) {
	UriUriA uri;
	const char * errorPos;
	const char * const uriString = "abc{}def";

	EXPECT_EQ(uriParseSingleUriA(&uri, uriString, &errorPos),
			URI_ERROR_SYNTAX);
	EXPECT_EQ(errorPos, uriString + strlen("abc"));

	uriFreeUriMembersA(&uri);
}

TEST(UriParseSingleSuite, ErrorNullFirstDetected) {
	UriUriA uri;
	const char * errorPos;

	EXPECT_EQ(uriParseSingleUriExA(&uri, NULL, "notnull", &errorPos),
			URI_ERROR_NULL);
}

TEST(UriParseSingleSuite, ErrorNullAfterLastDetected) {
	UriUriA uri;

	EXPECT_EQ(uriParseSingleUriExA(&uri, "foo", NULL, NULL), URI_SUCCESS);

	uriFreeUriMembersA(&uri);
}

TEST(UriParseSingleSuite, ErrorNullMemoryManagerDetected) {
	UriUriA uri;
	const char * errorPos;
	const char * const uriString = "somethingwellformed";

	EXPECT_EQ(uriParseSingleUriExMmA(&uri,
			uriString,
			uriString + strlen(uriString),
			&errorPos, NULL), URI_SUCCESS);

	EXPECT_EQ(uriFreeUriMembersMmA(&uri, NULL), URI_SUCCESS);
}

TEST(FreeUriMembersSuite, MultiFreeWorksFine) {
	UriUriA uri;

	EXPECT_EQ(uriParseSingleUriA(&uri, "file:///home/user/song.mp3", NULL),
			URI_SUCCESS);

	UriUriA uriBackup = uri;
	EXPECT_EQ(memcmp(&uriBackup, &uri, sizeof(UriUriA)), 0);

	uriFreeUriMembersA(&uri);

	// Did some pointers change (to NULL)?
	EXPECT_NE(memcmp(&uriBackup, &uri, sizeof(UriUriA)), 0);

	uriFreeUriMembersA(&uri);  // second time
}

namespace {
	void testFreeUriMembersFreesHostText(const char *const uriFirst) {  // issue #121
		const char *const uriAfterLast = uriFirst + strlen(uriFirst);
		UriUriA uri;

		EXPECT_EQ(uriParseSingleUriA(&uri, uriFirst, NULL), URI_SUCCESS);
		EXPECT_EQ(uriMakeOwnerA(&uri), URI_SUCCESS);

		EXPECT_EQ(uri.owner, URI_TRUE);
		EXPECT_TRUE(uri.hostText.first);
		EXPECT_TRUE(uri.hostText.afterLast);
		EXPECT_NE(uri.hostText.first, uri.hostText.afterLast);
		URI_EXPECT_RANGE_OUTSIDE(uri.hostText, uriFirst, uriAfterLast);

		uriFreeUriMembersA(&uri);

		EXPECT_FALSE(uri.hostText.first);
		EXPECT_FALSE(uri.hostText.afterLast);

		uriFreeUriMembersA(&uri);  // second time
	}
}  // namespace

TEST(FreeUriMembersSuite, FreeUriMembersFreesHostTextIp4) {  // issue #121
	testFreeUriMembersFreesHostText("//192.0.2.0");  // RFC 5737
}

TEST(FreeUriMembersSuite, FreeUriMembersFreesHostTextIp6) {  // issue #121
	testFreeUriMembersFreesHostText("//[2001:db8::]");  // RFC 3849
}

TEST(FreeUriMembersSuite, FreeUriMembersFreesHostTextRegname) {  // issue #121
	testFreeUriMembersFreesHostText("//host123.test");  // RFC 6761
}

TEST(FreeUriMembersSuite, FreeUriMembersFreesHostTextFuture) {  // issue #121
	testFreeUriMembersFreesHostText("//[v7.X]");  // arbitrary IPvFuture
}

TEST(MakeOwnerSuite, MakeOwner) {
	const char * const uriString = "scheme://user:pass@[v7.X]:55555/path/../path/?query#fragment";
	UriUriA uri;
	char * uriFirst = strdup(uriString);
	const size_t uriLen = strlen(uriFirst);
	char * uriAfterLast = uriFirst + uriLen;

	EXPECT_EQ(uriParseSingleUriExA(&uri, uriFirst, uriAfterLast, NULL), URI_SUCCESS);

	// After plain parse, all strings should point inside the original URI string
	EXPECT_EQ(uri.owner, URI_FALSE);
	URI_EXPECT_RANGE_BETWEEN(uri.scheme, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_BETWEEN(uri.userInfo, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_BETWEEN(uri.hostText, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_BETWEEN(uri.hostData.ipFuture, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_BETWEEN(uri.portText, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_BETWEEN(uri.pathHead->text, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_BETWEEN(uri.pathHead->next->text, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_BETWEEN(uri.pathHead->next->next->text, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_EMPTY(uri.pathHead->next->next->next->text);
	EXPECT_TRUE(uri.pathHead->next->next->next->next == NULL);
	URI_EXPECT_RANGE_BETWEEN(uri.query, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_BETWEEN(uri.fragment, uriFirst, uriAfterLast);

	EXPECT_EQ(uriMakeOwnerA(&uri), URI_SUCCESS);

	// After making owner, *none* of the strings should point inside the original URI string
	EXPECT_EQ(uri.owner, URI_TRUE);
	URI_EXPECT_RANGE_OUTSIDE(uri.scheme, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_OUTSIDE(uri.userInfo, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_OUTSIDE(uri.hostText, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_OUTSIDE(uri.hostData.ipFuture, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_OUTSIDE(uri.portText, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_OUTSIDE(uri.pathHead->text, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_OUTSIDE(uri.pathHead->next->text, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_OUTSIDE(uri.pathHead->next->next->text, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_EMPTY(uri.pathHead->next->next->next->text);
	EXPECT_TRUE(uri.pathHead->next->next->next->next == NULL);
	URI_EXPECT_RANGE_OUTSIDE(uri.query, uriFirst, uriAfterLast);
	URI_EXPECT_RANGE_OUTSIDE(uri.fragment, uriFirst, uriAfterLast);

	// Free originally used memory so we'd get violations on access with ASan
	uriAfterLast = NULL;
	free(uriFirst);
	uriFirst = NULL;

	// Can we recompose the URI without accessing any old freed memory?
	int charsRequired;
	EXPECT_EQ(uriToStringCharsRequiredA(&uri, &charsRequired), URI_SUCCESS);
	EXPECT_TRUE((charsRequired >= 0) && (charsRequired >= static_cast<int>(uriLen)));
	char * const uriRemake = new char[charsRequired + 1];
	EXPECT_TRUE(uriRemake != NULL);
	EXPECT_EQ(uriToStringA(uriRemake, &uri, charsRequired + 1, NULL), URI_SUCCESS);
	EXPECT_TRUE(! strcmp(uriString, uriRemake));
	delete [] uriRemake;

	uriFreeUriMembersA(&uri);
}

namespace {
	void testMakeOwnerCopiesHostText(const char *const uriFirst) {  // issue #121
		const char *const uriAfterLast = uriFirst + strlen(uriFirst);
		UriUriA uri;

		EXPECT_EQ(uriParseSingleUriA(&uri, uriFirst, NULL), URI_SUCCESS);
		EXPECT_EQ(uri.owner, URI_FALSE);
		URI_EXPECT_RANGE_BETWEEN(uri.hostText, uriFirst, uriAfterLast);

		EXPECT_EQ(uriMakeOwnerA(&uri), URI_SUCCESS);

		EXPECT_EQ(uri.owner, URI_TRUE);
		URI_EXPECT_RANGE_OUTSIDE(uri.hostText, uriFirst, uriAfterLast);

		uriFreeUriMembersA(&uri);
		uriFreeUriMembersA(&uri);  // tried freeing stack pointers before the fix
	}
} // namespace

TEST(MakeOwnerSuite, MakeOwnerCopiesHostTextIp4) {  // issue #121
	testMakeOwnerCopiesHostText("//192.0.2.0");  // RFC 5737
}

TEST(MakeOwnerSuite, MakeOwnerCopiesHostTextIp6) {  // issue #121
	testMakeOwnerCopiesHostText("//[2001:db8::]");  // RFC 3849
}

TEST(MakeOwnerSuite, MakeOwnerCopiesHostTextRegname) {  // issue #121
	testMakeOwnerCopiesHostText("//host123.test");  // RFC 6761
}

TEST(MakeOwnerSuite, MakeOwnerCopiesHostTextFuture) {  // issue #121
	testMakeOwnerCopiesHostText("//[v7.X]");  // arbitrary IPvFuture
}

TEST(ParseIpFourAddressSuite, FourSaneOctets) {
	unsigned char octetOutput[4];
	const char * const ipAddressText = "111.22.3.40";
	const int res = uriParseIpFourAddressA(octetOutput, ipAddressText,
			ipAddressText + strlen(ipAddressText));
	EXPECT_EQ(res, URI_SUCCESS);
	EXPECT_EQ(octetOutput[0], 111);
	EXPECT_EQ(octetOutput[1], 22);
	EXPECT_EQ(octetOutput[2], 3);
	EXPECT_EQ(octetOutput[3], 40);
}
