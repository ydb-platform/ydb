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

#include <gtest/gtest.h>

#include <uriparser/Uri.h>



// All testcases in this file are coming from
// http://cvs.4suite.org/viewcvs/4Suite/test/Lib/test_uri.py


namespace {

bool testAddOrRemoveBaseHelper(const char * ref, const char * base,
	const char * expected, bool add = true, bool domainRootMode = false) {
	UriParserStateA stateA;

	// Base
	UriUriA baseUri;
	stateA.uri = &baseUri;
	int res = uriParseUriA(&stateA, base);
	if (res != 0) {
		return false;
	}

	// Rel
	UriUriA relUri;
	stateA.uri = &relUri;
	res = uriParseUriA(&stateA, ref);
	if (res != 0) {
		uriFreeUriMembersA(&baseUri);
		return false;
	}

	// Expected result
	UriUriA expectedUri;
	stateA.uri = &expectedUri;
	res = uriParseUriA(&stateA, expected);
	if (res != 0) {
		uriFreeUriMembersA(&baseUri);
		uriFreeUriMembersA(&relUri);
		uriFreeUriMembersA(&expectedUri);
		return false;
	}

	// Transform
	UriUriA transformedUri;
	if (add) {
		res = uriAddBaseUriA(&transformedUri, &relUri, &baseUri);
	} else {
		res = uriRemoveBaseUriA(&transformedUri, &relUri, &baseUri,
				domainRootMode ? URI_TRUE : URI_FALSE);
	}
	if (res != 0) {
		uriFreeUriMembersA(&baseUri);
		uriFreeUriMembersA(&relUri);
		uriFreeUriMembersA(&expectedUri);
		uriFreeUriMembersA(&transformedUri);
		return false;
	}

	const bool equal = (URI_TRUE == uriEqualsUriA(&transformedUri, &expectedUri));
	if (!equal) {
		char transformedUriText[1024 * 8];
		char expectedUriText[1024 * 8];
		uriToStringA(transformedUriText, &transformedUri, 1024 * 8, NULL);
		uriToStringA(expectedUriText, &expectedUri, 1024 * 8, NULL);
		printf("\n\n\nExpected: \"%s\"\nReceived: \"%s\"\n\n\n", expectedUriText, transformedUriText);
	}

	uriFreeUriMembersA(&baseUri);
	uriFreeUriMembersA(&relUri);
	uriFreeUriMembersA(&expectedUri);
	uriFreeUriMembersA(&transformedUri);
	return equal;
}

}  // namespace


TEST(FourSuite, AbsolutizeTestCases) {
	const char * const BASE_URI[] = {
			"http://a/b/c/d;p?q",
			"http://a/b/c/d;p?q=1/2",
			"http://a/b/c/d;p=1/2?q",
			"fred:///s//a/b/c",
			"http:///s//a/b/c"};

	// ref, base, exptected

	// http://lists.w3.org/Archives/Public/uri/2004Feb/0114.html
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../c", "foo:a/b", "foo:c"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("foo:.", "foo:a", "foo:"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/foo/../../../bar", "zz:abc", "zz:/bar"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/foo/../bar", "zz:abc", "zz:/bar"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("foo/../../../bar", "zz:abc", "zz:bar"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("foo/../bar", "zz:abc", "zz:bar"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("zz:.", "zz:abc", "zz:"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/.", BASE_URI[0], "http://a/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/.foo", BASE_URI[0], "http://a/.foo"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper(".foo", BASE_URI[0], "http://a/b/c/.foo"));

	// http://gbiv.com/protocols/uri/test/rel_examples1.html
	// examples from RFC 2396
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g:h", BASE_URI[0], "g:h"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g", BASE_URI[0], "http://a/b/c/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./g", BASE_URI[0], "http://a/b/c/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g/", BASE_URI[0], "http://a/b/c/g/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/g", BASE_URI[0], "http://a/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("//g", BASE_URI[0], "http://g"));

	// changed with RFC 2396bis
	ASSERT_TRUE(testAddOrRemoveBaseHelper("?y", BASE_URI[0], "http://a/b/c/d;p?y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g?y", BASE_URI[0], "http://a/b/c/g?y"));

	// changed with RFC 2396bis
	ASSERT_TRUE(testAddOrRemoveBaseHelper("#s", BASE_URI[0], "http://a/b/c/d;p?q#s"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g#s", BASE_URI[0], "http://a/b/c/g#s"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g?y#s", BASE_URI[0], "http://a/b/c/g?y#s"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper(";x", BASE_URI[0], "http://a/b/c/;x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g;x", BASE_URI[0], "http://a/b/c/g;x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g;x?y#s", BASE_URI[0], "http://a/b/c/g;x?y#s"));

	// changed with RFC 2396bis
	ASSERT_TRUE(testAddOrRemoveBaseHelper("", BASE_URI[0], "http://a/b/c/d;p?q"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper(".", BASE_URI[0], "http://a/b/c/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./", BASE_URI[0], "http://a/b/c/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("..", BASE_URI[0], "http://a/b/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../", BASE_URI[0], "http://a/b/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../g", BASE_URI[0], "http://a/b/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../..", BASE_URI[0], "http://a/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../", BASE_URI[0], "http://a/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../g", BASE_URI[0], "http://a/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../../g", BASE_URI[0], "http://a/g")); // http://a/../g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../../../g", BASE_URI[0], "http://a/g")); // http://a/../../g

	// changed with RFC 2396bis
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/./g", BASE_URI[0], "http://a/g"));

	// changed with RFC 2396bis
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/../g", BASE_URI[0], "http://a/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g.", BASE_URI[0], "http://a/b/c/g."));
	ASSERT_TRUE(testAddOrRemoveBaseHelper(".g", BASE_URI[0], "http://a/b/c/.g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g..", BASE_URI[0], "http://a/b/c/g.."));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("..g", BASE_URI[0], "http://a/b/c/..g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./../g", BASE_URI[0], "http://a/b/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./g/.", BASE_URI[0], "http://a/b/c/g/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g/./h", BASE_URI[0], "http://a/b/c/g/h"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g/../h", BASE_URI[0], "http://a/b/c/h"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g;x=1/./y", BASE_URI[0], "http://a/b/c/g;x=1/y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g;x=1/../y", BASE_URI[0], "http://a/b/c/y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g?y/./x", BASE_URI[0], "http://a/b/c/g?y/./x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g?y/../x", BASE_URI[0], "http://a/b/c/g?y/../x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g#s/./x", BASE_URI[0], "http://a/b/c/g#s/./x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g#s/../x", BASE_URI[0], "http://a/b/c/g#s/../x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("http:g", BASE_URI[0], "http:g")); // http://a/b/c/g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("http:", BASE_URI[0], "http:")); // BASE_URI[0]

	// not sure where this one originated
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/a/b/c/./../../g", BASE_URI[0], "http://a/a/g"));

	// http://gbiv.com/protocols/uri/test/rel_examples2.html
	// slashes in base URI's query args
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g", BASE_URI[1], "http://a/b/c/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./g", BASE_URI[1], "http://a/b/c/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g/", BASE_URI[1], "http://a/b/c/g/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/g", BASE_URI[1], "http://a/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("//g", BASE_URI[1], "http://g"));

	// changed in RFC 2396bis
	// ASSERT_TRUE(testAddOrRemoveBaseHelper("?y", BASE_URI[1], "http://a/b/c/?y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("?y", BASE_URI[1], "http://a/b/c/d;p?y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g?y", BASE_URI[1], "http://a/b/c/g?y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g?y/./x", BASE_URI[1], "http://a/b/c/g?y/./x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g?y/../x", BASE_URI[1], "http://a/b/c/g?y/../x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g#s", BASE_URI[1], "http://a/b/c/g#s"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g#s/./x", BASE_URI[1], "http://a/b/c/g#s/./x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g#s/../x", BASE_URI[1], "http://a/b/c/g#s/../x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./", BASE_URI[1], "http://a/b/c/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../", BASE_URI[1], "http://a/b/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../g", BASE_URI[1], "http://a/b/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../", BASE_URI[1], "http://a/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../g", BASE_URI[1], "http://a/g"));

	// http://gbiv.com/protocols/uri/test/rel_examples3.html
	// slashes in path params
	// all of these changed in RFC 2396bis
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g", BASE_URI[2], "http://a/b/c/d;p=1/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./g", BASE_URI[2], "http://a/b/c/d;p=1/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g/", BASE_URI[2], "http://a/b/c/d;p=1/g/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g?y", BASE_URI[2], "http://a/b/c/d;p=1/g?y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper(";x", BASE_URI[2], "http://a/b/c/d;p=1/;x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g;x", BASE_URI[2], "http://a/b/c/d;p=1/g;x"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g;x=1/./y", BASE_URI[2], "http://a/b/c/d;p=1/g;x=1/y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g;x=1/../y", BASE_URI[2], "http://a/b/c/d;p=1/y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./", BASE_URI[2], "http://a/b/c/d;p=1/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../", BASE_URI[2], "http://a/b/c/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../g", BASE_URI[2], "http://a/b/c/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../", BASE_URI[2], "http://a/b/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../g", BASE_URI[2], "http://a/b/g"));

	// http://gbiv.com/protocols/uri/test/rel_examples4.html
	// double and triple slash, unknown scheme
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g:h", BASE_URI[3], "g:h"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g", BASE_URI[3], "fred:///s//a/b/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./g", BASE_URI[3], "fred:///s//a/b/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g/", BASE_URI[3], "fred:///s//a/b/g/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/g", BASE_URI[3], "fred:///g")); // may change to fred:///s//a/g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("//g", BASE_URI[3], "fred://g")); // may change to fred:///s//g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("//g/x", BASE_URI[3], "fred://g/x")); // may change to fred:///s//g/x
	ASSERT_TRUE(testAddOrRemoveBaseHelper("///g", BASE_URI[3], "fred:///g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./", BASE_URI[3], "fred:///s//a/b/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../", BASE_URI[3], "fred:///s//a/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../g", BASE_URI[3], "fred:///s//a/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../", BASE_URI[3], "fred:///s//")); // may change to fred:///s//a/../
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../g", BASE_URI[3], "fred:///s//g")); // may change to fred:///s//a/../g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../../g", BASE_URI[3], "fred:///s/g")); // may change to fred:///s//a/../../g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../../../g", BASE_URI[3], "fred:///g")); // may change to fred:///s//a/../../../g

	// http://gbiv.com/protocols/uri/test/rel_examples5.html
	// double and triple slash, well-known scheme
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g:h", BASE_URI[4], "g:h"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g", BASE_URI[4], "http:///s//a/b/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./g", BASE_URI[4], "http:///s//a/b/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("g/", BASE_URI[4], "http:///s//a/b/g/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/g", BASE_URI[4], "http:///g")); // may change to http:///s//a/g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("//g", BASE_URI[4], "http://g")); // may change to http:///s//g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("//g/x", BASE_URI[4], "http://g/x")); // may change to http:///s//g/x
	ASSERT_TRUE(testAddOrRemoveBaseHelper("///g", BASE_URI[4], "http:///g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./", BASE_URI[4], "http:///s//a/b/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../", BASE_URI[4], "http:///s//a/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../g", BASE_URI[4], "http:///s//a/g"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../", BASE_URI[4], "http:///s//")); // may change to http:///s//a/../
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../g", BASE_URI[4], "http:///s//g")); // may change to http:///s//a/../g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../../g", BASE_URI[4], "http:///s/g")); // may change to http:///s//a/../../g
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../../../g", BASE_URI[4], "http:///g")); // may change to http:///s//a/../../../g

	// from Dan Connelly's tests in http://www.w3.org/2000/10/swap/uripath.py
	ASSERT_TRUE(testAddOrRemoveBaseHelper("bar:abc", "foo:xyz", "bar:abc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../abc", "http://example/x/y/z", "http://example/x/abc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("http://example/x/abc", "http://example2/x/y/z", "http://example/x/abc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../r", "http://ex/x/y/z", "http://ex/x/r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("q/r", "http://ex/x/y", "http://ex/x/q/r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("q/r#s", "http://ex/x/y", "http://ex/x/q/r#s"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("q/r#s/t", "http://ex/x/y", "http://ex/x/q/r#s/t"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("ftp://ex/x/q/r", "http://ex/x/y", "ftp://ex/x/q/r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("", "http://ex/x/y", "http://ex/x/y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("", "http://ex/x/y/", "http://ex/x/y/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("", "http://ex/x/y/pdq", "http://ex/x/y/pdq"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("z/", "http://ex/x/y/", "http://ex/x/y/z/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("#Animal", "file:/swap/test/animal.rdf", "file:/swap/test/animal.rdf#Animal"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../abc", "file:/e/x/y/z", "file:/e/x/abc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/example/x/abc", "file:/example2/x/y/z", "file:/example/x/abc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../r", "file:/ex/x/y/z", "file:/ex/x/r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/r", "file:/ex/x/y/z", "file:/r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("q/r", "file:/ex/x/y", "file:/ex/x/q/r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("q/r#s", "file:/ex/x/y", "file:/ex/x/q/r#s"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("q/r#", "file:/ex/x/y", "file:/ex/x/q/r#"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("q/r#s/t", "file:/ex/x/y", "file:/ex/x/q/r#s/t"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("ftp://ex/x/q/r", "file:/ex/x/y", "ftp://ex/x/q/r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("", "file:/ex/x/y", "file:/ex/x/y"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("", "file:/ex/x/y/", "file:/ex/x/y/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("", "file:/ex/x/y/pdq", "file:/ex/x/y/pdq"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("z/", "file:/ex/x/y/", "file:/ex/x/y/z/"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("file://meetings.example.com/cal#m1", "file:/devel/WWW/2000/10/swap/test/reluri-1.n3", "file://meetings.example.com/cal#m1"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("file://meetings.example.com/cal#m1", "file:/home/connolly/w3ccvs/WWW/2000/10/swap/test/reluri-1.n3", "file://meetings.example.com/cal#m1"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./#blort", "file:/some/dir/foo", "file:/some/dir/#blort"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./#", "file:/some/dir/foo", "file:/some/dir/#"));

	// Ryan Lee
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./", "http://example/x/abc.efg", "http://example/x/"));

	// Graham Klyne's tests
	// http://www.ninebynine.org/Software/HaskellUtils/Network/UriTest.xls
	// 01-31 are from Connelly's cases

	// 32-49
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./q:r", "http://ex/x/y", "http://ex/x/q:r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./p=q:r", "http://ex/x/y", "http://ex/x/p=q:r"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("?pp/rr", "http://ex/x/y?pp/qq", "http://ex/x/y?pp/rr"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("y/z", "http://ex/x/y?pp/qq", "http://ex/x/y/z"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("local/qual@domain.org#frag", "mailto:local", "mailto:local/qual@domain.org#frag"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("more/qual2@domain2.org#frag", "mailto:local/qual1@domain1.org", "mailto:local/more/qual2@domain2.org#frag"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("y?q", "http://ex/x/y?q", "http://ex/x/y?q"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/x/y?q", "http://ex?p", "http://ex/x/y?q"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("c/d", "foo:a/b", "foo:a/c/d"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/c/d", "foo:a/b", "foo:/c/d"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("", "foo:a/b?c#d", "foo:a/b?c"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("b/c", "foo:a", "foo:b/c"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../b/c", "foo:/a/y/z", "foo:/a/b/c"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("./b/c", "foo:a", "foo:b/c"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/./b/c", "foo:a", "foo:/b/c"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../d", "foo://a//b/c", "foo://a/d"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper(".", "foo:a", "foo:"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("..", "foo:a", "foo:"));

	// 50-57 (cf. TimBL comments --
	// http://lists.w3.org/Archives/Public/uri/2003Feb/0028.html,
	// http://lists.w3.org/Archives/Public/uri/2003Jan/0008.html)
	ASSERT_TRUE(testAddOrRemoveBaseHelper("abc", "http://example/x/y%2Fz", "http://example/x/abc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../../x%2Fabc", "http://example/a/x/y/z", "http://example/a/x%2Fabc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../x%2Fabc", "http://example/a/x/y%2Fz", "http://example/a/x%2Fabc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("abc", "http://example/x%2Fy/z", "http://example/x%2Fy/abc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("q%3Ar", "http://ex/x/y", "http://ex/x/q%3Ar"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/x%2Fabc", "http://example/x/y%2Fz", "http://example/x%2Fabc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/x%2Fabc", "http://example/x/y/z", "http://example/x%2Fabc"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("/x%2Fabc", "http://example/x/y%2Fz", "http://example/x%2Fabc"));

	// 70-77
	ASSERT_TRUE(testAddOrRemoveBaseHelper("local2@domain2", "mailto:local1@domain1?query1", "mailto:local2@domain2"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("local2@domain2?query2", "mailto:local1@domain1", "mailto:local2@domain2?query2"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("local2@domain2?query2", "mailto:local1@domain1?query1", "mailto:local2@domain2?query2"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("?query2", "mailto:local@domain?query1", "mailto:local@domain?query2"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("local@domain?query2", "mailto:?query1", "mailto:local@domain?query2"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("?query2", "mailto:local@domain?query1", "mailto:local@domain?query2"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("http://example/a/b?c/../d", "foo:bar", "http://example/a/b?c/../d"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("http://example/a/b#c/../d", "foo:bar", "http://example/a/b#c/../d"));

	// 82-88
	ASSERT_TRUE(testAddOrRemoveBaseHelper("http:this", "http://example.org/base/uri", "http:this"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("http:this", "http:base", "http:this"));
	// Whole in the URI spec, see http://lists.w3.org/Archives/Public/uri/2007Aug/0003.html
	// ASSERT_TRUE(testAddOrRemoveBaseHelper(".//g", "f:/a", "f://g")); // ORIGINAL
	ASSERT_TRUE(testAddOrRemoveBaseHelper(".//g", "f:/a", "f:/.//g")); // FIXED ONE
	ASSERT_TRUE(testAddOrRemoveBaseHelper("b/c//d/e", "f://example.org/base/a", "f://example.org/base/b/c//d/e"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("m2@example.ord/c2@example.org", "mid:m@example.ord/c@example.org", "mid:m@example.ord/m2@example.ord/c2@example.org"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("mini1.xml", "file:///C:/DEV/Haskell/lib/HXmlToolbox-3.01/examples/", "file:///C:/DEV/Haskell/lib/HXmlToolbox-3.01/examples/mini1.xml"));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("../b/c", "foo:a/y/z", "foo:a/b/c"));
}



TEST(FourSuite, RelativizeTestCases) {
	const bool REMOVE_MODE = false;
	const bool DOMAIN_ROOT_MODE = true;

	// to convert, base, exptected

	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c", "s://ex/a/d", "b/c", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/b/b/c", "s://ex/a/d", "/b/b/c", REMOVE_MODE, DOMAIN_ROOT_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c", "s://ex/a/b/", "c", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://other.ex/a/b/", "s://ex/a/d", "//other.ex/a/b/", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c", "s://other.ex/a/d", "//ex/a/b/c", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("t://ex/a/b/c", "s://ex/a/d", "t://ex/a/b/c", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c", "t://ex/a/d", "s://ex/a/b/c", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a", "s://ex/b/c/d", "/a", REMOVE_MODE, DOMAIN_ROOT_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/b/c/d", "s://ex/a", "b/c/d", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c?h", "s://ex/a/d?w", "b/c?h", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c#h", "s://ex/a/d#w", "b/c#h", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c?h#i", "s://ex/a/d?w#j", "b/c?h#i", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a#i", "s://ex/a", "#i", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a?i", "s://ex/a", "?i", REMOVE_MODE));

	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/", "s://ex/a/b/", "", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b", "s://ex/a/b", "", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/", "s://ex/", "", REMOVE_MODE));

	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c", "s://ex/a/d/c", "../b/c", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c/", "s://ex/a/d/c", "../b/c/", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c/d", "s://ex/a/d/c/d", "../../b/c/d", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/c", "s://ex/d/e/f", "/a/b/c", REMOVE_MODE, DOMAIN_ROOT_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b/", "s://ex/a/c/d/e", "../../b/", REMOVE_MODE));

	// Some tests to ensure that empty path segments don't cause problems.
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/b", "s://ex/a//b/c", "../../b", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a///b", "s://ex/a/", ".///b", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a/", "s://ex/a///b", "../../", REMOVE_MODE));
	ASSERT_TRUE(testAddOrRemoveBaseHelper("s://ex/a//b/c", "s://ex/a/b", ".//b/c", REMOVE_MODE));
}


namespace {

int testParseUri(const char * uriText, const char ** expectedErrorPos = NULL) {
	UriParserStateA state;
	UriUriA uri;
	state.uri = &uri;
	int res = uriParseUriA(&state, uriText);
	if (expectedErrorPos != NULL) {
		*expectedErrorPos = state.errorPos;
	}
	uriFreeUriMembersA(&uri);
	return res;
}



bool testGoodUri(const char * uriText) {
	return (testParseUri(uriText) == 0);
}



bool testBadUri(const char * uriText, int expectedErrorOffset = -1) {
	const char * errorPos = NULL;
	const int ret = testParseUri(uriText, &errorPos);
	return ((ret == URI_ERROR_SYNTAX)
			&& (errorPos != NULL)
			&& (
				(expectedErrorOffset == -1)
				|| (errorPos == (uriText + expectedErrorOffset))
			));
}

}  // namespace



TEST(FourSuite, GoodUriReferences) {
	ASSERT_TRUE(testGoodUri("file:///foo/bar"));
	ASSERT_TRUE(testGoodUri("mailto:user@host?subject=blah"));
	ASSERT_TRUE(testGoodUri("dav:")); // empty opaque part / rel-path allowed by RFC 2396bis
	ASSERT_TRUE(testGoodUri("about:")); // empty opaque part / rel-path allowed by RFC 2396bis

	// the following test cases are from a Perl script by David A. Wheeler
	// at http://www.dwheeler.com/secure-programs/url.pl
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com/"));
	ASSERT_TRUE(testGoodUri("http://1.2.3.4/"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com/stuff"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com/stuff/"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com/hello%20world/"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com?name=obi"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com?name=obi+wan&status=jedi"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com?onery"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com#bottom"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com/yelp.html#bottom"));
	ASSERT_TRUE(testGoodUri("https://www.yahoo.com/"));
	ASSERT_TRUE(testGoodUri("ftp://www.yahoo.com/"));
	ASSERT_TRUE(testGoodUri("ftp://www.yahoo.com/hello"));
	ASSERT_TRUE(testGoodUri("demo.txt"));
	ASSERT_TRUE(testGoodUri("demo/hello.txt"));
	ASSERT_TRUE(testGoodUri("demo/hello.txt?query=hello#fragment"));
	ASSERT_TRUE(testGoodUri("/cgi-bin/query?query=hello#fragment"));
	ASSERT_TRUE(testGoodUri("/demo.txt"));
	ASSERT_TRUE(testGoodUri("/hello/demo.txt"));
	ASSERT_TRUE(testGoodUri("hello/demo.txt"));
	ASSERT_TRUE(testGoodUri("/"));
	ASSERT_TRUE(testGoodUri(""));
	ASSERT_TRUE(testGoodUri("#"));
	ASSERT_TRUE(testGoodUri("#here"));

	// Wheeler's script says these are invalid, but they aren't
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com?name=%00%01"));
	ASSERT_TRUE(testGoodUri("http://www.yaho%6f.com"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com/hello%00world/"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com/hello+world/"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com?name=obi&"));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com?name=obi&type="));
	ASSERT_TRUE(testGoodUri("http://www.yahoo.com/yelp.html#"));
	ASSERT_TRUE(testGoodUri("//"));

	// the following test cases are from a Haskell program by Graham Klyne
	// at http://www.ninebynine.org/Software/HaskellUtils/Network/URITest.hs
	ASSERT_TRUE(testGoodUri("http://example.org/aaa/bbb#ccc"));
	ASSERT_TRUE(testGoodUri("mailto:local@domain.org"));
	ASSERT_TRUE(testGoodUri("mailto:local@domain.org#frag"));
	ASSERT_TRUE(testGoodUri("HTTP://EXAMPLE.ORG/AAA/BBB#CCC"));
	ASSERT_TRUE(testGoodUri("//example.org/aaa/bbb#ccc"));
	ASSERT_TRUE(testGoodUri("/aaa/bbb#ccc"));
	ASSERT_TRUE(testGoodUri("bbb#ccc"));
	ASSERT_TRUE(testGoodUri("#ccc"));
	ASSERT_TRUE(testGoodUri("#"));
	ASSERT_TRUE(testGoodUri("A'C"));

	// escapes
	ASSERT_TRUE(testGoodUri("http://example.org/aaa%2fbbb#ccc"));
	ASSERT_TRUE(testGoodUri("http://example.org/aaa%2Fbbb#ccc"));
	ASSERT_TRUE(testGoodUri("%2F"));
	ASSERT_TRUE(testGoodUri("aaa%2Fbbb"));

	// ports
	ASSERT_TRUE(testGoodUri("http://example.org:80/aaa/bbb#ccc"));
	ASSERT_TRUE(testGoodUri("http://example.org:/aaa/bbb#ccc"));
	ASSERT_TRUE(testGoodUri("http://example.org./aaa/bbb#ccc"));
	ASSERT_TRUE(testGoodUri("http://example.123./aaa/bbb#ccc"));

	// bare authority
	ASSERT_TRUE(testGoodUri("http://example.org"));

	// IPv6 literals (from RFC2732):
	ASSERT_TRUE(testGoodUri("http://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:80/index.html"));
	ASSERT_TRUE(testGoodUri("http://[1080:0:0:0:8:800:200C:417A]/index.html"));
	ASSERT_TRUE(testGoodUri("http://[3ffe:2a00:100:7031::1]"));
	ASSERT_TRUE(testGoodUri("http://[1080::8:800:200C:417A]/foo"));
	ASSERT_TRUE(testGoodUri("http://[::192.9.5.5]/ipng"));
	ASSERT_TRUE(testGoodUri("http://[::FFFF:129.144.52.38]:80/index.html"));
	ASSERT_TRUE(testGoodUri("http://[2010:836B:4179::836B:4179]"));
	ASSERT_TRUE(testGoodUri("//[2010:836B:4179::836B:4179]"));

	// Random other things that crop up
	ASSERT_TRUE(testGoodUri("http://example/Andr&#567;"));
	ASSERT_TRUE(testGoodUri("file:///C:/DEV/Haskell/lib/HXmlToolbox-3.01/examples/"));
}



TEST(FourSuite, BadUriReferences) {
	ASSERT_TRUE(testBadUri("beepbeep\x07\x07", 8));
	ASSERT_TRUE(testBadUri("\n", 0));
	ASSERT_TRUE(testBadUri("::", 0)); // not OK, per Roy Fielding on the W3C uri list on 2004-04-01

	// the following test cases are from a Perl script by David A. Wheeler
	// at http://www.dwheeler.com/secure-programs/url.pl
	ASSERT_TRUE(testBadUri("http://www yahoo.com", 10));
	ASSERT_TRUE(testBadUri("http://www.yahoo.com/hello world/", 26));
	ASSERT_TRUE(testBadUri("http://www.yahoo.com/yelp.html#\"", 31));

	// the following test cases are from a Haskell program by Graham Klyne
	// at http://www.ninebynine.org/Software/HaskellUtils/Network/URITest.hs
	ASSERT_TRUE(testBadUri("[2010:836B:4179::836B:4179]", 0));
	ASSERT_TRUE(testBadUri(" ", 0));
	ASSERT_TRUE(testBadUri("%", 1));
	ASSERT_TRUE(testBadUri("A%Z", 2));
	ASSERT_TRUE(testBadUri("%ZZ", 1));
	ASSERT_TRUE(testBadUri("%AZ", 2));
	ASSERT_TRUE(testBadUri("A C", 1));
	ASSERT_TRUE(testBadUri("A\\'C", 1)); // r"A\'C"
	ASSERT_TRUE(testBadUri("A`C", 1));
	ASSERT_TRUE(testBadUri("A<C", 1));
	ASSERT_TRUE(testBadUri("A>C", 1));
	ASSERT_TRUE(testBadUri("A^C", 1));
	ASSERT_TRUE(testBadUri("A\\\\C", 1)); // r'A\\C'
	ASSERT_TRUE(testBadUri("A{C", 1));
	ASSERT_TRUE(testBadUri("A|C", 1));
	ASSERT_TRUE(testBadUri("A}C", 1));
	ASSERT_TRUE(testBadUri("A[C", 1));
	ASSERT_TRUE(testBadUri("A]C", 1));
	ASSERT_TRUE(testBadUri("A[**]C", 1));
	ASSERT_TRUE(testBadUri("http://[xyz]/", 8));
	ASSERT_TRUE(testBadUri("http://]/", 7));
	ASSERT_TRUE(testBadUri("http://example.org/[2010:836B:4179::836B:4179]", 19));
	ASSERT_TRUE(testBadUri("http://example.org/abc#[2010:836B:4179::836B:4179]", 23));
	ASSERT_TRUE(testBadUri("http://example.org/xxx/[qwerty]#a[b]", 23));

	// from a post to the W3C uri list on 2004-02-17
	// breaks at 22 instead of 17 because everything up to that point is a valid userinfo
	ASSERT_TRUE(testBadUri("http://w3c.org:80path1/path2", 22));
}



namespace {

bool normalizeAndCompare(const char * uriText,
		const char * expectedNormalized) {
	UriParserStateA stateA;
	int res;

	UriUriA testUri;
	stateA.uri = &testUri;
	res = uriParseUriA(&stateA, uriText);
	if (res != 0) {
		uriFreeUriMembersA(&testUri);
		return false;
	}

	// Expected result
	UriUriA expectedUri;
	stateA.uri = &expectedUri;
	res = uriParseUriA(&stateA, expectedNormalized);
	if (res != 0) {
		uriFreeUriMembersA(&testUri);
		uriFreeUriMembersA(&expectedUri);
		return false;
	}

	res = uriNormalizeSyntaxA(&testUri);
	if (res != 0) {
		uriFreeUriMembersA(&testUri);
		uriFreeUriMembersA(&expectedUri);
		return false;
	}

	const bool equalAfter = (URI_TRUE == uriEqualsUriA(&testUri, &expectedUri));
	uriFreeUriMembersA(&testUri);
	uriFreeUriMembersA(&expectedUri);
	return equalAfter;
}

}  // namespace



TEST(FourSuite, CaseNormalizationTests) {
	ASSERT_TRUE(normalizeAndCompare("HTTP://www.EXAMPLE.com/", "http://www.example.com/"));
	ASSERT_TRUE(normalizeAndCompare("example://A/b/c/%7bfoo%7d", "example://a/b/c/%7Bfoo%7D"));
}



TEST(FourSuite, PctEncNormalizationTests) {
	ASSERT_TRUE(normalizeAndCompare("http://host/%7Euser/x/y/z", "http://host/~user/x/y/z"));
	ASSERT_TRUE(normalizeAndCompare("http://host/%7euser/x/y/z", "http://host/~user/x/y/z"));
}



TEST(FourSuite, PathSegmentNormalizationTests) {
	ASSERT_TRUE(normalizeAndCompare("/a/b/../../c", "/c"));
	// ASSERT_TRUE(normalizeAndCompare("a/b/../../c", "a/b/../../c"));
	// Fixed:
	ASSERT_TRUE(normalizeAndCompare("a/b/../../c", "c"));
	ASSERT_TRUE(normalizeAndCompare("/a/b/././c", "/a/b/c"));
	// ASSERT_TRUE(normalizeAndCompare("a/b/././c", "a/b/././c"));
	// Fixed:
	ASSERT_TRUE(normalizeAndCompare("a/b/././c", "a/b/c"));
	ASSERT_TRUE(normalizeAndCompare("/a/b/../c/././d", "/a/c/d"));
	// ASSERT_TRUE(normalizeAndCompare("a/b/../c/././d", "a/b/../c/././d"));
	// Fixed:
	ASSERT_TRUE(normalizeAndCompare("a/b/../c/././d", "a/c/d"));
}
