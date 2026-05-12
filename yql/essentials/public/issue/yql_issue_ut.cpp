#include "yql_issue.h"
#include "yql_issue_message.h"

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/public/issue/protos/issue_message.pb.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/unicode/normalization/normalization.h>

#include <util/charset/utf8.h>
#include <util/charset/wide.h>
#include <util/string/builder.h>

#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

using namespace google::protobuf;
using namespace NYql;

Y_UNIT_TEST_SUITE(IssueTest) {
Y_UNIT_TEST(Ascii) {
    TIssue issue1("тест abc");
    UNIT_ASSERT_VALUES_EQUAL(issue1.GetMessage(), "тест abc");
    TIssue issue2("\xFF abc");
    UNIT_ASSERT_VALUES_EQUAL(issue2.GetMessage(), "? abc");
    TIssue issue3("");
    UNIT_ASSERT_VALUES_EQUAL(issue3.GetMessage(), "");
    TIssue issue4("abc");
    UNIT_ASSERT_VALUES_EQUAL(issue4.GetMessage(), "abc");
}
} // Y_UNIT_TEST_SUITE(IssueTest)

Y_UNIT_TEST_SUITE(TextWalkerTest) {
Y_UNIT_TEST(BasicTest) {
    TPosition pos;
    pos.Row = 1;

    TTextWalker walker(pos, false);
    walker.Advance(TStringBuf("a\r\taa"));

    UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(5, 1));
    walker.Advance(TStringBuf("\na"));
    UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(1, 2));
}

Y_UNIT_TEST(CrLfTest) {
    TPosition pos;
    pos.Row = 1;

    TTextWalker walker(pos, false);
    walker.Advance(TStringBuf("a\raa\r"));
    UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(5, 1));
    walker.Advance('\n');
    UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(0, 2));
    walker.Advance(TStringBuf("\r\r\ra"));
    UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(4, 2));
    walker.Advance('\r');
    UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(5, 2));
    walker.Advance('\n');
    UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(0, 3));
    walker.Advance('a');
    UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(1, 3));
}

Y_UNIT_TEST(UnicodeTest) {
    {
        TPosition pos;
        pos.Row = 1;

        TTextWalker walker(pos, false);
        walker.Advance(TStringBuf("привет"));

        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(12, 1));
    }

    {
        TPosition pos;
        pos.Row = 1;

        TTextWalker walker(pos, true);
        walker.Advance(TStringBuf("привет"));

        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(6, 1));
    }
}
} // Y_UNIT_TEST_SUITE(TextWalkerTest)

Y_UNIT_TEST_SUITE(ToOneLineStringTest) {
Y_UNIT_TEST(OneMessageTest) {
    TIssues issues;
    issues.AddIssue(TPosition(12, 34, "file.abc"), "error");
    UNIT_ASSERT_STRINGS_EQUAL(issues.ToOneLineString(), "{ file.abc:34:12: Error: error }");
}

Y_UNIT_TEST(SubIssuesTest) {
    TIssue issue(TPosition(12, 34, "file.abc"), "error");
    TIssue subissue("suberror");
    subissue.AddSubIssue(MakeIntrusive<TIssue>("subsuberror"));
    issue.AddSubIssue(MakeIntrusive<TIssue>(subissue));

    TIssues issues;
    issues.AddIssue(issue);
    UNIT_ASSERT_STRINGS_EQUAL(issues.ToOneLineString(), "{ file.abc:34:12: Error: error subissue: { <main>: Error: suberror subissue: { <main>: Error: subsuberror } } }");
}

Y_UNIT_TEST(ManyIssuesTest) {
    TIssue issue(TPosition(12, 34, "file.abc"), "error\n");
    issue.AddSubIssue(MakeIntrusive<TIssue>("suberror"));
    TIssues issues;
    issues.AddIssue(issue);
    issues.AddIssue(TPosition(100, 2, "abc.file"), "my\nmessage");
    UNIT_ASSERT_STRINGS_EQUAL(issues.ToOneLineString(), "[ { file.abc:34:12: Error: error subissue: { <main>: Error: suberror } } { abc.file:2:100: Error: my message } ]");
}
} // Y_UNIT_TEST_SUITE(ToOneLineStringTest)

Y_UNIT_TEST_SUITE(EscapeNonUtf8) {
Y_UNIT_TEST(Escape) {
    const TString nonUtf8String = "\xfe\xfa\xf5\xc2";
    UNIT_ASSERT(!IsUtf(nonUtf8String));

    // Check that our escaping correctly processes unicode pairs
    const TString toNormalize = "Ёлка";
    const TString nfd = WideToUTF8(Normalize<NUnicode::ENormalization::NFD>(UTF8ToWide(toNormalize))); // dots over 'ё' will be separate unicode symbol
    const TString nfc = WideToUTF8(Normalize<NUnicode::ENormalization::NFC>(UTF8ToWide(toNormalize))); // dots over 'ё' will be with with their letter
    UNIT_ASSERT_STRINGS_UNEQUAL(nfc, nfd);
    auto nonUtf8Messages = std::to_array<std::pair<TString, TString>>({
        {nonUtf8String, "????"},
        {TStringBuilder() << nonUtf8String << "Failed to parse file " << nonUtf8String << "עברית" << nonUtf8String, "????Failed to parse file ????עברית????"},
        {nfd, nfd},
        {nfc, nfc},
        {TStringBuilder() << nfc << nonUtf8String << nfd, TStringBuilder() << nfc << "????" << nfd},
        {TStringBuilder() << nfd << nonUtf8String << nfc, TStringBuilder() << nfd << "????" << nfc},
    });

    for (const auto& [src, dst] : nonUtf8Messages) {
        TIssue issue(src);
        UNIT_ASSERT_STRINGS_EQUAL(issue.GetMessage(), dst);
    }
}
} // Y_UNIT_TEST_SUITE(EscapeNonUtf8)
