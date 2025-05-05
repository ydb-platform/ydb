#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/issue/yql_issue.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include <library/cpp/unicode/normalization/normalization.h>

#include <util/charset/utf8.h>
#include <util/charset/wide.h>
#include <util/string/builder.h>

#include <google/protobuf/message.h>

using namespace google::protobuf;
using namespace NYdb::NIssue;

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
}

Y_UNIT_TEST_SUITE(ToMessage) {
    Y_UNIT_TEST(NonUtf8) {
        const std::string nonUtf8String = "\x7f\xf8\xf7\xff\xf8\x1f\xff\xf2\xaf\xbf\xfe\xfa\xf5\x7f\xfe\xfa\x27\x20\x7d\x20\x5d\x2e";
        UNIT_ASSERT(!IsUtf(nonUtf8String));
        TIssue issue;
        issue.SetMessage(nonUtf8String);

        Ydb::Issue::IssueMessage msg;
        IssueToMessage(issue, &msg);
        NYdb::TStringType serialized;
        UNIT_ASSERT(msg.SerializeToString(&serialized));
        Ydb::Issue::IssueMessage msg2;
        UNIT_ASSERT(msg2.ParseFromString(serialized));
    }
}

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
}

Y_UNIT_TEST_SUITE(EscapeNonUtf8) {
    Y_UNIT_TEST(Escape) {
        const std::string nonUtf8String = "\xfe\xfa\xf5\xc2";
        UNIT_ASSERT(!IsUtf(nonUtf8String));

        // Check that our escaping correctly processes unicode pairs
        const std::string toNormalize = "Ёлка";
        const std::string nfd = WideToUTF8(Normalize<NUnicode::ENormalization::NFD>(UTF8ToWide(toNormalize))); // dots over 'ё' will be separate unicode symbol
        const std::string nfc = WideToUTF8(Normalize<NUnicode::ENormalization::NFC>(UTF8ToWide(toNormalize))); // dots over 'ё' will be with with their letter
        UNIT_ASSERT_STRINGS_UNEQUAL(nfc, nfd);
        std::pair<std::string, std::string> nonUtf8Messages[] = {
            { nonUtf8String, "????" },
            { TStringBuilder() << nonUtf8String << "Failed to parse file " << nonUtf8String << "עברית" << nonUtf8String, "????Failed to parse file ????עברית????" },
            { nfd, nfd },
            { nfc, nfc },
            { TStringBuilder() << nfc << nonUtf8String << nfd, TStringBuilder() << nfc << "????" << nfd },
            { TStringBuilder() << nfd << nonUtf8String << nfc, TStringBuilder() << nfd << "????" << nfc },
        };

        for (const auto& [src, dst] : nonUtf8Messages) {
            TIssue issue(src);
            UNIT_ASSERT_STRINGS_EQUAL(issue.GetMessage(), dst);
        }
    }
}
