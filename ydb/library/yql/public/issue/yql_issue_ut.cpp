#include "yql_issue.h"
#include "yql_issue_message.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/public/issue/protos/issue_message.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <library/cpp/unicode/normalization/normalization.h>

#include <util/charset/utf8.h>
#include <util/charset/wide.h>
#include <util/string/builder.h>

#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

using namespace google::protobuf;
using namespace NYql;

void ensureMessageTypesSame(const Descriptor* a, const Descriptor* b, THashSet<TString>* visitedTypes);
void ensureFieldDescriptorsSame(const FieldDescriptor* a, const FieldDescriptor* b, THashSet<TString>* visitedTypes) {
    UNIT_ASSERT(a);
    UNIT_ASSERT(b);

    UNIT_ASSERT_VALUES_EQUAL(FieldDescriptor::TypeName(a->type()), FieldDescriptor::TypeName(b->type()));
    UNIT_ASSERT_VALUES_EQUAL((int)a->label(), (int)b->label());
    UNIT_ASSERT_VALUES_EQUAL(a->name(), b->name());
    UNIT_ASSERT_VALUES_EQUAL(a->number(), b->number());
    UNIT_ASSERT_VALUES_EQUAL(a->is_repeated(), b->is_repeated());
    UNIT_ASSERT_VALUES_EQUAL(a->is_packed(), b->is_packed());
    UNIT_ASSERT_VALUES_EQUAL(a->index(), b->index());
    if (a->type() == FieldDescriptor::TYPE_MESSAGE || a->type() == FieldDescriptor::TYPE_GROUP) {
        ensureMessageTypesSame(a->message_type(), b->message_type(), visitedTypes);
    }
}

void ensureMessageTypesSame(const Descriptor* a, const Descriptor* b, THashSet<TString>* visitedTypes) {
    UNIT_ASSERT(a);
    UNIT_ASSERT(b);
    if (!visitedTypes->insert(a->name()).second) {
        return;
    }

    UNIT_ASSERT_VALUES_EQUAL(a->name(), b->name());
    UNIT_ASSERT_VALUES_EQUAL(a->field_count(), b->field_count());

    for (int i = 0; i < a->field_count(); i++) {
        ensureFieldDescriptorsSame(a->field(i), b->field(i), visitedTypes);
    }
}

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

Y_UNIT_TEST_SUITE(IssueProtoTest) {
    Y_UNIT_TEST(KikimrYqlSameLayout) {
        Ydb::Issue::IssueMessage yqlIssue;
        NYql::NIssue::NProto::IssueMessage kikimrIssue;
        THashSet<TString> visitedTypes;
        ensureMessageTypesSame(yqlIssue.GetDescriptor(), kikimrIssue.GetDescriptor(), &visitedTypes);
    }

    Y_UNIT_TEST(BinarySerialization) {
        TIssue issueTo("root_issue");
        TString bin = IssueToBinaryMessage(issueTo);
        TIssue issueFrom = IssueFromBinaryMessage(bin);
        UNIT_ASSERT_EQUAL(issueTo, issueFrom);
    }

    Y_UNIT_TEST(WrongBinStringException) {
        UNIT_ASSERT_EXCEPTION(IssueFromBinaryMessage("qqq"), yexception);
    }
}


Y_UNIT_TEST_SUITE(TextWalkerTest) {
    Y_UNIT_TEST(BasicTest) {
        TPosition pos;
        pos.Row = 1;

        TTextWalker walker(pos);
        walker.Advance(TStringBuf("a\r\taa"));

        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(5, 1));
        walker.Advance(TStringBuf("\na"));
        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(1, 2));
    }

    Y_UNIT_TEST(CrLfTest) {
        TPosition pos;
        pos.Row = 1;

        TTextWalker walker(pos);
        walker.Advance(TStringBuf("a\raa\r"));
        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(4, 1));
        walker.Advance('\n');
        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(4, 1));
        walker.Advance(TStringBuf("\r\r\ra"));
        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(4, 2));
        walker.Advance('\r');
        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(4, 2));
        walker.Advance('\n');
        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(4, 2));
        walker.Advance('a');
        UNIT_ASSERT_VALUES_EQUAL(pos, TPosition(1, 3));
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

Y_UNIT_TEST_SUITE(ToStreamTest) {
    template <typename TIssueMessage>
    void CheckSerializationToStream(const TIssues& issues, const TString& expected) {
        google::protobuf::RepeatedPtrField<TIssueMessage> protoIssues;
        IssuesToMessage(issues, &protoIssues);
        TStringBuilder stream;
        stream << protoIssues;
        UNIT_ASSERT_STRINGS_EQUAL(stream, expected);
        };

    Y_UNIT_TEST(OneMessageTest) {
        TIssues issues;
        issues.AddIssue("error");
        CheckSerializationToStream<Ydb::Issue::IssueMessage>(issues, "{ message: \"error\" severity: 1 }");
        CheckSerializationToStream<NYql::NIssue::NProto::IssueMessage>(issues, "{ message: \"error\" issue_code: 0 severity: 1 }");
    }

    Y_UNIT_TEST(SubIssuesTest) {
        TIssue issue(TPosition(12, 34, "file.abc"), "error");
        TIssue subissue("suberror");
        subissue.AddSubIssue(MakeIntrusive<TIssue>("subsuberror"));
        issue.AddSubIssue(MakeIntrusive<TIssue>(subissue));
        TIssues issues;
        issues.AddIssue(issue);
        CheckSerializationToStream<Ydb::Issue::IssueMessage>(issues, "{ position { row: 34 column: 12 file: \"file.abc\" } message: \"error\" end_position { row: 34 column: 12 } severity: 1 issues { message: \"suberror\" severity: 1 issues { message: \"subsuberror\" severity: 1 } } }");
        CheckSerializationToStream<NYql::NIssue::NProto::IssueMessage>(issues, "{ position { row: 34 column: 12 file: \"file.abc\" } message: \"error\" end_position { row: 34 column: 12 } issue_code: 0 severity: 1 issues { message: \"suberror\" issue_code: 0 severity: 1 issues { message: \"subsuberror\" issue_code: 0 severity: 1 } } }");
    }

    Y_UNIT_TEST(ManyIssuesTest) {
        TIssue issue(TPosition(12, 34, "file.abc"), "error");
        issue.AddSubIssue(MakeIntrusive<TIssue>("suberror"));
        TIssues issues;
        issues.AddIssue(issue);
        issues.AddIssue(TPosition(100, 2, "abc.file"), "my message");
        CheckSerializationToStream<Ydb::Issue::IssueMessage>(issues, "{ position { row: 34 column: 12 file: \"file.abc\" } message: \"error\" end_position { row: 34 column: 12 } severity: 1 issues { message: \"suberror\" severity: 1 } }{ position { row: 2 column: 100 file: \"abc.file\" } message: \"my message\" end_position { row: 2 column: 100 } severity: 1 }");
        CheckSerializationToStream<NYql::NIssue::NProto::IssueMessage>(issues, "{ position { row: 34 column: 12 file: \"file.abc\" } message: \"error\" end_position { row: 34 column: 12 } issue_code: 0 severity: 1 issues { message: \"suberror\" issue_code: 0 severity: 1 } }{ position { row: 2 column: 100 file: \"abc.file\" } message: \"my message\" end_position { row: 2 column: 100 } issue_code: 0 severity: 1 }");
    }
}

Y_UNIT_TEST_SUITE(ToMessage) {
    Y_UNIT_TEST(NonUtf8) {
        const TString nonUtf8String = "\x7f\xf8\xf7\xff\xf8\x1f\xff\xf2\xaf\xbf\xfe\xfa\xf5\x7f\xfe\xfa\x27\x20\x7d\x20\x5d\x2e";
        UNIT_ASSERT(!IsUtf(nonUtf8String));
        TIssue issue;
        issue.SetMessage(nonUtf8String);

        Ydb::Issue::IssueMessage msg;
        IssueToMessage(issue, &msg);
        TString serialized;
        UNIT_ASSERT(msg.SerializeToString(&serialized));
        Ydb::Issue::IssueMessage msg2;
        UNIT_ASSERT(msg2.ParseFromString(serialized));
    }
}

Y_UNIT_TEST_SUITE(EscapeNonUtf8) {
    Y_UNIT_TEST(Escape) {
        const TString nonUtf8String = "\xfe\xfa\xf5\xc2";
        UNIT_ASSERT(!IsUtf(nonUtf8String));

        // Check that our escaping correctly processes unicode pairs
        const TString toNormalize = "Ёлка";
        const TString nfd = WideToUTF8(Normalize<NUnicode::ENormalization::NFD>(UTF8ToWide(toNormalize))); // dots over 'ё' will be separate unicode symbol
        const TString nfc = WideToUTF8(Normalize<NUnicode::ENormalization::NFC>(UTF8ToWide(toNormalize))); // dots over 'ё' will be with with their letter
        UNIT_ASSERT_STRINGS_UNEQUAL(nfc, nfd);
        std::pair<TString, TString> nonUtf8Messages[] = {
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
