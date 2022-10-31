#include "yql_issue.h"
#include "yql_issue_message.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/public/issue/protos/issue_message.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <util/charset/utf8.h>

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

Y_UNIT_TEST_SUITE(ToMessage) {
    Y_UNIT_TEST(NonUtf8) {
        TString s;
        int chars[] = {
            0x7f,
            0xf8,
            0xf7,
            0xff,
            0xf8,
            0x1f,
            0xff,
            0xf2,
            0xaf,
            0xbf,
            0xfe,
            0xfa,
            0xf5,
            0x7f,
            0xfe,
            0xfa,
            0x27,
            0x20,
            0x7d,
            0x20,
            0x5d,
            0x2e
        };
        for (int i : chars) {
            s.append(static_cast<char>(i));
        }
        UNIT_ASSERT(!IsUtf(s));
        TIssue issue;
        issue.SetMessage(s);

        Ydb::Issue::IssueMessage msg;
        IssueToMessage(issue, &msg);
        TString serialized;
        UNIT_ASSERT(msg.SerializeToString(&serialized));
        Ydb::Issue::IssueMessage msg2;
        UNIT_ASSERT(msg2.ParseFromString(serialized));
    }
}
