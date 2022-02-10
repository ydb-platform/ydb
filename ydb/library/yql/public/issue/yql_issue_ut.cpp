#include "yql_issue.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/public/issue/protos/issue_message.pb.h> 
#include <ydb/library/yql/public/issue/yql_issue_message.h> 
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

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
        UNIT_ASSERT_VALUES_EQUAL(issue1.Message, "тест abc");
        TIssue issue2("\xFF abc");
        UNIT_ASSERT_VALUES_EQUAL(issue2.Message, "? abc");
        TIssue issue3("");
        UNIT_ASSERT_VALUES_EQUAL(issue3.Message, "");
        TIssue issue4("abc");
        UNIT_ASSERT_VALUES_EQUAL(issue4.Message, "abc");
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
