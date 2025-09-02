#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/public/issue/protos/issue_message.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/library/yql/public/ydb_issue/ydb_issue_message.h>

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
