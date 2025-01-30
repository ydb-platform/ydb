#include "ydb_issue_message.h"
#include <yql/essentials/public/issue/yql_issue_message_impl.h>

#include <ydb/public/api/protos/ydb_issue_message.pb.h>

namespace NYql {

using namespace NIssue::NProto;

template
TIssue IssueFromMessage<Ydb::Issue::IssueMessage>(const Ydb::Issue::IssueMessage& issueMessage);
template
void IssuesFromMessage<Ydb::Issue::IssueMessage>(const ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& message, TIssues& issues);
template
void IssueToMessage<Ydb::Issue::IssueMessage>(const TIssue& topIssue, Ydb::Issue::IssueMessage* issueMessage);
template
void IssuesToMessage<Ydb::Issue::IssueMessage>(const TIssues& issues, ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* message);

TString IssueToBinaryMessage(const TIssue& issue) {
    TString result;
    Ydb::Issue::IssueMessage protobuf;
    IssueToMessage(issue, &protobuf);
    Y_PROTOBUF_SUPPRESS_NODISCARD protobuf.SerializeToString(&result);
    return result;
}

TIssue IssueFromBinaryMessage(const TString& binaryMessage) {
    Ydb::Issue::IssueMessage protobuf;
    if (!protobuf.ParseFromString(binaryMessage)) {
        ythrow yexception() << "unable to parse binary string as issue protobuf";
    }
    return IssueFromMessage(protobuf);
}

}

Y_DECLARE_OUT_SPEC(, google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>, stream, issues) {
    stream << JoinSeq("", issues);
}
