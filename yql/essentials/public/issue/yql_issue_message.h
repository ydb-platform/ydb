#pragma once

#include "yql_issue.h"

#include <util/generic/ylimits.h>

namespace NYql {

namespace NIssue {
namespace NProto {
class IssueMessage;
}
}

template<typename TIssueMessage>
TIssue IssueFromMessage(const TIssueMessage& issueMessage);
template<typename TIssueMessage>
void IssuesFromMessage(const ::google::protobuf::RepeatedPtrField<TIssueMessage>& message, TIssues& issues);

template<typename TIssueMessage>
TString IssuesFromMessageAsString(const ::google::protobuf::RepeatedPtrField<TIssueMessage>& message) {
    TIssues issues;
    IssuesFromMessage(message, issues);
    return issues.ToOneLineString();
}

NIssue::NProto::IssueMessage IssueToMessage(const TIssue& topIssue);

template<typename TIssueMessage>
void IssueToMessage(const TIssue& topIssue, TIssueMessage* message);
template<typename TIssueMessage>
void IssuesToMessage(const TIssues& issues, ::google::protobuf::RepeatedPtrField<TIssueMessage>* message);

TString IssueToBinaryMessage(const TIssue& issue);
TIssue IssueFromBinaryMessage(const TString& binaryMessage);

}
