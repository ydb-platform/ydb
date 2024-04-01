#include "status_code_counters.h"

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

#include <util/string/builder.h>

#include <queue>



namespace NFq {

namespace {

TString SeverityToString(NYql::ESeverity severity) {
    switch (severity) {
        case NYql::TSeverityIds::S_FATAL:
            return "F";
        case NYql::TSeverityIds::S_ERROR:
            return "E";
        case NYql::TSeverityIds::S_INFO:
            return "I";
        case NYql::TSeverityIds::S_WARNING:
            return "W";
        case NYql::TSeverityIds_ESeverityId_TSeverityIds_ESeverityId_INT_MIN_SENTINEL_DO_NOT_USE_:
        case NYql::TSeverityIds_ESeverityId_TSeverityIds_ESeverityId_INT_MAX_SENTINEL_DO_NOT_USE_:
            return "U";
    }
}

}

TStatusCodeCounters::TStatusCodeCounters(const TString& name, const ::NMonitoring::TDynamicCounterPtr& counters)
    : Name(name)
    , Counters(counters) {
    SubGroup = counters->GetSubgroup("subcomponent", Name);
}

void TStatusCodeCounters::IncByStatusCode(NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& issues) {
    auto statusCodeName = NYql::NDqProto::StatusIds::StatusCode_Name(statusCode) + MetricsSuffixFromIssues(issues);
    auto it = CountersByStatusCode.find(statusCode);
    if (it == CountersByStatusCode.end()) {
        it = CountersByStatusCode.insert({statusCode, SubGroup->GetCounter(statusCodeName, true)}).first;
    }
    it->second->Inc();
}

TStatusCodeCounters::~TStatusCodeCounters() {
    Counters->RemoveSubgroup("subcomponent", Name);
}

TString MetricsSuffixFromIssues(const NYql::TIssues& issues) {
    static const size_t MAX_DEPTH = 5;
    TStringBuilder builder;
    std::queue<NYql::TIssue> issuesQueue;
    for (const auto& issue: issues) {
        if (issuesQueue.size() == MAX_DEPTH) {
            break;
        }
        issuesQueue.push(issue);
    }

    size_t currentDepth = 0;
    while (!issuesQueue.empty() && currentDepth != MAX_DEPTH) {
        currentDepth++;
        auto issue = issuesQueue.front();
        issuesQueue.pop();

        for (const auto& subIssue: issue.GetSubIssues()) {
            if (currentDepth + issuesQueue.size() == MAX_DEPTH) {
                break;
            }
            issuesQueue.push(*subIssue);
        }

        builder << "__" << NYql::TIssuesIds::EIssueCode_Name(issue.GetCode())
            << "__" << NFq::SeverityToString(issue.GetSeverity());
    }

    return builder;
}

} // namespace NFq
