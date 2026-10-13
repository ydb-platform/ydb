#include "dq_compute_issues_buffer.h"

#include <algorithm>

namespace NYql {
namespace NDq {

TIssuesBuffer::TIssuesBuffer(ui32 capacity)
    : Capacity(capacity)
    , Issues(capacity)
{
    Y_ENSURE(Capacity);
}

ui32 TIssuesBuffer::GetAllAddedIssuesCount() const {
    return AllAddedIssuesCount;
}

void TIssuesBuffer::Push(const TIssues& issues) {
    AllAddedIssuesCount++;

    // Retain a bounded set of warnings for the final response, even when
    // intermediate state reports drain the transient issue buffer.
    for (const auto& issue : issues) {
        if (Warnings.Size() == Capacity) {
            break;
        }
        if (issue.GetSeverity() == TSeverityIds::S_WARNING) {
            Warnings.AddIssue(issue);
        }
    }

    auto& issueInfo = Issues.at((HeadIndex + ItemsCount) % Capacity);
    issueInfo.Issues = issues;
    issueInfo.OccuredAt = TInstant::Now();

    if (ItemsCount < Capacity) {
        ItemsCount++;
        return;
    }

    HeadIndex = (HeadIndex + 1) % Capacity;
}

std::vector<TIssueInfo> TIssuesBuffer::Dump() {
    std::rotate(Issues.begin(), std::next(Issues.begin(), HeadIndex), Issues.end());
    Issues.resize(ItemsCount);
    auto res = std::move(Issues);
    Clear();
    return res;
}

TIssues TIssuesBuffer::TakeWarnings() {
    TIssues result = std::move(Warnings);
    Warnings.Clear();
    return result;
}

void TIssuesBuffer::Clear() {
    Issues.clear();
    Issues.resize(Capacity);
    HeadIndex = 0;
    ItemsCount = 0;
}

}
}
