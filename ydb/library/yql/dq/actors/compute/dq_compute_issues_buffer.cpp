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

void TIssuesBuffer::Clear() {
    Issues.clear();
    Issues.resize(Capacity);
    HeadIndex = 0;
    ItemsCount = 0;
}

}
}
