#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/datetime/base.h>

#include <vector>

namespace NYql {
namespace NDq {

struct TIssueInfo {
    TIssues Issues;
    TInstant OccuredAt;
};

// Simple circular buffer
struct TIssuesBuffer {
public:
    explicit TIssuesBuffer(ui32 capacity);

    ui32 GetAllAddedIssuesCount() const;
    void Push(const TIssues& issues);
    std::vector<TIssueInfo> Dump();

private:
    void Clear();

private:
    ui32 Capacity;
    ui32 AllAddedIssuesCount = 0;

    std::vector<TIssueInfo> Issues;
    ui32 HeadIndex = 0;
    ui32 ItemsCount = 0;
};

}
}
