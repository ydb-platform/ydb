#pragma once

extern "C" {
struct List;
}

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYql {

class IPGParseEvents {
public:
    virtual ~IPGParseEvents() = default;
    virtual void OnResult(const List* raw) = 0;
    virtual void OnError(const TIssue& issue) = 0;
};

TString PrintPGTree(const List* raw);

void PGParse(const TString& input, IPGParseEvents& events);

}
