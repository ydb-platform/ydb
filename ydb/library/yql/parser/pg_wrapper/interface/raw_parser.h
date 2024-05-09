#pragma once

extern "C" {
struct List;
struct Node;
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

TString GetCommandName(Node* node);

void PGParse(const TString& input, IPGParseEvents& events);

}
