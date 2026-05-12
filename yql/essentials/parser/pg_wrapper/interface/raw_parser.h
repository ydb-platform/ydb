#pragma once

extern "C" {
    struct List;
    struct Node;
}

#include <yql/essentials/public/issue/yql_issue.h>

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

class TArenaMemoryContext;

class TPGParseResult {
public:
    void Visit(IPGParseEvents& events) const;

    TPGParseResult() = default;
    TPGParseResult(TPGParseResult&& other) = default;
    explicit TPGParseResult(TIssue&& issue);
    TPGParseResult(const List* raw, THolder<TArenaMemoryContext>&& arena);
    TPGParseResult& operator=(TPGParseResult&& other) = default;
    ~TPGParseResult();

private:
    using TAstData = std::pair<const List*, THolder<TArenaMemoryContext>>;
    using TData = std::variant<TAstData, TIssue>;

    TData Data_;
};

void PGParse(const TString& input, TPGParseResult& result);

} // namespace NYql
