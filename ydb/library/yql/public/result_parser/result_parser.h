#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYql {

struct IResultVisitor {
    virtual ~IResultVisitor() = default;

    virtual void OnLabel(const TString& label) = 0;
    virtual void OnPosition(const TPosition& pos) = 0;

    virtual void OnWriteBegin() = 0;
    virtual void OnWriteEnd() = 0;

};

template<bool ThrowIfNotImplemented = true>
struct TResultVisitorBase : public IResultVisitor {

    void OnLabel(const TString& label) override;
    void OnPosition(const TPosition& pos)  override;

    void OnWriteBegin()  override;
    void OnWriteEnd() override;
};

struct TResultParseOptions {
    bool ParseTypesOnly = false;
};

void ParseResult(const std::string_view& yson, IResultVisitor& visitor, const TResultParseOptions& options = {});

}
