#pragma once

#include <yql/essentials/parser/proto_ast/common.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NSQLTranslation {

class TErrorCollectorOverIssues : public NProtoAST::IErrorCollector {
public:
    TErrorCollectorOverIssues(NYql::TIssues& issues, size_t maxErrors, const TString& file)
        : IErrorCollector(maxErrors)
        , Issues_(issues)
        , File_(file)
    {
    }

private:
    void AddError(ui32 line, ui32 col, const TString& message) override {
        Issues_.AddIssue(NYql::TPosition(col, line, File_), message);
    }

private:
    NYql::TIssues& Issues_;
    const TString File_;
};

}  // namespace NSQLTranslation
