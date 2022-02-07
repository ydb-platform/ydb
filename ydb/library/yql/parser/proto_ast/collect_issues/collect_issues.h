#pragma once

#include <ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

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
