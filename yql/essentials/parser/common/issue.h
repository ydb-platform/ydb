#pragma once

#include "error.h"

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>

namespace NSQLTranslation {

class TErrorCollectorOverIssues: public NAST::IErrorCollector {
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

    void AddIssue(NYql::TIssue&& issue) override {
        issue.Position.File = File_;
        issue.EndPosition.File = File_;
        Issues_.AddIssue(std::forward<NYql::TIssue>(issue));
    }

private:
    NYql::TIssues& Issues_;
    const TString File_;
};

} // namespace NSQLTranslation
