#pragma once

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/yexception.h>
#include <util/generic/fwd.h>

namespace NAST {
static const char* INVALID_TOKEN_NAME = "nothing";
static const char* ABSENCE = " absence";

class TTooManyErrors: public yexception {
};

class IErrorCollector {
public:
    explicit IErrorCollector(size_t maxErrors);
    virtual ~IErrorCollector();

    // throws TTooManyErrors
    void Error(ui32 line, ui32 col, const TString& message);

    // throws TTooManyErrors
    void Report(NYql::TIssue&& issue);

private:
    void GuardTooManyErrors();

    virtual void AddError(ui32 line, ui32 col, const TString& message) = 0;

    virtual void AddIssue(NYql::TIssue&& issue) = 0;

protected:
    const size_t MaxErrors_;
    size_t NumErrors_;
};

} // namespace NAST
