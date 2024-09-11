#pragma once

#include <util/generic/yexception.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYql {

// This exception can separate code line and file name from the error message 
struct TCodeLineException: public yexception {

    TSourceLocation SourceLocation;
    mutable TString Message;
    TIssueCode Code;

    TCodeLineException(TIssueCode code);

    TCodeLineException(const TSourceLocation& sl, const TCodeLineException& t);

    virtual const char* what() const noexcept override;

    const char* GetRawMessage() const;

};

TCodeLineException operator+(const TSourceLocation& sl, TCodeLineException&& t);

#define YQL_ENSURE_CODELINE(CONDITION, CODE, ...)     \
    do {                                   \
        if (Y_UNLIKELY(!(CONDITION))) {    \
            ythrow TCodeLineException(CODE) << __VA_ARGS__; \
        }                                  \
    } while (0)

} // namespace NYql