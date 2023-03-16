#pragma once

#include <util/generic/yexception.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>

namespace NFq {

// This exception can separate code line and file name from the error message 
struct TCodeLineException: public yexception {

    TSourceLocation SourceLocation;
    mutable TString Message;
    TIssuesIds::EIssueCode Code;

    TCodeLineException(TIssuesIds::EIssueCode code);

    TCodeLineException(const TSourceLocation& sl, const TCodeLineException& t);

    virtual const char* what() const noexcept override;

    const char* GetRawMessage() const;

};

TCodeLineException operator+(const TSourceLocation& sl, TCodeLineException&& t);

} // namespace NFq