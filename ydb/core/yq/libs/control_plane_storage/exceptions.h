#pragma once

#include <util/generic/yexception.h>
#include <util/system/compiler.h>

#include <ydb/core/yq/libs/config/protos/issue_id.pb.h>

namespace NYq {

struct TControlPlaneStorageException: public yexception {
    TSourceLocation SourceLocation;
    mutable TString Message;
    TIssuesIds::EIssueCode Code;

    TControlPlaneStorageException(TIssuesIds::EIssueCode code);

    TControlPlaneStorageException(const TSourceLocation& sl, const TControlPlaneStorageException& t);

    const char* what() const noexcept override;

    const char* GetRawMessage() const;
};

TControlPlaneStorageException operator+(const TSourceLocation& sl, TControlPlaneStorageException&& t);

} // namespace NYq
