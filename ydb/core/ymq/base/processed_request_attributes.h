#pragma once

#include <ydb/core/ymq/base/action.h> 

#include <util/generic/string.h>

namespace NKikimr::NSQS {

struct TProcessedRequestAttributes {
    TProcessedRequestAttributes() = default;
    TProcessedRequestAttributes(TProcessedRequestAttributes&& rhs) = default;

    bool IsFifo = false;
    int HttpStatusCode = 0;

    ui64 RequestSizeInBytes = 0;
    ui64 ResponseSizeInBytes = 0;

    TString FolderId;
    TString ResourceId;
    TString SourceAddress;

    EAction Action;
};

} // namespace NKikimr::NSQS
