#include "error.h"

#include <ydb/core/ymq/error/error.h>

namespace NKikimr::NSqsTopic::V1 {

    NSQS::TError MakeError(const NKikimr::NSQS::TErrorClass& errorClass, const TString& message) {
        NSQS::TError error;
        MakeError(&error, errorClass, message);
        return error;
    }

    bool IsSenderFailure(const NSQS::TError& error) {
        const auto code = error.GetStatus();
        return 400 <= code && code < 500;
    }

} // namespace NKikimr::NSqsTopic::V1
