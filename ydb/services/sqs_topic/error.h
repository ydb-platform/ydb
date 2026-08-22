#pragma once

#include <ydb/core/protos/sqs.pb.h>
#include <ydb/library/http_proxy/error/error.h>

#include <util/generic/string.h>

namespace NKikimr::NSqsTopic::V1 {

    NSQS::TError MakeError(const NKikimr::NSQS::TErrorClass& errorClass, const TString& message = TString());

    bool IsSenderFailure(const NSQS::TError& error);

} // namespace NKikimr::NSqsTopic::V1
