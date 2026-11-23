#pragma once
#include "error.h"
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <util/generic/string.h>

namespace NKikimr::NSqsTopic::V1 {

    struct TMappedDescriberError {
        NKikimr::NPQ::NDescriber::EStatus DescriberStatus{};
        TMaybe<NSQS::TError> Error;
    };

    TMappedDescriberError MapDescriberStatus(const TString& topicPath, NKikimr::NPQ::NDescriber::EStatus status);

    // A prohibited write is a sender fault the client must not retry; an unavailable one is a retryable 503.
    NSQS::TError MapWriteModeError(Ydb::StatusIds::StatusCode status, const TString& message);

} // namespace NKikimr::NSqsTopic::V1
