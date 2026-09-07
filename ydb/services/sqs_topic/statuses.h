#pragma once

#include "error.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/library/http_proxy/error/error.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NSqsTopic::V1 {

    inline constexpr TStringBuf QUEUE_USED_BY_ANOTHER_SCHEME_OBJECT = "Queue name used by another scheme object";
    inline constexpr TStringBuf SPECIFIED_QUEUE_DOES_NOT_EXIST = "The specified queue doesn't exist";

    struct TMappedDescriberError {
        NKikimr::NPQ::NDescriber::EStatus DescriberStatus{};
        TMaybe<NSQS::TError> Error;
    };

    TMappedDescriberError MapDescriberStatus(const TString& topicPath, NKikimr::NPQ::NDescriber::EStatus status);

    struct TTopicDescribePolicy {
        TMaybe<TString> CdcUnsupportedMessage;
        const NSQS::TErrorClass* NotTopicError = &NSQS::NErrors::NON_EXISTENT_QUEUE;
        TString NotTopicMessage = TString(QUEUE_USED_BY_ANOTHER_SCHEME_OBJECT);
        bool NotFoundIsError = true;
        TString NotFoundMessage = TString(SPECIFIED_QUEUE_DOES_NOT_EXIST);
        bool UnauthorizedAsNotFound = false;
        TString UnknownErrorMessage;
    };

    TTopicDescribePolicy ExistingQueuePolicy(TMaybe<TString> cdcUnsupportedMessage = {});
    TTopicDescribePolicy CreateQueueDescribePolicy();
    TTopicDescribePolicy DeleteQueueDescribePolicy();
    TTopicDescribePolicy SetQueueAttributesDescribePolicy();
    TTopicDescribePolicy GetQueueAttributesDescribePolicy();

    const NPQ::NDescriber::TTopicInfo* TakeSingleTopic(
        const NPQ::NDescriber::TEvDescribeTopicsResponse& response);

    TMaybe<NSQS::TError> MapTopicInfoToSqsError(
        const TString& topicPath,
        const NPQ::NDescriber::TTopicInfo& info,
        const TTopicDescribePolicy& policy);

} // namespace NKikimr::NSqsTopic::V1
