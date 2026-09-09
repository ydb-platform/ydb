#include "statuses.h"

#include <ydb/library/http_proxy/error/error.h>
#include <ydb/core/ymq/error/error.h>

namespace NKikimr::NSqsTopic::V1 {

    TMappedDescriberError MapDescriberStatus(const TString& topicPath, NKikimr::NPQ::NDescriber::EStatus status) {
        TMappedDescriberError result{
            .DescriberStatus = status,
        };
        switch (status) {
            using enum NKikimr::NPQ::NDescriber::EStatus;
            case SUCCESS:
                break;
            case NOT_FOUND:
            case NOT_TOPIC:
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, NPQ::NDescriber::Description(topicPath, status));
                break;
            case UNAUTHORIZED:
            case UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::ACCESS_DENIED, NPQ::NDescriber::Description(topicPath, status));
                break;
            case BAD_REQUEST:
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::INVALID_PARAMETER_VALUE, NPQ::NDescriber::Description(topicPath, status));
                break;
            case UNKNOWN_ERROR:
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::INTERNAL_FAILURE, NPQ::NDescriber::Description(topicPath, status));
                break;
        }
        return result;
    }

    TTopicDescribePolicy ExistingQueuePolicy(TMaybe<TString> cdcUnsupportedMessage) {
        TTopicDescribePolicy policy;
        policy.CdcUnsupportedMessage = std::move(cdcUnsupportedMessage);
        return policy;
    }

    TTopicDescribePolicy CreateQueueDescribePolicy() {
        TTopicDescribePolicy policy;
        policy.CdcUnsupportedMessage = TString("Creating the changefeed is not supported");
        policy.NotTopicError = &NSQS::NErrors::INVALID_PARAMETER_VALUE;
        policy.NotFoundIsError = false;
        return policy;
    }

    TTopicDescribePolicy DeleteQueueDescribePolicy() {
        TTopicDescribePolicy policy;
        policy.CdcUnsupportedMessage = TString("Deleting the changefeed is not supported");
        policy.UnknownErrorMessage = "Failed to describe topic";
        return policy;
    }

    TTopicDescribePolicy SetQueueAttributesDescribePolicy() {
        TTopicDescribePolicy policy;
        policy.NotTopicError = &NSQS::NErrors::INVALID_PARAMETER_VALUE;
        return policy;
    }

    TTopicDescribePolicy GetQueueAttributesDescribePolicy() {
        TTopicDescribePolicy policy;
        policy.NotTopicMessage = TString(SPECIFIED_QUEUE_DOES_NOT_EXIST);
        return policy;
    }

    const NPQ::NDescriber::TTopicInfo* TakeSingleTopic(
        const NPQ::NDescriber::TEvDescribeTopicsResponse& response)
    {
        if (response.Topics.size() != 1) {
            return nullptr;
        }
        return &response.Topics.begin()->second;
    }

    TMaybe<NSQS::TError> MapTopicInfoToSqsError(
        const TString& topicPath,
        const NPQ::NDescriber::TTopicInfo& info,
        const TTopicDescribePolicy& policy)
    {
        using enum NPQ::NDescriber::EStatus;
        const TString describePath = info.RealPath ? info.RealPath : topicPath;
        switch (info.Status) {
            case SUCCESS:
                if (info.CdcStream && policy.CdcUnsupportedMessage) {
                    return MakeError(NSQS::NErrors::UNSUPPORTED_OPERATION, *policy.CdcUnsupportedMessage);
                }
                if (!info.Info) {
                    return MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                        "Failed to describe topic: creation is not completed");
                }
                return Nothing();
            case NOT_TOPIC:
                return MakeError(*policy.NotTopicError, policy.NotTopicMessage);
            case NOT_FOUND:
                if (!policy.NotFoundIsError) {
                    return Nothing();
                }
                return MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE, policy.NotFoundMessage);
            case UNAUTHORIZED:
                return MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE, policy.NotFoundMessage);
            case UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                return MakeError(NSQS::NErrors::ACCESS_DENIED, "Access denied");
            case BAD_REQUEST:
                return MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE,
                    NPQ::NDescriber::Description(topicPath, info.Status));
            case UNKNOWN_ERROR:
                if (!policy.UnknownErrorMessage.empty()) {
                    return MakeError(NSQS::NErrors::INTERNAL_FAILURE, policy.UnknownErrorMessage);
                }
                return MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                    NPQ::NDescriber::Description(describePath, info.Status));
        }
        return MakeError(NSQS::NErrors::INTERNAL_FAILURE, "Failed to describe topic");
    }

} // namespace NKikimr::NSqsTopic::V1
