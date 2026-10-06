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
            case Success:
                break;
            case NotFound:
            case NotTopic:
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, NPQ::NDescriber::Description(topicPath, status));
                break;
            case Unauthorized:
            case UnauthorizedWithDescribeAccess:
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::ACCESS_DENIED, NPQ::NDescriber::Description(topicPath, status));
                break;
            case BadRequest:
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::INVALID_PARAMETER_VALUE, NPQ::NDescriber::Description(topicPath, status));
                break;
            case UnknownError:
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
            case Success:
                if (info.CdcStream && policy.CdcUnsupportedMessage) {
                    return MakeError(NSQS::NErrors::UNSUPPORTED_OPERATION, *policy.CdcUnsupportedMessage);
                }
                if (!info.Info) {
                    return MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                        "Failed to describe topic: creation is not completed");
                }
                return Nothing();
            case NotTopic:
                return MakeError(*policy.NotTopicError, policy.NotTopicMessage);
            case NotFound:
                if (!policy.NotFoundIsError) {
                    return Nothing();
                }
                return MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE, policy.NotFoundMessage);
            case Unauthorized:
                return MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE, policy.NotFoundMessage);
            case UnauthorizedWithDescribeAccess:
                return MakeError(NSQS::NErrors::ACCESS_DENIED, "Access denied");
            case BadRequest:
                return MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE,
                    NPQ::NDescriber::Description(topicPath, info.Status));
            case UnknownError:
                if (!policy.UnknownErrorMessage.empty()) {
                    return MakeError(NSQS::NErrors::INTERNAL_FAILURE, policy.UnknownErrorMessage);
                }
                return MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                    NPQ::NDescriber::Description(describePath, info.Status));
        }
        return MakeError(NSQS::NErrors::INTERNAL_FAILURE, "Failed to describe topic");
    }

} // namespace NKikimr::NSqsTopic::V1
