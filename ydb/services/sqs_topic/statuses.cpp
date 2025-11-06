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
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::ACCESS_DENIED, NPQ::NDescriber::Description(topicPath, status));
                break;
            case UNKNOWN_ERROR:
                MakeError(&result.Error.ConstructInPlace(), NKikimr::NSQS::NErrors::INTERNAL_FAILURE, NPQ::NDescriber::Description(topicPath, status));
                break;
        }
        return result;
    }
} // namespace NKikimr::NSqsTopic::V1
