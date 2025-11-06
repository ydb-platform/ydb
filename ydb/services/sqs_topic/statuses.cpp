#include "statuses.h"

#include <ydb/services/datastreams/codes/datastreams_codes.h>

namespace NKikimr::NSqsTopic::V1 {

    TMappedDescriberError MapDescriberStatus(const TString& topicPath, NKikimr::NPQ::NDescriber::EStatus status) {
        TMappedDescriberError result{
            .DescriberStatus = status,
            .Error = {
                .Text = NPQ::NDescriber::Description(topicPath, status),
            },
        };

        switch (status) {
            using enum NKikimr::NPQ::NDescriber::EStatus;
            case SUCCESS:
                result.Error.StatusCode = Ydb::StatusIds::SUCCESS;
                result.Error.IssueCode = NYds::EErrorCodes::OK;
                break;
            case NOT_FOUND:
            case NOT_TOPIC:
                result.Error.StatusCode = Ydb::StatusIds::NOT_FOUND;
                result.Error.IssueCode = NYds::EErrorCodes::NOT_FOUND;
                break;
            case UNAUTHORIZED:
                result.Error.StatusCode = Ydb::StatusIds::UNAUTHORIZED;
                result.Error.IssueCode = NYds::EErrorCodes::ACCESS_DENIED;
                break;
            case UNKNOWN_ERROR:
                result.Error.StatusCode = Ydb::StatusIds::INTERNAL_ERROR;
                result.Error.IssueCode = NYds::EErrorCodes::ERROR;
                break;
        }
        return result;
    }
} // namespace NKikimr::NSqsTopic::V1
