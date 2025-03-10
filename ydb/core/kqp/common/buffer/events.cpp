#include "events.h"

namespace NKikimr {
namespace NKqp {

TEvKqpBuffer::TEvError::TEvError(
    const TString& message,
    NYql::NDqProto::StatusIds::StatusCode statusCode,
    const NYql::TIssues& subIssues)
    : Message(message)
    , StatusCode(statusCode)
    , SubIssues(subIssues) {
}

}
}
