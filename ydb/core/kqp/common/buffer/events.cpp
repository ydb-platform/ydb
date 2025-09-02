#include "events.h"

namespace NKikimr {
namespace NKqp {

TEvKqpBuffer::TEvError::TEvError(
    NYql::NDqProto::StatusIds::StatusCode statusCode,
    NYql::TIssues&& issues)
    : StatusCode(statusCode)
    , Issues(std::move(issues)) {
}

}
}
