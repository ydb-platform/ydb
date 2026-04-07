#include "events.h"

namespace NKikimr {
namespace NKqp {

TEvKqpBuffer::TEvError::TEvError(
    NYql::NDqProto::StatusIds::StatusCode statusCode,
    NYql::TIssues&& issues,
    std::optional<NYql::NDqProto::TDqTaskStats>&& stats)
    : StatusCode(statusCode)
    , Issues(std::move(issues))
    , Stats(std::move(stats)) {
}

}
}
