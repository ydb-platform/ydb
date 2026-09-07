#include "events.h"

namespace NKikimr {
namespace NKqp {

TEvKqpBuffer::TEvError::TEvError(
    NYql::NDqProto::StatusIds::StatusCode statusCode,
    NYql::TIssues&& issues,
    std::optional<NYql::NDqProto::TDqTaskStats>&& stats,
    TCommitDiagnostics&& commitDiagnostics)
    : StatusCode(statusCode)
    , Issues(std::move(issues))
    , Stats(std::move(stats))
    , CommitDiagnostics(std::move(commitDiagnostics)) {
}

}
}
