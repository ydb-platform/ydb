#include "mlp.h"

#include <ydb/core/persqueue/public/utils.h>

namespace NKikimr::NPQ::NMLP {

std::optional<TWriteBlockedError> GetWriteBlockedError(const NKikimrPQ::TPQTabletConfig& config) {
    const auto error = GetWriteModeError(config);
    if (!error) {
        return std::nullopt;
    }
    const auto status = error->Code == NPersQueue::NErrorCode::BAD_REQUEST
        ? Ydb::StatusIds::BAD_REQUEST
        : Ydb::StatusIds::UNAVAILABLE;
    return TWriteBlockedError{
        .Status = status,
        .Description = error->Reason,
    };
}

} // namespace NKikimr::NPQ::NMLP
