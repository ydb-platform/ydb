#include "mlp.h"

#include <ydb/core/persqueue/public/utils.h>

namespace NKikimr::NPQ::NMLP {

std::optional<TWriteProhibitedError> GetWriteProhibitedError(const NKikimrPQ::TPQTabletConfig& config) {
    const auto error = GetWriteModeError(config);
    if (!error || error->Code != NPersQueue::NErrorCode::BAD_REQUEST) {
        return std::nullopt;
    }
    return TWriteProhibitedError{
        .Status = Ydb::StatusIds::BAD_REQUEST,
        .Description = error->Reason,
    };
}

} // namespace NKikimr::NPQ::NMLP
