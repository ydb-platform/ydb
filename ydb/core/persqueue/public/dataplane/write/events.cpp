#include "events.h"

namespace NKikimr::NPQ::NDataplane::NWrite {

TEvDieCommand::TEvDieCommand(
        TString reason,
        Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
        std::optional<Ydb::StatusIds::StatusCode> statusOverride)
    : Reason(std::move(reason))
    , ErrorCode(errorCode)
    , StatusOverride(statusOverride)
{
}

TEvUnauthenticated::TEvUnauthenticated(TString reason)
    : Reason(std::move(reason))
{
}

TEvConsumedRequestUnits::TEvConsumedRequestUnits(ui64 amount)
    : Amount(amount)
{
}

} // namespace NKikimr::NPQ::NDataplane::NWrite
