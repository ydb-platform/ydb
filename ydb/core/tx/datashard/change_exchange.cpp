#include "change_exchange.h"

#include <util/string/builder.h>

namespace NKikimr::NDataShard {

/// TEvAddSender
TEvChangeExchange::TEvAddSender::TEvAddSender(const TTableId& userTableId, TEvChangeExchange::ESenderType type, const TPathId& pathId)
    : UserTableId(userTableId)
    , Type(type)
    , PathId(pathId)
{
}

TString TEvChangeExchange::TEvAddSender::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " UserTableId: " << UserTableId
        << " Type: " << Type
        << " PathId: " << PathId
    << " }";
}

/// TEvRemoveSender
TEvChangeExchange::TEvRemoveSender::TEvRemoveSender(const TPathId& pathId)
    : PathId(pathId)
{
}

TString TEvChangeExchange::TEvRemoveSender::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " PathId: " << PathId
    << " }";
}

}
