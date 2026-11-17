#include "change_exchange.h"

#include <util/generic/overloaded.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {

TEvChangeExchange::ESenderType GetSenderType(const TEvChangeExchange::TSenderParams& params) {
    using ESenderType = TEvChangeExchange::ESenderType;
    return std::visit(TOverloaded{
        [](const TEvChangeExchange::TAsyncIndexSender&) { return ESenderType::AsyncIndex; },
        [](const TEvChangeExchange::TCdcStreamSender&) { return ESenderType::CdcStream; },
    }, params);
}

/// TEvAddSender
TEvChangeExchange::TEvAddSender::TEvAddSender(const TTableId& userTableId, const TPathId& pathId, TSenderParams params)
    : UserTableId(userTableId)
    , Type(GetSenderType(params))
    , PathId(pathId)
    , Params(std::move(params))
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
