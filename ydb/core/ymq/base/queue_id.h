#pragma once

#include <util/generic/string.h>

namespace NKikimr::NSQS {

TString MakeQueueId(const ui16 serviceId, const ui64 uniqueNum, const TString& accountName);

} // namespace NKikimr::NSQS
