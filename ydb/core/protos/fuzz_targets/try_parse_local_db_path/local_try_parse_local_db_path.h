#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr {
namespace NGRpcService {

TMaybe<ui64> TryParseLocalDbPath(const TVector<TString>& path);

} // namespace NGRpcService
} // namespace NKikimr
