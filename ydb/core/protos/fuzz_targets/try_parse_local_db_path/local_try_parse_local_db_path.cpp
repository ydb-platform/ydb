#include "local_try_parse_local_db_path.h"

#include <util/string/cast.h>

namespace NKikimr {
namespace NGRpcService {

TMaybe<ui64> TryParseLocalDbPath(const TVector<TString>& path) {
    if (path.size() != 4 || path[1] != ".sys_tablets") {
        return {};
    }

    ui64 tabletId = -1;
    TString tabletIdStr = path[2];
    if (TryFromString<ui64>(tabletIdStr, tabletId)) {
        return tabletId;
    } else {
        return {};
    }
}

} // namespace NGRpcService
} // namespace NKikimr
