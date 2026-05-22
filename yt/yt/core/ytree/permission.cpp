#include "permission.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> FormatPermissions(EPermissionSet permissions)
{
    std::vector<std::string> result;
    for (auto value : TEnumTraits<EPermission>::GetDomainValues()) {
        if (Any(permissions & value)) {
            result.push_back(FormatEnum(value));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
