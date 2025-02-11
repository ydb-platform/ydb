#include "common.h"

#include <ydb/core/base/path.h>
#include <ydb/core/sys_view/auth/sort_helpers.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr {
namespace NSysView {

TVector<std::pair<TString, TString>> ConvertACLToString(const ::NACLibProto::TACL& acl, bool effective) {
    TVector<std::pair<TString, TString>> permissions;
    for (const NACLibProto::TACE& ace : acl.GetACE()) {
        if (ace.GetAccessType() != (ui32)NACLib::EAccessType::Allow) {
            continue;
        }
        if (!effective && ace.GetInherited()) {
            continue;
        }
        if (!ace.HasSID()) {
            continue;
        }
        auto acePermissions = ConvertACLMaskToYdbPermissionNames(ace.GetAccessRight());
        for (const auto& permission : acePermissions) {
            permissions.emplace_back(ace.GetSID(), std::move(permission));
        }
    }
    // Note: due to rights inheritance permissions may be duplicated
    NAuth::SortBatch(permissions, [](const auto& left, const auto& right) {
        return left.first < right.first ||
            left.first == right.first && left.second < right.second;
    }, false);
    return permissions;
}

} // NSysView
} // NKikimr