#include "service.h"

namespace NKikimr::NGRpcService::NYdbOverFq {

using NPerms = NKikimr::TEvTicketParser::TEvAuthorizeTicket;

namespace {

NFederatedQuery::TPermissionsVec GetFqCreateQueryPermissions() {
    return {
        NPerms::Required("yq.queries.create"),
        NPerms::Required("yq.queries.invoke"), // always execute_mode = ExecuteMode::RUN
        NPerms::Required("yq.resources.managePublic") // always visibility = FederatedQuery::Acl::SCOPE
    };
}

NFederatedQuery::TPermissionsVec GetFqDescribeQueryPermissions() {
    return {
        NPerms::Required("yq.queries.get"),
        NPerms::Optional("yq.queries.viewAst"),
        NPerms::Optional("yq.resources.viewPublic"),
        NPerms::Optional("yq.resources.viewPrivate"),
        NPerms::Optional("yq.queries.viewQueryText")
    };
}

NFederatedQuery::TPermissionsVec GetFqGetQueryStatusPermissions() {
    return {
        NPerms::Required("yq.queries.getStatus"),
        NPerms::Optional("yq.resources.viewPublic"),
        NPerms::Optional("yq.resources.viewPrivate")
    };
}

NFederatedQuery::TPermissionsVec GetFqGetResultDataPermissions() {
    return {
        NPerms::Required("yq.queries.getData"),
        NPerms::Optional("yq.resources.viewPublic"),
        NPerms::Optional("yq.resources.viewPrivate")
    };
}

template <typename TCombiner>
void AddTo(NFederatedQuery::TPermissionsVec& tgt, const NFederatedQuery::TPermissionsVec& src, TCombiner combine) {
    for (const auto& srcPerm : src) {
        bool added = false;
        for (auto& tgtPerm : tgt) {
            if (tgtPerm.Permission == srcPerm.Permission) {
                tgtPerm.Required = combine(tgtPerm.Required, srcPerm.Required);
                added = true;
                break;
            }
        }
        if (!added) {
            tgt.push_back(srcPerm);
        }
    }
}

template <typename... TPermissionsVecs>
NFederatedQuery::TPermissionsVec All(const TPermissionsVecs&... perms) {
    NFederatedQuery::TPermissionsVec initial;
    (AddTo(initial, perms, std::bit_or<bool>{}), ...);
    return initial;
}

template <typename... TPermissionsVecs>
NFederatedQuery::TPermissionsVec Any(const TPermissionsVecs&... perms) {
    NFederatedQuery::TPermissionsVec initial;
    (AddTo(initial, perms, std::bit_and<bool>{}), ...);
    return initial;
}
}

NFederatedQuery::TPermissionsVec GetCreateSessionPermissions() {
    return Any(
        GetFqCreateQueryPermissions(),
        GetFqGetQueryStatusPermissions(),
        GetFqDescribeQueryPermissions(),
        GetFqGetResultDataPermissions()
    );
}

NFederatedQuery::TPermissionsVec GetExecuteDataQueryPermissions() {
    return All(
        GetFqCreateQueryPermissions(),
        GetFqGetQueryStatusPermissions(),
        GetFqDescribeQueryPermissions(),
        GetFqGetResultDataPermissions()
    );
}

}