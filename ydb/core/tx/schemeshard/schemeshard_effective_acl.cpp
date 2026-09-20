#include "schemeshard_effective_acl.h"

#include <ydb/library/aclib/protos/identity/user_token.pb.h>

#include <algorithm>

namespace NKikimr {
namespace NSchemeShard {

TString TEffectiveACL::StripConnectRightACE(const TString& acl) {
    NACLibProto::TSecurityObject obj;
    Y_ABORT_UNLESS(obj.MutableACL()->ParseFromString(acl));

    auto* aces = obj.MutableACL()->MutableACE();
    aces->erase(
        std::remove_if(aces->begin(), aces->end(), [](const NACLibProto::TACE& ace) {
            return ace.GetAccessRight() == NACLib::EAccessRights::ConnectDatabase;
        }),
        aces->end());

    return obj.GetACL().SerializeAsString();
}

void TEffectiveACL::Init(const TString &effectiveACL)
{
    NACLibProto::TSecurityObject object;
    Y_ABORT_UNLESS(object.MutableACL()->ParseFromString(effectiveACL));

    Inited = true;
    Split(object);

    // InheritOnly is not filtered from self effective.
    // Record with InheritOnly doesn't counted at CheckAccess
    // InheritOnly flag is eliminated on inheriting
    // so here is enough to copy acl without filtering InheritOnly flags
    ForSelf = effectiveACL;
}

void TEffectiveACL::Update(const TEffectiveACL &parent, const TString &selfACL, bool isContainer, bool isTenantRoot)
{
    Y_DEBUG_ABORT_UNLESS(parent);

    if (!selfACL) {
        InheritFrom(parent, isContainer, isTenantRoot);
        return;
    }

    const TString noMatterOwner;

    NACLib::TSecurityObject parentObj(noMatterOwner, true);
    Y_ABORT_UNLESS(parentObj.MutableACL()->ParseFromString(parent.GetForChildren(isContainer)));

    NACLib::TSecurityObject selfObj(noMatterOwner, isContainer);
    Y_ABORT_UNLESS(selfObj.MutableACL()->ParseFromString(selfACL));

    NACLib::TSecurityObject effectiveObj = selfObj.MergeWithParent(parentObj); // merge is needed due to Deny records

    Inited = true;
    Split(effectiveObj);
    ForSelf = effectiveObj.GetACL().SerializeAsString();

    if (isTenantRoot) {
        // At the tenant boundary strip ConnectDatabase ACEs from children ACLs
        // so that connect right does not leak into child objects of the tenant.
        // ForSelf is kept as is: connect must be present on the tenant root.
        ForContainers = StripConnectRightACE(ForContainers);
        ForObjects = StripConnectRightACE(ForObjects);
    }
}

void TEffectiveACL::InheritFrom(const TEffectiveACL &parent, bool isContainer, bool isTenantRoot) {
    Inited = parent.Inited;

    ForContainers = parent.ForContainers;
    ForObjects = parent.ForObjects;

    ForSelf = parent.GetForChildren(isContainer);

    if (isTenantRoot) {
        // At the tenant boundary strip ConnectDatabase ACEs from children ACLs
        // so that connect right does not leak into child objects of the tenant.
        // ForSelf is kept as is: connect must be present on the tenant root.
        ForContainers = StripConnectRightACE(ForContainers);
        ForObjects = StripConnectRightACE(ForObjects);
    }
}

void TEffectiveACL::Split(const NACLibProto::TSecurityObject &obj) {
    auto equalResult = Filter(obj, NACLib::EInheritanceType::InheritContainer, ForContainers);

    if (equalResult) {
        ForObjects = ForContainers;
    } else {
        Filter(obj, NACLib::EInheritanceType::InheritObject, ForObjects);
    }
}

bool TEffectiveACL::Filter(const NACLibProto::TSecurityObject &obj, NACLib::EInheritanceType byType, TString &result) {
    bool resultsIsEqualForAllInheritTypes = true;

    NACLibProto::TSecurityObject filtered;

    for (const auto& ace: obj.GetACL().GetACE()) {
        resultsIsEqualForAllInheritTypes = resultsIsEqualForAllInheritTypes &&
                !((ace.GetInheritanceType() & NACLib::EInheritanceType::InheritContainer) ^ (ace.GetInheritanceType() & NACLib::EInheritanceType::InheritObject));

        if (ace.GetInheritanceType() & byType) {
            NACLibProto::TACE& addition = *filtered.MutableACL()->AddACE();
            addition.CopyFrom(ace);
            addition.SetInherited(true);
            addition.SetInheritanceType(addition.GetInheritanceType() & ~NACLib::EInheritanceType::InheritOnly);
        }
    }

    result = filtered.GetACL().SerializeAsString();

    return resultsIsEqualForAllInheritTypes;
}

}
}
