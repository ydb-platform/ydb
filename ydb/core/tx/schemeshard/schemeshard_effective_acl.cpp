#include "schemeshard_effective_acl.h"

namespace NKikimr {
namespace NSchemeShard {

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

void TEffectiveACL::Update(const TEffectiveACL &parent, const TString &selfACL, bool isContainer)
{
    Y_DEBUG_ABORT_UNLESS(parent);

    if (!selfACL) {
        InheritFrom(parent, isContainer);
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
}

void TEffectiveACL::InheritFrom(const TEffectiveACL &parent, bool isContainer) {
    Inited = parent.Inited;

    ForContainers = parent.ForContainers;
    ForObjects = parent.ForObjects;

    ForSelf = isContainer ? ForContainers : ForObjects;
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
