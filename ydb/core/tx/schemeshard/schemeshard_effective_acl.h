#pragma once

#include <ydb/library/aclib/aclib.h>

namespace NACLibProto {
    class TSecurityObject;
}

namespace NKikimr {
namespace NSchemeShard {

class TEffectiveACL {
    bool Inited = false;
    TString ForContainers;
    TString ForObjects;
    TString ForSelf;

public:
    TEffectiveACL() = default;

    void Init(const TString& effectiveACL);
    void Update(const TEffectiveACL& parent, const TString& selfACL, bool isContainer, bool isTenantRoot = false);

    operator bool () const { return Inited; }
    const TString& GetForChildren(bool isContainer) const { return isContainer ? ForContainers : ForObjects; }
    const TString& GetForSelf() const { return ForSelf; }

private:
    void InheritFrom(const TEffectiveACL& parent, bool isContainer, bool isTenantRoot);
    void Split(const NACLibProto::TSecurityObject& obj);
    bool Filter(const NACLibProto::TSecurityObject& obj, NACLib::EInheritanceType byType, TString& result);
    static TString StripConnectRightACE(const TString& acl);
};

}
}
