#include "access_control.h"

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf AccessControlObjectNamespacePath = "//sys/access_control_object_namespaces/%lv";
static constexpr TStringBuf AccessControlObjectPath = "//sys/access_control_object_namespaces/%lv/%lv";
static constexpr TStringBuf AccessControlObjectPrincipalPath = "//sys/access_control_object_namespaces/%lv/%lv/principal";

////////////////////////////////////////////////////////////////////////////////

TAccessControlObjectDescriptor::TAccessControlObjectDescriptor(
    EAccessControlObjectNamespace accessControlObjectNamespace,
    EAccessControlObject accessControlObject)
    : Namespace_(accessControlObjectNamespace)
    , Name_(accessControlObject)
{ }

TString TAccessControlObjectDescriptor::GetPath() const
{
    return Format(
        AccessControlObjectPath,
        Namespace_,
        Name_);
}

TString TAccessControlObjectDescriptor::GetPrincipalPath() const
{
    return Format(
        AccessControlObjectPrincipalPath,
        Namespace_,
        Name_);
}

////////////////////////////////////////////////////////////////////////////////

TAccessControlObjectDescriptor GetAccessControlObjectDescriptor(
    EAccessControlObject accessControlObjectName)
{
    auto iter = AccessControlObjects.find(accessControlObjectName);
    YT_VERIFY(!iter.IsEnd());

    return iter->second;
}

TString GetAccessControlObjectNamespacePath(EAccessControlObjectNamespace accessControlObjectNamespace)
{
    return Format(AccessControlObjectNamespacePath, accessControlObjectNamespace);
}

TString GetAccessControlObjectNamespaceName(EAccessControlObjectNamespace accessControlObjectNamespace)
{
    return Format("%lv", accessControlObjectNamespace);
}

TString GetAccessControlObjectName(EAccessControlObject accessControlObject)
{
    return Format("%lv", accessControlObject);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
