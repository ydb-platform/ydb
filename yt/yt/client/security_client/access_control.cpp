#include "access_control.h"

namespace NYT::NSecurityClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf AccessControlObjectNamespacePathPattern = "//sys/access_control_object_namespaces/%lv";
static constexpr TStringBuf AccessControlObjectPathPattern = "//sys/access_control_object_namespaces/%lv/%lv";
static constexpr TStringBuf AccessControlObjectPrincipalPathPattern = "//sys/access_control_object_namespaces/%lv/%lv/principal";

////////////////////////////////////////////////////////////////////////////////

TAccessControlObjectDescriptor::TAccessControlObjectDescriptor(
    EAccessControlObjectNamespace accessControlObjectNamespace,
    EAccessControlObject accessControlObject)
    : Namespace_(accessControlObjectNamespace)
    , Name_(accessControlObject)
{ }

TYPath TAccessControlObjectDescriptor::GetPath() const
{
    return Format(
        AccessControlObjectPathPattern,
        Namespace_,
        Name_);
}

TYPath TAccessControlObjectDescriptor::GetPrincipalPath() const
{
    return Format(
        AccessControlObjectPrincipalPathPattern,
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

TYPath GetAccessControlObjectNamespacePath(EAccessControlObjectNamespace accessControlObjectNamespace)
{
    return Format(AccessControlObjectNamespacePathPattern, accessControlObjectNamespace);
}

std::string GetAccessControlObjectNamespaceName(EAccessControlObjectNamespace accessControlObjectNamespace)
{
    return FormatEnum(accessControlObjectNamespace);
}

std::string GetAccessControlObjectName(EAccessControlObject accessControlObject)
{
    return FormatEnum(accessControlObject);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
