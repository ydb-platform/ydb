#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

class TAccessControlObjectDescriptor
{
public:
    DEFINE_BYVAL_RO_PROPERTY(EAccessControlObjectNamespace, Namespace);
    DEFINE_BYVAL_RO_PROPERTY(EAccessControlObject, Name);

public:
    TAccessControlObjectDescriptor(
        EAccessControlObjectNamespace accessControlObjectNamespace,
        EAccessControlObject accessControlObject);

    NYPath::TYPath GetPath() const;
    NYPath::TYPath GetPrincipalPath() const;
};

////////////////////////////////////////////////////////////////////////////////

#define ACCESS_CONTROL_ENTRY(namespace, name) \
    {(name), TAccessControlObjectDescriptor((namespace), (name))}

inline const std::vector<EAccessControlObjectNamespace> AccessControlObjectNamespaces = {
    EAccessControlObjectNamespace::AdminCommands
};

inline const THashMap<EAccessControlObject, TAccessControlObjectDescriptor> AccessControlObjects = {
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::BuildMasterSnapshot),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::BuildSnapshot),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::CollectCoverage),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::DestroyChunkLocations),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::DisableChunkLocations),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::DiscombobulateNonvotingPeers),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::ExitReadOnly),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::GetMasterConsistentState),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::MasterExitReadOnly),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::ResetDynamicallyPropagatedMasterCells),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::RequestRestart),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::ResurrectChunkLocations),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::SwitchLeader),
};

#undef ACCESS_CONTROL_ENTRY

////////////////////////////////////////////////////////////////////////////////

TAccessControlObjectDescriptor GetAccessControlObjectDescriptor(EAccessControlObject accessControlObjectName);

NYPath::TYPath GetAccessControlObjectNamespacePath(EAccessControlObjectNamespace accessControlObjectNamespace);

std::string GetAccessControlObjectNamespaceName(EAccessControlObjectNamespace accessControlObjectNamespace);

std::string GetAccessControlObjectName(EAccessControlObject accessControlObject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
