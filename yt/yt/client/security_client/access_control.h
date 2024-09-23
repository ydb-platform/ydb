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

    TString GetPath() const;
    TString GetPrincipalPath() const;
};

////////////////////////////////////////////////////////////////////////////////

#define ACCESS_CONTROL_ENTRY(namespace, name) \
    {(name), TAccessControlObjectDescriptor((namespace), (name))}

const std::vector<EAccessControlObjectNamespace> AccessControlObjectNamespaces = {
    EAccessControlObjectNamespace::AdminCommands
};

const THashMap<EAccessControlObject, TAccessControlObjectDescriptor> AccessControlObjects = {
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::DisableChunkLocations),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::DestroyChunkLocations),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::ResurrectChunkLocations),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::BuildSnapshot),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::BuildMasterSnapshot),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::GetMasterConsistentState),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::ExitReadOnly),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::MasterExitReadOnly),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::DiscombobulateNonvotingPeers),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::SwitchLeader),
    ACCESS_CONTROL_ENTRY(EAccessControlObjectNamespace::AdminCommands, EAccessControlObject::RequestRestart)
};

////////////////////////////////////////////////////////////////////////////////

TAccessControlObjectDescriptor GetAccessControlObjectDescriptor(EAccessControlObject accessControlObjectName);

TString GetAccessControlObjectNamespacePath(EAccessControlObjectNamespace accessControlObjectNamespace);

TString GetAccessControlObjectNamespaceName(EAccessControlObjectNamespace accessControlObjectNamespace);

TString GetAccessControlObjectName(EAccessControlObject accessControlObject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
