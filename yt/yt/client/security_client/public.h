#pragma once

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

using TAccountId = NObjectClient::TObjectId;
using TSubjectId = NObjectClient::TObjectId;
using TUserId = NObjectClient::TObjectId;
using TGroupId = NObjectClient::TObjectId;
using TNetworkProjectId = NObjectClient::TObjectId;
using TProxyRoleId = NObjectClient::TObjectId;
using TAccountResourceUsageLeaseId = NObjectClient::TObjectId;

extern const TString RootAccountName;
extern const TString RootAccountCypressPath;
extern const TString TmpAccountName;
extern const TString SysAccountName;
extern const TString IntermediateAccountName;
extern const TString ChunkWiseAccountingMigrationAccountName;
extern const TString SequoiaAccountName;

using NRpc::RootUserName;
extern const TString GuestUserName;
extern const TString JobUserName;
extern const TString SchedulerUserName;
extern const TString FileCacheUserName;
extern const TString OperationsCleanerUserName;
extern const TString OperationsClientUserName;
extern const TString TabletCellChangeloggerUserName;
extern const TString TabletCellSnapshotterUserName;
extern const TString TableMountInformerUserName;
extern const TString AlienCellSynchronizerUserName;
extern const TString QueueAgentUserName;
extern const TString YqlAgentUserName;
extern const TString TabletBalancerUserName;

extern const TString EveryoneGroupName;
extern const TString UsersGroupName;
extern const TString SuperusersGroupName;
extern const TString ReplicatorUserName;
extern const TString OwnerUserName;

using TSecurityTag = TString;
constexpr int MaxSecurityTagLength = 128;

DEFINE_ENUM(ESecurityAction,
    ((Undefined)(0))  // Intermediate state, used internally.
    ((Allow)    (1))  // Let'em go!
    ((Deny)     (2))  // No way!
);

DEFINE_ENUM(EAceInheritanceMode,
    ((ObjectAndDescendants)    (0))  // ACE applies both to the object itself and its descendants.
    ((ObjectOnly)              (1))  // ACE applies to the object only.
    ((DescendantsOnly)         (2))  // ACE applies to descendants only.
    ((ImmediateDescendantsOnly)(3))  // ACE applies to immediate (direct) descendants only.
);

YT_DEFINE_ERROR_ENUM(
    ((AuthenticationError)          (900))
    ((AuthorizationError)           (901))
    ((AccountLimitExceeded)         (902))
    ((UserBanned)                   (903))
    ((RequestQueueSizeLimitExceeded)(904))
    ((NoSuchAccount)                (905))
    ((NoSuchSubject)                (907))
    ((SafeModeEnabled)              (906))
    ((AlreadyPresentInGroup)        (908))
    ((IrreversibleAclModification)  (909))
);

// NB: Changing this list requires reign promotion.
DEFINE_ENUM(EProxyKind,
    ((Http)          (1))
    ((Rpc)           (2))
);

DEFINE_ENUM(EAccessControlObjectNamespace,
    (AdminCommands)
);

DEFINE_ENUM(EAccessControlObject,
    (DisableChunkLocations)
    (DestroyChunkLocations)
    (ResurrectChunkLocations)
    (BuildSnapshot)
    (BuildMasterSnapshot)
    (GetMasterConsistentState)
    (ExitReadOnly)
    (MasterExitReadOnly)
    (DiscombobulateNonvotingPeers)
    (SwitchLeader)
    (RequestRestart)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

