#pragma once

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

using TAccountId = NObjectClient::TObjectId;
using TSubjectId = NObjectClient::TObjectId;
using TUserId = NObjectClient::TObjectId;
using TGroupId = NObjectClient::TObjectId;
using TNetworkProjectId = NObjectClient::TObjectId;
using TProxyRoleId = NObjectClient::TObjectId;
using TAccountResourceUsageLeaseId = NObjectClient::TObjectId;

extern const std::string RootAccountName;
extern const NYPath::TYPath RootAccountCypressPath;
extern const std::string TmpAccountName;
extern const std::string SysAccountName;
extern const std::string IntermediateAccountName;
extern const std::string ChunkWiseAccountingMigrationAccountName;
extern const std::string SequoiaAccountName;

using NRpc::RootUserName;
extern const std::string GuestUserName;
extern const std::string JobUserName;
extern const std::string SchedulerUserName;
extern const std::string BundleControllerUserName;
extern const std::string FileCacheUserName;
extern const std::string OperationsCleanerUserName;
extern const std::string OperationsClientUserName;
extern const std::string TabletCellChangeloggerUserName;
extern const std::string TabletCellSnapshotterUserName;
extern const std::string TableMountInformerUserName;
extern const std::string AlienCellSynchronizerUserName;
extern const std::string QueueAgentUserName;
extern const std::string YqlAgentUserName;
extern const std::string TabletBalancerUserName;
extern const std::string PermissionCacheUserName;
extern const std::string ReplicatedTableTrackerUserName;
extern const std::string ChunkReplicaCacheUserName;

extern const std::string EveryoneGroupName;
extern const std::string UsersGroupName;
extern const std::string SuperusersGroupName;
extern const std::string AdminsGroupName;
extern const std::string ReplicatorUserName;
extern const std::string OwnerUserName;

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
    (CollectCoverage)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

