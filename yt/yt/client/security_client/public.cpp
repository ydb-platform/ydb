#include "public.h"

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

const std::string RootAccountName("root");
const NYPath::TYPath RootAccountCypressPath("//sys/account_tree");
const std::string TmpAccountName("tmp");
const std::string SysAccountName("sys");
const std::string IntermediateAccountName("intermediate");
const std::string ChunkWiseAccountingMigrationAccountName("chunk_wise_accounting_migration");
const std::string SequoiaAccountName("sequoia");

const std::string GuestUserName("guest");
const std::string JobUserName("job");
const std::string SchedulerUserName("scheduler");
const std::string ReplicatorUserName("replicator");
const std::string BundleControllerUserName("bundle_controller");
const std::string OwnerUserName("owner");
const std::string FileCacheUserName("file_cache");
const std::string OperationsCleanerUserName("operations_cleaner");
const std::string OperationsClientUserName("operations_client");
const std::string TabletCellChangeloggerUserName("tablet_cell_changelogger");
const std::string TabletCellSnapshotterUserName("tablet_cell_snapshotter");
const std::string TableMountInformerUserName("table_mount_informer");
const std::string AlienCellSynchronizerUserName("alien_cell_synchronizer");
const std::string QueueAgentUserName("queue_agent");
const std::string YqlAgentUserName("yql_agent");
const std::string TabletBalancerUserName("tablet_balancer");
const std::string PermissionCacheUserName("yt-permission-cache");
const std::string ReplicatedTableTrackerUserName("yt-replicated-table-tracker");
const std::string ChunkReplicaCacheUserName("yt-chunk-replica-cache");

const std::string EveryoneGroupName("everyone");
const std::string UsersGroupName("users");
const std::string SuperusersGroupName("superusers");
const std::string AdminsGroupName("admins");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
