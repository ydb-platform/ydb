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

const TString GuestUserName("guest");
const TString JobUserName("job");
const TString SchedulerUserName("scheduler");
const TString ReplicatorUserName("replicator");
const TString BundleControllerUserName("bundle_controller");
const TString OwnerUserName("owner");
const TString FileCacheUserName("file_cache");
const TString OperationsCleanerUserName("operations_cleaner");
const TString OperationsClientUserName("operations_client");
const TString TabletCellChangeloggerUserName("tablet_cell_changelogger");
const TString TabletCellSnapshotterUserName("tablet_cell_snapshotter");
const TString TableMountInformerUserName("table_mount_informer");
const TString AlienCellSynchronizerUserName("alien_cell_synchronizer");
const TString QueueAgentUserName("queue_agent");
const TString YqlAgentUserName("yql_agent");
const TString TabletBalancerUserName("tablet_balancer");

const TString EveryoneGroupName("everyone");
const TString UsersGroupName("users");
const TString SuperusersGroupName("superusers");
const TString AdminsGroupName("admins");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
