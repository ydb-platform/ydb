#pragma once

#include <util/generic/string.h>

namespace NKikimr::NSchemeShard {

// WARNING: DO NOT REORDER this constants
// reordering breaks update
enum ETxState {
    Invalid = 0,
    Waiting = 1,
    CreateParts = 2,  // get shards from Hive
    ConfigureParts = 3,  // send msg to shards to configure them
    DropParts = 4,  // send msg to shards that they are dropped
    DeleteParts = 5,  // send shards back to Hive (to drop their data)
    PublishTenantReadOnly = 6,  // send msg to tenant schemeshard to get it online in RO mode
    PublishGlobal = 7,  // start provide subdomains description as external domain
    RewriteOwners = 8,
    PublishTenant = 9,
    DoneMigrateTree = 10,
    DeleteTenantSS = 11,
    Propose = 128,  // propose operation to Coordinator (get global timestamp of operation)
    ProposedWaitParts = 129,
    ProposedDeleteParts = 130,
    TransferData = 131,
    NotifyPartitioningChanged = 132,
    Aborting = 133,
    DeleteExternalShards = 134,  // delete shards of the inner user objects in a extsubdomain
    DeletePrivateShards = 135,  // delete system shards of a extsubdomain
    WaitShadowPathPublication = 136,
    DeletePathBarrier = 137,
    SyncHive = 138,
    CopyTableBarrier = 139,
    ProposedCopySequence = 140,
    ProposedMoveSequence = 141,
    Done = 240,
    Aborted = 250,
};

TString TxStateName(ETxState s);

}  // namespace NKikimr::NSchemeShard
