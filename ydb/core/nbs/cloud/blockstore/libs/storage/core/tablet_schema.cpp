#include "tablet_schema.h"

#include <ydb/core/base/localdb.h>

namespace NYdb::NBS::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

NKikimr::NLocalDb::TCompactionPolicyPtr CreateIndexTablePolicy()
{
    auto policy = MakeIntrusive<NKikimr::NLocalDb::TCompactionPolicy>();

    // for fast NVMe devices we do not need large read ahead (large memory
    // overhead)
    policy->ReadAheadHiThreshold = 2 * 1024 * 1024;
    policy->ReadAheadLoThreshold = 1 * 1024 * 1024;

    // large number of deletion markers will slow down reads
    policy->DroppedRowsPercentToCompact = 10;

    policy->Generations.reserve(3);
    // first level: 8x8MB expected geometry (keep in cache, auto uplift up to
    // 8MB)
    policy->Generations.emplace_back(
        64 * 1024 * 1024,
        8,
        24,
        128 * 1024 * 1024,
        NKikimr::NLocalDb::LegacyQueueIdToTaskName(1),
        true);
    // second level: 5x64MB expected geometry
    policy->Generations.emplace_back(
        320 * 1024 * 1024,
        5,
        15,
        640 * 1024 * 1024,
        NKikimr::NLocalDb::LegacyQueueIdToTaskName(2),
        false);
    // third level: 5x320MB expected geometry
    policy->Generations.emplace_back(
        1600ull * 1024 * 1024,
        5,
        10,
        3200ull * 1024 * 1024,
        NKikimr::NLocalDb::LegacyQueueIdToTaskName(3),
        false);

    return policy;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void InitCompactionPolicy(
    NKikimr::NTable::TAlter& alter,
    ui32 tableId,
    ECompactionPolicy compactionPolicy)
{
    NKikimr::NLocalDb::TCompactionPolicyPtr policy;

    if (compactionPolicy == ECompactionPolicy::Default) {
        policy = NKikimr::NLocalDb::CreateDefaultUserTablePolicy();
    } else if (compactionPolicy == ECompactionPolicy::IndexTable) {
        policy = CreateIndexTablePolicy();
    }

    if (policy) {
        alter.SetCompactionPolicy(tableId, *policy);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage
