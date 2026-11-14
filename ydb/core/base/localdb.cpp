#include "localdb.h"

#include <ydb/core/protos/resource_broker.pb.h>

namespace NKikimr {
namespace NLocalDb {

TCompactionPolicy::TBackgroundPolicy::TBackgroundPolicy()
    : Threshold(101)
    , PriorityBase(100)
    , TimeFactor(1.0)
    , ResourceBrokerTask(BackgroundCompactionTaskName)
{}

TCompactionPolicy::TBackgroundPolicy::TBackgroundPolicy(ui32 threshold,
                                                        ui32 priorityBase,
                                                        double timeFactor,
                                                        const TString &resourceBrokerTask)
    : Threshold(threshold)
    , PriorityBase(priorityBase)
    , TimeFactor(timeFactor)
    , ResourceBrokerTask(resourceBrokerTask)
{}

TCompactionPolicy::TBackgroundPolicy::TBackgroundPolicy(const NKikimrCompaction::TCompactionPolicy::TBackgroundPolicy &policyPb)
    : Threshold(policyPb.HasThreshold() ? policyPb.GetThreshold() : 101)
    , PriorityBase(policyPb.HasPriorityBase() ? policyPb.GetPriorityBase() : 100)
    , TimeFactor(policyPb.HasTimeFactor() ? policyPb.GetTimeFactor() : 1.0)
    , ResourceBrokerTask(policyPb.HasResourceBrokerTask() ? policyPb.GetResourceBrokerTask() : BackgroundCompactionTaskName)
{}

void TCompactionPolicy::TBackgroundPolicy::Serialize(NKikimrCompaction::TCompactionPolicy::TBackgroundPolicy& policyPb) const
{
    policyPb.SetThreshold(Threshold);
    policyPb.SetPriorityBase(PriorityBase);
    policyPb.SetTimeFactor(TimeFactor);
    policyPb.SetResourceBrokerTask(ResourceBrokerTask);
}

TCompactionPolicy::TGenerationPolicy::TGenerationPolicy(ui64 sizeToCompact,
                                                        ui32 countToCompact,
                                                        ui32 forceCountToCompact,
                                                        ui64 forceSizeToCompact,
                                                        const TString &resourceBrokerTask,
                                                        bool keepInCache,
                                                        const TBackgroundPolicy &backgroundCompactionPolicy)
    : SizeToCompact(sizeToCompact)
    , CountToCompact(countToCompact)
    , ForceCountToCompact(forceCountToCompact)
    , ForceSizeToCompact(forceSizeToCompact)
    , CompactionBrokerQueue(Max<ui32>())
    , ResourceBrokerTask(resourceBrokerTask)
    , KeepInCache(keepInCache)
    , BackgroundCompactionPolicy(backgroundCompactionPolicy)
    , ExtraCompactionPercent(10)
    , ExtraCompactionExpPercent(110)
    , ExtraCompactionMinSize(16384)
    , ExtraCompactionExpMaxSize(sizeToCompact / countToCompact)
    , UpliftPartSize(sizeToCompact / countToCompact)
{}

TCompactionPolicy::TGenerationPolicy::TGenerationPolicy(const NKikimrCompaction::TCompactionPolicy::TGenerationPolicy &policyPb)
    : SizeToCompact(policyPb.HasSizeToCompact() ? policyPb.GetSizeToCompact() : 0)
    , CountToCompact(policyPb.HasCountToCompact() ? policyPb.GetCountToCompact() : 5)
    , ForceCountToCompact(policyPb.HasForceCountToCompact() ? policyPb.GetForceCountToCompact() : 8)
    , ForceSizeToCompact(policyPb.HasForceSizeToCompact() ? policyPb.GetForceSizeToCompact() : 0)
    , CompactionBrokerQueue(policyPb.HasCompactionBrokerQueue() ? policyPb.GetCompactionBrokerQueue() : 1)
    , ResourceBrokerTask(policyPb.HasResourceBrokerTask() ? policyPb.GetResourceBrokerTask() : TString())
    , KeepInCache(policyPb.HasKeepInCache() ? policyPb.GetKeepInCache() : false)
    , BackgroundCompactionPolicy(policyPb.HasBackgroundCompactionPolicy() ? policyPb.GetBackgroundCompactionPolicy() : TBackgroundPolicy())
    , ExtraCompactionPercent(policyPb.HasExtraCompactionPercent() ? policyPb.GetExtraCompactionPercent() : 10)
    , ExtraCompactionExpPercent(policyPb.HasExtraCompactionExpPercent() ? policyPb.GetExtraCompactionExpPercent() : 110)
    , ExtraCompactionMinSize(policyPb.HasExtraCompactionMinSize() ? policyPb.GetExtraCompactionMinSize() : 16384)
    , ExtraCompactionExpMaxSize(policyPb.HasExtraCompactionExpMaxSize() ? policyPb.GetExtraCompactionExpMaxSize() : SizeToCompact / CountToCompact)
    , UpliftPartSize(policyPb.HasUpliftPartSize() ? policyPb.GetUpliftPartSize() : SizeToCompact / CountToCompact)
    , TierId(policyPb.HasTierId() ? std::make_optional(policyPb.GetTierId()) : std::nullopt)
{
    if (!ResourceBrokerTask)
        ResourceBrokerTask = LegacyQueueIdToTaskName(CompactionBrokerQueue);
}

void TCompactionPolicy::TGenerationPolicy::Serialize(NKikimrCompaction::TCompactionPolicy::TGenerationPolicy& policyPb) const
{
    policyPb.SetSizeToCompact(SizeToCompact);
    policyPb.SetCountToCompact(CountToCompact);
    policyPb.SetForceCountToCompact(ForceCountToCompact);
    policyPb.SetForceSizeToCompact(ForceSizeToCompact);
    policyPb.SetCompactionBrokerQueue(CompactionBrokerQueue);
    policyPb.SetResourceBrokerTask(ResourceBrokerTask);
    policyPb.SetKeepInCache(KeepInCache);
    BackgroundCompactionPolicy.Serialize(*policyPb.MutableBackgroundCompactionPolicy());
    policyPb.SetExtraCompactionPercent(ExtraCompactionPercent);
    policyPb.SetExtraCompactionMinSize(ExtraCompactionMinSize);
    policyPb.SetExtraCompactionExpPercent(ExtraCompactionExpPercent);
    policyPb.SetExtraCompactionExpMaxSize(ExtraCompactionExpMaxSize);
    policyPb.SetUpliftPartSize(UpliftPartSize);
    if (TierId) {
        policyPb.SetTierId(*TierId);
    } else {
        policyPb.ClearTierId();
    }
}

TCompactionPolicy::TCompactionPolicy()
    : InMemSizeToSnapshot(4 * 1024 * 1024)
    , InMemStepsToSnapshot(300)
    , InMemForceStepsToSnapshot(500)
    , InMemForceSizeToSnapshot(16 * 1024 * 1024)
    , InMemCompactionBrokerQueue(0)
    , InMemResourceBrokerTask(LegacyQueueIdToTaskName(0))
    , ReadAheadHiThreshold(64 * 1024 * 1024)
    , ReadAheadLoThreshold(16 * 1024 * 1024)
    , MinDataPageSize(7*1024)
    , MinBTreeIndexNodeSize(7*1024)
    , MinBTreeIndexNodeKeys(6)
    , SnapshotCompactionBrokerQueue(0)
    , SnapshotResourceBrokerTask(LegacyQueueIdToTaskName(0))
    , BackupCompactionBrokerQueue(1)
    , BackupResourceBrokerTask(ScanTaskName)
    , DefaultTaskPriority(5)
    , BackgroundSnapshotPolicy()
    , LogOverheadSizeToSnapshot(16 * 1024 * 1024)
    , LogOverheadCountToSnapshot(500)
    , DroppedRowsPercentToCompact(50)
    , CompactionStrategy(NKikimrCompaction::CompactionStrategyUnset)
    , KeepEraseMarkers(false)
{}

TCompactionPolicy::TCompactionPolicy(const NKikimrCompaction::TCompactionPolicy& policyPb)
    : InMemSizeToSnapshot(policyPb.HasInMemSizeToSnapshot() ? policyPb.GetInMemSizeToSnapshot() : 4 * 1024 * 1024)
    , InMemStepsToSnapshot(policyPb.HasInMemStepsToSnapshot() ? policyPb.GetInMemStepsToSnapshot() : 300)
    , InMemForceStepsToSnapshot(policyPb.HasInMemForceStepsToSnapshot() ? policyPb.GetInMemForceStepsToSnapshot() : 500)
    , InMemForceSizeToSnapshot(policyPb.HasInMemForceSizeToSnapshot() ? policyPb.GetInMemForceSizeToSnapshot() : 16 * 1024 * 1024)
    , InMemCompactionBrokerQueue(policyPb.HasInMemCompactionBrokerQueue() ? policyPb.GetInMemCompactionBrokerQueue() : 0)
    , InMemResourceBrokerTask(policyPb.HasInMemResourceBrokerTask() ? policyPb.GetInMemResourceBrokerTask() : LegacyQueueIdToTaskName(0))
    , ReadAheadHiThreshold(policyPb.HasReadAheadHiThreshold() ? policyPb.GetReadAheadHiThreshold() : 64 * 1024 * 1024)
    , ReadAheadLoThreshold(policyPb.HasReadAheadLoThreshold() ? policyPb.GetReadAheadLoThreshold() : 16 * 1024 * 1024)
    , MinDataPageSize(policyPb.HasMinDataPageSize() ? policyPb.GetMinDataPageSize() : 7 * 1024)
    , MinBTreeIndexNodeSize(policyPb.HasMinBTreeIndexNodeSize() ? policyPb.GetMinBTreeIndexNodeSize() : 7 * 1024)
    , MinBTreeIndexNodeKeys(policyPb.HasMinBTreeIndexNodeKeys() ? policyPb.GetMinBTreeIndexNodeKeys() : 6)
    , SnapshotCompactionBrokerQueue(policyPb.HasSnapBrokerQueue() ? policyPb.GetSnapBrokerQueue() : 0)
    , SnapshotResourceBrokerTask(policyPb.HasSnapshotResourceBrokerTask() ? policyPb.GetSnapshotResourceBrokerTask() : LegacyQueueIdToTaskName(0))
    , BackupCompactionBrokerQueue(policyPb.HasBackupBrokerQueue() ? policyPb.GetBackupBrokerQueue() : 1)
    , BackupResourceBrokerTask(policyPb.HasBackupResourceBrokerTask() ? policyPb.GetBackupResourceBrokerTask() : ScanTaskName)
    , DefaultTaskPriority(policyPb.HasDefaultTaskPriority() ? policyPb.GetDefaultTaskPriority() : 5)
    , BackgroundSnapshotPolicy(policyPb.HasBackgroundSnapshotPolicy() ? policyPb.GetBackgroundSnapshotPolicy() : TBackgroundPolicy())
    , LogOverheadSizeToSnapshot(policyPb.HasLogOverheadSizeToSnapshot() ? policyPb.GetLogOverheadSizeToSnapshot() : 16 * 1024 * 1024)
    , LogOverheadCountToSnapshot(policyPb.HasLogOverheadCountToSnapshot() ? policyPb.GetLogOverheadCountToSnapshot() : 500)
    , DroppedRowsPercentToCompact(policyPb.HasDroppedRowsPercentToCompact() ? policyPb.GetDroppedRowsPercentToCompact() : 50)
    , CompactionStrategy(policyPb.GetCompactionStrategy())
    , KeepEraseMarkers(policyPb.HasKeepEraseMarkers() ? policyPb.GetKeepEraseMarkers() : false)
{
    if (!InMemResourceBrokerTask)
        InMemResourceBrokerTask = LegacyQueueIdToTaskName(InMemCompactionBrokerQueue);
    if (!SnapshotResourceBrokerTask)
        SnapshotResourceBrokerTask = LegacyQueueIdToTaskName(SnapshotCompactionBrokerQueue);
    if (!BackupResourceBrokerTask || IsLegacyQueueIdTaskName(BackupResourceBrokerTask))
        BackupResourceBrokerTask = ScanTaskName;
    Generations.reserve(policyPb.GenerationSize());
    for (ui32 i = 0; i < policyPb.GenerationSize(); ++i) {
        const auto& g = policyPb.GetGeneration(i);
        Y_DEBUG_ABORT_UNLESS(g.GetGenerationId() == i);
        Generations.emplace_back(g);
    }
    if (policyPb.HasShardPolicy()) {
        ShardPolicy.CopyFrom(policyPb.GetShardPolicy());
    }
}

void TCompactionPolicy::Serialize(NKikimrCompaction::TCompactionPolicy& policyPb) const {
    policyPb.SetInMemSizeToSnapshot(InMemSizeToSnapshot);
    policyPb.SetInMemStepsToSnapshot(InMemStepsToSnapshot);
    policyPb.SetInMemForceStepsToSnapshot(InMemForceStepsToSnapshot);
    policyPb.SetInMemForceSizeToSnapshot(InMemForceSizeToSnapshot);
    policyPb.SetInMemCompactionBrokerQueue(InMemCompactionBrokerQueue);
    policyPb.SetInMemResourceBrokerTask(InMemResourceBrokerTask);
    policyPb.SetReadAheadHiThreshold(ReadAheadHiThreshold);
    policyPb.SetReadAheadLoThreshold(ReadAheadLoThreshold);
    policyPb.SetMinDataPageSize(MinDataPageSize);
    policyPb.SetMinBTreeIndexNodeSize(MinBTreeIndexNodeSize);
    policyPb.SetMinBTreeIndexNodeKeys(MinBTreeIndexNodeKeys);
    policyPb.SetSnapBrokerQueue(SnapshotCompactionBrokerQueue);
    policyPb.SetSnapshotResourceBrokerTask(SnapshotResourceBrokerTask);
    policyPb.SetBackupBrokerQueue(BackupCompactionBrokerQueue);
    policyPb.SetBackupResourceBrokerTask(BackupResourceBrokerTask);
    policyPb.SetDefaultTaskPriority(DefaultTaskPriority);
    BackgroundSnapshotPolicy.Serialize(*policyPb.MutableBackgroundSnapshotPolicy());
    policyPb.SetLogOverheadSizeToSnapshot(LogOverheadSizeToSnapshot);
    policyPb.SetLogOverheadCountToSnapshot(LogOverheadCountToSnapshot);
    policyPb.SetDroppedRowsPercentToCompact(DroppedRowsPercentToCompact);
    if (CompactionStrategy != NKikimrCompaction::CompactionStrategyUnset) {
        policyPb.SetCompactionStrategy(CompactionStrategy);
    }
    if (KeepEraseMarkers) {
        policyPb.SetKeepEraseMarkers(KeepEraseMarkers);
    }
    if (ShardPolicy.ByteSizeLong() > 0) {
        policyPb.MutableShardPolicy()->CopyFrom(ShardPolicy);
    }

    for (ui32 i = 0; i < Generations.size(); ++i) {
        auto &g = *policyPb.AddGeneration();
        g.SetGenerationId(i);
        Generations[i].Serialize(g);
    }
}

TCompactionPolicyPtr CreateDefaultTablePolicy() {
    TCompactionPolicyPtr policy = new TCompactionPolicy;
    return policy;
}

TCompactionPolicyPtr CreateDefaultUserTablePolicy() {
    TCompactionPolicyPtr userPolicy = new TCompactionPolicy();
    userPolicy->Generations.reserve(3);
    userPolicy->Generations.push_back({0, 8, 8, 128 * 1024 * 1024,
                LegacyQueueIdToTaskName(1), true});
    userPolicy->Generations.push_back({40 * 1024 * 1024, 5, 16, 512 * 1024 * 1024,
                LegacyQueueIdToTaskName(2), false});
    userPolicy->Generations.push_back({400 * 1024 * 1024, 5, 16, 16ull * 1024 * 1024 * 1024,
                LegacyQueueIdToTaskName(3), false});
    return userPolicy;
}

TVector<ui32> TCompactionPolicy::GetTierIds() const {
    TVector<ui32> tiers{0};
    THashSet<ui32> seen{0};
    for (const auto& generation : Generations) {
        const ui32 tierId = generation.TierId.value_or(0);
        if (seen.insert(tierId).second) {
            tiers.push_back(tierId);
        }
    }
    return tiers;
}

ui32 TCompactionPolicy::GetFinalLevel(ui32 tierId) const {
    const auto tiers = GetTierIds();
    for (size_t slot = 0; slot < tiers.size(); ++slot) {
        if (tiers[slot] == tierId) {
            Y_ENSURE(slot < DefaultFinalLevel, "Too many compaction tier slots");
            const ui32 level = DefaultFinalLevel - static_cast<ui32>(slot);
            Y_ENSURE(level > Generations.size(), "Compaction final level overlaps a generation");
            return level;
        }
    }
    ythrow yexception() << "Unknown compaction tier " << tierId;
}

bool TCompactionPolicy::IsFinalLevel(ui32 level) const {
    return level > Generations.size() && level <= DefaultFinalLevel
        && DefaultFinalLevel - level < GetTierIds().size();
}

ui32 TCompactionPolicy::GetTierForLevel(ui32 level) const {
    if (level == 0) {
        return 0;
    }
    if (level <= Generations.size()) {
        return Generations[level - 1].TierId.value_or(0);
    }
    const auto tiers = GetTierIds();
    Y_ENSURE(level <= DefaultFinalLevel && DefaultFinalLevel - level < tiers.size(),
        "Unknown compaction level " << level);
    return tiers[DefaultFinalLevel - level];
}

bool ValidateCompactionPolicy(const TCompactionPolicy& policy, TString& err) {
    THashSet<ui32> seenTiers;
    std::optional<ui32> previousTier;
    for (size_t i = 0; i < policy.Generations.size(); ++i) {
        const ui32 tierId = policy.Generations[i].TierId.value_or(0);
        if (previousTier && *previousTier == tierId) {
            continue;
        }
        if (!seenTiers.insert(tierId).second) {
            err = Sprintf("Generation %u returns to tier %u: generations of each tier must form a contiguous block",
                (ui32)i, tierId);
            return false;
        }
        previousTier = tierId;
    }
    if (policy.Generations.size() + policy.GetTierIds().size() > TCompactionPolicy::DefaultFinalLevel) {
        err = "Compaction generations and tier final levels must fit into levels 1..255";
        return false;
    }
    return true;
}

bool ValidateCompactionPolicyChange(const TCompactionPolicy& oldPolicy, const TCompactionPolicy& newPolicy, TString& err) {
    if (newPolicy.Generations.size() < oldPolicy.Generations.size()) {
        err = Sprintf("Decreasing number of levels in compaction policy in not supported, old level count %u, new level count %u",
           (ui32)oldPolicy.Generations.size(), (ui32)newPolicy.Generations.size());
        return false;
    }
    if (!ValidateCompactionPolicy(newPolicy, err)) {
        return false;
    }
    const auto oldTiers = oldPolicy.GetTierIds();
    const auto newTiers = newPolicy.GetTierIds();
    for (size_t slot = 0; slot < oldTiers.size(); ++slot) {
        if (slot >= newTiers.size() || oldTiers[slot] != newTiers[slot]) {
            err = "Existing compaction tier slots must not be removed or reordered";
            return false;
        }
    }
    for (size_t index = 0; index < oldPolicy.Generations.size(); ++index) {
        const ui32 oldTier = oldPolicy.Generations[index].TierId.value_or(0);
        const ui32 newTier = newPolicy.Generations[index].TierId.value_or(0);
        if (oldTier != newTier) {
            err = Sprintf("Changing TierId of existing generation %u is not supported: old TierId %u, new TierId %u",
                (ui32)index + 1, oldTier, newTier);
            return false;
        }
    }
    return true;
}

TString LegacyQueueIdToTaskName(ui32 id)
{
    switch (id) {
    case 0:
        return "compaction_gen0";
    case 1:
        return "compaction_gen1";
    case 2:
        return "compaction_gen2";
    case 3:
        return "compaction_gen3";
    default:
        return UnknownTaskName;
    };
}

bool IsLegacyQueueIdTaskName(const TString& taskName)
{
    if (taskName.size() == LegacyQueueIdTaskNamePrefix.size() + 1 &&
        taskName.StartsWith(LegacyQueueIdTaskNamePrefix) &&
        taskName.back() >= '0' && taskName.back() <= '9')
    {
        return true;
    }

    return false;
}

const TString DefaultQueueName = "queue_default";
const TString UnknownTaskName = "unknown";
const TString TransactionTaskName = "transaction";
const TString ScanTaskName = "scan";
const TString BackgroundCompactionTaskName = "background_compaction";
const TString KqpResourceManagerTaskName = "kqp_query";
const TString KqpResourceManagerQueue = "queue_kqp_resource_manager";
const TString LegacyQueueIdTaskNamePrefix = "compaction_gen";

const TString ColumnShardCompactionIndexationQueue = "queue_cs_indexation";
const TString ColumnShardCompactionTtlQueue = "queue_cs_ttl";
const TString ColumnShardCompactionGeneralQueue = "queue_cs_general";
const TString ColumnShardCompactionNormalizerQueue = "queue_cs_normalizer";
}}
