#include "flat_comp_gen.h"
#include "flat_compaction_prepare.h"

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/cast.h>
#include <util/generic/hash_set.h>

#include <algorithm>

namespace NKikimr {
namespace NTable {
namespace NCompGen {

namespace {

static constexpr ui32 BAD_PRIORITY = Max<ui32>();
static constexpr ui32 PRIORITY_UPDATE_FACTOR = 20;

bool UsesTiering(const TScheme::TTableInfo& scheme) {
    if (scheme.SpecialTableType == NKikimrSchemeOp::ESpecialTableType::ESpecialTableTypeFulltextCompact
            || scheme.SpecialTableType == NKikimrSchemeOp::ESpecialTableType::ESpecialTableTypeFulltextCompactRelevance) {
        return false;
    }
    if (scheme.TieringPolicy) {
        return true;
    }
    for (const auto& generation : scheme.CompactionPolicy->Generations) {
        if (generation.TierId.value_or(0) != 0) {
            return true;
        }
    }
    return false;
}

}

void TGenCompactionParams::Describe(IOutputStream& out) const {
    out << "TGenCompactionParams{" << Table << ": gen " << Generation;

    if (Edge.Head == TEpoch::Max()) {
        out << " epoch +inf";
    } else {
        out << " epoch " << Edge.Head;
    }

    out << ", " << Parts.size() << " parts}";
}

struct TGenCompactionStrategy::TPartAggregator {
    TStats Stats;
    THashMap<ui64, TStats> StatsPerTablet;
    ui64 PartEpochCount = 0;

    TPartAggregator& Add(const TPartInfo& part) {
        Y_ENSURE(part.Epoch != TEpoch::Max(),
            "Unexpected part with an infinite epoch found");
        Stats += part.Stats;
        StatsPerTablet[part.Label.TabletID()] += part.Stats;
        if (LastEpoch != part.Epoch) {
            LastEpoch = part.Epoch;
            ++PartEpochCount;
        }
        return *this;
    }

    template<class Container>
    TPartAggregator& Add(const Container& container) {
        for (auto& part : container) {
            Add(part);
        }
        return *this;
    }

private:
    TEpoch LastEpoch = TEpoch::Max();
};

struct TGenCompactionStrategy::TExtraState {
    ui64 CurrentSize = 0;
    ui32 ExtraPercent = 0;
    ui32 ExtraExpPercent = 0;
    ui64 ExtraMinSize = 0;
    ui64 ExtraExpMaxSize = 0;

    void Add(ui64 size) {
        CurrentSize += size;
    }

    void InitFromPolicy(const TCompactionPolicy::TGenerationPolicy& policy) {
        ExtraPercent = policy.ExtraCompactionPercent;
        ExtraExpPercent = policy.ExtraCompactionExpPercent;
        ExtraMinSize = policy.ExtraCompactionMinSize;
        ExtraExpMaxSize = policy.ExtraCompactionExpMaxSize;
    }

    bool ExtrasAllowed() const {
        return CurrentSize > 0 && (
            ExtraMinSize > 0 ||
            ExtraPercent > 0 ||
            ExtraExpPercent > 0 && ExtraExpMaxSize > 0);
    }

    bool ExtraAllowed(ui64 size) const {
        ui64 extra = CurrentSize + size;
        return CurrentSize > 0 && (
            extra <= ExtraMinSize ||
            size <= CurrentSize * ExtraPercent / 100 ||
            extra <= ExtraExpMaxSize && size <= CurrentSize * ExtraExpPercent / 100);
    }
};

TGenCompactionStrategy::TPartInfo& TGenCompactionStrategy::TGeneration::PushFront(TPartView partView) {
    Y_ENSURE(TakenHeadParts == 0,
        "Attempting to prepend part to generation that has taken head parts");

    if (Parts.empty() || Parts.front().Epoch != partView->Epoch) {
        // This part becomes a new epoch edge
        ++PartEpochCount;
    }

    auto& front = Parts.emplace_front(std::move(partView));
    Stats += front.Stats;
    StatsPerTablet[front.Label.TabletID()] += front.Stats;
    return front;
}

TGenCompactionStrategy::TPartInfo& TGenCompactionStrategy::TGeneration::PushBack(TPartView partView) {
    Y_ENSURE(CompactingTailParts == 0,
        "Attempting to append part to generation that has compacting tail parts");

    if (Parts.empty() || Parts.back().Epoch != partView->Epoch) {
        // This part will become a new epoch edge
        ++PartEpochCount;
    } else if (Parts.size() == TakenHeadParts) {
        // The end of taken head is no longer an epoch edge
        Y_ENSURE(TakenHeadPartEpochCount > 0);
        --TakenHeadPartEpochCount;
    }

    auto& back = Parts.emplace_back(std::move(partView));
    Stats += back.Stats;
    StatsPerTablet[back.Label.TabletID()] += back.Stats;
    return back;
}

void TGenCompactionStrategy::TGeneration::PopFront() {
    Y_ENSURE(Parts.size() > CompactingTailParts,
        "Attempting to remove part crossing compacting tail parts");

    bool wasTaken = TakenHeadParts > 0;

    const auto& front = Parts.front();
    TEpoch epoch = front.Epoch;
    Stats -= front.Stats;
    StatsPerTablet[front.Label.TabletID()] -= front.Stats;
    if (wasTaken) {
        Y_ENSURE(TakenHeadBackingSize >= front.Stats.BackingSize);
        TakenHeadBackingSize -= front.Stats.BackingSize;
        --TakenHeadParts;
    }
    Parts.pop_front();

    if (Parts.empty() || Parts.front().Epoch != epoch) {
        // We just removed an epoch edge
        if (wasTaken) {
            Y_ENSURE(TakenHeadPartEpochCount > 0);
            --TakenHeadPartEpochCount;
        }
        Y_ENSURE(PartEpochCount > 0);
        --PartEpochCount;
    }
}

void TGenCompactionStrategy::TGeneration::PopBack() {
    Y_ENSURE(Parts.size() > TakenHeadParts,
        "Attempting to remove part crossing taken head parts");

    bool wasCompacting = CompactingTailParts > 0;

    auto& back = Parts.back();
    TEpoch epoch = back.Epoch;
    Stats -= back.Stats;
    StatsPerTablet[back.Label.TabletID()] -= back.Stats;
    if (wasCompacting) {
        --CompactingTailParts;
    }
    Parts.pop_back();

    if (Parts.empty() || Parts.front().Epoch != epoch) {
        // We just removed an epoch edge
        Y_ENSURE(PartEpochCount > 0);
        --PartEpochCount;
    } else if (Parts.size() == TakenHeadParts) {
        // The end of taken head is now an epoch edge
        ++TakenHeadPartEpochCount;
    }
}

TGenCompactionStrategy::TGenCompactionStrategy(
        ui32 table,
        ICompactionBackend* backend,
        IResourceBroker* broker,
        ITimeProvider* time,
        NUtil::ILogger* logger,
        TString taskNameSuffix)
    : Table(table)
    , Backend(backend)
    , Broker(broker)
    , Time(time)
    , Logger(logger)
    , TaskNameSuffix(std::move(taskNameSuffix))
{
}

TGenCompactionStrategy::~TGenCompactionStrategy()
{
}

void TGenCompactionStrategy::Start(TCompactionState state) {
    Y_ENSURE(!Policy, "Strategy has already been started");

    const auto* scheme = Backend->TableScheme(Table);

    Policy = scheme->CompactionPolicy;
    TierManaged = UsesTiering(*scheme);
    Generations.resize(Policy->Generations.size());

    for (auto& partView : Backend->TableParts(Table)) {
        auto label = partView->Label;
        ui32 level = state.PartLevels.Value(partView->Label, 255);
        Y_ENSURE(level > 0 && level <= Generations.size() || level == 255);
        auto& parts = level == 255 ? FinalParts : Generations.at(level - 1).Parts;
        parts.emplace_back(std::move(partView));
        KnownParts[label] = level;
    }

    for (auto& part : Backend->TableColdParts(Table)) {
        auto label = part->Label;
        ui32 level = state.PartLevels.Value(part->Label, 255);
        if (TierManaged) {
            // Borrowed unloaded sources belong to default, even when the log
            // associates them with a generation now bound to another tier.
            level = ResolveColdPartLevel(level, *Policy);
        } else {
            Y_ENSURE(level > 0 && level <= Generations.size() || level == 255);
        }
        ColdParts.emplace_back(std::move(part));
        KnownParts[label] = level;
    }

    THashMap<ui32, TEpoch> lastEpochs;
    for (size_t index = 0; index < Generations.size(); ++index) {
        auto& gen = Generations[index];
        TPartAggregator agg;
        if (gen.Parts) {
            gen.Parts.sort();

            // Epochs from different storage tiers may interleave.
            const ui32 tier = TierManaged ? TierForLevel(index + 1) : 0;
            const auto [it, inserted] = lastEpochs.emplace(tier, TEpoch::Max());
            Y_UNUSED(inserted);
            Y_ENSURE(gen.Parts.front().Epoch <= it->second);
            it->second = gen.Parts.back().Epoch;

            agg.Add(gen.Parts);
        }
        gen.Stats = agg.Stats;
        gen.StatsPerTablet = agg.StatsPerTablet;
        gen.PartEpochCount = agg.PartEpochCount;
    }

    // Sorting FinalParts is not required, but let's be nice
    FinalParts.sort();
    if (TierManaged) {
        ValidateTierEpochs();
    }

    UpdateStats();

    MaybeAutoStartForceCompaction();

    for (ui32 index : xrange(Generations.size())) {
        CheckGeneration(index + 1);
    }

    UpdateOverload();
    if (TierManaged) {
        CheckTierGenerations();
    }
}

void TGenCompactionStrategy::Stop() {
    if (MemCompactionId != 0) {
        Backend->CancelCompaction(MemCompactionId);
        MemCompactionId = 0;
    }

    for (auto& gen : Generations) {
        switch (std::exchange(gen.State, EState::Free)) {
            case EState::Free:
                break;
            case EState::Pending:
            case EState::PendingBackground: {
                // The task is scheduled, make sure to cancel it
                Y_ENSURE(gen.Task.TaskId != 0);
                Broker->CancelTask(gen.Task.TaskId);
                break;
            }
            case EState::Compacting: {
                // Compaction is running, cancel it
                Y_ENSURE(gen.Task.CompactionId != 0);
                Backend->CancelCompaction(gen.Task.CompactionId);
                break;
            }
        }

        gen.State = EState::Free;
        gen.Task.TaskId = 0;
        gen.Task.CompactionId = 0;
    }

    switch (std::exchange(FinalState.State, EState::Free)) {
        case EState::Free:
            break;
        case EState::Pending:
        case EState::PendingBackground:
            // The task is scheduled, make sure to cancel it
            Y_ENSURE(FinalState.Task.TaskId != 0);
            Broker->CancelTask(FinalState.Task.TaskId);
            break;
        case EState::Compacting:
            // Compaction is running, cancel it
            Y_ENSURE(FinalState.Task.CompactionId != 0);
            Backend->CancelCompaction(FinalState.Task.CompactionId);
            break;
    }

    FinalCompactionId = 0;
    FinalCompactionLevel = 0;
    FinalCompactionTaken = 0;
    ForcedState = EForcedState::None;
    ForcedMemCompactionId = 0;
    ForcedGeneration = 0;

    CurrentForcedGenCompactionId = 0;
    NextForcedGenCompactionId = 0;

    // Make it possible to Start again
    Generations.clear();
    FinalState = { };
    FinalParts.clear();
    ColdParts.clear();
    KnownParts.clear();
    Policy = nullptr;
    TierManaged = false;
    TierCompactions.clear();
    BusyTierParts.clear();
    TierConflicts.clear();
    SerialTierGeneration.reset();
    TierOccupancyRevision = 0;
    TierSweepGeneration.reset();
    NextTierSweep = 0;
}

void TGenCompactionStrategy::ReflectSchema() {
    const auto* scheme = Backend->TableScheme(Table);

    TString err;
    bool ok = NLocalDb::ValidateCompactionPolicyChange(*Policy, *scheme->CompactionPolicy, err);
    Y_ENSURE(ok, "Cannot change compaction policy for table " << scheme->Name << " id " << scheme->Id << ": " << err);

    const bool tierManaged = UsesTiering(*scheme);
    if (TierManaged || tierManaged) {
        for (size_t index = 0; index < Generations.size(); ++index) {
            const ui32 before = Policy->Generations[index].TierId.value_or(0);
            const ui32 after = scheme->CompactionPolicy->Generations[index].TierId.value_or(0);
            Y_ENSURE(before == after || (Generations[index].Parts.empty()
                    && UnloadedPartsAtLevel(index + 1) == 0),
                "Cannot change TierId of generation " << index + 1
                    << " while it contains live parts; evacuate the generation first");
        }
        CancelTierCompactions();
    }
    Policy = scheme->CompactionPolicy;
    TierManaged = tierManaged;
    if (!TierManaged) {
        TierConflicts.clear();
        SerialTierGeneration.reset();
    }
    Y_ENSURE(Generations.size() <= Policy->Generations.size(), "Cannot currently shrink the number of generations");
    Generations.resize(Policy->Generations.size());

    // Recheck all generations
    for (ui32 index : xrange(Generations.size())) {
        CheckGeneration(index + 1);
    }

    UpdateOverload();
    if (TierManaged) {
        CheckTierGenerations();
    }
}

void TGenCompactionStrategy::ReflectRemovedRowVersions() {
    if (Generations && MaybeAutoStartForceCompaction()) {
        CheckGeneration(1);
    }
}

ui64 TGenCompactionStrategy::GetBackingSize() {
    return Stats.BackingSize;
}

ui64 TGenCompactionStrategy::GetBackingSize(ui64 ownerTabletId) {
    if (const auto* stats = StatsPerTablet.FindPtr(ownerTabletId)) {
        return stats->BackingSize;
    }

    return 0;
}

void TGenCompactionStrategy::UpdateCompactions() {
    for (ui32 index : xrange(Generations.size())) {
        auto& gen = Generations[index];

        if (gen.State != EState::PendingBackground) {
            continue;
        }

        auto& genPolicy = Policy->Generations[index];
        auto priority = ComputeBackgroundPriority(index + 1, genPolicy, Time->Now());
        auto oldPriority = gen.Task.Priority;

        // Avoid task updates in case of small priority changes.
        if (priority < oldPriority &&
            (oldPriority - priority) >= oldPriority / PRIORITY_UPDATE_FACTOR)
        {
            UpdateTask(
                gen.Task,
                genPolicy.BackgroundCompactionPolicy.ResourceBrokerTask,
                priority);
        }
    }
    if (TierManaged) {
        ScheduleTierSweep();
    }
}

ui64 TGenCompactionStrategy::BeginMemCompaction(TTaskId taskId, TSnapEdge edge, ui64 forcedCompactionId) {
    Y_ENSURE(MemCompactionId == 0, "Unexpected concurrent mem compaction requests");

    if (TierManaged) {
        // A conflicting flush is deferred by the caller, preserving its
        // snapshot/forced request until reservations become available.
        if (!CanStartTierCompaction(0)) {
            TierConflicts[0].WaitRevision = TierOccupancyRevision;
            return 0;
        }
        TExtraState extra;
        MemCompactionId = PrepareTierCompaction(taskId, edge, 0, extra,
            forcedCompactionId != 0, false);
        if (MemCompactionId != 0 && forcedCompactionId != 0) {
            ForcedMemCompactionId = MemCompactionId;
            NextForcedGenCompactionId = forcedCompactionId;
        }
        return MemCompactionId;
    }

    TExtraState extra;
    if (forcedCompactionId == 0 && !Policy->Generations.empty() && ForcedState == EForcedState::None && edge.Head == TEpoch::Max()) {
        extra.InitFromPolicy(Policy->Generations[0]);
    }

    MemCompactionId = PrepareCompaction(
        taskId,
        edge,
        /* generation */ 0,
        extra);
    Y_ENSURE(MemCompactionId != 0);

    if (forcedCompactionId != 0) {
        // We remember the last forced mem compaction we have started
        ForcedMemCompactionId = MemCompactionId;
        // We also remember the last user-provided forced compaction id
        NextForcedGenCompactionId = forcedCompactionId;
    }

    return MemCompactionId;
}

bool TGenCompactionStrategy::ScheduleBorrowedCompaction() {
    const ui64 ownerTabletId = Backend->OwnerTabletId();
    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy ScheduleBorrowedCompaction for " << ownerTabletId;
    }
    
    if (TierManaged) {
        if (!CanStartTierCompaction(255) || FinalState.State != EState::Free
                || ForcedState != EForcedState::None) {
            return false;
        }
        bool hasBorrowed = false;
        for (const auto& [label, level] : KnownParts) {
            Y_UNUSED(level);
            hasBorrowed |= label.TabletID() != ownerTabletId;
        }
        if (!hasBorrowed) {
            return false;
        }
        SubmitTask(FinalState.Task, "compaction_borrowed", 5, 255);
        FinalState.State = EState::Pending;
        return true;
    }

    // Find if we actually have borrowed parts
    bool hasBorrowed = false;
    for (const auto& pr : KnownParts) {
        if (pr.first.TabletID() != ownerTabletId) {
            hasBorrowed = true;
            Y_ENSURE(pr.second == 255,
                "Borrowed part " << pr.first << " not in final parts");
        }
    }

    if (!hasBorrowed || ForcedState != EForcedState::None || FinalState.State != EState::Free || FinalCompactionId != 0) {
        if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
            logl << "TGenCompactionStrategy ScheduleBorrowedCompaction for " << ownerTabletId
                << " nothing to compact"
                << " has borrowed " << hasBorrowed << ", parts " << KnownParts.size()
                << ", forced state " << ForcedState << ", final state " << FinalState.State << ", id " << FinalCompactionId;
        }
        return false;
    }

    SubmitTask(FinalState.Task, "compaction_borrowed", /* priority */ 5, /* generation */ 255);
    FinalState.State = EState::Pending;
    return true;
}

TCompactionChanges TGenCompactionStrategy::CompactionFinished(
        ui64 compactionId,
        THolder<TCompactionParams> rawParams,
        THolder<TCompactionResult> result)
{
    auto* params = CheckedCast<TGenCompactionParams*>(rawParams.Get());
    ui32 generation = params->Generation;
    Y_ENSURE(generation <= Generations.size() || generation == 255, "Unexpected CompactionFinished generation");
    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy CompactionFinished for " << Backend->OwnerTabletId()
            << ": compaction " << compactionId << ", generation " << generation;
    }

    if (generation == 0) {
        Y_ENSURE(compactionId == MemCompactionId,
            "Unexpected CompactionFinished for gen 0");
        MemCompactionId = 0;
    } else if (generation == 255) {
        Y_ENSURE(FinalState.State == EState::Compacting);
        Y_ENSURE(compactionId == FinalState.Task.CompactionId,
            "Unexpected CompactionFinished for gen " << generation);
        FinalState.State = EState::Free;
        FinalState.Task.TaskId = 0;
        FinalState.Task.CompactionId = 0;
    } else {
        auto& gen = Generations[generation - 1];
        Y_ENSURE(gen.State == EState::Compacting);
        Y_ENSURE(compactionId == gen.Task.CompactionId,
            "Unexpected CompactionFinished for gen " << generation);
        gen.State = EState::Free;
        gen.Task.TaskId = 0;
        gen.Task.CompactionId = 0;
    }

    if (params->Tiering) {
        return FinishTierCompaction(compactionId, *params, *result);
    }

    TCompactionChanges changes;

    TVector<bool> checkNeeded(Generations.size());

    auto& sourceParts = params->Parts;

    if (compactionId == FinalCompactionId || generation == 255) {
        // All taken cold parts must match our list of cold parts
        Y_ENSURE(params->ColdParts.size() <= FinalCompactionTaken);
        for (auto& part : params->ColdParts) {
            Y_ENSURE(!ColdParts.empty());
            auto& front = ColdParts.front();
            Y_ENSURE(front->Label == part->Label);
            KnownParts.erase(front->Label);
            ColdParts.pop_front();
            --FinalCompactionTaken;
        }
        params->ColdParts.clear();
        // All taken final parts must be at the tail of our source parts
        Y_ENSURE(sourceParts.size() >= FinalCompactionTaken);
        auto partStart = sourceParts.end() - FinalCompactionTaken;
        auto partEnd = sourceParts.end();
        for (auto partIt = partStart; partIt != partEnd; ++partIt) {
            Y_ENSURE(!FinalParts.empty());
            auto& front = FinalParts.front();
            Y_ENSURE(front.Label == (*partIt)->Label);
            KnownParts.erase(front.Label);
            FinalParts.pop_front();
            --FinalCompactionTaken;
        }
        sourceParts.erase(partStart, partEnd);
        FinalCompactionId = 0;
        // Compaction policy may have increased the number of generations
        for (ui32 index : xrange(size_t(generation), Generations.size())) {
            checkNeeded[index] = true;
        }
    }

    if (generation < Generations.size()) {
        auto& nextGen = Generations[generation];
        if (nextGen.TakenHeadParts != 0) {
            // This compaction has taken parts from the head of the next generation
            // These parts are expected to be at the tail of our source parts
            Y_ENSURE(sourceParts.size() >= nextGen.TakenHeadParts);
            auto partStart = sourceParts.end() - nextGen.TakenHeadParts;
            auto partEnd = sourceParts.end();
            for (auto partIt = partStart; partIt != partEnd; ++partIt) {
                Y_ENSURE(!nextGen.Parts.empty());
                auto& front = nextGen.Parts.front();
                Y_ENSURE(front.Label == (*partIt)->Label);
                KnownParts.erase(front.Label);
                nextGen.PopFront();
            }
            Y_ENSURE(nextGen.TakenHeadParts == 0);
            Y_ENSURE(nextGen.TakenHeadBackingSize == 0);
            Y_ENSURE(nextGen.TakenHeadPartEpochCount == 0);
            sourceParts.erase(partStart, partEnd);
            checkNeeded[generation] = true;
        }
    }

    bool forcedCompactionContinue = false;

    if (generation == 0) {
        // This was a memtable compaction, we don't expect anything else
        Y_ENSURE(sourceParts.empty());

        // Check if we just finished the last forced mem compaction
        if (ForcedMemCompactionId && compactionId == ForcedMemCompactionId) {
            ForcedMemCompactionId = 0;

            // Continue compaction when we don't have some other compaction running
            if (ForcedState == EForcedState::None) {
                CurrentForcedGenCompactionId = std::exchange(NextForcedGenCompactionId, 0);
                forcedCompactionContinue = true;
            }
        }
    } else if (generation == 255) {
        // This was a final parts compaction, we don't expect anything else
        Y_ENSURE(sourceParts.empty());
    } else {
        ui32 sourceIndex = generation - 1;
        Y_ENSURE(sourceIndex < Generations.size());

        if (ForcedState == EForcedState::Compacting && ForcedGeneration == generation) {
            ForcedState = EForcedState::None;
            forcedCompactionContinue = true;
        }

        while (sourceParts) {
            auto& sourcePart = sourceParts.back();
            auto& sourceGen = Generations[sourceIndex];
            Y_ENSURE(sourceGen.Parts);
            auto& part = sourceGen.Parts.back();
            Y_ENSURE(part.Label == sourcePart->Label,
                "Failed at gen=" << generation << ", sourceIndex=" << sourceIndex << ", headTaken=" << sourceGen.TakenHeadParts);
            Y_ENSURE(sourceGen.CompactingTailParts > 0);
            KnownParts.erase(part.Label);
            sourceGen.PopBack();
            sourceParts.pop_back();
            checkNeeded[sourceIndex] = true;
        }

        Y_ENSURE(sourceParts.empty());

        // Recheck compacted generation, it's free and may need a new compaction
        checkNeeded[generation - 1] = true;
    }

    auto& newParts = result->Parts;
    Y_ENSURE(result->PartOutputIds.size() == newParts.size(),
        "Compaction output ids do not cover the resulting parts");

    TStats newStats;
    for (const auto& partView : newParts) {
        Y_ENSURE(result->PartOutputIds.contains(partView->Label), "Missing compaction output id");
        // All outputs still share the same target level. Tier-aware placement
        // will use PartOutputIds when tier compaction is introduced.
        Y_DEBUG_ABORT_UNLESS(partView->Epoch == result->Epoch);
        Y_DEBUG_ABORT_UNLESS(!KnownParts.contains(partView->Label),
            "New part %s is already known", partView->Label.ToString().data());
        newStats += TStats(partView);
    }

    // This will be an index where we place results
    ui32 target = generation != 255 ? generation : Generations.size();

    // After forced compaction we may want to place results as low as possible
    if (forcedCompactionContinue) {
        while (target < Generations.size()) {
            auto& candidate = Generations[target];
            if (!candidate.Parts.empty()) {
                // Cannot pass non-empty generations
                break;
            }
            // Try to move to the next generation
            ++target;
        }
        if (target == Generations.size()) {
            if (generation >= target || (FinalParts.empty() && ColdParts.empty())) {
                // The forced compaction has finished, uplift logic will kick in below
                forcedCompactionContinue = false;
                if (target > generation) {
                    target = generation;
                }
                OnForcedGenCompactionDone();
            } else {
                // We need to compact final parts, so we need to place results at the last generation
                --target;
            }
        }
    }

    while (newParts && target > 0 && !forcedCompactionContinue) {
        auto& candidate = Generations[target - 1];
        if (candidate.CompactingTailParts > 0) {
            // Cannot uplift to busy generations
            break;
        }
        const auto& policy = Policy->Generations[target - 1];
        if (newStats.BackingSize > policy.UpliftPartSize) {
            // Uplifting not allowed by policy
            break;
        }
        ui64 candidateSize = candidate.Stats.BackingSize + newStats.BackingSize;
        ui64 candidateParts = candidate.PartEpochCount;
        if (candidate.Parts.empty() || candidate.Parts.back().Epoch != result->Epoch) {
            ++candidateParts; // new parts will add an epoch edge
        }
        if (policy.ForceSizeToCompact <= candidateSize ||
            policy.ForceCountToCompact <= candidateParts ||
            (policy.SizeToCompact <= candidateSize && policy.CountToCompact <= candidateParts))
        {
            // Avoid a risk of immediately triggering another compaction
            break;
        }
        // It is possible to safely uplift part to the previous generation
        --target;
        if (!candidate.Parts.empty()) {
            // Some parts are preventing us from going further
            break;
        }
    }

    changes.NewPartsLevel = target == Generations.size() ? 255 : target + 1;

    if (newParts) {
        for (const auto& partView : newParts) {
            KnownParts[partView->Label] = changes.NewPartsLevel;
        }

        if (target == Generations.size()) {
            for (auto it = newParts.rbegin(); it != newParts.rend(); ++it) {
                auto& partView = *it;
                FinalParts.emplace_front(std::move(partView));
            }
        } else {
            auto& newGen = Generations[target];
            if (target < generation) {
                Y_ENSURE(!newGen.Parts || result->Epoch <= newGen.Parts.back().Epoch);
                for (auto it = newParts.begin(); it != newParts.end(); ++it) {
                    auto& partView = *it;
                    newGen.PushBack(std::move(partView));
                }
            } else {
                Y_ENSURE(!newGen.Parts || result->Epoch >= newGen.Parts.front().Epoch);
                for (auto it = newParts.rbegin(); it != newParts.rend(); ++it) {
                    auto& partView = *it;
                    newGen.PushFront(std::move(partView));
                }
            }

            // We just added some parts to target
            checkNeeded[target] = true;
        }
    }

    UpdateStats();

    if (forcedCompactionContinue) {
        Y_ENSURE(target < Generations.size());
        ForcedState = EForcedState::Pending;
        ForcedGeneration = target + 1;
        checkNeeded[target] = true;
    }

    if (Generations) {
        // Check various conditions for starting a forced compaction
        if (ForcedState == EForcedState::None) {
            bool startForcedCompaction = false;

            if (NextForcedGenCompactionId && ForcedMemCompactionId == 0) {
                // The previous forced compaction has finished, start gen compactions
                CurrentForcedGenCompactionId = std::exchange(NextForcedGenCompactionId, 0);
                startForcedCompaction = true;
            } else if (NeedToStartForceCompaction()) {
                startForcedCompaction = true;
            }

            if (startForcedCompaction) {
                ForcedState = EForcedState::Pending;
                ForcedGeneration = 1;
                checkNeeded[0] = true;
            }
        }

        // Always check the final generation
        checkNeeded.back() = true;

        for (ui32 index : xrange(Generations.size())) {
            if (checkNeeded[index]) {
                CheckGeneration(index + 1);
            }
        }
    }

    UpdateOverload();

    return changes;
}

void TGenCompactionStrategy::OnForcedGenCompactionDone() {
    if (CurrentForcedGenCompactionId != 0) {
        FinishedForcedGenCompactionId = std::exchange(CurrentForcedGenCompactionId, 0);
    }
    FinishedForcedGenCompactionTs = Time->Now();
}

void TGenCompactionStrategy::PartMerged(TPartView partView, ui32 level) {
    Y_ENSURE(level == 255, "Unexpected level of the merged part");
    if (TierManaged) {
        CancelTierCompactions();
    }

    const auto label = partView->Label;

    // Remove the old part data from our model (since it may have been changed)
    if (KnownParts.contains(label)) {
        Y_ENSURE(KnownParts[label] == level, "Borrowed part cannot be moved between levels");
        Y_ENSURE(FinalCompactionId == 0 || FinalCompactionTaken == 0,
            "Borrowed part attaching while final compaction is in progress");

        // Inefficient, but this case shouldn't happen in practice, since we
        // don't allow a diamond shaped split/merge sequence in datashards.
        for (auto it = ColdParts.begin(); it != ColdParts.end(); ++it) {
            Y_ENSURE((*it)->Label != label,
                "Cannot attach borrowed part, another cold part with the same label exists");
        }
        for (auto it = FinalParts.begin(); it != FinalParts.end(); ++it) {
            if (it->Label == label) {
                Stats -= it->Stats;
                StatsPerTablet[label.TabletID()] -= it->Stats;
                KnownParts.erase(it->Label);
                FinalParts.erase(it);
                break;
            }
        }

        // Make sure we use an up to date part info
        partView = Backend->TablePart(Table, label);
    }

    KnownParts[label] = level;
    auto& back = FinalParts.emplace_back(std::move(partView));
    Stats += back.Stats;
    StatsPerTablet[back.Label.TabletID()] += back.Stats;
    if (TierManaged) {
        RebuildTierStats();
        CheckTierGenerations();
    }

    // WARNING: we don't call UpdateStats here, so GarbageStatsAgg is not
    // recalculated properly. This is not a problem, because this method is
    // usually called when parts are borrowed, and we are not supposed to
    // have any of our own data at that stage. We would wait for the first
    // compaction to properly recalculate garbage stats histogram.
}

void TGenCompactionStrategy::PartMerged(TIntrusiveConstPtr<TColdPart> part, ui32 level) {
    Y_ENSURE(level == 255, "Unexpected level of the merged part");
    if (TierManaged) {
        CancelTierCompactions();
    }

    const auto label = part->Label;
    Y_ENSURE(!KnownParts.contains(label),
        "Borrowed part attaching when another part with the same label exists");

    ColdParts.emplace_back(std::move(part));
    KnownParts[label] = level;
    if (TierManaged) {
        CheckTierGenerations();
    }
}

TCompactionChanges TGenCompactionStrategy::PartsRemoved(TArrayRef<const TLogoBlobID> parts) {
    // Get rid of removed parts from current state
    for (TLogoBlobID label : parts) {
        KnownParts.erase(label);
    }

    // For simplicity just stop and start again
    auto state = SnapshotState();

    Stop();

    Start(std::move(state));

    // We don't have per-part state, so no changes
    return { };
}

TCompactionState TGenCompactionStrategy::SnapshotState() {
    TCompactionState state;
    state.PartLevels = KnownParts;
    return state;
}

TCompactionChanges TGenCompactionStrategy::ApplyChanges() {
    return { };
}

bool TGenCompactionStrategy::AllowForcedCompaction() {
    return (
        /* there is no forced compaction queued by the backend */
        ForcedMemCompactionId == 0 &&
        /* there is no pending or running forced gen compaction */
        ForcedState == EForcedState::None);
}

void TGenCompactionStrategy::OutputHtml(IOutputStream& out) {
    HTML(out) {
        if (TierManaged) {
            DIV_CLASS("row") {
                out << "Active tier compactions: " << TierCompactions.size()
                    << ", Mem input conflicts: " << GetTierCompactionConflicts(0)
                    << ", Final input conflicts: " << GetTierCompactionConflicts(255);
                if (SerialTierGeneration) {
                    out << ", Exclusive retry generation: " << *SerialTierGeneration;
                }
            }
        }
        ui32 nextGenerationId = 1;
        for (const auto& gen : Generations) {
            ui32 generation = nextGenerationId++;
            DIV_CLASS("row") {out << "Comp. generation: " << generation
                                << ", Parts count: " << gen.Parts.size()
                                << ", Backing size: " << gen.Stats.BackingSize
                                << ", Epochs count: " << gen.PartEpochCount
                                << ", Overload factor: " << gen.OverloadFactor
                                << ", Compaction state: " << gen.State; }
            if (TierManaged) {
                out << ", Input conflicts: " << GetTierCompactionConflicts(generation);
            }
            if (ForcedState != EForcedState::None && ForcedGeneration == generation) {
                out << ", Forced compaction: " << ForcedState;
            }
            if (gen.Task.TaskId) {
                out << ", Task #" << gen.Task.TaskId
                    << " (priority " << gen.Task.Priority
                    << ") submitted " << gen.Task.SubmissionTimestamp.ToStringLocal();
            }

            PRE() {
                for (const auto& part : gen.Parts) {
                    auto& bundleId = part.Label;

                    out << "Genstep: " << bundleId.Generation() << ":" << bundleId.Step()
                            << ", Backing size: " << part.Stats.BackingSize
                            << ", Base: " << bundleId << Endl;
                }
            }
        }

        if (FinalParts) {
            DIV_CLASS("row") {out << "Final parts count: " << FinalParts.size();}
            if (FinalState.State != EState::Free) {
                out << ", Compaction state: " << FinalState.State;
            }
            if (FinalState.Task.TaskId) {
                out << ", Task #" << FinalState.Task.TaskId
                    << " (priority " << FinalState.Task.Priority
                    << ") submitted " << FinalState.Task.SubmissionTimestamp.ToStringLocal();
            }
            PRE() {
                for (const auto& part : FinalParts) {
                    auto &bundleId = part.Label;

                    out << "Genstep: " << bundleId.Generation() << ":" << bundleId.Step()
                            << ", Backing size: " << part.Stats.BackingSize
                            << ", Base: " << bundleId << Endl;
                }
            }
        }

        if (ColdParts) {
            DIV_CLASS("row") {out << "Cold parts count: " << ColdParts.size();}
            PRE() {
                for (const auto& part : ColdParts) {
                    auto& bundleId = part->Label;

                    out << "Genstep: " << bundleId.Generation() << ":" << bundleId.Step()
                        << ", Base: " << bundleId << Endl;
                }
            }
        }
    }
}

void TGenCompactionStrategy::BeginGenCompaction(TTaskId taskId, ui32 generation) {
    if (TierManaged) {
        BeginTierGenCompaction(taskId, generation);
        return;
    }
    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy BeginGenCompaction for " << Backend->OwnerTabletId()
            << ": task " << taskId << ", generation " << generation;
    }

    // Special mode for final parts compaction
    if (generation == 255) {
        Y_ENSURE(FinalState.State == EState::Pending);
        Y_ENSURE(FinalState.Task.TaskId == taskId);

        auto cancelFinal = [&]() {
            Broker->FinishTask(taskId, EResourceStatus::Cancelled);
            FinalState.Task.TaskId = 0;

            FinalState.State = EState::Free;
        };

        if (FinalCompactionId != 0) {
            // Another final compaction is already compacting final parts for us
            if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                logl << "TGenCompactionStrategy BeginGenCompaction for " << Backend->OwnerTabletId()
                    << ": task " << taskId << ", generation " << generation
                    << " Cancel: another final compaction " << FinalCompactionId << " is already compacting final parts for us";
            }
            cancelFinal();
            return;
        }

        // final parts compaction, only valid when the last generation is free
        if (!Generations.empty()) {
            auto& gen = Generations.back();
            if (gen.State != EState::Free) {
                // The last generation will compact final parts for us
                if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                    logl << "TGenCompactionStrategy BeginGenCompaction for " << Backend->OwnerTabletId()
                        << ": task " << taskId << ", generation " << generation
                        << " Cancel: the last " << gen.Task.CompactionId << " " << gen.State << " generation will compact final parts for us";
                }
                cancelFinal();
                return;
            }
        } else {
            if (MemCompactionId != 0) {
                // The last generation is compacting final parts for us
                if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                    logl << "TGenCompactionStrategy BeginGenCompaction for " << Backend->OwnerTabletId()
                        << ": task " << taskId << ", generation " << generation
                        << " Cancel: the last mem " << MemCompactionId << " generation will compact final parts for us";
                }
                cancelFinal();
                return;
            }
        }

        TExtraState extra;
        auto params = MakeHolder<TGenCompactionParams>();
        params->Table = Table;
        params->TaskId = taskId;
        params->Generation = generation;
        FinalState.Task.CompactionId = PrepareCompaction(
            taskId,
            /* edge */ { },
            generation,
            extra);
        FinalState.State = EState::Compacting;
        return;
    }

    Y_ENSURE(generation > 0);
    Y_ENSURE(generation <= Generations.size());

    auto& gen = Generations[generation - 1];
    Y_ENSURE(gen.State == EState::Pending || gen.State == EState::PendingBackground);
    Y_ENSURE(gen.Task.TaskId == taskId);

    const bool forced = NeedToForceCompact(generation);

    auto cancelCompaction = [&]() {
        Broker->FinishTask(taskId, EResourceStatus::Cancelled);
        gen.Task.TaskId = 0;

        gen.State = EState::Free;

        Y_ENSURE(!forced, "Unexpected cancellation of a forced compaction");
    };

    if (DesiredMode(generation) == EDesiredMode::None) {
        // We no longer want to perform this compaction
        cancelCompaction();
        return;
    }

    TExtraState extra;
    if (!forced && generation < Generations.size()) {
        extra.InitFromPolicy(Policy->Generations[generation]);
    }

    auto params = MakeHolder<TGenCompactionParams>();
    params->Table = Table;
    params->TaskId = taskId;
    params->Generation = generation;
    gen.Task.CompactionId = PrepareCompaction(
        taskId,
        /* edge */ { },
        generation,
        extra);

    gen.State = EState::Compacting;

    if (forced) {
        // We have just started a forced compaction
        ForcedState = EForcedState::Compacting;
        Y_ENSURE(ForcedGeneration == generation);
    }
}

void TGenCompactionStrategy::SubmitTask(
        TGenCompactionStrategy::TCompactionTask& task, TString type, ui32 priority, ui32 generation)
{
    Y_ENSURE(task.TaskId == 0, "Task is already submitted");
    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy SubmitTask for " << Backend->OwnerTabletId()
            << ": type " << type << ", priority " << priority << ", generation " << generation;
    }

    task.SubmissionTimestamp = Time->Now();
    task.Priority = priority;

    TString name = Sprintf("gen%" PRIu32 "-table-%" PRIu32 "-%s",
                           generation, Table, TaskNameSuffix.data());

    task.TaskId = Broker->SubmitTask(
        std::move(name),
        TResourceParams(std::move(type))
            .WithCPU(1)
            .WithPriority(task.Priority),
        [this, generation](TTaskId taskId) {
            BeginGenCompaction(taskId, generation);
        });

    Y_DEBUG_ABORT_UNLESS(task.TaskId != 0, "Resource broker returned unexpected zero task id");
}

void TGenCompactionStrategy::UpdateTask(
        TGenCompactionStrategy::TCompactionTask& task, TString type, ui32 priority)
{
    Y_ENSURE(task.TaskId != 0, "Task was not submitted");
    task.Priority = priority;

    Broker->UpdateTask(
        task.TaskId,
        TResourceParams(std::move(type))
            .WithCPU(1)
            .WithPriority(task.Priority));
}

ui32 TGenCompactionStrategy::ComputeBackgroundPriority(
        ui32 generation,
        const TCompactionPolicy::TGenerationPolicy& policy,
        TInstant now) const
{
    Y_ENSURE(generation > 0);
    Y_ENSURE(generation <= Generations.size());

    if (NeedToForceCompact(generation)) {
        // This background compaction will be used for the forced compaction
        // TODO: figure out which priority we should use for such compactions
        return Policy->DefaultTaskPriority;
    }

    auto& gen = Generations[generation - 1];
    ui64 genSize = gen.Stats.BackingSize - gen.TakenHeadBackingSize;
    ui64 genParts = gen.PartEpochCount - gen.TakenHeadPartEpochCount;

    auto& bckgPolicy = policy.BackgroundCompactionPolicy;
    auto perc = Max(genSize * 100 / policy.ForceSizeToCompact,
                    genParts * 100 / policy.ForceCountToCompact);

    if (NeedToCompactAfterSplit(generation))
        perc = Max<ui32>(perc, bckgPolicy.Threshold);

    if (!perc || perc < bckgPolicy.Threshold)
        return BAD_PRIORITY;

    return ComputeBackgroundPriority(gen.Task, bckgPolicy, perc, now);
}

ui32 TGenCompactionStrategy::ComputeBackgroundPriority(
        const TGenCompactionStrategy::TCompactionTask& task,
        const TCompactionPolicy::TBackgroundPolicy& policy,
        ui32 percentage,
        TInstant now) const
{
    double priority = policy.PriorityBase;
    priority = priority * 100 / percentage;
    if (task.TaskId) {
        double factor = policy.TimeFactor;
        factor *= log((now - task.SubmissionTimestamp).Seconds());
        priority /= Max(factor, 1.0);
    }

    return priority;
}

void TGenCompactionStrategy::CheckOverload(ui32 generation) {
    auto& gen = Generations[generation - 1];
    auto& genPolicy = Policy->Generations[generation - 1];
    ui64 genSize = gen.Stats.BackingSize;
    ui32 genParts = gen.PartEpochCount;

    float overloadFactor = 0;

    auto mapToRange = [](float val, float minVal, float maxVal) -> float {
        if (val < minVal)
            return 0;
        if (val > maxVal)
            return 1;
        return (val - minVal) / (maxVal - minVal);
    };

    // TODO: make lo and hi watermarks configurable
    float loK = 1.5;
    float hiK = 3;
    overloadFactor = Max(overloadFactor, mapToRange(genSize, genPolicy.ForceSizeToCompact*loK, genPolicy.ForceSizeToCompact*hiK));
    overloadFactor = Max(overloadFactor, mapToRange(genParts, genPolicy.ForceCountToCompact*loK, genPolicy.ForceCountToCompact*hiK));
    gen.OverloadFactor = overloadFactor;
}

void TGenCompactionStrategy::CheckGeneration(ui32 generation) {
    Y_ENSURE(generation > 0);

    CheckOverload(generation);

    if (TierManaged && (!CanStartTierCompaction(generation)
            || (!HasFreeTierInputs(generation) && !NeedToForceCompact(generation)
                && SerialTierGeneration != generation))) {
        return;
    }

    auto& gen = Generations[generation - 1];

    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy CheckGeneration for " << Backend->OwnerTabletId()
            << " generation " << generation << ", state " << gen.State 
            << ", final id " << FinalCompactionId << ", final level " << FinalCompactionLevel;
    }

    if (gen.State != EState::Free && gen.State != EState::PendingBackground) {
        return;
    }

    // We shouldn't start compaction of generations that appeared after we
    // have already started compacting final parts, as this may compromise
    // consistency.
    if (FinalCompactionId != 0 && generation > FinalCompactionLevel) {
        return;
    }

    const auto& genPolicy = Policy->Generations[generation - 1];
    switch (DesiredMode(generation)) {
        case EDesiredMode::None:
            break;
        case EDesiredMode::Normal:
            // Replace background task or submit a new one.
            if (gen.State == EState::PendingBackground) {
                UpdateTask(
                    gen.Task,
                    genPolicy.ResourceBrokerTask,
                    Policy->DefaultTaskPriority);
            } else {
                SubmitTask(
                    gen.Task,
                    genPolicy.ResourceBrokerTask,
                    Policy->DefaultTaskPriority,
                    generation);
            }
            gen.State = EState::Pending;
            break;
        case EDesiredMode::Background:
            if (gen.State == EState::Free) {
                auto priority = ComputeBackgroundPriority(generation, genPolicy, Time->Now());
                if (priority != BAD_PRIORITY) {
                    SubmitTask(
                        gen.Task,
                        genPolicy.BackgroundCompactionPolicy.ResourceBrokerTask,
                        priority,
                        generation);
                    gen.State = EState::PendingBackground;
                }
            }
            break;
    }
}

TGenCompactionStrategy::EDesiredMode TGenCompactionStrategy::DesiredMode(ui32 generation) const {
    Y_ENSURE(generation > 0);
    Y_ENSURE(generation <= Generations.size());

    auto& gen = Generations[generation - 1];
    if (TierManaged) {
        if (NeedToForceCompact(generation) || TierSweepGeneration == generation) {
            return EDesiredMode::Normal;
        }
        TPartAggregator available;
        for (const auto& part : gen.Parts) {
            if (!BusyTierParts.contains(part.Label)) {
                available.Add(part);
            }
        }
        size_t unloaded = 0;
        size_t unloadedFinal = 0;
        for (const auto& part : ColdParts) {
            if (!BusyTierParts.contains(part->Label)) {
                const ui32 level = KnownParts.Value(part->Label, 255);
                unloaded += level == generation;
                unloadedFinal += level == 255;
            }
        }
        const bool hasFinal = TierForLevel(generation) == 0 && IsLastTierGeneration(generation)
            && (unloadedFinal != 0 || std::any_of(FinalParts.begin(), FinalParts.end(),
                [&](const auto& part) { return !BusyTierParts.contains(part.Label); }));
        if (unloaded != 0) {
            return EDesiredMode::Normal;
        }
        if (available.PartEpochCount == 0 && !hasFinal) {
            return EDesiredMode::None;
        }
        // Rewriting a single available epoch at the end of its tier does not
        // reduce it. Reserved epochs must not cause repeated singleton scans.
        if (available.PartEpochCount <= 1 && !hasFinal && IsLastTierGeneration(generation)) {
            return EDesiredMode::None;
        }
        const auto& policy = Policy->Generations[generation - 1];
        if (policy.ForceSizeToCompact <= available.Stats.BackingSize
                || policy.ForceCountToCompact <= available.PartEpochCount
                || (policy.SizeToCompact <= available.Stats.BackingSize
                    && policy.CountToCompact <= available.PartEpochCount)) {
            return EDesiredMode::Normal;
        }
        size_t finalEpochs = 0;
        if (hasFinal) {
            TEpoch previous = TEpoch::Max();
            for (const auto& part : FinalParts) {
                if (!BusyTierParts.contains(part.Label) && part.Epoch != previous) {
                    ++finalEpochs;
                    previous = part.Epoch;
                }
            }
            finalEpochs += unloadedFinal;
        }
        return available.PartEpochCount + finalEpochs > 1
            ? EDesiredMode::Background : EDesiredMode::None;
    }
    ui64 genSize = gen.Stats.BackingSize - gen.TakenHeadBackingSize;
    ui32 genParts = gen.PartEpochCount - gen.TakenHeadPartEpochCount;

    const auto& policy = Policy->Generations[generation - 1];
    if (policy.ForceSizeToCompact <= genSize ||
        policy.ForceCountToCompact <= genParts ||
        (policy.SizeToCompact <= genSize && policy.CountToCompact <= genParts))
    {
        return EDesiredMode::Normal;
    }

    if (NeedToForceCompact(generation)) {
        // FIXME: should forced compaction prefer background?
        return EDesiredMode::Normal;
    }

    // Background compaction only makes sense when there is more than one part available for compaction
    ui32 totalParts = genParts + (generation == Generations.size() ? FinalParts.size() + ColdParts.size() : 0);

    if (totalParts > 1 || NeedToCompactAfterSplit(generation)) {
        return EDesiredMode::Background;
    }

    return EDesiredMode::None;
}

// This is a simple heuristic that triggers compaction of borrowed parts (they all are at final level) on split dst
// tablet after some updates happen after split.
bool TGenCompactionStrategy::NeedToCompactAfterSplit(ui32 generation) const {
    Y_ENSURE(generation > 0);
    Y_ENSURE(generation <= Generations.size());

    // Last generation?
    if (generation != Generations.size())
        return false;

    // Too many final parts?
    size_t finalPartsCount = FinalParts.size() + ColdParts.size();
    if (finalPartsCount < 2)
        return false;
    if (finalPartsCount >= 100)
        return true;

    // Previous 2 generations are not empty?
    for (ui32 parent : xrange(generation > 2 ? generation - 2 : 0, generation)) {
        if (Generations[parent].Stats.BackingSize > 0) {
            return true;
        }
    }

    return false;
}

ui32 TGenCompactionStrategy::TierForLevel(ui32 level) const {
    if (level == 0 || level == 255) {
        return 0;
    }
    Y_ENSURE(level <= Policy->Generations.size(), "Unknown compaction generation");
    return Policy->Generations[level - 1].TierId.value_or(0);
}

bool TGenCompactionStrategy::IsLastTierGeneration(ui32 generation) const {
    Y_ENSURE(generation > 0 && generation <= Generations.size());
    return generation == Generations.size()
        || TierForLevel(generation) != TierForLevel(generation + 1);
}

size_t TGenCompactionStrategy::UnloadedPartsAtLevel(ui32 level) const {
    size_t count = 0;
    for (const auto& part : ColdParts) {
        count += KnownParts.Value(part->Label, 255) == level;
    }
    return count;
}

void TGenCompactionStrategy::ValidateTierEpochs() const {
    // Historical borrowed FinalParts may overlap configured default generations.
    // The prepare/placement helper reconciles those by widening its inputs;
    // do not reject such a restored table before it can run that operation.
    THashMap<ui32, TEpoch> previous;
    for (ui32 level = 1; level <= Generations.size(); ++level) {
        std::optional<TEpoch> minimum;
        std::optional<TEpoch> maximum;
        auto add = [&](TEpoch epoch) {
            minimum = minimum ? Min(*minimum, epoch) : epoch;
            maximum = maximum ? Max(*maximum, epoch) : epoch;
        };
        for (const auto& part : Generations[level - 1].Parts) {
            add(part.Epoch);
        }
        for (const auto& part : ColdParts) {
            if (KnownParts.Value(part->Label, 255) == level) {
                add(part->Epoch);
            }
        }
        if (minimum) {
            const auto [it, inserted] = previous.emplace(TierForLevel(level), TEpoch::Max());
            Y_UNUSED(inserted);
            Y_ENSURE(*maximum <= it->second,
                "Compaction epochs are not ordered within a tier");
            it->second = *minimum;
        }
    }
}

void TGenCompactionStrategy::RebuildTierStats() {
    THashMap<ui32, TEpoch> lastEpochs;
    for (size_t index = 0; index < Generations.size(); ++index) {
        auto& gen = Generations[index];
        gen.Parts.sort();
        TPartAggregator agg;
        agg.Add(gen.Parts);
        gen.Stats = agg.Stats;
        gen.StatsPerTablet = std::move(agg.StatsPerTablet);
        gen.PartEpochCount = agg.PartEpochCount;
        gen.CompactingTailParts = 0;
        gen.TakenHeadParts = 0;
        gen.TakenHeadBackingSize = 0;
        gen.TakenHeadPartEpochCount = 0;
        if (!gen.Parts.empty()) {
            const auto [it, inserted] = lastEpochs.emplace(TierForLevel(index + 1), TEpoch::Max());
            Y_UNUSED(inserted);
            Y_ENSURE(gen.Parts.front().Epoch <= it->second,
                "Compaction epochs are not ordered within a tier");
            it->second = gen.Parts.back().Epoch;
        }
        CheckOverload(index + 1);
    }
    FinalParts.sort();
    ValidateTierEpochs();
    UpdateStats();
    UpdateOverload();
}

ui32 TGenCompactionStrategy::GetTierCompactionConflicts(ui32 generation) const {
    const auto it = TierConflicts.find(generation);
    return it == TierConflicts.end() ? 0 : it->second.Count;
}

bool TGenCompactionStrategy::CanStartTierCompaction(ui32 generation) const {
    if (TierCompactions.contains(generation)) {
        return false;
    }
    if (SerialTierGeneration) {
        return *SerialTierGeneration == generation && TierCompactions.empty();
    }
    const auto it = TierConflicts.find(generation);
    return it == TierConflicts.end() || it->second.WaitRevision != TierOccupancyRevision;
}

bool TGenCompactionStrategy::HasFreeTierInputs(ui32 generation) const {
    const auto hasLoaded = [&](const auto& parts) {
        for (const auto& part : parts) {
            if (!BusyTierParts.contains(part.Label)) {
                return true;
            }
        }
        return false;
    };
    if (generation != 0 && generation != 255 && hasLoaded(Generations[generation - 1].Parts)) {
        return true;
    }
    const bool withFinal = generation == 255 || (generation != 0
        && TierForLevel(generation) == 0 && IsLastTierGeneration(generation));
    if (withFinal && hasLoaded(FinalParts)) {
        return true;
    }
    for (const auto& part : ColdParts) {
        const ui32 level = KnownParts.Value(part->Label, 255);
        if ((level == generation || (withFinal && level == 255))
                && !BusyTierParts.contains(part->Label)) {
            return true;
        }
    }
    return false;
}

void TGenCompactionStrategy::NoteTierCompactionConflict(ui32 generation) {
    auto& conflicts = TierConflicts[generation];
    if (conflicts.Count != Max<ui32>()) {
        ++conflicts.Count;
    }
    conflicts.WaitRevision = TierOccupancyRevision;
    if (!SerialTierGeneration && conflicts.Count >= Policy->TieringCompactionConflictLimit) {
        SerialTierGeneration = generation;
    }
}

bool TGenCompactionStrategy::TryReserveCompaction(TCompactionParams& rawParams) {
    if (!rawParams.Tiering) {
        return true;
    }
    auto& params = *CheckedCast<TGenCompactionParams*>(&rawParams);
    Y_ENSURE(params.Context, "Cannot reserve an unprepared compaction");
    if (!CanStartTierCompaction(params.Generation)) {
        TierConflicts[params.Generation].WaitRevision = TierOccupancyRevision;
        return false;
    }
    for (const auto& part : params.Context->Inputs) {
        if (BusyTierParts.contains(part.Label())) {
            NoteTierCompactionConflict(params.Generation);
            return false;
        }
    }
    for (const auto& [generation, active] : TierCompactions) {
        Y_UNUSED(generation);
        if (params.Context->Epochs && active.Context->Epochs
                && params.Context->Epochs->Min <= active.Context->Epochs->Max
                && active.Context->Epochs->Min <= params.Context->Epochs->Max) {
            // Frozen/status sources can make intervals overlap even when no
            // existing disk label is shared. Their outputs must not overlap.
            NoteTierCompactionConflict(params.Generation);
            return false;
        }
    }
    auto& settings = *params.Tiering;
    settings.UseConservativeMasks = !SerialTierGeneration.has_value();
    for (const auto& [generation, active] : TierCompactions) {
        if (active.Params->Tiering->HasMemoryInputs) {
            settings.HasUnknownRemaining = true;
        }
        Y_UNUSED(generation);
    }
    if (params.Generation == 0 && !TierCompactions.empty()) {
        // Older scans only captured existing parts. New memory keys may not
        // enter intermediate tiers behind those scans' captured cap masks.
        settings.AllowCooling = false;
    }
    auto& active = TierCompactions[params.Generation];
    active.Context = params.Context;
    active.Params = &params;
    for (const auto& part : params.Context->Inputs) {
        Y_ENSURE(BusyTierParts.emplace(part.Label(), params.Generation).second,
            "Compaction input was reserved twice");
    }
    return true;
}

void TGenCompactionStrategy::ReleaseTierCompaction(ui32 generation, ui64 compactionId, bool success) {
    const auto it = TierCompactions.find(generation);
    Y_ENSURE(it != TierCompactions.end() && it->second.CompactionId == compactionId,
        "Unexpected tier-managed compaction release");
    const bool successfulTac = success
        && it->second.Params->Mode == TCompactionParams::EMode::TierAware;
    for (const auto& part : it->second.Context->Inputs) {
        Y_ENSURE(BusyTierParts.erase(part.Label()) == 1, "Compaction input was not reserved");
    }
    TierCompactions.erase(it);
    ++TierOccupancyRevision;
    if (successfulTac) {
        TierConflicts.erase(generation);
        if (SerialTierGeneration == generation) {
            SerialTierGeneration.reset();
        }
    } else if (success) {
        // Regular completed its own request, but has not resolved TAC's
        // accumulated conflicts. Retain the count without a stale wait marker.
        const auto conflicts = TierConflicts.find(generation);
        if (conflicts != TierConflicts.end()) {
            conflicts->second.WaitRevision.reset();
        }
    }
}

void TGenCompactionStrategy::CheckTierGenerations() {
    if (!TierManaged) {
        return;
    }
    if (SerialTierGeneration) {
        // Drain running operations without preemption. Pending tasks must not
        // take the newly available inputs before the starved generation.
        const auto cancelPending = [&](ui32 generation, EState& state, TCompactionTask& task) {
            if (generation != *SerialTierGeneration
                    && (state == EState::Pending || state == EState::PendingBackground)) {
                Broker->CancelTask(task.TaskId);
                state = EState::Free;
                task = {};
            }
        };
        for (ui32 level = 1; level <= Generations.size(); ++level) {
            cancelPending(level, Generations[level - 1].State, Generations[level - 1].Task);
        }
        cancelPending(255, FinalState.State, FinalState.Task);
        if (!TierCompactions.empty()) {
            return;
        }
        const ui32 generation = *SerialTierGeneration;
        if (generation == 0) {
            Backend->RequestMemCompactionRetry(Table);
        } else {
            auto& state = generation == 255 ? FinalState.State : Generations[generation - 1].State;
            auto& task = generation == 255 ? FinalState.Task : Generations[generation - 1].Task;
            if (state == EState::Free) {
                const auto& type = generation == 255 ? Policy->InMemResourceBrokerTask
                    : Policy->Generations[generation - 1].ResourceBrokerTask;
                SubmitTask(task, type, Policy->DefaultTaskPriority, generation);
                state = EState::Pending;
            }
        }
        return;
    }
    const auto memory = TierConflicts.find(0);
    if (memory != TierConflicts.end() && memory->second.WaitRevision
            && CanStartTierCompaction(0)) {
        Backend->RequestMemCompactionRetry(Table);
    }
    if (ForcedState == EForcedState::Pending) {
        if (!Generations.empty() && ForcedGeneration != 255) {
            CheckGeneration(ForcedGeneration);
        } else if (FinalState.State == EState::Free && CanStartTierCompaction(255)) {
            ForcedGeneration = 255;
            SubmitTask(FinalState.Task, Policy->InMemResourceBrokerTask,
                Policy->DefaultTaskPriority, 255);
            FinalState.State = EState::Pending;
        }
        return;
    }
    for (ui32 level = 1; level <= Generations.size(); ++level) {
        CheckGeneration(level);
    }
    // FinalParts are a separate default-tier list, even when the last
    // configured generation belongs to a cold tier.
    size_t finalEpochs = 0;
    TEpoch previous = TEpoch::Max();
    for (const auto& part : FinalParts) {
        if (!BusyTierParts.contains(part.Label) && part.Epoch != previous) {
            ++finalEpochs;
            previous = part.Epoch;
        }
    }
    const bool hasUnloadedFinal = std::any_of(ColdParts.begin(), ColdParts.end(), [&](const auto& part) {
        return KnownParts.Value(part->Label, 255) == 255 && !BusyTierParts.contains(part->Label);
    });
    if (FinalState.State == EState::Free && CanStartTierCompaction(255)
            && HasFreeTierInputs(255)
            && (TierSweepGeneration == 255 || finalEpochs > 1 || hasUnloadedFinal)) {
        SubmitTask(FinalState.Task, Policy->InMemResourceBrokerTask,
            Policy->DefaultTaskPriority, 255);
        FinalState.State = EState::Pending;
    }
}

void TGenCompactionStrategy::CancelTierCompactions() {
    auto cancelTask = [&](EState& state, TCompactionTask& task) {
        if (state == EState::Pending || state == EState::PendingBackground) {
            Broker->CancelTask(task.TaskId);
        } else if (state == EState::Compacting) {
            Backend->CancelCompaction(task.CompactionId);
        }
        state = EState::Free;
        task = {};
    };
    for (auto& gen : Generations) {
        cancelTask(gen.State, gen.Task);
        gen.CompactingTailParts = 0;
        gen.TakenHeadParts = 0;
        gen.TakenHeadBackingSize = 0;
        gen.TakenHeadPartEpochCount = 0;
    }
    cancelTask(FinalState.State, FinalState.Task);
    if (MemCompactionId != 0) {
        Backend->CancelCompaction(MemCompactionId);
        if (ForcedMemCompactionId == MemCompactionId) {
            // The outer memory request still owns this forced id and will
            // retry it. A parallel disk completion must not advance it early.
            NextForcedGenCompactionId = 0;
        }
        MemCompactionId = 0;
        ForcedMemCompactionId = 0;
    }
    if (ForcedState == EForcedState::Compacting) {
        ForcedState = EForcedState::Pending;
    }
    FinalCompactionId = 0;
    FinalCompactionLevel = 0;
    FinalCompactionTaken = 0;
    TierSweepGeneration.reset();
    TierCompactions.clear();
    BusyTierParts.clear();
    ++TierOccupancyRevision;
    const auto memory = TierConflicts.find(0);
    if (memory != TierConflicts.end() && memory->second.WaitRevision) {
        // ReflectSchema may switch back to the legacy path, which will no
        // longer call CheckTierGenerations to wake an already deferred flush.
        Backend->RequestMemCompactionRetry(Table);
    }
}

void TGenCompactionStrategy::BeginTierGenCompaction(TTaskId taskId, ui32 generation) {
    Y_ENSURE(generation == 255 || (generation > 0 && generation <= Generations.size()));
    auto& state = generation == 255 ? FinalState.State : Generations[generation - 1].State;
    auto& task = generation == 255 ? FinalState.Task : Generations[generation - 1].Task;
    if ((state != EState::Pending && state != EState::PendingBackground) || task.TaskId != taskId) {
        Broker->FinishTask(taskId, EResourceStatus::Cancelled);
        return;
    }
    const bool forced = ForcedState == EForcedState::Pending && ForcedGeneration == generation;
    const bool sweep = TierSweepGeneration == generation;
    const bool serial = SerialTierGeneration == generation;
    if (!CanStartTierCompaction(generation)
            || (!forced && !serial && (!HasFreeTierInputs(generation)
                || (generation != 255 && !sweep && DesiredMode(generation) == EDesiredMode::None)))) {
        Broker->FinishTask(taskId, EResourceStatus::Cancelled);
        state = EState::Free;
        task = {};
        if (sweep) {
            TierSweepGeneration.reset();
        }
        return;
    }
    TExtraState extra;
    if (!forced && generation < Generations.size()
            && TierForLevel(generation) == TierForLevel(generation + 1)) {
        extra.InitFromPolicy(Policy->Generations[generation]);
    }
    const ui64 id = PrepareTierCompaction(taskId, {}, generation, extra, forced, sweep);
    if (id == 0) {
        Broker->FinishTask(taskId, EResourceStatus::Cancelled);
        state = EState::Free;
        task = {};
        CheckTierGenerations();
        return;
    }
    state = EState::Compacting;
    task.CompactionId = id;
    if (forced) {
        ForcedState = EForcedState::Compacting;
    }
}

ui64 TGenCompactionStrategy::PrepareTierCompaction(TTaskId taskId, TSnapEdge edge,
        ui32 generation, TExtraState& extra, bool forced, bool sweep)
{
    Y_ENSURE(CanStartTierCompaction(generation), "Tier-managed compaction cannot start yet");
    auto params = MakeHolder<TGenCompactionParams>();
    params->Table = Table;
    params->TaskId = taskId;
    params->Edge = edge;
    params->Generation = generation;
    params->TierId = TierForLevel(generation);
    params->TieringForced = forced;
    params->Tiering = MakeHolder<TTieringCompaction>();
    params->Tiering->PartLevels = KnownParts;
    const bool cool = Backend->IsDataTieringEnabled() && Backend->TableScheme(Table)->TieringPolicy;
    // An exclusive retry must run TAC even when cooling is disabled: only a
    // successful TAC resolves the accumulated conflicts and releases the mode.
    params->Mode = sweep || cool || SerialTierGeneration == generation
        ? TCompactionParams::EMode::TierAware : TCompactionParams::EMode::Regular;
    params->KeepInCache = generation == 0
        || (generation != 255 && Policy->Generations[generation - 1].KeepInCache);
    params->PreferredLevel = generation == 0 ? 255 : generation;
    if (generation == 0) {
        for (ui32 level = 1; level <= Generations.size(); ++level) {
            if (TierForLevel(level) == 0) {
                params->PreferredLevel = level;
                break;
            }
        }
    } else if (generation != 255) {
        if (!IsLastTierGeneration(generation)) {
            params->PreferredLevel = generation + 1;
        } else if (params->TierId == 0) {
            params->PreferredLevel = 255;
        }
    }

    if (forced) {
        params->Parts = Backend->TableParts(Table);
        params->ColdParts = Backend->TableColdParts(Table);
    } else {
        if (generation > 0 && generation != 255) {
            for (const auto& part : Generations[generation - 1].Parts) {
                if (!BusyTierParts.contains(part.Label)) {
                    params->Parts.push_back(part.PartView);
                    extra.Add(part.Stats.BackingSize);
                }
            }
            for (const auto& part : ColdParts) {
                if (KnownParts.Value(part->Label, 255) == generation
                        && !BusyTierParts.contains(part->Label)) {
                    params->ColdParts.push_back(part);
                }
            }
            if (!IsLastTierGeneration(generation) && extra.ExtrasAllowed()) {
                for (const auto& part : Generations[generation].Parts) {
                    if (BusyTierParts.contains(part.Label) || !extra.ExtraAllowed(part.Stats.BackingSize)) {
                        break;
                    }
                    params->Parts.push_back(part.PartView);
                    extra.Add(part.Stats.BackingSize);
                }
            }
        }
        if (generation == 255 || (generation != 0 && params->TierId == 0
                && IsLastTierGeneration(generation))) {
            for (const auto& part : FinalParts) {
                if (!BusyTierParts.contains(part.Label)) {
                    params->Parts.push_back(part.PartView);
                }
            }
            for (const auto& part : ColdParts) {
                if (KnownParts.Value(part->Label, 255) == 255
                        && !BusyTierParts.contains(part->Label)) {
                    params->ColdParts.push_back(part);
                }
            }
        }
    }
    params->IsFinal = false;
    const ui64 id = Backend->BeginCompaction(std::move(params));
    if (id != 0) {
        auto it = TierCompactions.find(generation);
        Y_ENSURE(it != TierCompactions.end(), "Backend did not reserve compaction inputs");
        it->second.CompactionId = id;
    }
    return id;
}

void TGenCompactionStrategy::ScheduleTierSweep() {
    if (SerialTierGeneration || TierSweepGeneration
            || ForcedState != EForcedState::None
            || !Backend->IsDataTieringEnabled() || !Backend->TableScheme(Table)->TieringPolicy) {
        return;
    }
    const size_t count = Generations.size() + 1;
    for (size_t step = 0; step < count; ++step) {
        const size_t index = (NextTierSweep + step) % count;
        const ui32 level = index == Generations.size() ? 255 : index + 1;
        const bool hasParts = level == 255 ? !FinalParts.empty() || UnloadedPartsAtLevel(255) != 0
            : !Generations[index].Parts.empty() || UnloadedPartsAtLevel(level) != 0;
        const auto candidateState = level == 255 ? FinalState.State : Generations[index].State;
        if (candidateState != EState::Free || !hasParts
                || !CanStartTierCompaction(level) || !HasFreeTierInputs(level)) {
            continue;
        }
        TierSweepGeneration = level;
        NextTierSweep = (index + 1) % count;
        auto& state = level == 255 ? FinalState.State : Generations[index].State;
        auto& task = level == 255 ? FinalState.Task : Generations[index].Task;
        const auto& type = level == 255 ? Policy->InMemResourceBrokerTask
            : Policy->Generations[index].ResourceBrokerTask;
        SubmitTask(task, type, Policy->DefaultTaskPriority, level);
        state = EState::Pending;
        break;
    }
}

ui32 TGenCompactionStrategy::ChooseTierLevel(ui32 tierId, TEpoch epoch, ui32 preferredLevel) const {
    // The backend has already replaced its inventory by the time completion is
    // called. Placement must use the strategy's remaining sources instead.
    TVector<TCompactionPartInfo> remaining;
    for (ui32 level = 1; level <= Generations.size(); ++level) {
        for (const auto& part : Generations[level - 1].Parts) {
            remaining.emplace_back(part.PartView, level, TierForLevel(level));
        }
    }
    for (const auto& part : FinalParts) {
        remaining.emplace_back(part.PartView, 255, 0);
    }
    for (const auto& part : ColdParts) {
        const ui32 level = KnownParts.Value(part->Label, 255);
        Y_ENSURE(TierForLevel(level) == 0, "Unloaded compaction parts must belong to the default tier");
        remaining.emplace_back(part, level, 0);
    }
    const auto level = FindTierCompactionLevel(*Policy, remaining, tierId, epoch, preferredLevel);
    Y_ENSURE(level, "No generation can accept the compaction epoch in tier " << tierId);
    return *level;
}

ui32 TGenCompactionStrategy::UpliftTierLevel(ui32 tierId, TEpoch epoch,
        const TStats& outputStats, ui32 preferredLevel) const {
    ui32 candidate = preferredLevel;
    if (candidate == 255) {
        candidate = 0;
        for (ui32 level = 1; level <= Generations.size(); ++level) {
            if (TierForLevel(level) == tierId) {
                candidate = level;
            }
        }
    } else if (candidate > 0 && candidate <= Generations.size()) {
        --candidate;
    } else {
        return preferredLevel;
    }
    while (candidate > 0 && TierForLevel(candidate) == tierId) {
        const auto& gen = Generations[candidate - 1];
        const auto& policy = Policy->Generations[candidate - 1];
        if (outputStats.BackingSize > policy.UpliftPartSize) {
            break;
        }
        const ui64 size = gen.Stats.BackingSize + outputStats.BackingSize;
        ui64 epochs = gen.PartEpochCount;
        if (gen.Parts.empty() || gen.Parts.back().Epoch != epoch) {
            ++epochs;
        }
        if (size >= policy.ForceSizeToCompact || epochs >= policy.ForceCountToCompact
                || (size >= policy.SizeToCompact && epochs >= policy.CountToCompact)
                || ChooseTierLevel(tierId, epoch, candidate) != candidate) {
            break;
        }
        preferredLevel = candidate;
        if (!gen.Parts.empty() || UnloadedPartsAtLevel(candidate) != 0) {
            break;
        }
        --candidate;
    }
    return preferredLevel;
}

TCompactionChanges TGenCompactionStrategy::FinishTierCompaction(ui64 compactionId,
        TGenCompactionParams& params, TCompactionResult& result)
{
    Y_ENSURE(params.Context, "Tier-managed compaction has no prepared inputs");
    ReleaseTierCompaction(params.Generation, compactionId, true);
    if (TierSweepGeneration == params.Generation) {
        TierSweepGeneration.reset();
    }
    THashSet<TLogoBlobID> leaving;
    for (const auto& part : params.Context->Inputs) {
        const auto it = KnownParts.find(part.Label());
        Y_ENSURE(it != KnownParts.end() && it->second == part.Level,
            "Tier-managed compaction input placement has changed");
        leaving.insert(part.Label());
        KnownParts.erase(it);
    }
    size_t removed = 0;
    for (auto& gen : Generations) {
        for (auto it = gen.Parts.begin(); it != gen.Parts.end();) {
            if (leaving.contains(it->Label)) {
                it = gen.Parts.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
    }
    for (auto it = FinalParts.begin(); it != FinalParts.end();) {
        if (leaving.contains(it->Label)) {
            it = FinalParts.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    for (auto it = ColdParts.begin(); it != ColdParts.end();) {
        if (leaving.contains((*it)->Label)) {
            it = ColdParts.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    Y_ENSURE(removed == leaving.size(), "Tier-managed compaction inputs are missing from the strategy");
    Y_ENSURE(result.PartOutputIds.size() == result.Parts.size(), "Missing tier-managed compaction output ids");
    RebuildTierStats();
    THashMap<ui32, TStats> statsByTier;
    for (const auto& part : result.Parts) {
        const auto output = result.PartOutputIds.find(part->Label);
        Y_ENSURE(output != result.PartOutputIds.end(), "Missing compaction part output id");
        const auto tier = params.Tiering->OutputTiers.find(output->second);
        Y_ENSURE(tier != params.Tiering->OutputTiers.end(), "Missing compaction output tier");
        statsByTier[tier->second] += TStats(part);
    }
    ui32 sourcePreferred = params.PreferredLevel;
    if (const auto* sourceStats = statsByTier.FindPtr(params.TierId)) {
        sourcePreferred = UpliftTierLevel(params.TierId, result.Epoch, *sourceStats, sourcePreferred);
    }
    TCompactionChanges changes;
    for (const auto& part : result.Parts) {
        const auto output = result.PartOutputIds.find(part->Label);
        Y_ENSURE(output != result.PartOutputIds.end(), "Missing compaction part output id");
        const auto tier = params.Tiering->OutputTiers.find(output->second);
        Y_ENSURE(tier != params.Tiering->OutputTiers.end(), "Missing compaction output tier");
        Y_ENSURE(params.Context->Epochs && part->Epoch == params.Context->Epochs->Max
                && result.Epoch == part->Epoch,
            "Tier-managed compaction result has an unexpected epoch");
        const ui32 preferred = tier->second == params.TierId ? sourcePreferred : Max<ui32>();
        const ui32 level = ChooseTierLevel(tier->second, part->Epoch, preferred);
        Y_ENSURE(!KnownParts.contains(part->Label), "Duplicate resulting compaction part");
        KnownParts[part->Label] = level;
        changes.NewPartLevels.emplace(part->Label, level);
        if (changes.NewPartLevels.size() == 1) {
            changes.NewPartsLevel = level;
        }
        if (level == 255) {
            FinalParts.emplace_back(part);
        } else {
            Generations[level - 1].Parts.emplace_back(part);
        }
    }
    RebuildTierStats();

    // An independently validated switch outside another operation's epoch
    // interval may replace its remaining sources. Advance only this known
    // lineage; arbitrary external changes must still invalidate publication.
    for (auto& [generation, active] : TierCompactions) {
        Y_UNUSED(generation);
        Y_ENSURE(!params.Context->Epochs || !active.Context->Epochs
            || params.Context->Epochs->Max < active.Context->Epochs->Min
            || active.Context->Epochs->Max < params.Context->Epochs->Min,
            "Concurrent compactions have overlapping epoch intervals");
        auto& other = *active.Params->Tiering;
        auto& inventory = other.ExpectedInventory;
        size_t erased = 0;
        for (auto it = inventory.begin(); it != inventory.end();) {
            if (leaving.contains(it->Label())) {
                it = inventory.erase(it);
                ++erased;
            } else {
                ++it;
            }
        }
        Y_ENSURE(erased == leaving.size(), "Concurrent publication lineage is incomplete");
        for (const auto& label : leaving) {
            other.PartLevels.erase(label);
        }
        for (const auto& part : result.Parts) {
            const ui32 level = changes.NewPartLevels.at(part->Label);
            inventory.emplace_back(Backend->TablePart(Table, part->Label), level, TierForLevel(level));
            other.PartLevels[part->Label] = level;
        }
    }

    if (params.Generation == 0 && ForcedMemCompactionId == compactionId) {
        ForcedMemCompactionId = 0;
    }
    if (params.TieringForced) {
        CurrentForcedGenCompactionId = Max(CurrentForcedGenCompactionId,
            std::exchange(NextForcedGenCompactionId, 0));
        ForcedState = EForcedState::None;
        ForcedGeneration = 0;
        OnForcedGenCompactionDone();
    } else if (NextForcedGenCompactionId != 0 && ForcedMemCompactionId == 0) {
        CurrentForcedGenCompactionId = std::exchange(NextForcedGenCompactionId, 0);
        ForcedState = EForcedState::Pending;
        ForcedGeneration = Generations.empty() ? 255 : 1;
    } else {
        MaybeAutoStartForceCompaction();
    }
    CheckTierGenerations();
    return changes;
}

void TGenCompactionStrategy::CompactionCancelled(ui64 compactionId, const TCompactionParams& rawParams) {
    if (!rawParams.Tiering && MemCompactionId == compactionId) {
        MemCompactionId = 0;
        if (ForcedMemCompactionId == compactionId) {
            ForcedMemCompactionId = 0;
            NextForcedGenCompactionId = 0;
        }
        return;
    }
    if (!rawParams.Tiering) {
        return;
    }
    const auto& params = static_cast<const TGenCompactionParams&>(rawParams);
    const auto active = TierCompactions.find(params.Generation);
    if (active == TierCompactions.end() || active->second.CompactionId != compactionId) {
        return;
    }
    ReleaseTierCompaction(params.Generation, compactionId, false);
    if (params.Generation == 0) {
        MemCompactionId = 0;
        if (ForcedMemCompactionId == compactionId) {
            ForcedMemCompactionId = 0;
            NextForcedGenCompactionId = 0;
        }
        // The caller retains the original snapshot/forced request and decides
        // when to retry it. Other generations may continue in parallel.
        CheckTierGenerations();
        return;
    }
    auto& state = params.Generation == 255 ? FinalState.State : Generations[params.Generation - 1].State;
    auto& task = params.Generation == 255 ? FinalState.Task : Generations[params.Generation - 1].Task;
    state = EState::Free;
    task = {};
    if (params.TieringForced && ForcedState == EForcedState::Compacting) {
        ForcedState = EForcedState::Pending;
    }
    CheckTierGenerations();
}

ui64 TGenCompactionStrategy::PrepareCompaction(
        TTaskId taskId,
        TSnapEdge edge,
        ui32 generation,
        TExtraState& extra)
{
    Y_ENSURE(generation <= Generations.size() || generation == 255);
    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy PrepareCompaction for " << Backend->OwnerTabletId()
            << ": task " << taskId << ", edge " << edge.Head << "/" << edge.TxStamp << ", generation " << generation;
    }

    auto params = MakeHolder<TGenCompactionParams>();
    params->Table = Table;
    params->TaskId = taskId;
    params->Edge = edge;
    params->Generation = generation;

    if (edge.Head > TEpoch::Zero()) {
        extra.Add(Backend->TableMemSize(Table, edge.Head));
    }

    if (generation > 0 && generation != 255) {
        bool first = true;
        for (ui32 index : xrange(generation - 1, generation)) {
            auto& gen = Generations.at(index);
            size_t skip = first ? gen.TakenHeadParts : 0;
            Y_ENSURE(gen.TakenHeadParts == skip);
            Y_ENSURE(gen.CompactingTailParts == 0);
            for (auto& part : gen.Parts) {
                if (skip > 0) {
                    --skip;
                    continue;
                }
                params->Parts.emplace_back(part.PartView);
                extra.Add(part.Stats.BackingSize);
                ++gen.CompactingTailParts;
            }
            first = false;
        }
    }

    // Extend with some parts from the next generation
    if (generation < Generations.size()) {
        auto& nextGen = Generations.at(generation);
        Y_ENSURE(nextGen.TakenHeadParts == 0);
        Y_ENSURE(nextGen.TakenHeadBackingSize == 0);
        Y_ENSURE(nextGen.TakenHeadPartEpochCount == 0);
        if (extra.ExtrasAllowed() && !NeedToForceCompact(generation + 1)) {
            Y_ENSURE(nextGen.Parts.size() >= nextGen.CompactingTailParts);
            size_t available = nextGen.Parts.size() - nextGen.CompactingTailParts;
            TEpoch lastEpoch = TEpoch::Max();
            for (auto& part : nextGen.Parts) {
                Y_ENSURE(part.Epoch != TEpoch::Max(),
                    "Unexpected part with an infinite epoch found");

                if (std::exchange(lastEpoch, part.Epoch) == part.Epoch) {
                    // The last part we grabbed wasn't an epoch edge
                    Y_ENSURE(nextGen.TakenHeadPartEpochCount > 0);
                    --nextGen.TakenHeadPartEpochCount;
                }

                if (!available) {
                    break;
                }
                if (!extra.ExtraAllowed(part.Stats.BackingSize)) {
                    break;
                }

                params->Parts.emplace_back(part.PartView);
                extra.Add(part.Stats.BackingSize);
                ++nextGen.TakenHeadParts;
                nextGen.TakenHeadBackingSize += part.Stats.BackingSize;
                ++nextGen.TakenHeadPartEpochCount;
                --available;
            }
        }
    } else {
        Y_ENSURE(generation == Generations.size() || generation == 255);
        for (auto& part : FinalParts) {
            params->Parts.emplace_back(part.PartView);
            extra.Add(part.Stats.BackingSize);
        }
        for (const auto& part : ColdParts) {
            params->ColdParts.emplace_back(part);
        }
    }

    // Always keep parts generated from memtables in cache
    params->KeepInCache = generation != 255 && (generation == 0 || Policy->Generations[generation - 1].KeepInCache);

    if (Policy->KeepEraseMarkers) {
        // Keep erase markers when asked by compaction policy
        params->IsFinal = false;
    } else {
        // Don't keep erase markers if there are no parts below
        params->IsFinal = (generation == Generations.size() || generation == 255) || (FinalParts.empty() && ColdParts.empty());
    }
    if (params->IsFinal) {
        for (ui32 child : xrange(generation, ui32(Generations.size()))) {
            auto& gen = Generations[child];
            if (gen.Parts.size() > (child == generation ? gen.TakenHeadParts : 0)) {
                params->IsFinal = false;
                break;
            }
        }
    }

    ui64 compactionId = Backend->BeginCompaction(std::move(params));
    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy PrepareCompaction for " << Backend->OwnerTabletId()
            << " started compaction " << compactionId << " generation " << generation;
    }

    if (generation == Generations.size() || generation == 255) {
        Y_ENSURE(FinalCompactionId == 0, "Multiple final compactions not allowed");
        if (generation != 255) {
            FinalCompactionId = compactionId;

            // If we started final compaction, make sure we cancel any previous final parts compaction
            if (FinalState.State != EState::Free) {
                switch (FinalState.State) {
                    case EState::Free:
                        break;
                    case EState::Pending:
                    case EState::PendingBackground:
                        // The task is scheduled, make sure to cancel it
                        Y_ENSURE(FinalState.Task.TaskId != 0);
                        if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                            logl << "TGenCompactionStrategy PrepareCompaction for " << Backend->OwnerTabletId()
                                << " cancel pending compaction " << FinalState.Task.CompactionId;
                        }
                        Broker->CancelTask(FinalState.Task.TaskId);
                        break;
                    case EState::Compacting:
                        // Compaction is running, cancel it
                        Y_ENSURE(FinalState.Task.CompactionId != 0);
                        if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                            logl << "TGenCompactionStrategy PrepareCompaction for " << Backend->OwnerTabletId()
                                << " cancel running compaction " << FinalState.Task.CompactionId;
                        }
                        Backend->CancelCompaction(FinalState.Task.CompactionId);
                        break;
                }
                FinalState.State = EState::Free;
                FinalState.Task.TaskId = 0;
                FinalState.Task.CompactionId = 0;
            }
        }
        FinalCompactionLevel = generation;
        FinalCompactionTaken = FinalParts.size() + ColdParts.size();
    }

    return compactionId;
}

void TGenCompactionStrategy::UpdateStats() {
    Stats.Reset();
    StatsPerTablet.clear();
    for (const auto& gen : Generations) {
        Stats += gen.Stats;
        for (const auto& kv : gen.StatsPerTablet) {
            StatsPerTablet[kv.first] += kv.second;
        }
    }
    for (const auto& part : FinalParts) {
        Stats += part.Stats;
        StatsPerTablet[part.Label.TabletID()] += part.Stats;
    }

    // This rebuild is pretty expensive, however UpdateStats is only called
    // when we start or compact something, so right now it's not a very big
    // concern.
    // TODO: make it possible to incrementally update this aggregate
    {
        NPage::TGarbageStatsAggBuilder builder;
        auto process = [&](TPartInfo& part) {
            if (part.PartView->GarbageStats) {
                builder.Add(part.PartView->GarbageStats);
            }
        };
        for (auto& gen : Generations) {
            for (auto& part : gen.Parts) {
                process(part);
            }
        }
        for (auto& part : FinalParts) {
            process(part);
        }
        GarbageStatsAgg = builder.Build();
    }
}

void TGenCompactionStrategy::UpdateOverload() {
    OverloadFactor = 0.0;
    for (const auto& gen : Generations) {
        OverloadFactor = Max(OverloadFactor, gen.OverloadFactor);
    }
}

ui32 TGenCompactionStrategy::DroppedBytesPercent() const {
    if (const auto& ranges = Backend->TableRemovedRowVersions(Table)) {
        auto it = ranges.begin();
        if (it->Lower.IsMin()) {
            ui64 bytes = GarbageStatsAgg.GetGarbageBytes(it->Upper);
            if (bytes > 0 && Stats.BackingSize > 0) {
                return bytes * 100 / Stats.BackingSize;
            }
        }
    }

    return 0;
}

bool TGenCompactionStrategy::NeedToStartForceCompaction() const {
    if (Stats.DroppedRowsPercent() >= Policy->DroppedRowsPercentToCompact && !Policy->KeepEraseMarkers) {
        // Table has too many dropped rows, compact everything
        return true;
    }

    if (DroppedBytesPercent() >= Policy->DroppedRowsPercentToCompact && !Policy->KeepEraseMarkers) {
        // Table has too much garbage, compact everything
        return true;
    }

    return false;
}

bool TGenCompactionStrategy::MaybeAutoStartForceCompaction() {
    // Check if maybe we need to start a forced compaction
    // WARNING: we only do this check when we have at least one sst that
    // belongs to our own tablet, i.e. not borrowed. This is so we don't
    // compact borrowed data mid-merge, which might cause epochs to become
    // out of sync across generations.
    if (Generations &&
        ForcedState == EForcedState::None &&
        (CompactBorrowedGarbageAllowed || StatsPerTablet.contains(Backend->OwnerTabletId())))
    {
        bool startForcedCompaction = NeedToStartForceCompaction();
        if (startForcedCompaction) {
            ForcedState = EForcedState::Pending;
            ForcedGeneration = 1;
            return true;
        }
    }

    return false;
}

void TGenCompactionStrategy::AllowBorrowedGarbageCompaction() {
    if (!CompactBorrowedGarbageAllowed) {
        CompactBorrowedGarbageAllowed = true;
        ReflectRemovedRowVersions();
    }
}

}
}
}
