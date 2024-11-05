#include "flat_comp_gen.h"

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/cast.h>

namespace NKikimr {
namespace NTable {
namespace NCompGen {

namespace {

static constexpr ui32 BAD_PRIORITY = Max<ui32>();
static constexpr ui32 PRIORITY_UPDATE_FACTOR = 20;

}

void TGenCompactionParams::Describe(IOutputStream& out) const noexcept {
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

    TPartAggregator& Add(const TPartInfo& part) noexcept {
        Y_ABORT_UNLESS(part.Epoch != TEpoch::Max(),
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
    TPartAggregator& Add(const Container& container) noexcept {
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

TGenCompactionStrategy::TPartInfo& TGenCompactionStrategy::TGeneration::PushFront(TPartView partView) noexcept {
    Y_ABORT_UNLESS(TakenHeadParts == 0,
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

TGenCompactionStrategy::TPartInfo& TGenCompactionStrategy::TGeneration::PushBack(TPartView partView) noexcept {
    Y_ABORT_UNLESS(CompactingTailParts == 0,
        "Attempting to append part to generation that has compacting tail parts");

    if (Parts.empty() || Parts.back().Epoch != partView->Epoch) {
        // This part will become a new epoch edge
        ++PartEpochCount;
    } else if (Parts.size() == TakenHeadParts) {
        // The end of taken head is no longer an epoch edge
        Y_ABORT_UNLESS(TakenHeadPartEpochCount > 0);
        --TakenHeadPartEpochCount;
    }

    auto& back = Parts.emplace_back(std::move(partView));
    Stats += back.Stats;
    StatsPerTablet[back.Label.TabletID()] += back.Stats;
    return back;
}

void TGenCompactionStrategy::TGeneration::PopFront() noexcept {
    Y_ABORT_UNLESS(Parts.size() > CompactingTailParts,
        "Attempting to remove part crossing compacting tail parts");

    bool wasTaken = TakenHeadParts > 0;

    const auto& front = Parts.front();
    TEpoch epoch = front.Epoch;
    Stats -= front.Stats;
    StatsPerTablet[front.Label.TabletID()] -= front.Stats;
    if (wasTaken) {
        Y_ABORT_UNLESS(TakenHeadBackingSize >= front.Stats.BackingSize);
        TakenHeadBackingSize -= front.Stats.BackingSize;
        --TakenHeadParts;
    }
    Parts.pop_front();

    if (Parts.empty() || Parts.front().Epoch != epoch) {
        // We just removed an epoch edge
        if (wasTaken) {
            Y_ABORT_UNLESS(TakenHeadPartEpochCount > 0);
            --TakenHeadPartEpochCount;
        }
        Y_ABORT_UNLESS(PartEpochCount > 0);
        --PartEpochCount;
    }
}

void TGenCompactionStrategy::TGeneration::PopBack() noexcept {
    Y_ABORT_UNLESS(Parts.size() > TakenHeadParts,
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
        Y_ABORT_UNLESS(PartEpochCount > 0);
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
    Y_ABORT_UNLESS(!Policy, "Strategy has already been started");

    const auto* scheme = Backend->TableScheme(Table);

    Policy = scheme->CompactionPolicy;
    Generations.resize(Policy->Generations.size());

    for (auto& partView : Backend->TableParts(Table)) {
        auto label = partView->Label;
        ui32 level = state.PartLevels.Value(partView->Label, 255);
        Y_ABORT_UNLESS(level > 0 && level <= Generations.size() || level == 255);
        auto& parts = level == 255 ? FinalParts : Generations.at(level - 1).Parts;
        parts.emplace_back(std::move(partView));
        KnownParts[label] = level;
    }

    for (auto& part : Backend->TableColdParts(Table)) {
        auto label = part->Label;
        ui32 level = state.PartLevels.Value(part->Label, 255);
        Y_ABORT_UNLESS(level > 0 && level <= Generations.size() || level == 255);
        ColdParts.emplace_back(std::move(part));
        KnownParts[label] = level;
    }

    TEpoch lastEpoch = TEpoch::Max();
    for (auto& gen : Generations) {
        TPartAggregator agg;
        if (gen.Parts) {
            gen.Parts.sort();

            // Verify epoch is decreasing from level to level
            Y_ABORT_UNLESS(gen.Parts.front().Epoch <= lastEpoch);
            lastEpoch = gen.Parts.back().Epoch;

            agg.Add(gen.Parts);
        }
        gen.Stats = agg.Stats;
        gen.StatsPerTablet = agg.StatsPerTablet;
        gen.PartEpochCount = agg.PartEpochCount;
    }

    // Sorting FinalParts is not required, but let's be nice
    FinalParts.sort();

    UpdateStats();

    MaybeAutoStartForceCompaction();

    for (ui32 index : xrange(Generations.size())) {
        CheckGeneration(index + 1);
    }

    UpdateOverload();
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
                Y_ABORT_UNLESS(gen.Task.TaskId != 0);
                Broker->CancelTask(gen.Task.TaskId);
                break;
            }
            case EState::Compacting: {
                // Compaction is running, cancel it
                Y_ABORT_UNLESS(gen.Task.CompactionId != 0);
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
            Y_ABORT_UNLESS(FinalState.Task.TaskId != 0);
            Broker->CancelTask(FinalState.Task.TaskId);
            break;
        case EState::Compacting:
            // Compaction is running, cancel it
            Y_ABORT_UNLESS(FinalState.Task.CompactionId != 0);
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
    FinishedForcedGenCompactionId = 0;
    FinishedForcedGenCompactionTs = {};

    // Make it possible to Start again
    Generations.clear();
    FinalState = { };
    FinalParts.clear();
    ColdParts.clear();
    KnownParts.clear();
    Policy = nullptr;
}

void TGenCompactionStrategy::ReflectSchema() {
    const auto* scheme = Backend->TableScheme(Table);

    TString err;
    bool ok = NLocalDb::ValidateCompactionPolicyChange(*Policy, *scheme->CompactionPolicy, err);
    Y_ABORT_UNLESS(ok, "table %s id %u: %s", scheme->Name.data(), scheme->Id, err.data());

    Policy = scheme->CompactionPolicy;
    Y_ABORT_UNLESS(Generations.size() <= Policy->Generations.size(), "Cannot currently shrink the number of generations");
    Generations.resize(Policy->Generations.size());

    // Recheck all generations
    for (ui32 index : xrange(Generations.size())) {
        CheckGeneration(index + 1);
    }

    UpdateOverload();
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
}

ui64 TGenCompactionStrategy::BeginMemCompaction(TTaskId taskId, TSnapEdge edge, ui64 forcedCompactionId) {
    Y_ABORT_UNLESS(MemCompactionId == 0, "Unexpected concurrent mem compaction requests");

    TExtraState extra;
    if (forcedCompactionId == 0 && !Policy->Generations.empty() && ForcedState == EForcedState::None && edge.Head == TEpoch::Max()) {
        extra.InitFromPolicy(Policy->Generations[0]);
    }

    MemCompactionId = PrepareCompaction(
        taskId,
        edge,
        /* generation */ 0,
        extra);
    Y_ABORT_UNLESS(MemCompactionId != 0);

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
    
    // Find if we actually have borrowed parts
    bool hasBorrowed = false;
    for (const auto& pr : KnownParts) {
        if (pr.first.TabletID() != ownerTabletId) {
            hasBorrowed = true;
            Y_ABORT_UNLESS(pr.second == 255,
                "Borrowed part %s not in final parts", pr.first.ToString().c_str());
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
    Y_ABORT_UNLESS(generation <= Generations.size() || generation == 255, "Unexpected CompactionFinished generation");
    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy CompactionFinished for " << Backend->OwnerTabletId()
            << ": compaction " << compactionId << ", generation " << generation;
    }

    if (generation == 0) {
        Y_ABORT_UNLESS(compactionId == MemCompactionId,
            "Unexpected CompactionFinished for gen 0");
        MemCompactionId = 0;
    } else if (generation == 255) {
        Y_ABORT_UNLESS(FinalState.State == EState::Compacting);
        Y_ABORT_UNLESS(compactionId == FinalState.Task.CompactionId,
            "Unexpected CompactionFinished for gen %" PRIu32, generation);
        FinalState.State = EState::Free;
        FinalState.Task.TaskId = 0;
        FinalState.Task.CompactionId = 0;
    } else {
        auto& gen = Generations[generation - 1];
        Y_ABORT_UNLESS(gen.State == EState::Compacting);
        Y_ABORT_UNLESS(compactionId == gen.Task.CompactionId,
            "Unexpected CompactionFinished for gen %" PRIu32, generation);
        gen.State = EState::Free;
        gen.Task.TaskId = 0;
        gen.Task.CompactionId = 0;
    }

    TCompactionChanges changes;

    TVector<bool> checkNeeded(Generations.size());

    auto& sourceParts = params->Parts;

    if (compactionId == FinalCompactionId || generation == 255) {
        // All taken cold parts must match our list of cold parts
        Y_ABORT_UNLESS(params->ColdParts.size() <= FinalCompactionTaken);
        for (auto& part : params->ColdParts) {
            Y_ABORT_UNLESS(!ColdParts.empty());
            auto& front = ColdParts.front();
            Y_ABORT_UNLESS(front->Label == part->Label);
            KnownParts.erase(front->Label);
            ColdParts.pop_front();
            --FinalCompactionTaken;
        }
        params->ColdParts.clear();
        // All taken final parts must be at the tail of our source parts
        Y_ABORT_UNLESS(sourceParts.size() >= FinalCompactionTaken);
        auto partStart = sourceParts.end() - FinalCompactionTaken;
        auto partEnd = sourceParts.end();
        for (auto partIt = partStart; partIt != partEnd; ++partIt) {
            Y_ABORT_UNLESS(!FinalParts.empty());
            auto& front = FinalParts.front();
            Y_ABORT_UNLESS(front.Label == (*partIt)->Label);
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
            Y_ABORT_UNLESS(sourceParts.size() >= nextGen.TakenHeadParts);
            auto partStart = sourceParts.end() - nextGen.TakenHeadParts;
            auto partEnd = sourceParts.end();
            for (auto partIt = partStart; partIt != partEnd; ++partIt) {
                Y_ABORT_UNLESS(!nextGen.Parts.empty());
                auto& front = nextGen.Parts.front();
                Y_ABORT_UNLESS(front.Label == (*partIt)->Label);
                KnownParts.erase(front.Label);
                nextGen.PopFront();
            }
            Y_ABORT_UNLESS(nextGen.TakenHeadParts == 0);
            Y_ABORT_UNLESS(nextGen.TakenHeadBackingSize == 0);
            Y_ABORT_UNLESS(nextGen.TakenHeadPartEpochCount == 0);
            sourceParts.erase(partStart, partEnd);
            checkNeeded[generation] = true;
        }
    }

    bool forcedCompactionContinue = false;

    if (generation == 0) {
        // This was a memtable compaction, we don't expect anything else
        Y_ABORT_UNLESS(sourceParts.empty());

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
        Y_ABORT_UNLESS(sourceParts.empty());
    } else {
        ui32 sourceIndex = generation - 1;
        Y_ABORT_UNLESS(sourceIndex < Generations.size());

        if (ForcedState == EForcedState::Compacting && ForcedGeneration == generation) {
            ForcedState = EForcedState::None;
            forcedCompactionContinue = true;
        }

        while (sourceParts) {
            auto& sourcePart = sourceParts.back();
            auto& sourceGen = Generations[sourceIndex];
            Y_ABORT_UNLESS(sourceGen.Parts);
            auto& part = sourceGen.Parts.back();
            Y_ABORT_UNLESS(part.Label == sourcePart->Label,
                "Failed at gen=%u, sourceIndex=%u, headTaken=%lu",
                generation, sourceIndex, sourceGen.TakenHeadParts);
            Y_ABORT_UNLESS(sourceGen.CompactingTailParts > 0);
            KnownParts.erase(part.Label);
            sourceGen.PopBack();
            sourceParts.pop_back();
            checkNeeded[sourceIndex] = true;
        }

        Y_ABORT_UNLESS(sourceParts.empty());

        // Recheck compacted generation, it's free and may need a new compaction
        checkNeeded[generation - 1] = true;
    }

    auto& newParts = result->Parts;

    TStats newStats;
    for (const auto& partView : newParts) {
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
                Y_ABORT_UNLESS(!newGen.Parts || result->Epoch <= newGen.Parts.back().Epoch);
                for (auto it = newParts.begin(); it != newParts.end(); ++it) {
                    auto& partView = *it;
                    newGen.PushBack(std::move(partView));
                }
            } else {
                Y_ABORT_UNLESS(!newGen.Parts || result->Epoch >= newGen.Parts.front().Epoch);
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
        Y_ABORT_UNLESS(target < Generations.size());
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
    Y_ABORT_UNLESS(level == 255, "Unexpected level of the merged part");

    const auto label = partView->Label;

    // Remove the old part data from our model (since it may have been changed)
    if (KnownParts.contains(label)) {
        Y_ABORT_UNLESS(KnownParts[label] == level, "Borrowed part cannot be moved between levels");
        Y_ABORT_UNLESS(FinalCompactionId == 0 || FinalCompactionTaken == 0,
            "Borrowed part attaching while final compaction is in progress");

        // Inefficient, but this case shouldn't happen in practice, since we
        // don't allow a diamond shaped split/merge sequence in datashards.
        for (auto it = ColdParts.begin(); it != ColdParts.end(); ++it) {
            Y_ABORT_UNLESS((*it)->Label != label,
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

    // WARNING: we don't call UpdateStats here, so GarbageStatsAgg is not
    // recalculated properly. This is not a problem, because this method is
    // usually called when parts are borrowed, and we are not supposed to
    // have any of our own data at that stage. We would wait for the first
    // compaction to properly recalculate garbage stats histogram.
}

void TGenCompactionStrategy::PartMerged(TIntrusiveConstPtr<TColdPart> part, ui32 level) {
    Y_ABORT_UNLESS(level == 255, "Unexpected level of the merged part");

    const auto label = part->Label;
    Y_ABORT_UNLESS(!KnownParts.contains(label),
        "Borrowed part attaching when another part with the same label exists");

    ColdParts.emplace_back(std::move(part));
    KnownParts[label] = level;
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
        ui32 nextGenerationId = 1;
        for (const auto& gen : Generations) {
            ui32 generation = nextGenerationId++;
            DIV_CLASS("row") {out << "Comp. generation: " << generation
                                << ", Parts count: " << gen.Parts.size()
                                << ", Backing size: " << gen.Stats.BackingSize
                                << ", Epochs count: " << gen.PartEpochCount
                                << ", Overload factor: " << gen.OverloadFactor
                                << ", Compaction state: " << gen.State; }
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
    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TGenCompactionStrategy BeginGenCompaction for " << Backend->OwnerTabletId()
            << ": task " << taskId << ", generation " << generation;
    }

    // Special mode for final parts compaction
    if (generation == 255) {
        Y_ABORT_UNLESS(FinalState.State == EState::Pending);
        Y_ABORT_UNLESS(FinalState.Task.TaskId == taskId);

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

    Y_ABORT_UNLESS(generation > 0);
    Y_ABORT_UNLESS(generation <= Generations.size());

    auto& gen = Generations[generation - 1];
    Y_ABORT_UNLESS(gen.State == EState::Pending || gen.State == EState::PendingBackground);
    Y_ABORT_UNLESS(gen.Task.TaskId == taskId);

    const bool forced = NeedToForceCompact(generation);

    auto cancelCompaction = [&]() {
        Broker->FinishTask(taskId, EResourceStatus::Cancelled);
        gen.Task.TaskId = 0;

        gen.State = EState::Free;

        Y_ABORT_UNLESS(!forced, "Unexpected cancellation of a forced compaction");
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
        Y_ABORT_UNLESS(ForcedGeneration == generation);
    }
}

void TGenCompactionStrategy::SubmitTask(
        TGenCompactionStrategy::TCompactionTask& task, TString type, ui32 priority, ui32 generation)
{
    Y_ABORT_UNLESS(task.TaskId == 0, "Task is already submitted");
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
    Y_ABORT_UNLESS(task.TaskId != 0, "Task was not submitted");
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
    Y_ABORT_UNLESS(generation > 0);
    Y_ABORT_UNLESS(generation <= Generations.size());

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
    Y_ABORT_UNLESS(generation > 0);

    CheckOverload(generation);

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
    Y_ABORT_UNLESS(generation > 0);
    Y_ABORT_UNLESS(generation <= Generations.size());

    auto& gen = Generations[generation - 1];
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
    Y_ABORT_UNLESS(generation > 0);
    Y_ABORT_UNLESS(generation <= Generations.size());

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

ui64 TGenCompactionStrategy::PrepareCompaction(
        TTaskId taskId,
        TSnapEdge edge,
        ui32 generation,
        TExtraState& extra)
{
    Y_ABORT_UNLESS(generation <= Generations.size() || generation == 255);
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
            Y_ABORT_UNLESS(gen.TakenHeadParts == skip);
            Y_ABORT_UNLESS(gen.CompactingTailParts == 0);
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
        Y_ABORT_UNLESS(nextGen.TakenHeadParts == 0);
        Y_ABORT_UNLESS(nextGen.TakenHeadBackingSize == 0);
        Y_ABORT_UNLESS(nextGen.TakenHeadPartEpochCount == 0);
        if (extra.ExtrasAllowed() && !NeedToForceCompact(generation + 1)) {
            Y_ABORT_UNLESS(nextGen.Parts.size() >= nextGen.CompactingTailParts);
            size_t available = nextGen.Parts.size() - nextGen.CompactingTailParts;
            TEpoch lastEpoch = TEpoch::Max();
            for (auto& part : nextGen.Parts) {
                Y_ABORT_UNLESS(part.Epoch != TEpoch::Max(),
                    "Unexpected part with an infinite epoch found");

                if (std::exchange(lastEpoch, part.Epoch) == part.Epoch) {
                    // The last part we grabbed wasn't an epoch edge
                    Y_ABORT_UNLESS(nextGen.TakenHeadPartEpochCount > 0);
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
        Y_ABORT_UNLESS(generation == Generations.size() || generation == 255);
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
        Y_ABORT_UNLESS(FinalCompactionId == 0, "Multiple final compactions not allowed");
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
                        Y_ABORT_UNLESS(FinalState.Task.TaskId != 0);
                        if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                            logl << "TGenCompactionStrategy PrepareCompaction for " << Backend->OwnerTabletId()
                                << " cancel pending compaction " << FinalState.Task.CompactionId;
                        }
                        Broker->CancelTask(FinalState.Task.TaskId);
                        break;
                    case EState::Compacting:
                        // Compaction is running, cancel it
                        Y_ABORT_UNLESS(FinalState.Task.CompactionId != 0);
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
