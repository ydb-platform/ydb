#include "blob_manager.h"
#include "history_cutter.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS_BS

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

namespace {

class TCutHistoryBarrierActor: public TActorBootstrapped<TCutHistoryBarrierActor> {
public:
    TCutHistoryBarrierActor(const TActorId& tabletActorId, const TActorId& launcherActorId, ui64 tabletId, ui32 currentGen, ui32 channel,
        ui32 group, ui32 fromGen, ui32 nextFromGen)
        : TabletActorId(tabletActorId)
        , LauncherActorId(launcherActorId)
        , TabletId(tabletId)
        , CurrentGen(currentGen)
        , Channel(channel)
        , Group(group)
        , FromGen(fromGen)
        , NextFromGen(nextFromGen)
    {
        AFL_VERIFY(NextFromGen > 0);   // NextFromGen - 1 below would underflow to collect-everything
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWait);
        SendBarrier(ctx);
    }

    void HandleWakeup(const TActorContext& ctx) {
        SendBarrier(ctx);
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev, const TActorContext& ctx) {
        const auto status = ev->Get()->Status;
        if (status == NKikimrProto::OK || status == NKikimrProto::ALREADY) {
            // ALREADY means the barrier is already at or beyond the requested level — safe to cut.
            auto req = MakeHolder<TEvTablet::TEvCutTabletHistory>();
            req->Record.SetTabletID(TabletId);
            req->Record.SetChannel(Channel);
            req->Record.SetFromGeneration(FromGen);
            req->Record.SetGroupID(Group);
            ctx.Send(LauncherActorId, req.Release());
            ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvCutHistoryBarrierDone(Channel, FromGen, true));
            Die(ctx);
            return;
        }
        if (status == NKikimrProto::BLOCKED || ++Retries >= MaxRetries) {
            ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvCutHistoryBarrierDone(Channel, FromGen, false));
            Die(ctx);
            return;
        }
        // Linear backoff: an immediate retry against an overloaded group would only add load.
        ctx.Schedule(TDuration::Seconds(1) * Retries, new NActors::TEvents::TEvWakeup());
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            CFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeup);
        }
    }

private:
    static constexpr int MaxRetries = 3;

    void SendBarrier(const TActorContext& ctx) {
        const ui32 perGenerationCounter =
            TBlobManager::AllocateGCPerGenerationCounter(TEvBlobStorage::TEvCollectGarbage::PerGenerationCounterStepSize(nullptr, nullptr));
        auto ev = MakeHolder<TEvBlobStorage::TEvCollectGarbage>(TabletId, CurrentGen, perGenerationCounter, Channel, /*collect=*/true,
            /*collectGeneration=*/NextFromGen - 1, /*collectStep=*/Max<ui32>(), /*keep=*/nullptr, /*doNotKeep=*/nullptr, TInstant::Max(),
            /*issueKeepFlag=*/false, TWriteSource::ColumnShardGC, /*hard=*/true);
        SendToBSProxy(ctx, Group, ev.Release());
    }

    TActorId TabletActorId;
    TActorId LauncherActorId;
    ui64 TabletId = 0;
    ui32 CurrentGen = 0;
    ui32 Channel = 0;
    ui32 Group = 0;
    ui32 FromGen = 0;
    ui32 NextFromGen = 0;
    int Retries = 0;
};

// Asks BlobStorage whether each candidate range still holds a blob of ours, instead of scanning the portion index.
class TCutHistoryRangeProbeActor: public TActorBootstrapped<TCutHistoryRangeProbeActor> {
public:
    TCutHistoryRangeProbeActor(const TActorId& tabletActorId, ui64 tabletId, TVector<TRangeProbe>&& probes, ui64 round)
        : TabletActorId(tabletActorId)
        , TabletId(tabletId)
        , Probes(std::move(probes))
        , Round(round)
        , Answered(Probes.size(), false)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWait);
        Deadline = ctx.Now() + ProbeTimeout;
        ctx.Schedule(ProbeTimeout, new NActors::TEvents::TEvWakeup());
        SendMore(ctx);
        CheckFinish(ctx);
    }

    void Handle(TEvBlobStorage::TEvRangeResult::TPtr& ev, const TActorContext& ctx) {
        const ui64 index = ev->Cookie;
        if (index >= Probes.size() || Answered[index]) {
            return;
        }
        Answered[index] = true;
        --InFlight;
        const auto* msg = ev->Get();
        const auto& probe = Probes[index];
        if (msg->Status != NKikimrProto::OK) {
            // Every non-OK status is ambiguous, and an ambiguous range must never authorise a hard barrier.
            Disprove(probe, /*failure=*/true);
        } else if (AnyOf(msg->Responses, [&](const TEvBlobStorage::TEvRangeResult::TResponse& resp) {
                       return IsLiveBlobOfProbe(resp, probe);
                   })) {
            Disprove(probe, /*failure=*/false);
        }
        SendMore(ctx);
        CheckFinish(ctx);
    }

    void HandleTimeout(const TActorContext& ctx) {
        for (size_t i = 0; i < Probes.size(); ++i) {
            if (!Answered[i]) {
                Answered[i] = true;
                Disprove(Probes[i], /*failure=*/true);
            }
        }
        Finish(ctx);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvRangeResult, Handle);
            CFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
        }
    }

private:
    static constexpr TDuration ProbeTimeout = TDuration::Minutes(1);

    // A blob id sorts channel before generation, so the request already isolates the channel; this re-checks it anyway.
    bool IsLiveBlobOfProbe(const TEvBlobStorage::TEvRangeResult::TResponse& resp, const TRangeProbe& probe) const {
        // DoNotKeep is this tablet's own delete declaration coming back, so it cannot pin the window.
        if (resp.DoNotKeep && !resp.Keep) {
            return false;
        }
        if (resp.Id.TabletID() != TabletId || resp.Id.Channel() != probe.Channel) {
            return false;
        }
        const ui32 gen = resp.Id.Generation();
        return gen >= probe.FromGeneration && gen < probe.NextFromGeneration;
    }

    void Disprove(const TRangeProbe& probe, const bool failure) {
        Disproved.emplace_back(probe.Channel, probe.FromGeneration);
        Failures += failure;
    }

    void SendMore(const TActorContext& ctx) {
        while (InFlight < THistoryCutterWrapper::MaxRangeProbesInFlight && NextProbe < Probes.size()) {
            SendProbe(NextProbe++, ctx);
        }
    }

    void SendProbe(const size_t index, const TActorContext& ctx) {
        const auto& probe = Probes[index];
        // No successor generation means no upper bound to probe, and NextFromGeneration - 1 would underflow.
        if (!probe.NextFromGeneration) {
            Answered[index] = true;
            Disprove(probe, /*failure=*/true);
            return;
        }
        const TLogoBlobID from(TabletId, probe.FromGeneration, 0, probe.Channel, 0, 0);
        const TLogoBlobID to(
            TabletId, probe.NextFromGeneration - 1, Max<ui32>(), probe.Channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
        auto request = MakeHolder<TEvBlobStorage::TEvRange>(TabletId, from, to, /*mustRestoreFirst=*/false, Deadline, /*isIndexOnly=*/true);
        SendToBSProxy(ctx, probe.Group, request.Release(), index);
        ++InFlight;
    }

    void CheckFinish(const TActorContext& ctx) {
        if (AllOf(Answered, [](const bool answered) {
                return answered;
            })) {
            Finish(ctx);
        }
    }

    void Finish(const TActorContext& ctx) {
        if (std::exchange(Finished, true)) {
            return;
        }
        ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvCutHistoryRangeProbeDone(Round, std::move(Disproved), Failures));
        Die(ctx);
    }

    TActorId TabletActorId;
    ui64 TabletId = 0;
    TVector<TRangeProbe> Probes;
    ui64 Round = 0;
    TVector<bool> Answered;
    TVector<std::pair<ui32, ui32>> Disproved;
    ui64 Failures = 0;
    size_t NextProbe = 0;
    ui32 InFlight = 0;
    TInstant Deadline;
    bool Finished = false;
};

}   // anonymous namespace

NActors::IActor* CreateCutHistoryRangeProbeActor(
    const TActorId& tabletActorId, const ui64 tabletId, TVector<TRangeProbe>&& probes, const ui64 round) {
    return new TCutHistoryRangeProbeActor(tabletActorId, tabletId, std::move(probes), round);
}

THistoryCutterWrapper::THistoryCutterWrapper(const TIntrusivePtr<TTabletStorageInfo>& tabletInfo, const ui32 currentGen,
    const std::weak_ptr<NOlap::TBlobManager>& manager, const std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>& sharedBlobs,
    const TActorId& tabletActorId, const NColumnShard::THistoryCutterCounters& signals)
    : Signals(signals)
    , TabletInfo(tabletInfo)
    , CurrentGen(currentGen)
    , Manager(manager)
    , SharedBlobs(sharedBlobs)
    , TabletActorId(tabletActorId)
{
}

TDuration THistoryCutterWrapper::GetNominateCadence() {
    if (!HasAppData()) {
        return DefaultNominateCadence;
    }
    const ui32 seconds = AppDataVerified().ColumnShardConfig.GetCutHistoryNominateCadenceSeconds();
    return seconds ? TDuration::Seconds(seconds) : DefaultNominateCadence;
}

ui32 THistoryCutterWrapper::GetMaxDrainChecksPerNomination() {
    if (!HasAppData()) {
        return DefaultMaxDrainChecksPerNomination;
    }
    const ui32 checks = AppDataVerified().ColumnShardConfig.GetCutHistoryMaxDrainChecksPerNomination();
    return checks ? checks : DefaultMaxDrainChecksPerNomination;
}

EProofSource THistoryCutterWrapper::GetProofSource() {
    if (!HasAppData()) {
        return EProofSource::Portions;
    }
    switch (AppDataVerified().ColumnShardConfig.GetCutHistoryProofSource()) {
        case NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE:
            return EProofSource::BsRange;
        case NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_COMPARE:
            return EProofSource::Compare;
        default:
            return EProofSource::Portions;
    }
}

TVector<TRangeProbe> THistoryCutterWrapper::BuildRangeProbes() const {
    TVector<TRangeProbe> probes;
    probes.reserve(SweepSurvivors.size());
    for (const auto& key : SweepSurvivors) {
        const ui32 nextFromGen = GetNextFromGeneration(key);
        if (!nextFromGen || key.Channel >= static_cast<ui32>(TabletInfo->Channels.size())) {
            continue;
        }
        // Exact match, not GroupForGeneration: the barrier must go to this entry's group, not a live successor.
        const auto* entry = FindIfPtr(TabletInfo->Channels[key.Channel].History, [&key](const TTabletChannelInfo::THistoryEntry& historyEntry) {
            return historyEntry.FromGeneration == key.FromGeneration;
        });
        if (!entry) {
            continue;
        }
        probes.push_back(TRangeProbe{ key.Channel, key.FromGeneration, nextFromGen, entry->GroupID });
    }
    return probes;
}

void THistoryCutterWrapper::OnRangeProbeComplete(
    const ui64 round, THashSet<TEntryKey>&& disproved, const ui64 failures, const TActorContext& ctx) {
    Signals.OnRangeProbeCompleted(failures);
    // A verdict from an abandoned round says nothing about this one; TryNominate cannot start a round while one runs.
    if (round != SweepRound) {
        return;
    }
    if (RoundProofSource == EProofSource::BsRange) {
        OnBatchComplete(disproved, /*exhausted=*/true, ctx);
        return;
    }
    RangeVerdict = std::move(disproved);
    CompareVerdicts();
}

void THistoryCutterWrapper::CompareVerdicts() {
    if (!PortionVerdict || !RangeVerdict) {
        return;
    }
    ui64 rangeOnly = 0;
    ui64 portionsOnly = 0;
    for (const auto& key : *RangeVerdict) {
        rangeOnly += !PortionVerdict->contains(key);
    }
    for (const auto& key : *PortionVerdict) {
        portionsOnly += !RangeVerdict->contains(key);
    }
    if (rangeOnly || portionsOnly) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "cut_history_proof_disagreement")("range_only", rangeOnly)(
            "portions_only", portionsOnly)("round", SweepRound);
    }
    Signals.OnRangeProbeDisagreement(rangeOnly, portionsOnly);
    PortionVerdict.reset();
    RangeVerdict.reset();
}

bool THistoryCutterWrapper::IsEnabled() const {
    if (NYDBTest::TControllers::GetColumnShardController()->IsCSCutHistoryEnabled()) {
        return true;
    }
    return HasAppData() && AppData()->FeatureFlags.GetEnableColumnshardGroupDecommission();
}

bool THistoryCutterWrapper::SeenGroupsCheckPasses(
    const std::vector<TTabletChannelInfo::THistoryEntry>& hist, const ui32 fromGeneration, const THashSet<ui32>& cutFromGenerations) {
    const auto target = FindIf(hist, [fromGeneration](const TTabletChannelInfo::THistoryEntry& entry) {
        return entry.FromGeneration == fromGeneration;
    });
    if (target == hist.end()) {
        return false;
    }
    return !AnyOf(hist.begin(), target, [&](const TTabletChannelInfo::THistoryEntry& entry) {
        return entry.GroupID == target->GroupID && !cutFromGenerations.contains(entry.FromGeneration);
    });
}

bool THistoryCutterWrapper::SeenGroupsCheckPasses(const TEntryKey& key) const {
    if (key.Channel >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return false;
    }
    THashSet<ui32> cutFromGenerations;
    for (const auto& [stateKey, state] : CutState) {
        if (stateKey.Channel == key.Channel && state == ECutState::Cut) {
            cutFromGenerations.insert(stateKey.FromGeneration);
        }
    }
    return SeenGroupsCheckPasses(TabletInfo->Channels[key.Channel].History, key.FromGeneration, cutFromGenerations);
}

ui32 THistoryCutterWrapper::GetNextFromGeneration(const TEntryKey& key) const {
    if (key.Channel >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return 0;
    }
    const auto& hist = TabletInfo->Channels[key.Channel].History;
    // History ascends by FromGeneration; end() means the active entry, never a cut candidate.
    const auto next = UpperBound(hist.begin(), hist.end(), key.FromGeneration, TTabletChannelInfo::THistoryEntry::TCmp());
    if (next == hist.begin() || next == hist.end() || (next - 1)->FromGeneration != key.FromGeneration) {
        return 0;
    }
    return next->FromGeneration;
}

bool THistoryCutterWrapper::IsDrained(const TEntryKey& key) const {
    const ui32 nextGen = GetNextFromGeneration(key);
    if (!nextGen) {
        return false;
    }
    const auto manager = Manager.lock();
    if (!manager) {
        return false;
    }
    if (!manager->HasNoBlobsInRange(key.Channel, key.FromGeneration, nextGen)) {
        return false;
    }
    // Shared-out blobs sit in no GC queue, but a hard barrier would collect them under the borrower.
    const auto sharedBlobs = SharedBlobs.lock();
    if (!sharedBlobs) {
        return false;
    }
    return !sharedBlobs->HasSharedBlobsInRange(TabletInfo->TabletID, key.Channel, key.FromGeneration, nextGen);
}

bool THistoryCutterWrapper::GetEntryKey(const TLogoBlobID& blobId, TEntryKey& out) const {
    if (blobId.TabletID() != TabletInfo->TabletID) {
        return false;
    }
    const ui32 ch = blobId.Channel();
    if (ch < 2 || ch >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return false;
    }
    if (blobId.Generation() == CurrentGen) {
        return false;
    }
    const auto& hist = TabletInfo->Channels[ch].History;
    // UpperBound lands one past the owner: begin() means uncovered, end() means the active entry.
    const auto entry = UpperBound(hist.begin(), hist.end(), blobId.Generation(), TTabletChannelInfo::THistoryEntry::TCmp());
    if (entry == hist.begin() || entry == hist.end()) {
        return false;
    }
    out = TEntryKey{ ch, (entry - 1)->FromGeneration };
    return true;
}

void THistoryCutterWrapper::PublishLevels(const std::optional<ui64> sweepCandidates) {
    const ui64 candidates = sweepCandidates.value_or(Published.SweepCandidates);
    const ui64 poisoned = PoisonedChannels.size();
    const ui64 disproved = DisprovedAt.size();
    Signals.OnLevelsDelta((i64)candidates - (i64)Published.SweepCandidates, (i64)poisoned - (i64)Published.ChannelsPoisoned,
        (i64)disproved - (i64)Published.EntriesDisproved);
    Published.SweepCandidates = candidates;
    Published.ChannelsPoisoned = poisoned;
    Published.EntriesDisproved = disproved;
}

void THistoryCutterWrapper::IncrementCounter(const TEntryKey& key) {
    ++Counters[key];
}

void THistoryCutterWrapper::DecrementCounter(const TEntryKey& key) {
    auto it = Counters.find(key);
    // Unreachable while PortionKeys fences OnPortionRemoved; poisoning guards a future divergence of the two maps.
    if (it == Counters.end() || it->second == 0) {
        if (PoisonedChannels.insert(key.Channel).second) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "cut_history_channel_poisoned")("channel", key.Channel)(
                "from_generation", key.FromGeneration)("reason", "counter_underflow");
            PublishLevels();
        }
        return;
    }
    if (--it->second == 0) {
        Counters.erase(it);
    }
}

void THistoryCutterWrapper::OnPortionAdded(const TPortionDataAccessor& accessor) {
    if (!IsEnabled()) {
        return;
    }
    const ui64 portionId = accessor.GetPortionInfo().GetPortionId();
    THashSet<TEntryKey>& portionKeySet = PortionKeys[portionId];
    for (const auto& blobId : accessor.GetBlobIds()) {
        TEntryKey key;
        if (!GetEntryKey(blobId.GetLogoBlobId(), key)) {
            continue;
        }
        if (portionKeySet.insert(key).second) {
            IncrementCounter(key);
        }
    }
}

void THistoryCutterWrapper::OnPortionRemoved(const ui64 portionId) {
    if (!IsEnabled()) {
        return;
    }
    const THashSet<TEntryKey>* keys = PortionKeys.FindPtr(portionId);
    if (!keys) {
        return;
    }
    for (const auto& key : *keys) {
        DecrementCounter(key);
    }
    PortionKeys.erase(portionId);
}

void THistoryCutterWrapper::OnBootComplete(const THashMap<ui64, std::vector<TUnifiedBlobId>>& portionBlobIds) {
    Counters.clear();
    CutState.clear();
    PoisonedChannels.clear();
    PortionKeys.clear();
    DisprovedAt.clear();
    LastNominateAt = TInstant::Zero();
    NextChannelToCheck = TGlobal::FirstDataChannel;
    SweepInFlight = false;
    SweepCandidates.reset();
    SweepSurvivors.clear();
    SweepPortionIds.clear();
    SweepPortionOffset = 0;
    RangeProbeIssued = false;
    PortionVerdict.reset();
    RangeVerdict.reset();
    PublishLevels(0);

    if (!IsEnabled()) {
        return;
    }
    for (const auto& [portionId, blobIds] : portionBlobIds) {
        THashSet<TEntryKey>& portionKeySet = PortionKeys[portionId];
        for (const auto& blobId : blobIds) {
            TEntryKey key;
            if (!GetEntryKey(blobId.GetLogoBlobId(), key)) {
                continue;
            }
            if (portionKeySet.insert(key).second) {
                IncrementCounter(key);
            }
        }
    }
}

bool THistoryCutterWrapper::TryNominateAtBoot(const TActorContext& ctx) {
    if (!IsEnabled() || SweepInFlight) {
        return false;
    }
    const auto manager = Manager.lock();
    if (!manager) {
        return false;
    }
    const ui32 channelCount = static_cast<ui32>(TabletInfo->Channels.size());
    TVector<TEntryKey> batch;
    ui64 deferred = 0;
    for (ui32 ch = TGlobal::FirstDataChannel; ch < channelCount; ++ch) {
        const auto& hist = TabletInfo->Channels[ch].History;
        for (int i = 0; i < static_cast<int>(hist.size()) - 1; ++i) {
            const TEntryKey key{ ch, hist[i].FromGeneration };
            const ui32 nextGen = GetNextFromGeneration(key);
            if (!nextGen || !SeenGroupsCheckPasses(key)) {
                continue;
            }
            // A delete still owed to this range would be stranded by the cut, so defer to a later boot.
            if (manager->HasPendingDeletesInRange(ch, key.FromGeneration, nextGen)) {
                ++deferred;
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "cut_history_boot_deferred")("channel", ch)(
                    "from_generation", key.FromGeneration)("next_from_generation", nextGen);
                continue;
            }
            batch.push_back(key);
        }
    }
    Signals.OnBootProbeDeferred(deferred);
    if (batch.empty()) {
        return false;
    }
    Signals.OnBootProbeNominated(batch.size());
    for (const auto& key : batch) {
        CutState[key] = ECutState::Verifying;
        NYDBTest::TControllers::GetColumnShardController()->OnHistoryEntryNominated(key.Channel, key.FromGeneration);
    }
    SweepInFlight = true;
    ++SweepRound;
    RangeProbeIssued = false;
    RoundProofSource = EProofSource::BsRange;
    PortionVerdict.reset();
    RangeVerdict.reset();
    Signals.OnNomination();
    PublishLevels(batch.size());
    SweepSurvivors = batch;
    SweepCandidates = std::make_shared<const TVector<TEntryKey>>(std::move(batch));
    ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvStartCutHistorySweep());
    return true;
}

bool THistoryCutterWrapper::TryNominate(const TActorContext& ctx) {
    if (!IsEnabled()) {
        return false;
    }
    if (SweepInFlight) {
        return false;
    }
    // Candidate evaluation scans the GC queues, so rate-limit rather than scan per enqueue.
    if (LastNominateAt && ctx.Now() - LastNominateAt < GetNominateCadence()) {
        return false;
    }
    LastNominateAt = ctx.Now();

    // The next round resumes from the first channel this one did not fully service.
    ui32 drainChecks = 0;
    const ui32 channelCount = static_cast<ui32>(TabletInfo->Channels.size());
    if (channelCount <= TGlobal::FirstDataChannel) {
        return false;
    }
    if (NextChannelToCheck < TGlobal::FirstDataChannel || NextChannelToCheck >= channelCount) {
        NextChannelToCheck = TGlobal::FirstDataChannel;
    }
    const ui32 dataChannels = channelCount - TGlobal::FirstDataChannel;
    const ui32 firstChannel = NextChannelToCheck;
    TVector<TEntryKey> batch;
    for (ui32 idx = 0; idx < dataChannels; ++idx) {
        const ui32 ch = TGlobal::FirstDataChannel + (firstChannel - TGlobal::FirstDataChannel + idx) % dataChannels;
        if (drainChecks >= GetMaxDrainChecksPerNomination()) {
            NextChannelToCheck = ch;
            break;
        }
        NextChannelToCheck = TGlobal::FirstDataChannel + (ch - TGlobal::FirstDataChannel + 1) % dataChannels;
        if (PoisonedChannels.contains(ch)) {
            continue;
        }
        const auto& hist = TabletInfo->Channels[ch].History;
        for (int i = 0; i < static_cast<int>(hist.size()) - 1; ++i) {
            const TEntryKey key{ ch, hist[i].FromGeneration };
            if (const auto* state = CutState.FindPtr(key); state && *state != ECutState::None) {
                continue;
            }
            if (const auto* count = Counters.FindPtr(key); count && *count != 0) {
                continue;
            }
            if (const auto* disproval = DisprovedAt.FindPtr(key);
                disproval && ctx.Now() - disproval->At < GetDisprovedCooldown(disproval->Attempts)) {
                continue;
            }
            if (!SeenGroupsCheckPasses(key)) {
                continue;
            }
            if (drainChecks >= GetMaxDrainChecksPerNomination()) {
                break;
            }
            ++drainChecks;
            if (!IsDrained(key)) {
                continue;
            }
            batch.push_back(key);
            NYDBTest::TControllers::GetColumnShardController()->OnHistoryEntryNominated(key.Channel, key.FromGeneration);
        }
    }

    if (batch.empty()) {
        return false;
    }

    for (const auto& key : batch) {
        CutState[key] = ECutState::Verifying;
    }
    SweepInFlight = true;
    ++SweepRound;
    RangeProbeIssued = false;
    RoundProofSource = GetProofSource();
    // A verdict left over from a round whose counterpart never arrived must not be compared against this one.
    PortionVerdict.reset();
    RangeVerdict.reset();
    Signals.OnNomination();
    PublishLevels(batch.size());
    SweepSurvivors = batch;
    SweepCandidates = std::make_shared<const TVector<TEntryKey>>(std::move(batch));
    SweepPortionIds.clear();
    SweepPortionOffset = 0;

    ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvStartCutHistorySweep());
    return true;
}

void THistoryCutterWrapper::SetPortionSnapshot(TVector<std::pair<TInternalPathId, ui64>>&& ids) {
    SweepPortionIds = std::move(ids);
    SweepPortionOffset = 0;
}

TVector<std::pair<TInternalPathId, ui64>> THistoryCutterWrapper::GetNextBatch(size_t batchSize, bool& isLast) {
    TVector<std::pair<TInternalPathId, ui64>> batch;
    const size_t remaining = SweepPortionIds.size() - SweepPortionOffset;
    const size_t take = (remaining > batchSize) ? batchSize : remaining;
    for (size_t i = 0; i < take; ++i) {
        batch.push_back(SweepPortionIds[SweepPortionOffset + i]);
    }
    SweepPortionOffset += take;
    isLast = (SweepPortionOffset >= SweepPortionIds.size());
    return batch;
}

void THistoryCutterWrapper::OnBatchComplete(const THashSet<TEntryKey>& disproved, bool exhausted, const TActorContext& ctx) {
    for (const auto& key : disproved) {
        auto& state = DisprovedAt[key];
        state.At = ctx.Now();
        ++state.Attempts;
        CutState[key] = ECutState::None;
    }
    if (!disproved.empty()) {
        PublishLevels();
        EraseIf(SweepSurvivors, [&](const TEntryKey& key) {
            return disproved.contains(key);
        });
    }

    if (!exhausted) {
        ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvStartCutHistorySweep());
        return;
    }

    SweepInFlight = false;
    Signals.OnSweepCompleted();
    PublishLevels(0);
    if (RoundProofSource == EProofSource::Compare && SweepCandidates) {
        const THashSet<TEntryKey> survivors(SweepSurvivors.begin(), SweepSurvivors.end());
        THashSet<TEntryKey> disprovedByPortions;
        for (const auto& key : *SweepCandidates) {
            if (!survivors.contains(key)) {
                disprovedByPortions.insert(key);
            }
        }
        PortionVerdict = std::move(disprovedByPortions);
        CompareVerdicts();
    }
    SweepCandidates.reset();
    SweepPortionIds.clear();
    SweepPortionOffset = 0;

    for (const auto& key : SweepSurvivors) {
        if (const auto* count = Counters.FindPtr(key); count && *count != 0) {
            CutState[key] = ECutState::None;
            continue;
        }
        if (!IsDrained(key)) {
            CutState[key] = ECutState::None;
            continue;
        }
        // History may have changed since nomination: the same-group gate must hold at barrier-send time.
        if (!SeenGroupsCheckPasses(key)) {
            CutState[key] = ECutState::None;
            continue;
        }

        const ui32 nextFromGen = GetNextFromGeneration(key);
        if (!nextFromGen) {
            CutState[key] = ECutState::None;
            continue;
        }

        std::optional<ui32> groupId;
        if (key.Channel < static_cast<ui32>(TabletInfo->Channels.size())) {
            // Exact match, not GroupForGeneration: once cut, that generation resolves to a different live group.
            if (const auto* entry =
                    FindIfPtr(TabletInfo->Channels[key.Channel].History, [&key](const TTabletChannelInfo::THistoryEntry& historyEntry) {
                        return historyEntry.FromGeneration == key.FromGeneration;
                    })) {
                groupId = entry->GroupID;
            }
        }
        if (!groupId) {
            CutState[key] = ECutState::None;
            continue;
        }

        DisprovedAt.erase(key);
        PublishLevels();
        CutState[key] = ECutState::SentBarrier;
        ctx.Register(new TCutHistoryBarrierActor(
            TabletActorId, LauncherActorId, TabletInfo->TabletID, CurrentGen, key.Channel, *groupId, key.FromGeneration, nextFromGen));
    }
    SweepSurvivors.clear();

    // Safety net: the disproved loop settled these already, so reset without counting an attempt.
    for (auto& [key, state] : CutState) {
        if (state == ECutState::Verifying) {
            state = ECutState::None;
        }
    }
}

void THistoryCutterWrapper::OnBarrierResult(const TEntryKey& key, bool ok, TInstant now) {
    auto* state = CutState.FindPtr(key);
    if (!state) {
        return;
    }
    Signals.OnBarrierResult(ok);
    if (ok) {
        *state = ECutState::Cut;
        NYDBTest::TControllers::GetColumnShardController()->OnHistoryEntryCut(key.Channel, key.FromGeneration);
    } else {
        *state = ECutState::None;
        // A failure enters the disproval cooldown instead of retrying every cadence; Attempts restarts
        // at 1 because nomination erased the record, so repeated failures plateau at cooldown(1).
        auto& disproval = DisprovedAt[key];
        disproval.At = now;
        ++disproval.Attempts;
        PublishLevels();
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
