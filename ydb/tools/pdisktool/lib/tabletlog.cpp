#include "tabletlog.h"

#include <ydb/core/protos/tablet.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NKikimr::NPDiskTool {

namespace {

// A zero entry sizes an array by its tail length and a log entry by its step, so both are refused when
// they are beyond anything a real tablet produces instead of being turned into an allocation.
constexpr ui32 MaxZeroTail = 4u << 20;
constexpr ui32 MaxGenerationSteps = 4u << 20;
constexpr ui32 MaxCandidates = 64;

struct TEntry {
    enum EStatus {
        StatusUnknown,
        StatusOk,
        StatusBody,

        // set by the zero entry tail definition
        StatusMustBePresent,
        StatusMustBeIgnored,
        StatusMustBeIgnoredBody,
    };

    EStatus Status = StatusUnknown;
    TVector<TLogoBlobID> References;
    TVector<ui32> DependsOn;
    bool IsSnapshot = false;
    bool IsTotalSnapshot = false;
    bool Broken = false;
    TString EmbeddedLogBody;
    TVector<TEvTablet::TCommitMetadata> EmbeddedMetadata;
    TVector<TLogoBlobID> GcDiscovered;
    TVector<TLogoBlobID> GcLeft;

    void BecomeConfirmed() {
        switch (Status) {
            case StatusUnknown:
            case StatusMustBeIgnored:
                Status = StatusMustBePresent;
                break;
            case StatusBody:
            case StatusMustBeIgnoredBody:
                Status = StatusOk;
                break;
            case StatusOk:
            case StatusMustBePresent:
                break;
        }
    }

    void BecomeDeclined() {
        switch (Status) {
            case StatusUnknown:
            case StatusMustBePresent:
                Status = StatusMustBeIgnored;
                break;
            case StatusOk:
            case StatusBody:
                Status = StatusMustBeIgnoredBody;
                break;
            case StatusMustBeIgnored:
            case StatusMustBeIgnoredBody:
                break;
        }
    }

    void UpdateReferences(const NKikimrTabletBase::TTabletLogEntry& x) {
        if (const ui32 size = x.ReferencesSize()) {
            References.resize(size);
            for (ui32 i = 0; i != size; ++i) {
                References[i] = LogoBlobIDFromLogoBlobID(x.GetReferences(i));
            }
        }

        if (x.DependsOnSize()) {
            DependsOn.insert(DependsOn.begin(), x.GetDependsOn().begin(), x.GetDependsOn().end());
        }

        if (x.HasIsTotalSnapshot()) {
            IsTotalSnapshot = x.GetIsTotalSnapshot();
        }

        if (x.HasIsSnapshot()) {
            IsSnapshot = x.GetIsSnapshot();
        }

        if (x.HasEmbeddedLogBody()) {
            // A commit is either embedded or referenced, and a snapshot is never embedded. An entry
            // that claims both is not something to guess about, so it counts as a missing entry.
            if (!References.empty() || IsSnapshot) {
                Broken = true;
            } else {
                EmbeddedLogBody = x.GetEmbeddedLogBody();
            }
        }

        if (const ui32 size = x.GcDiscoveredSize()) {
            GcDiscovered.resize(size);
            for (ui32 i = 0; i != size; ++i) {
                GcDiscovered[i] = LogoBlobIDFromLogoBlobID(x.GetGcDiscovered(i));
            }
        }

        if (const ui32 size = x.GcLeftSize()) {
            GcLeft.resize(size);
            for (ui32 i = 0; i != size; ++i) {
                GcLeft[i] = LogoBlobIDFromLogoBlobID(x.GetGcLeft(i));
            }
        }

        if (const size_t size = x.EmbeddedMetadataSize()) {
            EmbeddedMetadata.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                const auto& meta = x.GetEmbeddedMetadata(i);
                EmbeddedMetadata.emplace_back(meta.GetKey(), meta.GetData());
            }
        }

        switch (Status) {
            case StatusUnknown:
                Status = StatusBody;
                break;
            case StatusMustBePresent:
                Status = StatusOk;
                break;
            case StatusMustBeIgnored:
                Status = StatusMustBeIgnoredBody;
                break;
            case StatusOk:
            case StatusBody:
            case StatusMustBeIgnoredBody:
                break;
        }
    }
};

struct TGeneration {
    TVector<TEntry> Body;
    std::pair<ui32, ui32> PrevGeneration{0, 0};
    ui32 NextGeneration = 0;
    ui32 Base = 1;
    ui32 Cutoff = Max<ui32>();
    bool HasZeroEntry = false;
    NKikimrTabletBase::TTabletLogEntry ZeroEntryContent;

    TEntry* Get(ui32 step) {
        if (step < Base) {
            return nullptr;
        }
        const ui32 idx = step - Base;
        return idx < Body.size() ? &Body[idx] : nullptr;
    }

    bool Ensure(ui32 step) {
        if (step < Base || step - Base >= MaxGenerationSteps) {
            return false;
        }
        const ui32 idx = step - Base;
        if (idx >= Body.size()) {
            Body.resize(idx + 1);
        }
        return true;
    }
};

class TBuilder {
public:
    TBuilder(TBlobStore& store, ui64 tabletId, TIssueLog& issues)
        : Store(store)
        , TabletId(tabletId)
        , Issues(issues)
    {}

    // The whole of the production request, minus the actor hops: take the key entry, walk the log
    // range the blob store holds, mark the generations from the zero entries and lay out the graph.
    bool Run(const TLogoBlobID& keyId, bool tolerateGaps, TTabletLogHistory& out);

    const TString& Reason() const {
        return Reason_;
    }

    TTabletLogHistoryStats Stats;

private:
    bool Refuse(TString reason) {
        Reason_ = std::move(reason);
        return false;
    }

    TGeneration& Generation(ui32 gen) {
        TGeneration& x = LogInfo[gen];
        if (gen == Snapshot.first) {
            x.Base = Snapshot.second;
        }
        return x;
    }

    bool ProcessZeroEntry(ui32 gen, const NKikimrTabletBase::TTabletLogEntry& e);
    bool ProcessLogEntry(const TLogoBlobID& id, const NKikimrTabletBase::TTabletLogEntry& e);
    bool RebuildGenSequence();
    bool FillGenerationEntries(std::pair<ui32, ui32> confirmed, TGeneration& prev,
        const NKikimrTabletBase::TTabletLogEntry& e);
    void ScanRefsToCheck();
    bool BuildHistory(TTabletLogHistory& out);

    TBlobStore& Store;
    const ui64 TabletId;
    TIssueLog& Issues;
    bool TolerateGaps = false;
    TString Reason_;

    std::pair<ui32, ui32> Latest{0, 0};
    std::pair<ui32, ui32> Snapshot{0, 0};
    std::pair<ui32, ui32> Confirmed{0, 0};
    TMap<ui32, TGeneration> LogInfo;
    TSet<TLogoBlobID> MissingRefs;
};

bool TBuilder::ProcessZeroEntry(ui32 gen, const NKikimrTabletBase::TTabletLogEntry& e) {
    if (!e.HasZeroConfirmed() || !e.HasZeroTailSz()) {
        return Refuse(TStringBuilder() << "zero entry of generation " << gen
            << " has no confirmed stamp or no tail size");
    }
    if (e.GetZeroTailSz() > MaxZeroTail) {
        return Refuse(TStringBuilder() << "zero entry of generation " << gen << " claims a tail of "
            << e.GetZeroTailSz() << " step(s)");
    }
    if (e.ZeroTailBitmaskSize() != (e.GetZeroTailSz() + 63) / 64) {
        return Refuse(TStringBuilder() << "zero entry of generation " << gen << " has a "
            << e.ZeroTailBitmaskSize() << " word bitmask for a tail of " << e.GetZeroTailSz());
    }

    TGeneration& current = Generation(gen);
    current.HasZeroEntry = true;
    current.ZeroEntryContent.CopyFrom(e);
    ++Stats.ZeroEntries;
    return true;
}

bool TBuilder::ProcessLogEntry(const TLogoBlobID& id, const NKikimrTabletBase::TTabletLogEntry& e) {
    if (!e.HasSnapshot() || !e.HasConfirmed()) {
        return Refuse(TStringBuilder() << id.ToString() << " has no snapshot or confirmed stamp");
    }

    TGeneration& gx = Generation(id.Generation());
    if (!gx.Ensure(id.Step())) {
        return Refuse(TStringBuilder() << id.ToString() << " is outside the generation window ["
            << gx.Base << ", " << (ui64(gx.Base) + MaxGenerationSteps) << ")");
    }

    // Synthetic entries exist for follower sync only and carry no commit of their own.
    if (id.Cookie() == 0) {
        gx.Get(id.Step())->UpdateReferences(e);
    }
    ++Stats.LogEntries;
    return true;
}

bool TBuilder::FillGenerationEntries(std::pair<ui32, ui32> confirmed, TGeneration& prev,
        const NKikimrTabletBase::TTabletLogEntry& e)
{
    if (confirmed.first == Snapshot.first) {
        prev.Base = Snapshot.second;
    }

    const ui32 tailsz = e.GetZeroTailSz();
    const ui64 gensz = ui64(confirmed.second) + tailsz;
    if (gensz > Max<ui32>() || gensz < prev.Base) {
        return Refuse(TStringBuilder() << "zero entry confirms generation " << confirmed.first
            << " up to step " << gensz << ", below its base " << prev.Base);
    }
    if (!prev.Ensure(gensz)) {
        return Refuse(TStringBuilder() << "generation " << confirmed.first << " would span "
            << (gensz - prev.Base + 1) << " step(s), more than this tool keeps in memory");
    }
    prev.Cutoff = gensz; // later entries are of no interest

    { // static part, mark as confirmed
        for (ui32 step = prev.Base; step <= confirmed.second; ++step) {
            if (TEntry* x = prev.Get(step)) {
                x->BecomeConfirmed();
            }
        }
    }

    { // tail part, mark according to the bitmask
        ui64 mask = 0;
        ui64 val = 0;
        ui32 step = confirmed.second + 1;
        for (ui32 i = 0; i < tailsz; ++i, mask <<= 1, ++step) {
            if (mask == 0) {
                mask = 1;
                val = e.GetZeroTailBitmask(i / 64);
            }
            const bool ok = val & mask;
            if (TEntry* x = prev.Get(step)) {
                if (ok) {
                    x->BecomeConfirmed();
                } else {
                    x->BecomeDeclined();
                }
            }
        }

        for (ui32 end = ui32(prev.Body.size() + prev.Base); step < end; ++step) {
            if (TEntry* x = prev.Get(step)) {
                x->BecomeDeclined();
            }
        }
    }

    return true;
}

bool TBuilder::RebuildGenSequence() {
    if (LogInfo.empty()) {
        return true;
    }

    auto it = LogInfo.end();
    --it;

    TSet<ui32> visited;
    for (;;) {
        const ui32 gen = it->first;
        TGeneration& current = it->second;
        if (!current.HasZeroEntry || gen == Snapshot.first) {
            break;
        }
        if (!visited.insert(gen).second) {
            return Refuse(TStringBuilder() << "the zero entry chain loops at generation " << gen);
        }

        const auto confirmed = ExpandGenStepPair(current.ZeroEntryContent.GetZeroConfirmed());
        const ui32 prevGeneration = confirmed.first;
        if (prevGeneration < Snapshot.first) {
            return Refuse(TStringBuilder() << "zero entry of generation " << gen << " confirms "
                << prevGeneration << ":" << confirmed.second << ", before the snapshot at "
                << Snapshot.first << ":" << Snapshot.second);
        }
        if (prevGeneration >= gen) {
            return Refuse(TStringBuilder() << "zero entry of generation " << gen << " confirms "
                << prevGeneration << ":" << confirmed.second << ", which is not an earlier generation");
        }

        current.PrevGeneration = confirmed;
        TGeneration& prev = Generation(prevGeneration);
        prev.NextGeneration = gen;

        if (confirmed.first > 0 && !FillGenerationEntries(confirmed, prev, current.ZeroEntryContent)) {
            return false;
        }

        it = LogInfo.find(prevGeneration);
        Y_ABORT_UNLESS(it != LogInfo.end()); // Generation() above created it
    }

    LogInfo.erase(LogInfo.begin(), it);
    return true;
}

void TBuilder::ScanRefsToCheck() {
    if (Latest.first != Confirmed.first) {
        return;
    }
    TGeneration* gx = LogInfo.FindPtr(Latest.first);
    if (!gx) {
        return;
    }

    for (i64 pi = i64(gx->Body.size()) - 1; pi >= 0; --pi) {
        const ui32 step = gx->Base + ui32(pi);
        if (step <= Confirmed.second) {
            break;
        }
        TEntry& entry = gx->Body[pi];
        if (entry.Status != TEntry::StatusBody) {
            continue;
        }
        for (const TLogoBlobID& ref : entry.References) {
            // Blobs of other tablets are borrowed parts; the leader does not vouch for them either.
            if (ref.TabletID() == TabletId && !Store.CanRestore(ref)) {
                if (MissingRefs.insert(ref).second) {
                    ++Stats.MissingReferences;
                }
            }
        }
    }
}

bool TBuilder::BuildHistory(TTabletLogHistory& out) {
    if (LogInfo.empty()) {
        return Refuse("no log entries in the snapshot to key entry range");
    }

    TIntrusivePtr<TEvTablet::TDependencyGraph> graph(new TEvTablet::TDependencyGraph(Snapshot));
    bool valid = true;
    std::pair<ui32, ui32> gapAt(Max<ui32>(), Max<ui32>());

    auto gap = [&](std::pair<ui32, ui32> id) {
        ++Stats.Gaps;
        gapAt = id;
        if (TolerateGaps) {
            // Maximum effort: an incomplete history still restores everything up to the hole, so the
            // entries collected so far are kept instead of being thrown away.
            return;
        }
        graph->Invalidate();
        valid = false;
    };

    TSet<ui32> visited;
    for (auto gen = LogInfo.begin(), egen = LogInfo.end();;) {
        const ui32 generation = gen->first;
        TGeneration& gx = gen->second;
        const bool isTailGeneration = Latest.first == generation && Confirmed.first == generation;
        bool hasSnapshotInGeneration = (generation == 0);
        ui32 lastUnbrokenTailEntry = Confirmed.second;

        for (ui32 i = 0, e = ui32(gx.Body.size()); i != e; ++i) {
            const ui32 step = gx.Base + i;
            const bool isTail = isTailGeneration && step > Confirmed.second;
            ui32 generationSnapshotStep = 0;

            TEntry& entry = gx.Body[i];
            const std::pair<ui32, ui32> id(generation, step);

            auto include = [&]() {
                if (entry.EmbeddedLogBody) {
                    graph->AddEntry(id, std::move(entry.EmbeddedLogBody), std::move(entry.GcDiscovered),
                        std::move(entry.GcLeft), std::move(entry.EmbeddedMetadata));
                } else {
                    graph->AddEntry(id, std::move(entry.References), entry.IsSnapshot,
                        std::move(entry.GcDiscovered), std::move(entry.GcLeft),
                        std::move(entry.EmbeddedMetadata));
                }
            };

            if (isTail) {
                if (entry.Status == TEntry::StatusOk) {
                    // Nothing marks a tail entry as confirmed, so this is a stray blob; take it, as
                    // its body is there and something did confirm it.
                    include();
                    continue;
                }
                if (entry.Status != TEntry::StatusBody || entry.Broken) {
                    ++Stats.DeclinedEntries;
                    continue;
                }

                bool dependsOk = true;
                for (auto it = entry.DependsOn.begin(); dependsOk && it != entry.DependsOn.end(); ++it) {
                    const ui32 x = *it;
                    if (x >= step) {
                        // A commit that depends on a later step cannot be replayed in order.
                        dependsOk = false;
                        break;
                    }
                    const TEntry* dep = gx.Get(x);
                    dependsOk = x < gx.Base || x <= Confirmed.second || x <= generationSnapshotStep
                        || (dep && dep->Status == TEntry::StatusOk);
                }

                bool refsOk = true;
                for (auto it = entry.References.begin(); refsOk && it != entry.References.end(); ++it) {
                    refsOk = !MissingRefs.contains(*it);
                }

                const bool snapOk = entry.IsTotalSnapshot
                    || (entry.IsSnapshot && step == lastUnbrokenTailEntry + 1);

                if (refsOk && (snapOk || dependsOk)) {
                    const bool isSnapshot = entry.IsSnapshot;
                    include();
                    if (lastUnbrokenTailEntry + 1 == step) {
                        lastUnbrokenTailEntry = step;
                    }
                    if (isSnapshot) {
                        generationSnapshotStep = step;
                        hasSnapshotInGeneration = true;
                    }
                } else {
                    ++Stats.DeclinedEntries;
                }
            } else if (step <= gx.Cutoff) {
                switch (entry.Status) {
                    case TEntry::StatusOk:
                        if (entry.Broken) {
                            gap(id);
                            break;
                        }
                        hasSnapshotInGeneration |= entry.IsSnapshot;
                        include();
                        break;
                    case TEntry::StatusMustBeIgnored:
                    case TEntry::StatusMustBeIgnoredBody:
                        ++Stats.DeclinedEntries;
                        break;
                    default:
                        // A confirmed commit whose body never turned up.
                        gap(id);
                        break;
                }
            }
        }

        if (!hasSnapshotInGeneration && !gx.HasZeroEntry) {
            gap({generation, 0});
        }

        if (gx.NextGeneration == 0) {
            ++gen;
            if (gen == egen) {
                break;
            }
            // The zero entry chain stopped short of the newest generation, so the generations in
            // between were never linked and their commits cannot be ordered.
            gap({generation, Max<ui32>()});
        } else {
            gen = LogInfo.find(gx.NextGeneration);
            if (gen == egen || !visited.insert(gx.NextGeneration).second) {
                return Refuse(TStringBuilder() << "the generation chain is broken at " << generation);
            }
        }
    }

    if (!valid) {
        return Refuse(TStringBuilder() << "no log entry for " << TabletId << ":" << gapAt.first
            << ":" << gapAt.second);
    }

    out.Graph = std::move(graph);
    out.Snapshot = out.Graph->Snapshot;
    out.Confirmed = Confirmed;
    out.Latest = Latest;
    out.Ok = true;
    return true;
}

bool TBuilder::Run(const TLogoBlobID& keyId, bool tolerateGaps, TTabletLogHistory& out) {
    TolerateGaps = tolerateGaps;
    LogInfo.clear();
    MissingRefs.clear();

    const TString* body = Store.Get(keyId);
    if (!body) {
        return Refuse("the key entry blob cannot be reassembled");
    }
    NKikimrTabletBase::TTabletLogEntry keyEntry;
    if (!keyEntry.ParseFromString(*body)) {
        ++Stats.ParseFailures;
        return Refuse("the key entry blob is not a tablet log entry");
    }
    if (!keyEntry.HasSnapshot()) {
        return Refuse("the key entry has no snapshot stamp");
    }

    Latest = {keyId.Generation(), keyId.Step()};
    Snapshot = ExpandGenStepPair(keyEntry.GetSnapshot());
    if (Snapshot > Latest) {
        return Refuse(TStringBuilder() << "the snapshot at " << Snapshot.first << ":" << Snapshot.second
            << " is ahead of the key entry");
    }

    ui32 lastGen = 0;
    ui32 lastStep = 0;
    if (keyId.Step() == 0) {
        Confirmed = {Latest.first, 0};
        if (!ProcessZeroEntry(keyId.Generation(), keyEntry)) {
            return false;
        }
        lastGen = Latest.first;
        lastStep = 0;
    } else {
        if (!keyEntry.HasConfirmed()) {
            return Refuse("the key entry has no confirmed step");
        }
        Confirmed = {Latest.first, keyEntry.GetConfirmed()};
        if (Confirmed.second > keyId.Step()) {
            return Refuse(TStringBuilder() << "the key entry confirms step " << Confirmed.second
                << ", which is past its own step " << keyId.Step());
        }
        if (!ProcessLogEntry(keyId, keyEntry)) {
            return false;
        }
        lastGen = Latest.first;
        lastStep = keyId.Cookie() ? Latest.second : Latest.second - 1;

        TGeneration& gx = Generation(lastGen);
        for (ui32 i = gx.Base; i <= Confirmed.second; ++i) {
            if (TEntry* x = gx.Get(i)) {
                x->BecomeConfirmed();
            }
        }
    }

    // The two TEvRange requests of the production code cover [Snapshot, Confirmed] and the tail up to
    // the entry before the key one; here that is a walk over what the blob store already holds.
    const TLogoBlobID from(TabletId, Snapshot.first, Snapshot.second, 0, 0, 0);
    const TLogoBlobID to(TabletId, lastGen, lastStep, 0, TLogoBlobID::MaxBlobSize,
        TLogoBlobID::MaxCookie, 0, TLogoBlobID::MaxCrcMode);
    for (const TLogoBlobID& id : Store.Range(from, to)) {
        if (id.TabletID() != TabletId || id.Channel() != 0 || id == keyId) {
            continue;
        }
        const TString* entryBody = Store.Get(id);
        if (!entryBody) {
            // A log entry that cannot be reassembled shows up as a missing body later, which is
            // exactly what it is.
            continue;
        }
        NKikimrTabletBase::TTabletLogEntry entry;
        if (!entry.ParseFromString(*entryBody)) {
            ++Stats.ParseFailures;
            Issues.Warning("tablet-log", TStringBuilder() << id.ToString()
                << " is not a tablet log entry, skipped");
            continue;
        }
        if (id.Step() == 0) {
            if (!ProcessZeroEntry(id.Generation(), entry)) {
                // Not the key entry, so this only costs the generations it would have linked.
                Issues.Warning("tablet-log", TStringBuilder() << id.ToString() << " unusable: " << Reason_);
                Reason_.clear();
            }
        } else if (!ProcessLogEntry(id, entry)) {
            Issues.Warning("tablet-log", TStringBuilder() << id.ToString() << " unusable: " << Reason_);
            Reason_.clear();
        }
    }

    if (!RebuildGenSequence()) {
        return false;
    }
    ScanRefsToCheck();
    if (!BuildHistory(out)) {
        return false;
    }
    out.KeyEntry = keyId;
    return true;
}

} // namespace

TTabletLogHistory RebuildTabletHistory(TBlobStore& store, ui64 tabletId, ui32 maxGeneration,
        TIssueLog& issues)
{
    TTabletLogHistory result;

    const TLogoBlobID first(tabletId, 0, 0, 0, 0, 0);
    const TLogoBlobID last(tabletId, Max<ui32>(), Max<ui32>(), 0, TLogoBlobID::MaxBlobSize,
        TLogoBlobID::MaxCookie, 0, TLogoBlobID::MaxCrcMode);

    TVector<TLogoBlobID> candidates;
    for (const TLogoBlobID& id : store.Range(first, last)) {
        if (id.TabletID() != tabletId || id.Channel() != 0) {
            continue;
        }
        if (maxGeneration && id.Generation() > maxGeneration) {
            continue;
        }
        if (store.CanRestore(id)) {
            candidates.push_back(id);
        }
    }
    // TEvDiscover answers with the last blob of channel 0, and the blob order is by generation, step
    // and cookie, so the newest candidate is the last one.
    Reverse(candidates.begin(), candidates.end());
    if (candidates.size() > MaxCandidates) {
        candidates.resize(MaxCandidates);
    }

    if (candidates.empty()) {
        issues.Error("tablet-log", TStringBuilder() << "No usable channel 0 blob of tablet " << tabletId
            << " in the input; without a log entry there is nothing to replay");
        return result;
    }

    // First the history the tablet itself would have accepted, then, if none of the candidates gives
    // one, the same walk with the holes reported instead of refused.
    for (bool tolerateGaps : {false, true}) {
        for (const TLogoBlobID& candidate : candidates) {
            TBuilder builder(store, tabletId, issues);
            ++result.Stats.CandidatesTried;
            if (builder.Run(candidate, tolerateGaps, result)) {
                const auto stats = builder.Stats;
                const ui32 tried = result.Stats.CandidatesTried;
                result.Stats = stats;
                result.Stats.CandidatesTried = tried;
                result.Stats.GapsTolerated = tolerateGaps;
                if (candidate != candidates.front()) {
                    issues.Warning("tablet-log", TStringBuilder() << "Recovered from key entry "
                        << candidate.ToString() << " instead of the newest one "
                        << candidates.front().ToString());
                }
                if (tolerateGaps) {
                    issues.Warning("tablet-log", TStringBuilder() << "No candidate gives a complete log,"
                        << " continuing with " << result.Stats.Gaps << " missing entry position(s);"
                        << " the recovered state may be older or partial");
                }
                return result;
            }
            issues.Warning("tablet-log", TStringBuilder() << "Key entry " << candidate.ToString()
                << " rejected: " << builder.Reason());
        }
    }

    issues.Error("tablet-log", TStringBuilder() << "None of the " << candidates.size()
        << " candidate key entries of tablet " << tabletId << " yields a log history");
    return result;
}

} // namespace NKikimr::NPDiskTool
