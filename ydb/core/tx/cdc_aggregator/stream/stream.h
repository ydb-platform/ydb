#pragma once

#include "barrier.h"
#include "stream_changes.h"
#include "stream_config.h"

#include <util/generic/hash.h>
#include <util/generic/set.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

enum class EStreamPhase : ui8 {
    // Reports are accepted, the barrier is not published yet.
    Configured = 1,
    // SchemeShard has configured every participant: the barrier may be published.
    Active = 2,
};

// Replacement of a source by split/merge. It is kept forever: it answers
// retries of the same replacement and rejects late reports of the parent.
struct TSourceReplacement {
    TSourceId Parent = 0;
    // Sorted, unique, never empty.
    TVector<TSourceId> Children;
    // Split/merge operation, for diagnostics only.
    ui64 OperationId = 0;
};

// Sorts children; returns false with an error for a replacement that would
// leave the stream without a successor of the parent.
bool NormalizeSourceReplacement(TSourceReplacement& replacement, TString& error);

// Progress of a source; empty until its first report.
using TSourceProgress = std::optional<TBarrier>;

enum class EConfigureOutcome {
    Configured,
    AlreadyConfigured,
    Conflict,
};

enum class EActivateOutcome {
    Activated,
    AlreadyActive,
    VersionMismatch,
};

enum class EDeliveredOutcome {
    Advanced,
    // A duplicate or an older version: the stored progress is kept.
    NotAdvanced,
    UnknownSource,
    RetiredSource,
};

enum class EReplaceOutcome {
    Replaced,
    AlreadyReplaced,
    UnknownSource,
    Conflict,
};

// Aggregate of one CDC stream: the current set of sources, their delivered
// progress and the ready barrier, the minimum progress over all sources.
//
// Invariants:
// - progress of a source and the ready barrier never decrease;
// - a source without progress blocks the barrier; an empty set has none;
// - a source leaves the set only through a replacement and never returns;
// - a replacement removes the parent and adds the children in one step;
// - the barrier payload is the payload reported with the barrier version.
//
// Commands record what they changed into TStreamChanges. Every command except
// Configure requires a configured stream.
class TStream {
public:
    TStream() = default;

    static TStream Restore(
        TStreamConfig config,
        EStreamPhase phase,
        THashMap<TSourceId, TSourceProgress> sources,
        std::optional<TBarrier> readyBarrier,
        THashMap<TSourceId, TSourceReplacement> replacements);

    bool IsConfigured() const {
        return Config.has_value();
    }

    const TStreamConfig& GetConfig() const;
    EStreamPhase GetPhase() const;

    const std::optional<TBarrier>& GetReadyBarrier() const {
        return ReadyBarrier;
    }

    const THashMap<TSourceId, TSourceProgress>& GetSources() const {
        return Sources;
    }

    const THashMap<TSourceId, TSourceReplacement>& GetReplacements() const {
        return Replacements;
    }

    // nullptr for a source outside the current set.
    const TSourceProgress* FindSourceProgress(TSourceId source) const;

    size_t CountSourcesWithoutProgress() const {
        return Sources.size() - ProgressIndex.size();
    }

    EConfigureOutcome Configure(const TStreamConfig& config, TStreamChanges& changes);
    EActivateOutcome Activate(ui64 configVersion, TStreamChanges& changes);
    EDeliveredOutcome ApplyDelivered(TSourceId source, const TBarrier& delivered, TStreamChanges& changes);
    EReplaceOutcome ReplaceSource(const TSourceReplacement& replacement, TStreamChanges& changes);

private:
    void AddSourceWithoutProgress(TSourceId source);
    void SetSourceProgress(TSourceId source, const TBarrier& delivered);
    void EraseSource(TSourceId source);
    void AdvanceReadyBarrier(TStreamChanges& changes);

private:
    std::optional<TStreamConfig> Config;
    EStreamPhase Phase = EStreamPhase::Configured;
    THashMap<TSourceId, TSourceProgress> Sources;
    // Sources with progress ordered by version; the first one is the minimum.
    // Keeps a report O(log S) instead of a scan over all sources.
    TSet<std::pair<TRowVersion, TSourceId>> ProgressIndex;
    std::optional<TBarrier> ReadyBarrier;
    // By parent.
    THashMap<TSourceId, TSourceReplacement> Replacements;
};

} // namespace NKikimr::NCdcAggregator
