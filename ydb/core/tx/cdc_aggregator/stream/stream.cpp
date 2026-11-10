#include "stream.h"

#include <util/generic/yexception.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

bool NormalizeSourceReplacement(TSourceReplacement& replacement, TString& error) {
    if (!replacement.Parent) {
        error = "Parent is not set";
        return false;
    }
    if (replacement.Children.empty()) {
        error = "No children";
        return false;
    }

    auto& children = replacement.Children;
    std::sort(children.begin(), children.end());
    if (std::adjacent_find(children.begin(), children.end()) != children.end()) {
        error = "Duplicate child";
        return false;
    }
    if (children.front() == 0) {
        error = "Zero child id";
        return false;
    }
    if (std::binary_search(children.begin(), children.end(), replacement.Parent)) {
        error = "Parent is its own child";
        return false;
    }

    return true;
}

TStream TStream::Restore(
        TStreamConfig config,
        EStreamPhase phase,
        THashMap<TSourceId, TSourceProgress> sources,
        std::optional<TBarrier> readyBarrier,
        THashMap<TSourceId, TSourceReplacement> replacements)
{
    TStream stream;
    stream.Config = std::move(config);
    stream.Phase = phase;
    stream.ReadyBarrier = std::move(readyBarrier);
    stream.Replacements = std::move(replacements);

    for (auto& [source, progress] : sources) {
        Y_ENSURE(!stream.Replacements.contains(source), "Retired source " << source << " is in the current set");
        if (progress) {
            stream.ProgressIndex.emplace(progress->Version, source);
        }
    }
    stream.Sources = std::move(sources);

    return stream;
}

const TStreamConfig& TStream::GetConfig() const {
    Y_ENSURE(Config, "Stream is not configured");
    return *Config;
}

EStreamPhase TStream::GetPhase() const {
    Y_ENSURE(Config, "Stream is not configured");
    return Phase;
}

const TSourceProgress* TStream::FindSourceProgress(TSourceId source) const {
    auto it = Sources.find(source);
    return it != Sources.end() ? &it->second : nullptr;
}

EConfigureOutcome TStream::Configure(const TStreamConfig& config, TStreamChanges& changes) {
    if (Config) {
        return *Config == config ? EConfigureOutcome::AlreadyConfigured : EConfigureOutcome::Conflict;
    }

    Y_ENSURE(!config.Sources.empty());
    Config = config;
    Phase = EStreamPhase::Configured;
    changes.Config = true;

    for (const auto source : config.Sources) {
        AddSourceWithoutProgress(source);
        changes.UpsertedSources.insert(source);
    }

    return EConfigureOutcome::Configured;
}

EActivateOutcome TStream::Activate(ui64 configVersion, TStreamChanges& changes) {
    Y_ENSURE(Config, "Stream is not configured");

    if (configVersion != Config->Version) {
        return EActivateOutcome::VersionMismatch;
    }
    if (Phase == EStreamPhase::Active) {
        return EActivateOutcome::AlreadyActive;
    }

    Phase = EStreamPhase::Active;
    changes.Config = true;
    return EActivateOutcome::Activated;
}

EDeliveredOutcome TStream::ApplyDelivered(TSourceId source, const TBarrier& delivered, TStreamChanges& changes) {
    Y_ENSURE(Config, "Stream is not configured");

    auto it = Sources.find(source);
    if (it == Sources.end()) {
        // A report never adds a source: the set changes only by configuration
        // and replacements.
        return Replacements.contains(source) ? EDeliveredOutcome::RetiredSource : EDeliveredOutcome::UnknownSource;
    }

    // Altering the resolved interval or retrying an unacknowledged heartbeat
    // may bring an older version. The stored one remains valid, so it is kept.
    const auto& progress = it->second;
    if (progress && delivered.Version <= progress->Version) {
        return EDeliveredOutcome::NotAdvanced;
    }

    SetSourceProgress(source, delivered);
    changes.UpsertedSources.insert(source);
    AdvanceReadyBarrier(changes);
    return EDeliveredOutcome::Advanced;
}

EReplaceOutcome TStream::ReplaceSource(const TSourceReplacement& replacement, TStreamChanges& changes) {
    Y_ENSURE(Config, "Stream is not configured");

    // A parent is replaced at most once: tablet ids are never reused and
    // a split or merge retires its source tablets.
    if (auto it = Replacements.find(replacement.Parent); it != Replacements.end()) {
        return it->second.Children == replacement.Children ? EReplaceOutcome::AlreadyReplaced : EReplaceOutcome::Conflict;
    }
    if (!Sources.contains(replacement.Parent)) {
        return EReplaceOutcome::UnknownSource;
    }
    for (const auto child : replacement.Children) {
        if (Replacements.contains(child)) {
            return EReplaceOutcome::Conflict;
        }
    }

    EraseSource(replacement.Parent);
    changes.ErasedSources.insert(replacement.Parent);

    // In a merge every parent names the same child: the child that is already
    // in the set keeps its progress.
    for (const auto child : replacement.Children) {
        if (!Sources.contains(child)) {
            AddSourceWithoutProgress(child);
            changes.UpsertedSources.insert(child);
        }
    }

    Replacements.emplace(replacement.Parent, replacement);
    changes.AddedReplacements.push_back(replacement.Parent);

    // Children normally have no progress and block the barrier; a child that
    // reported already may let it advance once the parent is gone.
    AdvanceReadyBarrier(changes);
    return EReplaceOutcome::Replaced;
}

void TStream::AddSourceWithoutProgress(TSourceId source) {
    const bool inserted = Sources.emplace(source, std::nullopt).second;
    Y_ENSURE(inserted, "Duplicate source " << source);
}

void TStream::SetSourceProgress(TSourceId source, const TBarrier& delivered) {
    auto& progress = Sources.at(source);
    if (progress) {
        ProgressIndex.erase(std::make_pair(progress->Version, source));
    }
    progress = delivered;
    ProgressIndex.emplace(delivered.Version, source);
}

void TStream::EraseSource(TSourceId source) {
    auto it = Sources.find(source);
    Y_ENSURE(it != Sources.end(), "Unknown source " << source);
    if (const auto& progress = it->second) {
        ProgressIndex.erase(std::make_pair(progress->Version, source));
    }
    Sources.erase(it);
}

void TStream::AdvanceReadyBarrier(TStreamChanges& changes) {
    if (Sources.empty() || ProgressIndex.size() != Sources.size()) {
        return;
    }

    const auto& [version, source] = *ProgressIndex.begin();
    // A child may report a version below the published barrier; the barrier
    // stays where it is.
    if (ReadyBarrier && version <= ReadyBarrier->Version) {
        return;
    }

    ReadyBarrier = *Sources.at(source);
    changes.ReadyBarrier = true;
}

} // namespace NKikimr::NCdcAggregator
