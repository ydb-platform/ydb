#include "stream.h"

#include <util/generic/yexception.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

bool TStreamChanges::IsEmpty() const {
    return !Config
        && UpsertedSources.empty()
        && ErasedSources.empty()
        && UpsertedHeartbeats.empty()
        && ErasedHeartbeats.empty()
        && !CurrentHeartbeat;
}

TStream TStream::Restore(
        TTrackingConfig config,
        THashMap<ui64, std::optional<TRowVersion>> sources,
        const TMap<TRowVersion, TString>& heartbeats,
        std::optional<NPQ::THeartbeat> currentHeartbeat)
{
    TStream stream;
    stream.Config = std::move(config);
    stream.CurrentHeartbeat = std::move(currentHeartbeat);

    for (const auto& [source, version] : sources) {
        if (!version) {
            continue;
        }
        auto it = heartbeats.find(*version);
        Y_ENSURE(it != heartbeats.end(), "Source " << source << " has no heartbeat data for version " << *version);
        auto& held = stream.Heartbeats[*version];
        held.Data = it->second;
        ++held.SourceCount;
        ++stream.SourcesWithHeartbeat;
    }
    Y_ENSURE(stream.Heartbeats.size() == heartbeats.size(), "Heartbeat data is not held by any source");
    stream.Sources = std::move(sources);

    return stream;
}

const TTrackingConfig& TStream::GetConfig() const {
    Y_ENSURE(Config, "Tracking is not configured");
    return *Config;
}

EConfigureOutcome TStream::Configure(const TTrackingConfig& config, TStreamChanges& changes) {
    if (Config) {
        return *Config == config ? EConfigureOutcome::Success : EConfigureOutcome::Conflict;
    }

    Config = config;
    changes.Config = true;
    for (const auto source : config.SourceDataShardIds) {
        AddSource(source, changes);
    }

    return EConfigureOutcome::Success;
}

ESourceOutcome TStream::WriteHeartbeat(ui64 source, const NPQ::THeartbeat& heartbeat, TStreamChanges& changes) {
    Y_ENSURE(Config, "Tracking is not configured");

    auto it = Sources.find(source);
    if (it == Sources.end()) {
        return ESourceOutcome::UnknownSource;
    }

    auto& version = it->second;
    if (version && heartbeat.Version <= *version) {
        return ESourceOutcome::Success;
    }

    if (version) {
        ReleaseHeartbeat(*version, changes);
    } else {
        ++SourcesWithHeartbeat;
    }
    version = heartbeat.Version;
    HoldHeartbeat(heartbeat, changes);
    changes.UpsertedSources.insert(source);

    AdvanceCurrentHeartbeat(changes);
    return ESourceOutcome::Success;
}

ESourceOutcome TStream::ChangePartitioning(ui64 parent, const TVector<ui64>& children, TStreamChanges& changes) {
    Y_ENSURE(Config, "Tracking is not configured");

    if (!Sources.contains(parent)) {
        const bool applied = std::all_of(children.begin(), children.end(), [this](ui64 child) {
            return Sources.contains(child);
        });
        return applied ? ESourceOutcome::Success : ESourceOutcome::UnknownSource;
    }

    EraseSource(parent, changes);
    for (const auto child : children) {
        if (!Sources.contains(child)) {
            AddSource(child, changes);
        }
    }

    AdvanceCurrentHeartbeat(changes);
    return ESourceOutcome::Success;
}

void TStream::AddSource(ui64 source, TStreamChanges& changes) {
    const bool inserted = Sources.emplace(source, std::nullopt).second;
    Y_ENSURE(inserted, "Duplicate source " << source);
    changes.UpsertedSources.insert(source);
}

void TStream::EraseSource(ui64 source, TStreamChanges& changes) {
    auto it = Sources.find(source);
    Y_ENSURE(it != Sources.end(), "Unknown source " << source);
    if (const auto& version = it->second) {
        ReleaseHeartbeat(*version, changes);
        --SourcesWithHeartbeat;
    }
    Sources.erase(it);
    changes.ErasedSources.insert(source);
}

void TStream::HoldHeartbeat(const NPQ::THeartbeat& heartbeat, TStreamChanges& changes) {
    auto [it, inserted] = Heartbeats.try_emplace(heartbeat.Version);
    if (inserted) {
        it->second.Data = heartbeat.Data;
        changes.UpsertedHeartbeats[heartbeat.Version] = heartbeat.Data;
        changes.ErasedHeartbeats.erase(heartbeat.Version);
    }
    ++it->second.SourceCount;
}

void TStream::ReleaseHeartbeat(const TRowVersion& version, TStreamChanges& changes) {
    auto it = Heartbeats.find(version);
    Y_ENSURE(it != Heartbeats.end() && it->second.SourceCount > 0, "Heartbeat " << version << " is not held");
    if (--it->second.SourceCount == 0) {
        Heartbeats.erase(it);
        changes.UpsertedHeartbeats.erase(version);
        changes.ErasedHeartbeats.insert(version);
    }
}

void TStream::AdvanceCurrentHeartbeat(TStreamChanges& changes) {
    if (Sources.empty() || SourcesWithHeartbeat != Sources.size()) {
        return;
    }

    const auto& [version, held] = *Heartbeats.begin();
    if (CurrentHeartbeat && version <= CurrentHeartbeat->Version) {
        return;
    }

    CurrentHeartbeat = NPQ::THeartbeat{.Version = version, .Data = held.Data};
    changes.CurrentHeartbeat = true;
}

} // namespace NKikimr::NCdcAggregator
