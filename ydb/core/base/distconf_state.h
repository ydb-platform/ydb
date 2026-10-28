#pragma once

#include <ydb/core/protos/config.pb.h>

#include <optional>

namespace NKikimr::NStorage {

using EDistconfState = NKikimrBlobStorage::TDistconfState::E;

inline EDistconfState ResolveDistconfNodeState(
        std::optional<EDistconfState> configuredHostState,
        std::optional<EDistconfState> configuredDataCenterState,
        std::optional<EDistconfState> persistedHostState,
        std::optional<EDistconfState> persistedDataCenterState,
        bool overrideDistconfState) {
    using TState = NKikimrBlobStorage::TDistconfState;

    const auto resolve = [&](std::optional<EDistconfState> configured, std::optional<EDistconfState> persisted) {
        if (overrideDistconfState && configured) {
            return *configured;
        }
        return persisted.value_or(configured.value_or(TState::ACTIVE));
    };

    return resolve(configuredHostState, persistedHostState) == TState::EXCLUDED
            || resolve(configuredDataCenterState, persistedDataCenterState) == TState::EXCLUDED
        ? TState::EXCLUDED : TState::ACTIVE;
}

inline EDistconfState ResolveDistconfNodeState(
        const NKikimrConfig::TStaticNameserviceConfig::TNode& node,
        const NKikimrConfig::TSelfManagementConfig& config,
        std::optional<EDistconfState> persistedHostState = std::nullopt,
        std::optional<EDistconfState> persistedDataCenterState = std::nullopt) {
    std::optional<EDistconfState> configuredHostState;
    if (node.HasDistconfState()) {
        configuredHostState = node.GetDistconfState();
    }

    const auto& location = node.HasLocation() ? node.GetLocation() : node.GetWalleLocation();
    std::optional<EDistconfState> configuredDataCenterState;
    for (const auto& item : config.GetDataCenterStates()) {
        if (item.GetName() == location.GetDataCenter()) {
            if (item.HasState()) {
                configuredDataCenterState = item.GetState();
            }
            break;
        }
    }

    return ResolveDistconfNodeState(configuredHostState, configuredDataCenterState, persistedHostState,
        persistedDataCenterState, config.GetOverrideDistconfState());
}

}
