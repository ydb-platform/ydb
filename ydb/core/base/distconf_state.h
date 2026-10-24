#pragma once

#include <ydb/core/protos/config.pb.h>

#include <optional>

namespace NKikimr::NStorage {

using EDistconfState = NKikimrConfig::TStaticNameserviceConfig::TNode::EDistconfState;

inline EDistconfState ResolveDistconfNodeState(
        const NKikimrConfig::TStaticNameserviceConfig::TNode& node,
        const NKikimrConfig::TSelfManagementConfig& config,
        std::optional<EDistconfState> persistedHostState = std::nullopt,
        std::optional<EDistconfState> persistedDataCenterState = std::nullopt) {
    using TNode = NKikimrConfig::TStaticNameserviceConfig::TNode;

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

    const auto resolve = [&](std::optional<EDistconfState> configured, std::optional<EDistconfState> persisted) {
        if (config.GetOverrideDistconfState() && configured) {
            return *configured;
        }
        return persisted.value_or(configured.value_or(TNode::ACTIVE));
    };

    return resolve(configuredHostState, persistedHostState) == TNode::EXCLUDED
            || resolve(configuredDataCenterState, persistedDataCenterState) == TNode::EXCLUDED
        ? TNode::EXCLUDED : TNode::ACTIVE;
}

}
