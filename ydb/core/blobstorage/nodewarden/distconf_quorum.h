#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    // Generate set of mandatory pile ids for configuration quorum.
    THashSet<TBridgePileId> GetMandatoryPileIds(const NKikimrBlobStorage::TStorageConfig& config,
            const THashSet<TBridgePileId>& pileIdQuorumOverride);

    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TNodeIdentifier> successful,
        const THashMap<TString, TBridgePileId>& bridgePileNameMap, TBridgePileId singleBridgePileId,
        const TNodeWardenConfig& nwConfig, TStringStream *out, bool allowConfigQuorum = false);

    using TSuccessfulDisk = std::tuple<TNodeIdentifier, TString, std::optional<ui64>>;

    // Ensure configuration has quorum in both disk and storage ways for current and previous configuration.
    bool HasConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        const THashMap<TString, TBridgePileId>& bridgePileNameMap, TBridgePileId singleBridgePileId,
        const TNodeWardenConfig& nwConfig, bool mindPrev, TStringStream *out = nullptr);

} // NKikimr::NStorage
