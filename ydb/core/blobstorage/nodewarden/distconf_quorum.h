#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    NKikimrBlobStorage::TStorageConfig MakeDistconfQuorumConfig(const NKikimrBlobStorage::TStorageConfig& config,
        const TNodeWardenConfig& nwConfig);

    bool HasDistconfExclusions(const NKikimrBlobStorage::TStorageConfig& config, const TNodeWardenConfig& nwConfig);
    bool HasDistconfExclusions(const NKikimrBlobStorage::TStorageConfig& config);

    bool IsForcedDistconfExclusionTransition(const NKikimrBlobStorage::TStorageConfig& previous,
        const NKikimrBlobStorage::TStorageConfig& proposed, const TNodeWardenConfig& nwConfig);

    // Generate set of mandatory pile ids for configuration quorum.
    THashSet<TBridgePileId> GetMandatoryPileIds(const NKikimrBlobStorage::TStorageConfig& config,
            const THashSet<TBridgePileId>& pileIdQuorumOverride);

    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TNodeIdentifier> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, TStringStream *out,
        bool allowConfigQuorum = false);

    using TSuccessfulDisk = std::tuple<TNodeIdentifier, TString, std::optional<ui64>>;

    bool HasDistconfMetadataQuorum(const NKikimrBlobStorage::TStorageConfig& config,
        std::span<TSuccessfulDisk> successful, TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig,
        IOutputStream *out = nullptr, const char *name = "config", bool mindGuids = false);

    bool HasOriginalConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, bool mindPrev,
        TStringStream *out = nullptr);

    bool HasDiskQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, IOutputStream *out, const char *name);

    bool HasConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, bool mindPrev,
        TStringStream *out = nullptr);

} // NKikimr::NStorage
