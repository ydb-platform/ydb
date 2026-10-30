#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TNodeIdentifier> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, TStringStream *out,
        bool allowConfigQuorum = false);

    using TSuccessfulDisk = std::tuple<TNodeIdentifier, TString, std::optional<ui64>>;

    bool HasDiskQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, IOutputStream *out, const char *name);

    // Ensure configuration has quorum in both disk and storage ways for current and previous configuration.
    bool HasConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, bool mindPrev,
        TStringStream *out = nullptr);

} // NKikimr::NStorage
