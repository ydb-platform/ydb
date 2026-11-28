#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TNodeIdentifier> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, TStringStream *out,
        bool allowConfigQuorum = false);

    using TSuccessfulDisk = std::tuple<TNodeIdentifier, TString, std::optional<ui64>>;

    bool HasDiskQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, IOutputStream *out, const char *name);

    // Require disk and storage quorum for the proposal and storage quorum for the supplied previous layout.
    bool HasConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, const NKikimrBlobStorage::TStorageConfig *prev = nullptr,
        TStringStream *out = nullptr);

} // NKikimr::NStorage
