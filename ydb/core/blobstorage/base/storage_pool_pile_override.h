#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>

#include <optional>

namespace NKikimr {

// A missing kind selects static groups. An empty string is an ordinary (empty) pool kind.
const NKikimrBlobStorage::TStoragePoolPileOverride* FindStoragePoolPileOverride(
    const NKikimrConfig::TBlobStorageConfig& config, TStringBuf pileName, std::optional<TStringBuf> kind);

std::optional<TString> ValidateStoragePoolPileOverrides(const NKikimrConfig::TBlobStorageConfig& config);

void ApplyStoragePoolPileOverride(NKikimrBlobStorage::TDefineStoragePool& pool,
    const NKikimrBlobStorage::TStoragePoolPileOverride& rule);

// Preserves other self-management settings. Replacing PDiskFilter also replaces its PDiskType shorthand.
void ApplyStoragePoolPileOverride(NKikimrConfig::TSelfManagementConfig& config,
    const NKikimrBlobStorage::TStoragePoolPileOverride& rule);

} // namespace NKikimr
