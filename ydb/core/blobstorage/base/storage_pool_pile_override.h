#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>

#include <optional>

namespace NKikimr {

using TStoragePoolPileOverrideRules = google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TStoragePoolPileOverride>;

// A missing kind selects static groups. An empty string is an ordinary (empty) pool kind.
const NKikimrBlobStorage::TStoragePoolPileOverride* FindStoragePoolPileOverride(
    const TStoragePoolPileOverrideRules& rules, TStringBuf pileName, std::optional<TStringBuf> kind);
const NKikimrBlobStorage::TStoragePoolPileOverride* FindStoragePoolPileOverride(
    const NKikimrConfig::TBlobStorageConfig& config, TStringBuf pileName, std::optional<TStringBuf> kind);

// Whether some rule has settings for the pile (for its static groups or for any kind of pools).
bool HasStoragePoolPileOverrides(const TStoragePoolPileOverrideRules& rules, TStringBuf pileName);
bool HasStoragePoolPileOverrides(const NKikimrConfig::TBlobStorageConfig& config, TStringBuf pileName);

std::optional<TString> ValidateStoragePoolPileOverrides(const NKikimrConfig::TBlobStorageConfig& config);

void ApplyStoragePoolPileOverride(NKikimrBlobStorage::TDefineStoragePool& pool,
    const NKikimrBlobStorage::TStoragePoolPileOverride& rule);

// Preserves other self-management settings. Replacing PDiskFilter also replaces its PDiskType shorthand.
void ApplyStoragePoolPileOverride(NKikimrConfig::TSelfManagementConfig& config,
    const NKikimrBlobStorage::TStoragePoolPileOverride& rule);

} // namespace NKikimr
