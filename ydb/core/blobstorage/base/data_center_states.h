#pragma once

#include <ydb/core/protos/blobstorage_base3.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>

#include <expected>
#include <unordered_map>

namespace NKikimr {

using TDataCenterStates = std::unordered_map<TString, NKikimrBlobStorage::TDataCenterState::EState>;

// Preserve explicitly supplied states. Unlisted datacenters are included.
std::expected<TDataCenterStates, TString> ParseDataCenterStates(
    const NKikimrBlobStorage::TDefineDataCenterStates& command);
NKikimrBlobStorage::TDefineDataCenterStates SerializeDataCenterStates(const TDataCenterStates& states);

} // namespace NKikimr
