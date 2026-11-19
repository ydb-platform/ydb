#pragma once

#include <ydb/core/protos/blobstorage_base.pb.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>

#include <expected>
#include <unordered_map>

namespace NKikimr {

using TDataCenterStates = std::unordered_map<TString, NKikimrBlobStorage::TDataCenterState::EState>;

// Preserve explicitly supplied states. Unlisted datacenters are included.
std::expected<TDataCenterStates, TString> ParseDataCenterStates(
    const google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TDataCenterStates::TDataCenter>& dataCenters);

inline std::expected<TDataCenterStates, TString> ParseDataCenterStates(const NKikimrBlobStorage::TDataCenterStates& states) {
    return ParseDataCenterStates(states.GetDataCenter());
}

NKikimrBlobStorage::TDataCenterStates SerializeDataCenterStates(const TDataCenterStates& states);

} // namespace NKikimr
