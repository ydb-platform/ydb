#pragma once

#include <ydb/core/protos/blobstorage_base.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>

#include <array>
#include <expected>
#include <unordered_map>

namespace NKikimr {

using TDataCenterStates = std::unordered_map<TString,
    NKikimrBlobStorage::TDefineDataCenterStates::TDataCenter::EDataCenterState>;
using TFailRealmStates = std::array<NKikimrBlobStorage::TGroupInfo::TFailRealm::EState, 3>;

// Preserve explicitly supplied states. Unlisted datacenters are included.
std::expected<TDataCenterStates, TString> ParseDataCenterStates(
    const NKikimrBlobStorage::TDefineDataCenterStates& command);
NKikimrBlobStorage::TDefineDataCenterStates SerializeDataCenterStates(const TDataCenterStates& states);

// RFC 190, Table 1. Validates all realm changes together, preserving physical realm identities.
std::expected<void, TString> ValidateFailRealmStateTransition(const TFailRealmStates& before, const TFailRealmStates& after);

} // namespace NKikimr
