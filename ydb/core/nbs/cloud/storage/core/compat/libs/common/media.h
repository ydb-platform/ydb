#pragma once

#include <ydb/core/nbs/cloud/storage/core/compat/protos/media.pb.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

bool IsDiskRegistryMediaKind(NProto::EStorageMediaKind mediaKind);
bool IsBlobStorageMediaKind(NProto::EStorageMediaKind mediaKind);
bool IsReliableDiskRegistryMediaKind(NProto::EStorageMediaKind mediaKind);
bool IsDiskRegistryLocalMediaKind(NProto::EStorageMediaKind mediaKind);
bool IsNonReplicatedMediaKind(NProto::EStorageMediaKind mediaKind);
bool IsReliableMediaKind(NProto::EStorageMediaKind mediaKind);
TString MediaKindToString(NProto::EStorageMediaKind mediaKind);
TString MediaKindToStatsString(NProto::EStorageMediaKind mediaKind);
TString MediaKindToComputeType(NProto::EStorageMediaKind mediaKind);
bool ParseMediaKind(const TStringBuf s, NProto::EStorageMediaKind* mediaKind);

}   // namespace NCloud
