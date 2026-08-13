#pragma once

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

#include <optional>

namespace NKikimr {
namespace NPDisk {

// reads metadata from PDisk (if available); returns std::nullopt when the PDisk has no metadata
std::optional<NKikimrBlobStorage::TPDiskMetadataRecord> ReadPDiskMetadata(const TString& path, const TMainKey& mainKey);

// writes metadata record to PDisk (throws on error)
void WritePDiskMetadata(const TString& path, const NKikimrBlobStorage::TPDiskMetadataRecord& record, const TMainKey& mainKey);

} // NPDisk
} // NKikimr
