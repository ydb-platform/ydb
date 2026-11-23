#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Size of one checksummed unit. Matches the DDisk integrity unit: checksums
// are one raw XXH3-64 per 4 KiB of payload, independent of the volume block
// size.
constexpr ui32 ChecksumUnitSize = 4_KB;

// One raw XXH3-64 per ChecksumUnitSize bytes of payload, in payload order.
// Empty means the checksums were not calculated. A non-empty value always
// has exactly payloadBytes / ChecksumUnitSize entries.
using TBlockChecksums = TVector<ui64>;

// Whether a producer calculates TBlockChecksums for a write.
enum class EChecksumMode
{
    // The producer leaves the checksum vector empty.
    Disabled,

    // The producer fills the checksum vector for the whole payload.
    Enabled,
};

// Checksums of sglist in the DDisk format: one XXH3-64 per ChecksumUnitSize
// bytes, in order. How the bytes are split across buffers does not change
// the result, and it matches NDDisk::CalculatePayloadChecksums over the same
// bytes.
//
// The total size must be a non-zero multiple of ChecksumUnitSize. Empty
// buffers are skipped. A buffer with a null data pointer and a non-zero size
// is rejected: that is a zero range (TBlockDataRef::CreateZeroBlock), and
// write sglists carry real bytes.
[[nodiscard]] TBlockChecksums CalculateBlockChecksums(const TSgList& sglist);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
