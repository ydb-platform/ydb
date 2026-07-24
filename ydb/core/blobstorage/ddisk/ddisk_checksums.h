#pragma once

#include "defs.h"

#include <ydb/library/actors/util/rope.h>

#include <vector>

namespace NKikimr::NDDisk {

// Checksum unit size (4 KiB), independent of LBA format. Matches MinSectorSize / DataAlignment.
constexpr size_t IntegrityUnitSize = 4096;
constexpr size_t IntegritySubBlockSize = 512;

// DataChunk / IntegrityChunk are PDisk chunks (default 128 MiB). IntegrityChunk reserves
// 128 KiB at the front for its own metadata (TIntegrityChunkHeader replicas, etc.).
constexpr size_t IntegrityChunkSize = 128_MB;
constexpr size_t IntegrityChunkHeaderRegionSize = 128_KB;
constexpr ui32 DataBlocksPerChunk = IntegrityChunkSize / IntegrityUnitSize;

// Placeholder magics (PDisk-style unique values); not yet finalized for production format.
constexpr ui64 MagicIntegrityBlock = 0x1B71E6A7B10C4E55ull;
constexpr ui64 MagicIntegritySubBlock = 0x51B71E6A75B10C4Eull;
constexpr ui64 MagicIntegrityChunkHeader = 0xC8A7C8A71B71E6A7ull;

enum class EIntegrityFormatVersion : ui16 {
    BaseAwupf4KiB = 1,
    Awupf512B = 2,
};

// Computes an XXH3-64 checksum over exactly numBytes bytes starting at it. The iterator is passed by
// value, so the caller's own iterator is not advanced. This is a raw data checksum with no identity
// or salt mixed in.
ui64 CalculateBlockChecksum(TRope::TConstIterator it, size_t numBytes);

// Splits payload into IntegrityUnitSize (4 KiB) blocks and computes a checksum for each block, in order.
// payload.size() must be a non-zero multiple of IntegrityUnitSize (same as MinSectorSize).
std::vector<ui64> CalculatePayloadChecksums(const TRope& payload);

// Incremental IntegrityBlockDigest helpers (RFC): Contribution is XXH3_64 over
// (vchunkGeneration || blockIdx || blockChecksum); UpdateRoot XORs out the old contribution and
// XORs in the new one.
ui64 Contribution(ui64 vchunkGeneration, ui64 blockIdx, ui64 blockChecksum);
void UpdateRoot(ui64& integrityBlockDigest, ui64 vchunkGeneration, ui64 idx, ui64 oldCsum, ui64 newCsum);

#pragma pack(push, 1)

struct TIntegrityBlockHeader {
    ui64 Magic;
    ui64 BlockChecksum; // checksum over the whole 4 KiB block with this field zeroed

    ui16 Flags;
    ui16 FormatVersion;

    ui32 ChecksumBlockIdx; // index inside IntegrityExtent

    ui64 OwnerId; // tablet/user disk owner
    ui64 VChunkId;
    ui64 VChunkGeneration; // changes on allocate/free/allocate

    ui64 IntegrityChunkId;
    ui64 IntegrityExtentId; // optional, or service chunk + extent slot
    ui64 IntegrityChunkGeneration; // changes when this chunk is reinitialized/reused

    ui64 IntegrityBlockDigest; // Incremental checksum of data block checksums

    ui8 UsedBlocksBitmap[2]; // which of 500 blocks are used
    ui8 Reserved[14]; // pad so packed sizeof == 96
};

static_assert(sizeof(TIntegrityBlockHeader) == 96);

// Base format (AWUPF >= 4 KiB): how many data-block checksums fit after the header in one unit.
constexpr ui32 ChecksumsPerIntegrityBlock =
    (IntegrityUnitSize - sizeof(TIntegrityBlockHeader)) / sizeof(ui64);
static_assert(ChecksumsPerIntegrityBlock == 500);

struct TIntegrityBlock {
    TIntegrityBlockHeader Header;
    ui64 Checksums[ChecksumsPerIntegrityBlock];
};

static_assert(sizeof(TIntegrityBlock) == IntegrityUnitSize);

struct TIntegritySubBlockHeader {
    ui64 Magic;
    ui64 SubBlockChecksum; // XXH3_64 over this 512 B sub-block with this field zeroed
    ui32 ChecksumBlockIdx; // parent TIntegrityBlock index inside IntegrityExtent
    ui16 SubBlockIdx; // 1..7
    ui16 FormatVersion;
    ui64 VChunkGeneration;
};

static_assert(sizeof(TIntegritySubBlockHeader) == 32);

// AWUPF = 512 B: TIntegrityBlock is still one IntegrityUnitSize I/O, split into self-contained
// sub-blocks. Sub-block 0 carries TIntegrityBlockHeader; sub-blocks 1..N-1 carry mini-headers.
constexpr ui32 IntegritySubBlocksPerBlock = IntegrityUnitSize / IntegritySubBlockSize;
static_assert(IntegritySubBlocksPerBlock == 8);

constexpr ui32 ChecksumsPerIntegritySubBlock =
    (IntegritySubBlockSize - sizeof(TIntegritySubBlockHeader)) / sizeof(ui64);
static_assert(ChecksumsPerIntegritySubBlock == 60);

constexpr ui32 ChecksumsInIntegrityBlockHeaderSubBlock =
    (IntegritySubBlockSize - sizeof(TIntegrityBlockHeader)) / sizeof(ui64);
static_assert(ChecksumsInIntegrityBlockHeaderSubBlock == 52);

constexpr ui32 ChecksumsPerIntegrityBlockAwupf512 =
    ChecksumsInIntegrityBlockHeaderSubBlock
    + (IntegritySubBlocksPerBlock - 1) * ChecksumsPerIntegritySubBlock;
static_assert(ChecksumsPerIntegrityBlockAwupf512 == 472);

struct TIntegritySubBlock {
    TIntegritySubBlockHeader Header;
    ui64 Checksums[ChecksumsPerIntegritySubBlock];
};

static_assert(sizeof(TIntegritySubBlock) == IntegritySubBlockSize);

// IntegrityExtent covers one DataChunk: ceil(data blocks / checksums per integrity block).
constexpr ui32 IntegrityBlocksPerExtent =
    (DataBlocksPerChunk + ChecksumsPerIntegrityBlock - 1) / ChecksumsPerIntegrityBlock;
static_assert(IntegrityBlocksPerExtent == 66);

constexpr ui32 IntegrityBlocksPerExtentAwupf512 =
    (DataBlocksPerChunk + ChecksumsPerIntegrityBlockAwupf512 - 1) / ChecksumsPerIntegrityBlockAwupf512;
static_assert(IntegrityBlocksPerExtentAwupf512 == 70);

constexpr size_t IntegrityExtentSize = IntegrityBlocksPerExtent * IntegrityUnitSize;
constexpr size_t IntegrityExtentSizeAwupf512 = IntegrityBlocksPerExtentAwupf512 * IntegrityUnitSize;

constexpr ui32 IntegrityExtentsPerChunk =
    (IntegrityChunkSize - IntegrityChunkHeaderRegionSize) / IntegrityExtentSize;
static_assert(IntegrityExtentsPerChunk == 496);

constexpr ui32 IntegrityExtentsPerChunkAwupf512 =
    (IntegrityChunkSize - IntegrityChunkHeaderRegionSize) / IntegrityExtentSizeAwupf512;
static_assert(IntegrityExtentsPerChunkAwupf512 == 467);

struct TIntegrityChunkHeader {
    ui64 Magic;
    ui64 HeaderChecksum; // XXH3_64 over this 4 KiB block with HeaderChecksum = 0
    ui32 FormatVersion;
    ui32 HeaderSize;

    ui64 DDiskId;
    ui64 PDiskGuid;
    ui64 IntegrityChunkId; // PDisk chunk id
    ui64 IntegrityChunkGeneration; // changes when this chunk is reinitialized/reused

    ui64 Flags;

    ui8 Reserved[IntegrityUnitSize - (
        sizeof(ui64) * 7 + // Magic, HeaderChecksum, DDiskId, PDiskGuid, IntegrityChunkId, IntegrityChunkGeneration, Flags
        sizeof(ui32) * 2   // FormatVersion, HeaderSize
    )];
};

static_assert(sizeof(TIntegrityChunkHeader) == IntegrityUnitSize);

#pragma pack(pop)

static_assert(DataBlocksPerChunk == 32768);

} // namespace NKikimr::NDDisk
