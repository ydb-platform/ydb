#pragma once

#include <ydb/library/actors/util/rope.h>

#include <util/generic/size_literals.h>

#include <vector>

namespace NKikimr::NDDisk {

// Checksum unit size (4 KiB), independent of LBA format. Matches MinSectorSize / DataAlignment.
constexpr size_t IntegrityUnitSize = 4096;
constexpr size_t IntegritySubBlockSize = 512;
constexpr ui32 IntegrityPairSlots = 2;

// DataChunk / IntegrityChunk are PDisk chunks whose size comes from the PDisk format at runtime.
// IntegrityChunk reserves a fixed 128 KiB region at the front for its own metadata
// (TIntegrityChunkHeader replicas, etc.).
constexpr size_t IntegrityChunkHeaderRegionSize = 128_KB;

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

// Raw XXH3-64 over a contiguous buffer. Unlike the TRope variant above there are no alignment or
// size restrictions; used for self-checksums of integrity metadata blocks (BlockChecksum /
// HeaderChecksum fields computed with the field itself zeroed).
ui64 CalculateRawChecksum(const void* data, size_t size);

// Splits payload into IntegrityUnitSize (4 KiB) blocks and computes a checksum for each block, in order.
// payload.size() must be a non-zero multiple of IntegrityUnitSize (same as MinSectorSize).
std::vector<ui64> CalculatePayloadChecksums(const TRope& payload);

// Incremental IntegrityBlockDigest helpers (RFC): Contribution is XXH3_64 over
// (vchunkGeneration || blockIdx || blockChecksum); UpdateRoot XORs out the old contribution and
// XORs in the new one.
ui64 Contribution(ui64 vchunkGeneration, ui64 blockIdx, ui64 blockChecksum);
void UpdateRoot(ui64& integrityBlockDigest, ui64 vchunkGeneration, ui64 idx, ui64 oldCsum, ui64 newCsum);

// On-disk checksums are salted with their logical identity. The wire protocol and the in-memory
// cache always carry pure XXH3_64(data) values; sealing/unsealing happens only at the integrity
// block boundary. XOR makes the operation reversible without recomputing data checksums.
ui64 CalculateChecksumIdentitySalt(ui64 ddiskId, ui64 pdiskGuid, ui64 tabletId,
    ui64 vChunkIndex, ui64 blockIdx);
ui64 SealBlockChecksum(ui64 pureChecksum, ui64 ddiskId, ui64 pdiskGuid, ui64 tabletId,
    ui64 vChunkIndex, ui64 blockIdx);
ui64 UnsealBlockChecksum(ui64 storedChecksum, ui64 ddiskId, ui64 pdiskGuid, ui64 tabletId,
    ui64 vChunkIndex, ui64 blockIdx);

// Pure checksum returned for a never-written data block.
ui64 GetZeroBlockChecksum();

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

    ui64 PairSequenceNumber; // per-pair monotonic sequence number (ping-pong slots): write max(seqA, seqB) + 1

    ui8 UsedBlocksBitmap[62]; // 496 bits: which of the covered data blocks are used (492 in base format)
    ui8 Reserved[10]; // pad so packed sizeof == 160
};

static_assert(sizeof(TIntegrityBlockHeader) == 160);

// Base format (AWUPF >= 4 KiB): how many data-block checksums fit after the header in one unit.
constexpr ui32 ChecksumsPerIntegrityBlock =
    (IntegrityUnitSize - sizeof(TIntegrityBlockHeader)) / sizeof(ui64);
static_assert(ChecksumsPerIntegrityBlock == 492);

// The bitmap must have a bit for every checksum slot of the block.
static_assert(sizeof(TIntegrityBlockHeader::UsedBlocksBitmap) * 8 >= ChecksumsPerIntegrityBlock);

struct TIntegrityBlock {
    TIntegrityBlockHeader Header;
    ui64 Checksums[ChecksumsPerIntegrityBlock];
};

static_assert(sizeof(TIntegrityBlock) == IntegrityUnitSize);

struct TIntegrityBlockIdentity {
    ui64 OwnerId = 0;
    ui64 VChunkId = 0;
    ui64 VChunkGeneration = 0;
    ui64 IntegrityChunkId = 0;
    ui64 IntegrityExtentId = 0;
    ui64 IntegrityChunkGeneration = 0;
    ui32 ChecksumBlockIdx = 0;
};

// Validates a single slot without trusting any of its fields first. The self-checksum is checked
// before identity/generation fields are used for winner selection.
bool ValidateIntegrityBlock(const TIntegrityBlock& block, const TIntegrityBlockIdentity& expected);

// Returns the winning slot index (0 for A, 1 for B), or -1 when neither slot is valid. A valid
// slot with the larger PairSequenceNumber wins; equal sequence numbers deterministically prefer B.
i32 SelectIntegrityBlockWinner(const TIntegrityBlock (&slots)[IntegrityPairSlots],
    const TIntegrityBlockIdentity& expected);

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
static_assert(ChecksumsInIntegrityBlockHeaderSubBlock == 44);

constexpr ui32 ChecksumsPerIntegrityBlockAwupf512 =
    ChecksumsInIntegrityBlockHeaderSubBlock
    + (IntegritySubBlocksPerBlock - 1) * ChecksumsPerIntegritySubBlock;
static_assert(ChecksumsPerIntegrityBlockAwupf512 == 464);

struct TIntegritySubBlock {
    TIntegritySubBlockHeader Header;
    ui64 Checksums[ChecksumsPerIntegritySubBlock];
};

static_assert(sizeof(TIntegritySubBlock) == IntegritySubBlockSize);

// On disk every TIntegrityBlock lives in a ping-pong pair of adjacent 4 KiB slots (A/B, read
// together as one 8 KiB I/O; never rewritten in place because we do not rely on AWUPF).

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

} // namespace NKikimr::NDDisk
