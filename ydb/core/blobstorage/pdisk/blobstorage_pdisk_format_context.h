#pragma once

#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_crypto.h"
#include "blobstorage_pdisk_data.h"

#include <ydb/core/base/logoblob.h>

#include <memory>

namespace NKikimr::NPDisk {

// What an owner needs to build or check data-chunk sectors itself, so that the PDisk threads do not
// copy, encrypt or hash the data. Immutable and shared: the chunk key exists once, here, has no
// getter and is wiped when the last reference goes away.
//
// The on-disk sector layout is exactly the one TChunkWriter produces: payload and canary encrypted
// with the stream cypher started at the sector nonce, then TDataSectorFooter{nonce, version, hash},
// the hash taken over the encrypted sector. The nonce of sector i of a chunk is the chunk nonce + i.
class TFormatContext {
public:
    TFormatContext(const TDiskFormat& format, bool encryptData);
    ~TFormatContext();

    TFormatContext(const TFormatContext&) = delete;
    TFormatContext& operator=(const TFormatContext&) = delete;

    ui32 GetSectorSize() const { return SectorSize; }
    ui32 GetSectorPayloadSize() const { return SectorPayloadSize; }
    ui32 GetChunkSize() const { return ChunkSize; }
    bool IsPlainDataChunks() const { return PlainDataChunks; }

    // Raw bytes needed to hold the given payload, whole sectors.
    ui64 RawSize(ui64 payloadSize) const {
        return (payloadSize + SectorPayloadSize - 1) / SectorPayloadSize * SectorSize;
    }

    // Seals the sectors of one write: one cypher per sealer, StartMessage per sector.
    class TSealer {
    public:
        TSealer(std::shared_ptr<const TFormatContext> ctx, TChunkIdx chunkIdx, ui64 chunkNonce, TLogoBlobID blobId);

        // Copies (or encrypts) `size` payload bytes into `sector`, pads the payload with zeroes, adds the
        // canary and the footer. `size` is at most the sector payload size.
        void Seal(ui8 *sector, const void *payload, ui32 size, ui32 sectorIdx);

        // The same for a payload that is already at the beginning of `sector`.
        void SealInPlace(ui8 *sector, ui32 size, ui32 sectorIdx);

    private:
        void Finish(ui8 *sector, ui32 size, ui32 sectorIdx);

        std::shared_ptr<const TFormatContext> Ctx;
        const TChunkIdx ChunkIdx;
        const ui64 ChunkNonce;
        const TLogoBlobID BlobId;
        TPDiskStreamCypher Cypher;
        TPDiskHashCalculator Hasher;
    };

    // Checks and opens the sectors of one read. The hash mode found for one sector is tried first for
    // the next one.
    class TOpener {
    public:
        TOpener(std::shared_ptr<const TFormatContext> ctx, TChunkIdx chunkIdx, ui64 chunkNonce, TLogoBlobID blobId);

        // Checks the nonce, the hash and the canary of `sector` and writes `size` payload bytes starting
        // at payload offset `from` to `dst`. Decrypts in place, so `sector` is clobbered. Returns false
        // for a sector that does not check out; `dst` is then left untouched.
        bool Open(ui8 *dst, ui8 *sector, ui32 sectorIdx, ui32 from, ui32 size);

    private:
        std::shared_ptr<const TFormatContext> Ctx;
        const TChunkIdx ChunkIdx;
        const ui64 ChunkNonce;
        const TLogoBlobID BlobId;
        TPDiskHashCalculator Hasher;
        // Keyed once per read; which one opens a sector is recorded in its footer.
        TPDiskStreamCypher PlainCypher;
        TPDiskStreamCypher EncryptedCypher;
    };

private:
    const ui32 SectorSize;
    const ui32 SectorPayloadSize;
    const ui32 ChunkSize;
    const ui64 DataMagic;
    const bool PlainDataChunks;
    const bool EncryptData;
    TKey ChunkKey;
};

using TFormatContextPtr = std::shared_ptr<const TFormatContext>;

// A copy of the disk format for an owner (TEvYardInitResult::DiskFormat): its keys are wiped when it is destroyed.
TDiskFormatPtr MakeDiskFormatCopy(const TDiskFormat& format);

} // NKikimr::NPDisk
