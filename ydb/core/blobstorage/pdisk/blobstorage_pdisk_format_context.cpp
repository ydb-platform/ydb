#include "blobstorage_pdisk_format_context.h"

#include <ydb/core/blobstorage/crypto/secured_block.h>

namespace NKikimr::NPDisk {

TFormatContext::TFormatContext(const TDiskFormat& format, bool encryptData)
    : SectorSize(format.SectorSize)
    , SectorPayloadSize(format.SectorPayloadSize())
    , ChunkSize(format.ChunkSize)
    , DataMagic(format.MagicDataChunk)
    , PlainDataChunks(format.IsPlainDataChunks())
    , EncryptData(encryptData)
    , ChunkKey(format.ChunkKey)
{}

TFormatContext::~TFormatContext() {
    SecureWipeBuffer((ui8*)&ChunkKey, sizeof(ChunkKey));
}

////////////////////////////////////////////////////////////////////////////
// TSealer
////////////////////////////////////////////////////////////////////////////

TFormatContext::TSealer::TSealer(std::shared_ptr<const TFormatContext> ctx, TChunkIdx chunkIdx, ui64 chunkNonce,
        TLogoBlobID blobId)
    : Ctx(std::move(ctx))
    , ChunkIdx(chunkIdx)
    , ChunkNonce(chunkNonce)
    , BlobId(blobId)
    , Cypher(Ctx->EncryptData)
{
    Y_ABORT_UNLESS(!Ctx->PlainDataChunks, "plain data chunks have no sector footers");
    Cypher.SetKey(Ctx->ChunkKey);
}

void TFormatContext::TSealer::Seal(ui8 *sector, const void *payload, ui32 size, ui32 sectorIdx) {
    Y_ABORT_UNLESS(size <= Ctx->SectorPayloadSize);
    Cypher.StartMessage(ChunkNonce + sectorIdx);
    Cypher.Encrypt(sector, payload, size);
    Finish(sector, size, sectorIdx);
}

void TFormatContext::TSealer::SealInPlace(ui8 *sector, ui32 size, ui32 sectorIdx) {
    Y_ABORT_UNLESS(size <= Ctx->SectorPayloadSize);
    Cypher.StartMessage(ChunkNonce + sectorIdx);
    Cypher.InplaceEncrypt(sector, size);
    Finish(sector, size, sectorIdx);
}

void TFormatContext::TSealer::Finish(ui8 *sector, ui32 size, ui32 sectorIdx) {
    // The keystream continues where the payload ended, as in TSectorWriter::WriteZeroes and FinalizeWrite.
    Cypher.EncryptZeroes(sector + size, Ctx->SectorPayloadSize - size);
    Cypher.Encrypt(sector + Ctx->SectorPayloadSize, &Canary, CanarySize);

    TDataSectorFooter &footer = *(TDataSectorFooter*)(sector + Ctx->SectorSize - sizeof(TDataSectorFooter));
    footer.SetVersionAndEncryption(Ctx->EncryptData);
    footer.Nonce = ChunkNonce + sectorIdx;
    const ui64 sectorOffset = (ui64)Ctx->ChunkSize * ChunkIdx + (ui64)Ctx->SectorSize * sectorIdx;
    footer.Hash = Hasher.HashSector(sectorOffset, Ctx->DataMagic, sector, Ctx->SectorSize, BlobId);
}

////////////////////////////////////////////////////////////////////////////
// TOpener
////////////////////////////////////////////////////////////////////////////

TFormatContext::TOpener::TOpener(std::shared_ptr<const TFormatContext> ctx, TChunkIdx chunkIdx, ui64 chunkNonce,
        TLogoBlobID blobId)
    : Ctx(std::move(ctx))
    , ChunkIdx(chunkIdx)
    , ChunkNonce(chunkNonce)
    , BlobId(blobId)
{
    Y_ABORT_UNLESS(!Ctx->PlainDataChunks, "plain data chunks have no sector footers");
}

bool TFormatContext::TOpener::Open(ui8 *dst, ui8 *sector, ui32 sectorIdx, ui32 from, ui32 size) {
    Y_ABORT_UNLESS(from + size <= Ctx->SectorPayloadSize);
    const TDataSectorFooter &footer = *(const TDataSectorFooter*)(sector + Ctx->SectorSize - sizeof(TDataSectorFooter));
    if (footer.Nonce != ChunkNonce + sectorIdx) {
        return false;
    }
    const ui64 sectorOffset = (ui64)Ctx->ChunkSize * ChunkIdx + (ui64)Ctx->SectorSize * sectorIdx;
    if (!Hasher.CheckSectorHash(sectorOffset, Ctx->DataMagic, sector, Ctx->SectorSize, footer.Hash, BlobId)) {
        return false;
    }
    // Whether this sector is encrypted is recorded in its own footer, not taken from the configuration.
    TPDiskStreamCypher cypher(footer.IsEncrypted());
    cypher.SetKey(Ctx->ChunkKey);
    cypher.StartMessage(footer.Nonce);
    cypher.InplaceEncrypt(sector, Ctx->SectorPayloadSize + CanarySize);
    if (CanarySize && memcmp(sector + Ctx->SectorPayloadSize, &Canary, CanarySize) != 0) {
        return false;
    }
    memcpy(dst, sector + from, size);
    return true;
}

} // NKikimr::NPDisk
