#include "sector.h"

#include <util/system/unaligned_mem.h>

namespace NKikimr::NPDiskTool {

bool CheckSectorHash(
    const TDiskFormat& format,
    ui64 offset,
    ui64 magic,
    const ui8* sector,
    const TLogoBlobID& blobId)
{
    const auto* footer = reinterpret_cast<const TDataSectorFooter*>(
        sector + format.SectorSize - sizeof(TDataSectorFooter));
    TPDiskHashCalculator hasher;
    return hasher.CheckSectorHash(offset, magic, sector, format.SectorSize, footer->Hash, blobId);
}

void DecryptInPlace(ui8* data, ui32 size, const TKey& key, ui64 nonce, bool enableEncryption) {
    TPDiskStreamCypher cypher(enableEncryption);
    cypher.SetKey(key);
    cypher.StartMessage(nonce);
    cypher.InplaceEncrypt(data, size);
}

bool CheckCanary(const TDiskFormat& format, const ui8* sector) {
    const ui64 canary = ReadUnaligned<ui64>(
        sector + format.SectorSize - NPDisk::CanarySize - sizeof(TDataSectorFooter));
    return canary == NPDisk::Canary;
}

static TRestoredSector FinishDecrypted(
    const TDiskFormat& format,
    ui8* rawSector,
    const TKey& key,
    TIssueLog& issues,
    const TString& location)
{
    TRestoredSector out;
    auto* footer = reinterpret_cast<TDataSectorFooter*>(
        rawSector + format.SectorSize - sizeof(TDataSectorFooter));
    out.Nonce = footer->Nonce;
    out.Encrypted = footer->IsEncrypted();
    out.DecryptedSector.assign(rawSector, rawSector + format.SectorSize);

    DecryptInPlace(out.DecryptedSector.data(), format.SectorSize - sizeof(TDataSectorFooter),
        key, footer->Nonce, footer->IsEncrypted());

    if (!CheckCanary(format, out.DecryptedSector.data())) {
        issues.Warning(location, TStringBuilder() << "Canary mismatch nonce# " << footer->Nonce, true);
    }

    const ui32 payloadSize = format.SectorPayloadSize();
    out.Payload.assign(out.DecryptedSector.begin(), out.DecryptedSector.begin() + payloadSize);
    out.Ok = true;
    return out;
}

TRestoredSector RestoreTripleCopy(
    IDeviceReader& device,
    const TDiskFormat& format,
    ui64 offset,
    ui64 magic,
    const TKey& key,
    TIssueLog& issues,
    const TString& location,
    ESectorRef ref)
{
    TVector<ui8> raw(format.SectorSize * NPDisk::ReplicationFactor);
    device.Pread(raw.data(), raw.size(), offset, issues);

    ui64 maxNonce = 0;
    ui32 lastGood = Max<ui32>();
    ui32 goodFlags = 0;
    ui32 goodCount = 0;

    for (ui32 i = 0; i < NPDisk::ReplicationFactor; ++i) {
        ui8* sector = raw.data() + i * format.SectorSize;
        // Triple-copy sectors share the first replica's offset for hashing.
        if (!CheckSectorHash(format, offset, magic, sector)) {
            continue;
        }
        auto* footer = reinterpret_cast<TDataSectorFooter*>(
            sector + format.SectorSize - sizeof(TDataSectorFooter));
        const ui64 nonce = footer->Nonce;
        if (nonce > maxNonce) {
            maxNonce = nonce;
            lastGood = i;
            goodFlags = (1u << i);
            goodCount = 1;
        } else if (nonce == maxNonce) {
            lastGood = i;
            goodFlags |= (1u << i);
            ++goodCount;
        }
    }

    TRestoredSector out;
    out.GoodCount = goodCount;
    out.GoodFlags = goodFlags;
    out.LastGoodIdx = lastGood;
    if (lastGood == Max<ui32>()) {
        if (ref == ESectorRef::Referenced) {
            issues.Warning(location, TStringBuilder() << "No good replica at offset# " << offset);
        }
        return out;
    }
    ui8* winner = raw.data() + lastGood * format.SectorSize;
    out = FinishDecrypted(format, winner, key, issues, location);
    out.GoodCount = goodCount;
    out.GoodFlags = goodFlags;
    out.LastGoodIdx = lastGood;
    return out;
}

TRestoredSector RestoreOneSector(
    IDeviceReader& device,
    const TDiskFormat& format,
    ui64 offset,
    ui64 magic,
    const TKey& key,
    bool decrypt,
    TIssueLog& issues,
    const TString& location,
    const TLogoBlobID& blobId,
    ESectorRef ref)
{
    TVector<ui8> raw(format.SectorSize);
    device.Pread(raw.data(), raw.size(), offset, issues);

    TRestoredSector out;
    if (!CheckSectorHash(format, offset, magic, raw.data(), blobId)) {
        if (ref == ESectorRef::Referenced) {
            issues.Warning(location, TStringBuilder() << "Bad sector hash offset# " << offset);
        }
        return out;
    }
    out.GoodCount = 1;
    out.GoodFlags = 1;
    out.LastGoodIdx = 0;
    if (!decrypt) {
        out.Ok = true;
        out.DecryptedSector = raw;
        auto* footer = reinterpret_cast<TDataSectorFooter*>(
            raw.data() + format.SectorSize - sizeof(TDataSectorFooter));
        out.Nonce = footer->Nonce;
        out.Encrypted = footer->IsEncrypted();
        out.Payload.assign(raw.begin(), raw.begin() + format.SectorPayloadSize());
        return out;
    }
    return FinishDecrypted(format, raw.data(), key, issues, location);
}

} // namespace NKikimr::NPDiskTool
