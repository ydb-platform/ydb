#pragma once

#include "device.h"

namespace NKikimr::NPDiskTool {

struct TRestoredSector {
    bool Ok = false;
    ui32 GoodCount = 0;
    ui32 GoodFlags = 0;
    ui32 LastGoodIdx = Max<ui32>();
    ui64 Nonce = 0;
    bool Encrypted = true;
    TVector<ui8> Payload; // decrypted payload (without footer/canary)
    TVector<ui8> DecryptedSector; // full sector after decrypt of payload+canary, footer still original
};

// Whether anything on disk points at the sector being read.
//
// A chunk is committed as a whole, but only part of it is usually written: huge slots are allocated
// ahead of the data, and an SST leaves free space in its tail. Those sectors carry no valid hash, and
// that is the normal steady state -- not damage. Only report a hash mismatch when some index, log
// record or blob part actually references the sector, otherwise a healthy disk drowns the real errors.
enum class ESectorRef {
    Referenced,
    Unreferenced,
};

// Triple-copy restore (syslog / next-chunk-reference): pick the replica with the highest nonce among valid hashes.
// For triple copy, all replicas are hashed with the same `offset` (first replica offset).
TRestoredSector RestoreTripleCopy(
    IDeviceReader& device,
    const TDiskFormat& format,
    ui64 offset,
    ui64 magic,
    const TKey& key,
    TIssueLog& issues,
    const TString& location,
    ESectorRef ref = ESectorRef::Referenced);

// Single-sector restore (log / data chunk).
TRestoredSector RestoreOneSector(
    IDeviceReader& device,
    const TDiskFormat& format,
    ui64 offset,
    ui64 magic,
    const TKey& key,
    bool decrypt,
    TIssueLog& issues,
    const TString& location,
    const TLogoBlobID& blobId = {},
    ESectorRef ref = ESectorRef::Referenced);

bool CheckSectorHash(
    const TDiskFormat& format,
    ui64 offset,
    ui64 magic,
    const ui8* sector,
    const TLogoBlobID& blobId = {});

void DecryptInPlace(ui8* data, ui32 size, const TKey& key, ui64 nonce, bool enableEncryption);

bool CheckCanary(const TDiskFormat& format, const ui8* sector);

} // namespace NKikimr::NPDiskTool
