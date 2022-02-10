#pragma once

#include "defs.h"

namespace NKikimr {

class TLogSignature {
    static const ui8 ChunkCommitMask = 0x80;

    ui8 Signature;

public:
    enum E : ui8 {
        First = 0,
        SignatureLogoBlob = 1,              // deprecated
        SignatureBlock = 2,
        SignatureGC = 3,
        SignatureSyncLogIdx = 4,
        SignatureHullLogoBlobsDB = 5,
        SignatureHullBlocksDB = 6,
        SignatureHullBarriersDB = 7,
        SignatureHullCutLog = 8,
        SignatureLocalSyncData = 9,
        SignatureSyncerState = 10,
        SignatureHandoffDelLogoBlob = 11,
        SignatureHugeBlobAllocChunk = 12,
        SignatureHugeBlobFreeChunk = 13,
        SignatureHugeBlobEntryPoint = 14,
        SignatureHugeLogoBlob = 15,
        SignatureLogoBlobOpt = 16,  // optimized LogoBlob record
        SignaturePhantomBlobs = 17,
        SignatureIncrHugeChunks = 18,
        SignatureIncrHugeDeletes = 19,
        SignatureAnubisOsirisPut = 20,
        SignatureAddBulkSst = 21,
        SignatureScrub = 22,
        Max = 23
    };

    TLogSignature(ui8 val = 0, bool hasCommit = false)
        : Signature(val)
    {
        if (hasCommit) {
            SetCommitRecord();
        }
    }

    operator ui8() const {
        return Signature;
    }

    void SetCommitRecord() {
        Signature |= ChunkCommitMask;
    }

    bool HasCommitRecord() const {
        return Signature & ChunkCommitMask;
    }

    ui32 GetUnmasked() const {
        return Signature & ~ChunkCommitMask;
    }

    TString ToString() const {
        switch(Signature) {
            case First:                                 return "First";
            case SignatureLogoBlob:                     return "LogoBlob";
            case SignatureBlock:                        return "Block";
            case SignatureGC:                           return "GC";
            case SignatureSyncLogIdx:                   return "SyncLogIdx";
            case SignatureHullLogoBlobsDB:              return "HullLogoBlobsDB";
            case SignatureHullBlocksDB:                 return "HullBlocksDB";
            case SignatureHullBarriersDB:               return "HullBarriersDB";
            case SignatureHullCutLog:                   return "HullCutLog";
            case SignatureLocalSyncData:                return "LocalSyncData";
            case SignatureSyncerState:                  return "SyncerState";
            case SignatureHandoffDelLogoBlob:           return "HandoffDelLogoBlob";
            case SignatureHugeBlobAllocChunk:           return "HugeBlobAllocChunk";
            case SignatureHugeBlobFreeChunk:            return "HugeBlobFreeChunk";
            case SignatureHugeBlobEntryPoint:           return "HugeBlobEntryPoint";
            case SignatureHugeLogoBlob:                 return "SignatureHugeLogoBlob";
            case SignatureLogoBlobOpt:                  return "SignatureLogoBlobOpt";
            case SignaturePhantomBlobs:                 return "PhantomBlobs";
            case SignatureIncrHugeChunks:               return "IncrHugeChunks";
            case SignatureIncrHugeDeletes:              return "IncrHugeDeletes";
            case SignatureAnubisOsirisPut:              return "SignatureAnubisOsirisPut";
            case SignatureAddBulkSst:                   return "SignatureAddBulkSst";
            case SignatureScrub:                        return "SignatureScrub";
            case Max:                                   return "Max";
        }
        return TStringBuilder() << "Unknown(" << static_cast<ui32>(Signature) << "(";
    }
};

static_assert(sizeof(TLogSignature) == 1, "for compatibility");

}
