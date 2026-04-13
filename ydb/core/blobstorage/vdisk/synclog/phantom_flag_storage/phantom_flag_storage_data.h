#pragma once

#include "phantom_flag_thresholds.h"

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>

#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr::NSyncLog {

using TPhantomFlagStorageDataProto = NKikimrVDiskData::TPhantomFlagStorageData;

enum EPhantomFlagStorageItem : ui8 {
    SkipOneByte = 0b00,
    Flag        = 0b01,
    Threshold   = 0b10,
    Skip        = 0b11,
    // All types other than Flag and Threshold must contain ui32 field
    // equal to the total serialized size of structure
    // (including Type and Size) fields
    // in the beginning of byte serialization, for compatibility reasons:
    // when attempting to read unsupported field it fill be interpreted
    // as Skip{ Size } and skipped
    // SkipOneByte records are used to skip alignment blocks 
};

class TPhantomFlagStorageItem {
public:
    struct TSkip {
        ui32 Size;
    };

    struct TFlag {
        TLogoBlobRec Record;
    };

    using TThreshold = TPhantomFlagThresholds::TThreshold;

public:
    static TPhantomFlagStorageItem CreateSkip(ui32 skipSize);
    static TPhantomFlagStorageItem CreateFlag(const TLogoBlobRec* blobRec);
    static TPhantomFlagStorageItem CreateThreshold(ui32 orderNumber, ui64 tabletId, ui8 channel,
            ui32 generation, ui32 step);
    static TPhantomFlagStorageItem CreateThreshold(ui32 orderNumber, const TLogoBlobID& blobId);

    static TPhantomFlagStorageItem DeserializeFromRaw(const char* data);
    void Serialize(TString* buffer) const;
    ui32 SerializedSize() const;
    static void AlignWriteBlock(TString* buffer, ui32 appendBlockSize, ui32 sizeLimit); 

    EPhantomFlagStorageItem GetType() const;

    TSkip GetSkip() const;
    TThreshold GetThreshold() const;
    TFlag GetFlag() const;

private:
    std::variant<TSkip, TFlag, TThreshold> Data;
};

struct TPhantomFlagStorageData {
    struct TChunk {
        ui32 DataSize;
    };

    void Deserialize(const TPhantomFlagStorageDataProto& proto);
    void Serialize(TPhantomFlagStorageDataProto* proto) const;

    std::unordered_map<ui32, TChunk> Chunks;
    ui32 ChunkSize;
};

}  // namespace NKikimr::NSyncLog
