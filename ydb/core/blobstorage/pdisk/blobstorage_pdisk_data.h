#pragma once
#include "defs.h"
#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_crypto.h"
#include "blobstorage_pdisk_defs.h"
#include "blobstorage_pdisk_state.h"

#include <ydb/core/util/text.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////
// PDisk On-disk structures
////////////////////////////////////////////////////////////////////////////

static_assert(sizeof(TOwner) == 1, "TOwner size mismatch.");
static_assert(sizeof(TLogSignature) == 1, "TSignature size mismatch.");

const ui64 MagicNextLogChunkReferenceId = 0x709DA7A709DA7A11;
const ui64 MagicLogChunkId = 0x11170915A71FE111;
const ui64 MagicDataChunkId = 0xDA7AC8A2CDA7AC8A;
const ui64 MagicSysLogChunkId = 0x5957095957095957;
const ui64 MagicFormatChunkId = 0xF088A7F088A7F088;
constexpr ui64 MagicIncompleteFormat = 0x5b48add808b31984;
constexpr ui64 MagicIncompleteFormatSize = 512; // Bytes
constexpr ui64 MagicMetadataFormatSector = 0xb5bf641dbca863d2;

const ui64 Canary = 0x0123456789abcdef;
constexpr ui32 CanarySize = 8;

constexpr ui32 LogErasureDataParts = 4;

constexpr ui32 FormatSectorSize = 32 * (1 << 10); // 32 KiB
constexpr ui32 DefaultSectorSize = 4 * (1 << 10); // 4 KiB
constexpr ui32 ReplicationFactor = 3;
constexpr ui32 RecordsInSysLog = 16;

constexpr ui64 FullSizeDiskMinimumSize = 800ull * (1 << 30); // 800GB, all disks smaller are considered "small"
constexpr ui32 SmallDiskMaximumChunkSize = 32 * (1 << 20); // 32MB

#define PDISK_FORMAT_VERSION 3
#define PDISK_DATA_VERSION 2
#define PDISK_DATA_VERSION_2 3
#define PDISK_DATA_VERSION_3 4
#define PDISK_SYS_LOG_RECORD_VERSION_2 2
#define PDISK_SYS_LOG_RECORD_VERSION_3 3
#define PDISK_SYS_LOG_RECORD_VERSION_4 4
// #define PDISK_SYS_LOG_RECORD_VERSION_5 5 // It was used in reverted commits, just avoid this version
#define PDISK_SYS_LOG_RECORD_VERSION_6 6
#define PDISK_SYS_LOG_RECORD_VERSION_7 7
#define PDISK_SYS_LOG_RECORD_INCOMPATIBLE_VERSION_1000 1000
#define FORMAT_TEXT_SIZE 1024

#define NONCE_JUMP_DLOG_CHUNKS 16
#define NONCE_JUMP_DLOG_RECORDS 4

#pragma pack(push, 1)
struct TDataSectorFooter {
    ui64 Nonce;
    ui64 Version;
    THash Hash;

    TDataSectorFooter()
        : Nonce(0)
        , Version(PDISK_DATA_VERSION)
        , Hash(0)
    {}
};

struct TParitySectorFooter {
    ui64 Nonce;
    THash Hash;

    TParitySectorFooter()
        : Nonce(0)
        , Hash(0)
    {}
};

enum ELogPageFlags {
    LogPageFirst = 1,
    LogPageLast = 1 << 1,
    LogPageTerminator = 1 << 2,
    LogPageNonceJump1 = 1 << 3,
    LogPageNonceJump2 = 1 << 4
};

struct TLogPageHeader {
    ui8 Version;
    ui8 Flags;
    ui8 A;
    ui8 B;
    ui32 Size;

    TLogPageHeader(ui8 flags, ui32 size)
        : Version(PDISK_DATA_VERSION)
        , Flags(flags)
        , A('A')
        , B('B')
        , Size(size)
    {}

    TString ToString() const {
        TStringStream str;
        str << "{Version# " << (ui32)Version
            << " Flags# ";
        PrintFlags(Flags, str);
        str << " Size# " << Size;
        str << "}";
        return str.Str();
    }

    static void PrintFlags(ui8 flags, TStringStream &inOutStr) {
        bool isFirst = true;
        isFirst = NText::OutFlag(isFirst, flags == 0, "None", inOutStr);
        isFirst = NText::OutFlag(isFirst, flags & LogPageFirst, "LogPageFirst", inOutStr);
        isFirst = NText::OutFlag(isFirst, flags & LogPageLast, "LogPageLast", inOutStr);
        isFirst = NText::OutFlag(isFirst, flags & LogPageTerminator, "LogPageTerminator", inOutStr);
        isFirst = NText::OutFlag(isFirst, flags & LogPageNonceJump1, "LogPageNonceJump1", inOutStr);
        isFirst = NText::OutFlag(isFirst, flags & LogPageNonceJump2, "LogPageNonceJump2", inOutStr);
        NText::OutFlag(isFirst, isFirst, "Unknown", inOutStr);
    }
};

struct TLogRecordHeader {
    ui8 Version;
    TOwner OwnerId;
    ui8 A;
    ui8 B;
    TLogSignature Signature;
    ui64 OwnerLsn;

    TLogRecordHeader(TOwner ownerId, TLogSignature signature, ui64 ownerLsn)
        : Version(PDISK_DATA_VERSION)
        , OwnerId(ownerId)
        , A('A')
        , B('B')
        , Signature(signature)
        , OwnerLsn(ownerLsn)
    {}
};

struct TFirstLogPageHeader {
    ui8 Version;
    ui8 Flags;
    ui8 A;
    ui8 B;
    ui32 Size;

    ui64 DataSize;
    TLogRecordHeader LogRecordHeader;

    TFirstLogPageHeader(ui8 flags, ui32 size, ui64 dataSize, TOwner ownerId, TLogSignature signature, ui64 ownerLsn)
        : Version(PDISK_DATA_VERSION)
        , Flags(flags)
        , A('A')
        , B('B')
        , Size(size)
        , DataSize(dataSize)
        , LogRecordHeader(ownerId, signature, ownerLsn)
    {}
};

struct TNonceJumpLogPageHeader1 {
    ui8 Version;
    ui8 Flags;
    ui8 A;
    ui8 B;
    ui64 PreviousNonce;

    TNonceJumpLogPageHeader1(ui8 flags, ui64 previousNonce)
        : Version(PDISK_DATA_VERSION)
        , Flags(flags)
        , A('A')
        , B('B')
        , PreviousNonce(previousNonce)
    {}
};

struct TNonceJumpLogPageHeader2 {
    ui8 Version;
    ui8 Flags;
    ui8 A;
    ui8 B;
    ui64 PreviousNonce;

    // For debug only
    ui32 PreviousLogTails[NONCE_JUMP_DLOG_RECORDS][NONCE_JUMP_DLOG_CHUNKS];
    ui32 PreviousNonces[NONCE_JUMP_DLOG_RECORDS];
    ui64 PreviousInstants[NONCE_JUMP_DLOG_RECORDS];

    TNonceJumpLogPageHeader2()
        : Version(PDISK_DATA_VERSION)
        , Flags(0)
        , A('A')
        , B('B')
        , PreviousNonce(0)
    {
        TInstant now = TInstant::Now();
        for (ui32 r = 0; r < NONCE_JUMP_DLOG_RECORDS; ++r) {
            for (ui32 c = 0; c < NONCE_JUMP_DLOG_CHUNKS; ++c) {
                PreviousLogTails[r][c] = 0;
            }
            PreviousNonces[r] = 0;
            PreviousInstants[r] = now.GetValue();
        }
    }

    TNonceJumpLogPageHeader2(ui8 flags, ui64 previousNonce, TNonceJumpLogPageHeader2 &prevHeader,
            TList<TLogChunkInfo> &logChunkList)
        : Version(PDISK_DATA_VERSION)
        , Flags(flags)
        , A('A')
        , B('B')
        , PreviousNonce(previousNonce)
    {
        // Init record {0}
        {
            auto it = logChunkList.rbegin();
            i32 c = 0;
            while (c < NONCE_JUMP_DLOG_CHUNKS && it != logChunkList.rend()) {
                PreviousLogTails[0][c] = it->ChunkIdx;
                ++c;
                ++it;
            }
            while (c < NONCE_JUMP_DLOG_CHUNKS) {
                PreviousLogTails[0][c] = 0;
                ++c;
            }
            PreviousNonces[0] = prevHeader.PreviousNonce;
            PreviousInstants[0] = TInstant::Now().GetValue();
        }
        // Init records [1..NONCE_JUMP_DLOG_RECORDS)
        for (ui32 r = 1; r < NONCE_JUMP_DLOG_RECORDS; ++r) {
            for (ui32 c = 0; c < NONCE_JUMP_DLOG_CHUNKS; ++c) {
                PreviousLogTails[r][c] = prevHeader.PreviousLogTails[r - 1][c];
            }
            PreviousNonces[r] = prevHeader.PreviousNonces[r - 1];
            PreviousInstants[r] = prevHeader.PreviousInstants[r - 1];
        }
    }

    TString ToString(bool isMultiline) {
        const char *x = isMultiline ? "\n" : "";
        TStringStream str;
        str << "{TNonceJumpLogPageHeader2 Version# " << (ui32)Version << x;
        str << " Flags# ";
        TLogPageHeader::PrintFlags(Flags, str);
        str << x << " A# " << (ui32)A << x;
        str << " B# " << (ui32)B << x;
        ui64 previousNonce = PreviousNonce;
        str << " PreviousNonce# " << previousNonce << x;
        str << " PreviousLogTails# " << x;
        for (ui32 r = 0; r < NONCE_JUMP_DLOG_RECORDS; ++r) {
            str << "r" << r;
            str << " pn" << PreviousNonces[r];
            str << " pi" << TInstant::MicroSeconds(PreviousInstants[r]).ToString();
            str << " {c";
            for (ui32 c = 0; c < NONCE_JUMP_DLOG_CHUNKS; ++c) {
                if (c) {
                    str << " c";
                }
                str << PreviousLogTails[r][c];
            }
            str << "}" << x;
        }
        str << "}" << x;
        return str.Str();
    }
};

static_assert(sizeof(TNonceJumpLogPageHeader2) + sizeof(TDataSectorFooter) <= 512, "Data structures may not fit");

struct TCommitRecordFooter {
    ui64 UserDataSize;
    ui64 FirstLsnToKeep;
    ui32 CommitCount;
    ui32 DeleteCount;
    bool IsStartingPoint;

    TCommitRecordFooter(ui64 userDataSize, ui64 firstLsnToKeep, ui32 commitCount, ui32 deleteCount,
            bool isStartingPoint)
        : UserDataSize(userDataSize)
        , FirstLsnToKeep(firstLsnToKeep)
        , CommitCount(commitCount)
        , DeleteCount(deleteCount)
        , IsStartingPoint(isStartingPoint)
    {}
};

enum ENonce {
    NonceSysLog = 0,
    NonceLog,
    NonceData,
    NonceCount
};

#pragma pack(push, 8)
struct TNonceSet {
    ui64 Version;
    ui64 Value[NonceCount];

    TNonceSet()
        : Version(PDISK_DATA_VERSION)
    {}

    TString ToString() const {
        return ToString(false);
    }

    TString ToString(bool isMultiline) const {
        TStringStream str;
        const char *x = isMultiline ? "\n" : "";
        str << "{TNonceSet" << x;
        str << " Version# " << Version << x;
        str << " NonceSysLog# " << Value[NonceSysLog] << x;
        str << " NonceLog# " << Value[NonceLog] << x;
        str << " NonceData# " << Value[NonceData] << x;
        static_assert(NonceData + 1 == NonceCount, "Looks like you forgot to output some nonces here!");
        str << "}";
        return str.Str();
    }
};

struct TSysLogRecord {
    // TODO: use atomics here
    ui64 Version;
    TNonceSet Nonces;
    TChunkIdx LogHeadChunkIdx;
    ui32 Reserved1;
    ui64 LogHeadChunkPreviousNonce;
    TVDiskID OwnerVDisks[256];

    TSysLogRecord()
        : Version(PDISK_SYS_LOG_RECORD_VERSION_7)
        , LogHeadChunkIdx(0)
        , Reserved1(0)
        , LogHeadChunkPreviousNonce((ui64)-1)
    {
        for (size_t i = 0; i < 256; ++i) {
            OwnerVDisks[i] = TVDiskID::InvalidId;
        }
    }

    TString ToString() const {
        return ToString(false);
    }

    TString ToString(bool isMultiline) const {
        TStringStream str;
        const char *x = isMultiline ? "\n" : "";
        str << "{TSysLogRecord" << x;
        str << " Version# " << Version << x;
        str << " NonceSet# " << Nonces.ToString(isMultiline) << x;
        str << " LogHeadChunkIdx# " << LogHeadChunkIdx << x;
        str << " LogHeadChunkPreviousNonce# " << LogHeadChunkPreviousNonce << x;
        for (ui32 i = 0; i < 256; ++i) {
            if (OwnerVDisks[i] != TVDiskID::InvalidId) {
                str << " Owner[" << i << "]# " << OwnerVDisks[i].ToString() << x;
            }
        }
        str << "}";
        return str.Str();
    }
};

struct TSysLogFirstNoncesToKeep {
    ui64 FirstNonceToKeep[256];

    TSysLogFirstNoncesToKeep() {
        Clear();
    }

    void Clear() {
        for (size_t i = 0; i < 256; ++i) {
            FirstNonceToKeep[i] = 0;
        }
    }

    TString ToString() const {
        return ToString(false);
    }

    TString ToString(bool isMultiline) const {
        TStringStream str;
        const char *x = isMultiline ? "\n" : "";
        str << "{TSysLogFirstNoncesToKeep" << x;
        for (ui32 i = 0; i < 256; ++i) {
            str << " n" << i << "# " << FirstNonceToKeep[i];
            if (i % 16 == 15) {
                str << x;
            } else {
                str << ",";
            }
        }
        str << "}";
        return str.Str();
    }
};

struct TMetadataHeader {
    ui64 Nonce;
    ui64 SequenceNumber; // of stored metadata
    ui16 RecordIndex; // index of current record
    ui16 TotalRecords; // total number of records for the this SequenceNumber
    ui32 Length; // length of stored data, in bytes
    THash DataHash; // of data only, not including header at all
    THash HeaderHash; // of header only, not including HeaderHash

    void Encrypt(TPDiskStreamCypher& cypher) {
        cypher.StartMessage(Nonce);
        cypher.InplaceEncrypt(&SequenceNumber, sizeof(TMetadataHeader) - sizeof(ui64));
    }

    void EncryptData(TPDiskStreamCypher& cypher) {
        TMetadataHeader header = *this;
        cypher.StartMessage(Nonce);
        cypher.InplaceEncrypt(&SequenceNumber, sizeof(TMetadataHeader) - sizeof(ui64) + Length);
        *this = header;
    }

    bool CheckHash() const {
        TPDiskHashCalculator hasher;
        hasher.Hash(this, sizeof(TMetadataHeader) - sizeof(THash));
        return hasher.GetHashResult() == HeaderHash;
    }

    void SetHash() {
        TPDiskHashCalculator hasher;
        hasher.Hash(this, sizeof(TMetadataHeader) - sizeof(THash));
        HeaderHash = hasher.GetHashResult();
    }

    bool CheckDataHash() const {
        TPDiskHashCalculator hasher;
        hasher.Hash(this + 1, Length);
        return hasher.GetHashResult() == DataHash;
    }
};

struct TMetadataFormatSector {
    ui64 Magic; // MagicMetadataFormatSector
    TKey DataKey; // data is encrypted with this key
    ui64 Offset; // direct offset of latest stored metadata in this block device
    ui64 Length; // length of stored metadata, including header
    ui64 SequenceNumber; // sequence number of stored record
};
#pragma pack(pop)

struct TChunkInfo {
    ui8 Version;
    TOwner OwnerId;
    ui64 Nonce;

    TChunkInfo()
        : Version(PDISK_DATA_VERSION)
        , OwnerId(0)
        , Nonce(0)
    {}
};
static_assert(sizeof(TOwner) == 1, "TOwner size is intended to be 1 byte (range 0..255)"); // Owner[256]

struct TChunkTrimInfo {
    ui8 TrimMask;

    static constexpr ui64 ChunksPerRecord = 8;

    static ui64 RecordsForChunkCount(ui32 chunkCount) {
        return (chunkCount + ChunksPerRecord - 1) / ChunksPerRecord;
    }

    static ui64 SizeForChunkCount(ui32 chunkCount) {
        // Write chunkCount in first 64 bits of chunk trim record
        return RecordsForChunkCount(chunkCount) * sizeof(TChunkTrimInfo);
    }

    TChunkTrimInfo(ui64 mask)
        : TrimMask(mask)
    {}

    void SetChunkTrimmed(ui8 idx) {
        Y_ABORT_UNLESS(idx < ChunksPerRecord);
        TrimMask |= (1 << idx);
    }

    void SetChunkUntrimmed(ui8 idx) {
        Y_ABORT_UNLESS(idx < ChunksPerRecord);
        TrimMask &= ~(1 << idx);
    }

    bool IsChunkTrimmed(ui8 idx) {
        Y_ABORT_UNLESS(idx < ChunksPerRecord);
        return TrimMask & (1 << idx);
    }
};

struct TNextLogChunkReference2 {
    ui32 Version;
    TChunkIdx NextChunk;
    TInstant CreatedAt; // Absent in Reference 1

    TNextLogChunkReference2()
        : Version(PDISK_DATA_VERSION_2)
        , NextChunk(0)
        , CreatedAt(TInstant::Now())
    {}
};

struct TNextLogChunkReference3 : public TNextLogChunkReference2 {
    // Version should be PDISK_DATA_VERSION_3

    // In typical case should be zero
    // In case of splicing means first nonce of next chunk
    ui64 NextChunkFirstNonce;
    // Should be zero
    ui8 IsNotCompatible;

    TNextLogChunkReference3() {
        Version = PDISK_DATA_VERSION_3;
        IsNotCompatible = 0;
    }
};

#pragma pack(pop)

enum EFormatFlags {
    FormatFlagErasureEncodeUserChunks = 1, // Deprecated, user chunks is never erasure encoded
    FormatFlagErasureEncodeUserLog = 1 << 1, // Deprecated, user log is never erasure encoded
    FormatFlagErasureEncodeSysLog = 1 << 2,  // Always on, flag is useless
    FormatFlagErasureEncodeFormat = 1 << 3,  // Always on, flag is useless
    FormatFlagErasureEncodeNextChunkReference = 1 << 4,  // Always on, flag is useless
    FormatFlagEncryptFormat = 1 << 5,  // Always on, flag is useless
    FormatFlagEncryptData = 1 << 6,  // Always on, flag is useless
    FormatFlagFormatInProgress = 1 << 7,  // Not implemented (Must be OFF for a formatted disk)
};

struct TDiskFormat {
    ui64 Version;
    ui64 DiskSize;

    ui64 Guid;
    TKey SysLogKey;
    TKey LogKey;
    TKey ChunkKey;

    ui64 MagicNextLogChunkReference;
    ui64 MagicLogChunk;
    ui64 MagicDataChunk;
    ui64 MagicSysLogChunk;
    ui64 MagicFormatChunk;

    ui32 ChunkSize;
    ui32 SectorSize;
    ui32 SysLogSectorCount;

    ui32 SystemChunkCount;
    char FormatText[FORMAT_TEXT_SIZE];

    THash HashVersion2;

    // All the fields above are present since Version 2
    ui64 DiskFormatSize;  // To determine Hash position for versions > 2
    ui64 TimestampUs;
    ui64 FormatFlags;     // Flags default to 0

    THash Hash;

    TString FormatFlagsToString(ui64 flags) const {
        TStringStream str;
        bool isFirst = true;
        isFirst = NText::OutFlag(isFirst, flags == 0, "None", str);
        isFirst = NText::OutFlag(isFirst, flags & FormatFlagErasureEncodeUserChunks, "ErasureEncodeUserChunks", str);
        isFirst = NText::OutFlag(isFirst, flags & FormatFlagErasureEncodeUserLog, "ErasureEncodeUserLog", str);
        isFirst = NText::OutFlag(isFirst, flags & FormatFlagErasureEncodeSysLog, "ErasureEncodeSysLog", str);
        isFirst = NText::OutFlag(isFirst, flags & FormatFlagErasureEncodeFormat, "ErasureEncodeFormat", str);
        isFirst = NText::OutFlag(isFirst, flags & FormatFlagErasureEncodeNextChunkReference,
            "ErasureEncodeNextChunkReference", str);
        isFirst = NText::OutFlag(isFirst, flags & FormatFlagEncryptFormat, "EncryptFormat", str);
        isFirst = NText::OutFlag(isFirst, flags & FormatFlagEncryptData, "EncryptData", str);
        isFirst = NText::OutFlag(isFirst, flags & FormatFlagFormatInProgress, "FormatFlagFormatInProgress", str);
        NText::OutFlag(isFirst, isFirst, "Unknown", str);
        return str.Str();
    }

    TString ToString() const {
        return ToString(false);
    }

    TString ToString(bool isMultiline) const {
        TStringStream str;
        const char *x = isMultiline ? "\n" : "";
        str << "{TDiskFormat" << x;
        str << " Version: " << Version << x;
        str << " DiskSize: " << DiskSize << " bytes (" << (DiskSize / 1000000000ull) << " GB)" << x;
        str << " Guid: " << Guid << x;
        // Don't output keys since it's extremely unsafe
        // str << " SysLogKey: " << SysLogKey << x;
        // str << " LogKey: " << LogKey << x;
        // str << " ChunkKey: " << ChunkKey << x;
        str << " MagicNextLogChunkReference: " << MagicNextLogChunkReference << x;
        str << " MagicLogChunk: " << MagicLogChunk << x;
        str << " MagicDataChunk: " << MagicDataChunk << x;
        str << " MagicSysLogChunk: " << MagicSysLogChunk << x;
        str << " MagicFormatChunk: " << MagicFormatChunk << x;
        str << " ChunkSize: " << ChunkSize << " bytes (" << (ChunkSize / 1000000ull) << " MB)" << x;
        str << " SectorSize: " << SectorSize << x;
        str << " SysLogSectorCount: " << SysLogSectorCount << x;
        str << " SystemChunkCount: " << SystemChunkCount << x;
        str << " FormatText: \"" << FormatText << "\"" << x;
        str << " DiskFormatSize: " << DiskFormatSize << " (current sizeof: " << sizeof(TDiskFormat) << ")" << x;
        TInstant time = TInstant::MicroSeconds(TimestampUs);
        str << " TimestampUs: " << TimestampUs << " (" << time.ToString() << ")" << x;
        str << " FormatFlags: {" << FormatFlagsToString(FormatFlags) << "}" << x;
        str << "}";
        return str.Str();
    }

    bool IsErasureEncodeUserChunks() const {
        return FormatFlags & FormatFlagErasureEncodeUserChunks;
    }

    bool IsErasureEncodeUserLog() const {
        return FormatFlags & FormatFlagErasureEncodeUserLog;
    }

    bool IsErasureEncodeSysLog() const {
        return FormatFlags & FormatFlagErasureEncodeSysLog;
    }

    bool IsErasureEncodeFormat() const {
        return FormatFlags & FormatFlagErasureEncodeFormat;
    }

    bool IsErasureEncodeNextChunkReference() const {
        return FormatFlags & FormatFlagErasureEncodeNextChunkReference;
    }

    bool IsEncryptFormat() const {
        return FormatFlags & FormatFlagEncryptFormat;
    }

    bool IsEncryptData() const {
        return FormatFlags & FormatFlagEncryptData;
    }

    bool IsFormatInProgress() const {
        return FormatFlags & FormatFlagFormatInProgress;
    }

    void SetFormatInProgress(bool isInProgress) {
        FormatFlags &= ~FormatFlagFormatInProgress;
        if (isInProgress) {
            FormatFlags |= FormatFlagFormatInProgress;
        }
    }

    ui64 Offset(TChunkIdx chunkIdx, ui32 sectorIdx, ui64 offset) const {
        return (ui64)ChunkSize * chunkIdx + (ui64)SectorSize * sectorIdx + offset;
    }

    ui64 Offset(TChunkIdx chunkIdx, ui32 sectorIdx) const {
        return (ui64)ChunkSize * chunkIdx + (ui64)SectorSize * sectorIdx;
    }

    ui64 SectorPayloadSize() const {
        return SectorSize - sizeof(TDataSectorFooter) - CanarySize;
    }

    ui32 DiskSizeChunks() const {
        return DiskSize / ChunkSize;
    }

    ui32 GetUserAccessibleChunkSize() const {
        const ui32 userSectors = ChunkSize / SectorSize;
        return userSectors * SectorPayloadSize();
    }

    ui32 SysLogSectorsPerRecord() const {
        ui32 sectorPayload = SectorSize - CanarySize - sizeof(TDataSectorFooter);
        ui32 diskChunks = DiskSizeChunks();
        ui32 baseSysLogRecordSize = sizeof(TSysLogRecord)
                + diskChunks * sizeof(TChunkInfo)
                + sizeof(TSysLogFirstNoncesToKeep)
                + TChunkTrimInfo::SizeForChunkCount(diskChunks);
        ui32 sysLogFirstSectorPayload = sectorPayload - sizeof(TFirstLogPageHeader);
        ui32 sysLogExtraSectorPayload = sectorPayload - sizeof(TLogPageHeader);
        ui32 sysLogExtraSectorCount = 0;
        if (baseSysLogRecordSize > sysLogFirstSectorPayload) {
            ui32 extraSize = baseSysLogRecordSize - sysLogFirstSectorPayload;
            sysLogExtraSectorCount = (extraSize + sysLogExtraSectorPayload - 1) / sysLogExtraSectorPayload;
        }
        ui32 sysLogSectorsPerRecord = 1 + sysLogExtraSectorCount;
        return sysLogSectorsPerRecord;
    }

    ui64 FirstSysLogSectorIdx() const {
        ui64 formatBytes = FormatSectorSize * ReplicationFactor;
        return (formatBytes + SectorSize - 1) / SectorSize;
    }

    void PrepareMagic(TKey &key, ui64 nonce, ui64 &magic) {
        NPDisk::TPDiskStreamCypher cypher(true);
        cypher.SetKey(key);
        cypher.StartMessage(nonce);
        cypher.InplaceEncrypt(&magic, sizeof(magic));
    }

    void InitMagic() {
        MagicFormatChunk = MagicFormatChunkId;
        NPDisk::TPDiskHashCalculator hash;
        hash.Hash(&Guid, sizeof(Guid));
        hash.Hash(&MagicNextLogChunkReferenceId, sizeof(MagicNextLogChunkReferenceId));
        MagicNextLogChunkReference = hash.GetHashResult();
        hash.Hash(&MagicLogChunkId, sizeof(MagicLogChunkId));
        MagicLogChunk = hash.GetHashResult();
        hash.Hash(&MagicDataChunkId, sizeof(MagicDataChunkId));
        MagicDataChunk = hash.GetHashResult();
        hash.Hash(&MagicSysLogChunkId, sizeof(MagicSysLogChunkId));
        MagicSysLogChunk = hash.GetHashResult();

        PrepareMagic(LogKey, (ui64)-1, MagicNextLogChunkReference);
        PrepareMagic(LogKey , (ui64)-2, MagicLogChunk);
        PrepareMagic(ChunkKey , (ui64)-1, MagicDataChunk);
        PrepareMagic(SysLogKey , (ui64)-1, MagicSysLogChunk);
    }

    ui64 GetUsedSize() const {
        if (Version == 2) {
            ui64 size = (char*)&HashVersion2 - (char*)this;
            size += sizeof(THash);
            return size;
        }
        return DiskFormatSize;
    }

    ui64 RoundUpToSectorSize(ui64 size) const { // assuming SectorSize is a power of 2
        Y_DEBUG_ABORT_UNLESS(IsPowerOf2(SectorSize));
        return (size + SectorSize - 1) & ~ui64(SectorSize - 1);
    }

    bool IsHashOk(ui64 bufferSize) const {
        NPDisk::TPDiskHashCalculator hashCalculator;
        if (Version == 2) {
            ui64 size = (char*)&HashVersion2 - (char*)this;
            hashCalculator.Hash(this, size);
            bool isOk = (HashVersion2 == hashCalculator.GetHashResult());
            return isOk;
        } else {
            if (DiskFormatSize <= sizeof(THash)) {
                return false;
            }
            if (DiskFormatSize > bufferSize) {
                return false;
            }
            ui64 size = DiskFormatSize - sizeof(THash);
            hashCalculator.Hash(this, size);
            bool isOk = (Hash == hashCalculator.GetHashResult());
            return isOk;
        }
    }

    void SetHash() {
        // Set an invalid HashVersion2 to prevent Version2 code from trying to read incompatible disks
        {
            NPDisk::TPDiskHashCalculator hashCalculator;
            ui64 size = (char*)&HashVersion2 - (char*)this;
            hashCalculator.Hash(this, size);
            HashVersion2 = hashCalculator.GetHashResult();
            // Spoil the hash to break compatibility
            HashVersion2++;
        }
        // Set Hash
        {
            NPDisk::TPDiskHashCalculator hashCalculator;
            Y_ABORT_UNLESS(DiskFormatSize > sizeof(THash));
            ui64 size = DiskFormatSize - sizeof(THash);
            hashCalculator.Hash(this, size);
            Hash = hashCalculator.GetHashResult();
        }
    }

    void Clear() {
        Version = PDISK_FORMAT_VERSION;
        DiskSize = 0;
        Guid = 0;
        SysLogKey = 0;
        LogKey = 1;
        ChunkKey = 2;
        ChunkSize = 128 * 1024 * 1024;
        SectorSize = DefaultSectorSize;
        SysLogSectorCount = 1024;
        SystemChunkCount = 1;
        HashVersion2 = 0;
        DiskFormatSize = sizeof(TDiskFormat);
        TimestampUs = TInstant::Now().MicroSeconds();
        FormatFlags =
            // FormatFlagErasureEncodeUserChunks |
            // FormatFlagErasureEncodeUserLog |
            FormatFlagErasureEncodeSysLog |
            FormatFlagErasureEncodeFormat |
            FormatFlagErasureEncodeNextChunkReference |
            FormatFlagEncryptFormat |
            FormatFlagEncryptData;
        Hash = 0;

        memset(FormatText, 0, sizeof(FormatText));
        InitMagic();
    }

    void UpgradeFrom(const TDiskFormat &format) {
        Clear();
        // Upgrade from version 2
        TimestampUs = 0;
        // Fill the flags according to actual Version2 settings
        FormatFlags =
            FormatFlagErasureEncodeUserChunks |
            FormatFlagErasureEncodeUserLog |
            FormatFlagErasureEncodeSysLog |
            FormatFlagErasureEncodeFormat |
            FormatFlagErasureEncodeNextChunkReference |
            FormatFlagEncryptFormat |
            FormatFlagEncryptData;
        Y_ABORT_UNLESS(format.Version <= Version);
        Y_ABORT_UNLESS(format.GetUsedSize() <= sizeof(TDiskFormat));
        memcpy(this, &format, format.GetUsedSize());
    }

    bool IsDiskSmall() {
        return DiskSize < FullSizeDiskMinimumSize;
    }
};

union TDiskFormatSector {
    TDiskFormat Format;
    char Raw[FormatSectorSize];
};

static_assert(sizeof(TDiskFormat) <= FormatSectorSize, "TDiskFormat size too lagre!");


struct TCheckDiskFormatResult {
    bool IsFormatPresent;
    bool IsReencryptionRequired;

    TCheckDiskFormatResult(bool isFormatPresent, bool isReencryptionRequired)
        : IsFormatPresent(isFormatPresent)
        , IsReencryptionRequired(isReencryptionRequired)
    {}
};

struct TPDiskFormatBigChunkException : public yexception {
};

} // NPDisk
} // NKikimr

