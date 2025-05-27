#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <util/generic/bitmap.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {

    namespace NHuge {

        ////////////////////////////////////////////////////////////////////////////
        // TMask
        ////////////////////////////////////////////////////////////////////////////
        using TMask = TDynBitMap;

        ////////////////////////////////////////////////////////////////////////////
        // TFreeRes
        ////////////////////////////////////////////////////////////////////////////
        struct TFreeRes {
            ui32 ChunkId = 0;
            bool InLockedChunks = false;

            TFreeRes() = default;
            TFreeRes(ui32 chunkId, bool inLockedChunks)
                : ChunkId(chunkId)
                , InLockedChunks(inLockedChunks)
            {}

            void Output(IOutputStream &str) const;

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // THeapStat
        ////////////////////////////////////////////////////////////////////////////
        struct THeapStat {
            ui32 CurrentlyUsedChunks = 0;
            ui32 CanBeFreedChunks = 0;
            std::vector<ui32> LockedChunks;

            THeapStat() = default;
            THeapStat(ui32 currentlyUsedChunks, ui32 canBeFreedChunks, std::vector<ui32> lockedChunks)
                : CurrentlyUsedChunks(currentlyUsedChunks)
                , CanBeFreedChunks(canBeFreedChunks)
                , LockedChunks(std::move(lockedChunks))
            {}

            THeapStat &operator+=(const THeapStat &s) {
                CurrentlyUsedChunks += s.CurrentlyUsedChunks;
                CanBeFreedChunks += s.CanBeFreedChunks;
                LockedChunks.insert(LockedChunks.end(), s.LockedChunks.begin(), s.LockedChunks.end());
                return *this;
            }

            bool operator==(const THeapStat &s) const {
                return CurrentlyUsedChunks == s.CurrentlyUsedChunks && CanBeFreedChunks == s.CanBeFreedChunks;
            }

            void Output(IOutputStream &str) const {
                str << "{CurrentlyUsedChunks# " << CurrentlyUsedChunks
                    << " CanBeFreedChunks# " << CanBeFreedChunks << "}";
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // THugeSlot
        ////////////////////////////////////////////////////////////////////////////
        class THugeSlot {
            ui32 ChunkId;
            ui32 Offset;
            ui32 Size;

        public:
            THugeSlot()
                : ChunkId(0)
                , Offset(0)
                , Size(0)
            {}

            THugeSlot(ui32 chunkId, ui32 offset, ui32 size)
                : ChunkId(chunkId)
                , Offset(offset)
                , Size(size)
            {}

            ui32 GetChunkId() const {
                return ChunkId;
            }

            ui32 GetOffset() const {
                return Offset;
            }

            ui32 GetSize() const {
                return Size;
            }

            TDiskPart GetDiskPart() const {
                return TDiskPart(ChunkId, Offset, Size);
            }

            TString ToString() const {
                TStringStream str;
                str << "[" << ChunkId << " " << Offset << " " << Size << "]";
                return str.Str();
            }

            bool operator ==(const THugeSlot &id) const {
                return ChunkId == id.ChunkId && Offset == id.Offset;
            }

            bool operator <(const THugeSlot &id) const {
                return ChunkId < id.ChunkId || (ChunkId == id.ChunkId && Offset < id.Offset);
            }

            ui64 Hash() const {
                return MultiHash(ChunkId, Offset);
            }

            void Serialize(IOutputStream &str) const {
                str.Write(&ChunkId, sizeof(ui32));
                str.Write(&Offset, sizeof(ui32));
                str.Write(&Size, sizeof(ui32));
            }

            bool Parse(const char *b, const char *e) {
                if (e < b || size_t(e - b) != SerializedSize)
                    return false;

                ChunkId = ReadUnaligned<ui32>(b);
                b += sizeof(ui32);
                Offset = ReadUnaligned<ui32>(b);
                b += sizeof(ui32);
                Size = ReadUnaligned<ui32>(b);
                return true;
            }
            static const ui32 SerializedSize;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TAllocChunkRecoveryLogRec
        ////////////////////////////////////////////////////////////////////////////
        struct TAllocChunkRecoveryLogRec {
            ui32 ChunkId;

            TAllocChunkRecoveryLogRec(ui32 chunkId)
                : ChunkId(chunkId)
            {}

            TString Serialize() const;
            bool ParseFromString(const TString &data);
            bool ParseFromArray(const char* data, size_t size);
            TString ToString() const;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TFreeChunkRecoveryLogRec
        ////////////////////////////////////////////////////////////////////////////
        struct TFreeChunkRecoveryLogRec {
            TVector<ui32> ChunkIds;

            TFreeChunkRecoveryLogRec(const TVector<ui32> &ids)
                : ChunkIds(ids)
            {}

            TString Serialize() const;
            bool ParseFromString(const TString &data);
            bool ParseFromArray(const char* data, size_t size);
            TString ToString() const;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TPutRecoveryLogRec
        ////////////////////////////////////////////////////////////////////////////
        struct TPutRecoveryLogRec {
            TLogoBlobID LogoBlobID;
            TIngress Ingress;
            TDiskPart DiskAddr;

            TPutRecoveryLogRec(const TLogoBlobID &logoBlobID, const TIngress &ingress,
                               const TDiskPart &diskAddr)
                : LogoBlobID(logoBlobID)
                , Ingress(ingress)
                , DiskAddr(diskAddr)
            {}

            TString Serialize() const;
            bool ParseFromString(const TString &data);
            bool ParseFromArray(const char* data, size_t size);
            TString ToString() const;
        };

    } // NHuge

} // NKikimr

template<>
struct THash<NKikimr::NHuge::THugeSlot> {
    inline ui64 operator()(const NKikimr::NHuge::THugeSlot& x) const noexcept {
        return x.Hash();
    }
};

