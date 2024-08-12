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
            TMask Mask;
            ui32 MaskSize = 0;

            TFreeRes() = default;
            TFreeRes(ui32 chunkId, TMask mask, ui32 maskSize)
                : ChunkId(chunkId)
                , Mask(mask)
                , MaskSize(maskSize)
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


        ////////////////////////////////////////////////////////////////////////////
        // TBlobMerger
        ////////////////////////////////////////////////////////////////////////////
        class TBlobMerger {
            using TCircaLsns = TVector<ui64>;

        public:
            TBlobMerger() = default;

            void Clear() {
                DiskPtrs.clear();
                Deleted.clear();
                Parts.Clear();
                CircaLsns.clear();
            }

            void SetEmptyFromAnotherMerger(const TBlobMerger *fromMerger) {
                Clear();
                Deleted.insert(Deleted.end(), fromMerger->SavedData().begin(), fromMerger->SavedData().end());
                Deleted.insert(Deleted.end(), fromMerger->DeletedData().begin(), fromMerger->DeletedData().end());
            }

            void Add(const TDiskPart *begin, const TDiskPart *end, const NMatrix::TVectorType &parts, ui64 circaLsn) {
                if (DiskPtrs.empty()) {
                    Parts = parts;
                    DiskPtrs = {begin, end};
                    CircaLsns = TCircaLsns(end - begin, circaLsn);
                    Y_ABORT_UNLESS(DiskPtrs.size() == Parts.CountBits());
                } else {
                    Merge(begin, end, parts, circaLsn);
                }
            }

            void AddDeletedPart(const TDiskPart &part) {
                Deleted.push_back(part);
            }

            void AddMetadataParts(NMatrix::TVectorType parts) {
                // this is special case for mirror3of4, where data can have empty TDiskPart
                std::array<TDiskPart, 8> zero;
                zero.fill(TDiskPart());
                // empty TDiskPart at a position means that every circaLsn shoud work
                const ui64 circaLsn = Max<ui64>();
                Merge(zero.begin(), zero.begin() + parts.CountBits(), parts, circaLsn);
            }

            bool Empty() const {
                return Parts.Empty();
            }

            void Swap(TBlobMerger &m) {
                DiskPtrs.swap(m.DiskPtrs);
                Deleted.swap(m.Deleted);
                Parts.Swap(m.Parts);
                CircaLsns.swap(m.CircaLsns);
            }

            TBlobType::EType GetBlobType() const {
                Y_ABORT_UNLESS(!Empty());
                return DiskPtrs.size() == 1 ? TBlobType::HugeBlob : TBlobType::ManyHugeBlobs;
            }

            ui32 GetNumParts() const {
                return DiskPtrs.size();
            }

            void Output(IOutputStream &str) const {
                if (Empty()) {
                    str << "empty";
                } else {
                    str << "{Parts# " << Parts.ToString();
                    str << " CircaLsns# " << FormatList(CircaLsns);
                    str << " DiskPtrs# ";
                    FormatList(str, DiskPtrs);
                    str << " Deleted# ";
                    FormatList(str, Deleted);
                    str << "}";
                }
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }

            const NMatrix::TVectorType& GetParts() const {
                return Parts;
            }

            const TVector<TDiskPart> &SavedData() const {
                return DiskPtrs;
            }

            const TVector<TDiskPart> &DeletedData() const {
                return Deleted;
            }

            const TCircaLsns &GetCircaLsns() const {
                return CircaLsns;
            }

        private:
            typedef TVector<TDiskPart> TDiskPtrs;

            TDiskPtrs DiskPtrs;
            TDiskPtrs Deleted;
            NMatrix::TVectorType Parts;
            TCircaLsns CircaLsns;

            void Merge(const TDiskPart *begin, const TDiskPart *end, const NMatrix::TVectorType &parts, ui64 circaLsn)
            {
                Y_ABORT_UNLESS(end - begin == parts.CountBits());
                Y_DEBUG_ABORT_UNLESS(Parts.GetSize() == parts.GetSize());
                const ui8 maxSize = parts.GetSize();
                TDiskPtrs newDiskPtrs;
                TCircaLsns newCircaLsns;
                newDiskPtrs.reserve(maxSize);
                newCircaLsns.reserve(maxSize);

                TDiskPtrs::const_iterator locBegin = DiskPtrs.begin();
                TDiskPtrs::const_iterator locEnd = DiskPtrs.end();
                TDiskPtrs::const_iterator locIt = locBegin;
                const TDiskPart *it = begin;

                for (ui8 i = 0; i < maxSize; i++) {
                    if (Parts.Get(i) && parts.Get(i)) {
                        // both
                        Y_DEBUG_ABORT_UNLESS(locIt != locEnd && it != end);
                        if (CircaLsns[locIt - locBegin] < circaLsn) {
                            // incoming value wins
                            newDiskPtrs.push_back(*it);
                            newCircaLsns.push_back(circaLsn);
                            Deleted.push_back(*locIt);
                        } else {
                            // already seen value wins
                            newDiskPtrs.push_back(*locIt);
                            newCircaLsns.push_back(CircaLsns[locIt - locBegin]);
                            Deleted.push_back(*it);
                        }
                        ++locIt;
                        ++it;
                    } else if (Parts.Get(i)) {
                        Y_DEBUG_ABORT_UNLESS(locIt != locEnd);
                        newDiskPtrs.push_back(*locIt);
                        newCircaLsns.push_back(CircaLsns[locIt - locBegin]);
                        ++locIt;
                    } else if (parts.Get(i)) {
                        Y_DEBUG_ABORT_UNLESS(it != end);
                        newDiskPtrs.push_back(*it);
                        newCircaLsns.push_back(circaLsn);
                        ++it;
                    }
                }

                Parts |= parts;
                DiskPtrs.swap(newDiskPtrs);
                CircaLsns.swap(newCircaLsns);
                Y_ABORT_UNLESS(DiskPtrs.size() == Parts.CountBits());
            }
        };

    } // NHuge

} // NKikimr

template<>
struct THash<NKikimr::NHuge::THugeSlot> {
    inline ui64 operator()(const NKikimr::NHuge::THugeSlot& x) const noexcept {
        return x.Hash();
    }
};

