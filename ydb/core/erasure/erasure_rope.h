#pragma once

#include <array>

#include <ydb/core/debug/valgrind_check.h>

#include <util/stream/str.h>
#include <util/generic/string.h>
#include <util/generic/bt_exception.h>
#include <util/string/builder.h>

#include <util/generic/list.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/library/actors/util/rope.h>
#include <library/cpp/digest/crc32c/crc32c.h>

namespace NKikimr {
namespace NErasureRope {

class TRopeHelpers {
public:
    using Iterator = TRope::TConstIterator;

    static const ui32 RopeBlockSize = 32 * 1024 - 128;

    static TRope CreateRope(size_t size, char c = '\0') {
        TRope rope;
        for (size_t i = 0; i < size / RopeBlockSize; ++i) {
            rope.Insert(rope.End(), RopeFromStringReference(TString(RopeBlockSize, c)));
        }

        if (rope.GetSize() < size) {
            rope.Insert(rope.End(), RopeFromStringReference(TString(size - rope.GetSize(), c)));
        }

        return rope;
    }

    static TRope RopeUninitialized(size_t size) {
        TRope rope;
        for (size_t i = 0; i < size / RopeBlockSize; ++i) {
            rope.Insert(rope.End(), RopeFromStringReference(TString::Uninitialized(RopeBlockSize)));
        }

        if (rope.GetSize() < size) {
            rope.Insert(rope.End(),RopeFromStringReference(TString::Uninitialized(size - rope.GetSize())));
        }

        return rope;
    }

    static TRope RopeCopy(const TRope& src) {
        TRope copy = RopeUninitialized(src.GetSize());
        TRopeUtils::Memcpy(copy.Begin(), src.Begin(), src.GetSize());
        return copy;
    }

    static TRope RopeFromStringMemcpy(const TString& string) {
        TRope rope = RopeUninitialized(string.size());
        TRopeUtils::Memcpy(rope.Begin(), string.data(), string.size());
        return rope;
    }

    static TRope RopeFromStringReference(TString string) {
        TRope rope;
        if (string.Empty()) {
            return rope;
        }
        rope.Insert(rope.End(), TRope(std::move(string)));
        return rope;
    }

    static TRope& ResetWithFullCopy(TRope& rope) {
        rope = RopeCopy(rope);
        return rope;
    }

    static TRope& Resize(TRope& rope, size_t size) {
        if (size > rope.GetSize()) {
            rope.Insert(rope.End(), CreateRope(size - rope.GetSize()));
        } else {
            rope.EraseBack(rope.GetSize() - size);
        }
        return rope;
    }

    static bool Is8Aligned(const TRope& buffer) {
        Iterator begin = buffer.Begin();
        while (begin.Valid()) {
            intptr_t address = (intptr_t) begin.ContiguousData();
            ui64 size = begin.ContiguousSize();
            begin.AdvanceToNextContiguousBlock();
            if (address % 8 != 0 || (begin.Valid() && size % 8 != 0)) {
                return false;
            }
        }
        return true;
    }

    static ui32 GetCrc32c(Iterator begin, size_t size) {
        ui32 hash = 0;
        while (size) {
            Y_ABORT_UNLESS(begin.Valid());
            size_t len = std::min(size, begin.ContiguousSize());
            hash = Crc32cExtend(hash, begin.ContiguousData(), len);
            begin += len;
            size -= len;
        }
        return hash;
    }

    class TRopeFastView {
    private:
        Iterator Begin;
        Iterator Current;

        char* BlockData = nullptr;
        char* Boost = nullptr;
        ui64 BlockBeginIndx = 0;
        ui64 BlockEndIndx = 0;
        ui64 Offset = 0;

    public:
        explicit TRopeFastView(const TRope& rope)
            : TRopeFastView(rope.Begin()) {
        }

        explicit TRopeFastView(Iterator begin)
            : Begin(begin)
            , Current(Begin)
            , BlockData(const_cast<char*>(Begin.ContiguousData()))
            , Boost(BlockData)
            , BlockBeginIndx(0)
            , BlockEndIndx(Begin.ContiguousSize())
            , Offset(0) {
        }

        TRopeFastView() = default;

        ui64 GetContiguousSize(size_t pos) {
            Y_DEBUG_ABORT_UNLESS(pos >= Offset);
            return (BlockBeginIndx <= (pos - Offset) && (pos - Offset) < BlockEndIndx)
                                                             ? (BlockEndIndx - pos + Offset) : 0;
        }

        char* DataPtrAt(size_t pos) {
            Y_DEBUG_ABORT_UNLESS(pos >= Offset);
            return (BlockBeginIndx <= (pos - Offset) && (pos - Offset) < BlockEndIndx)
                                     ? BlockData + (pos - BlockBeginIndx) - Offset : UpdateCurrent(pos - Offset);
        }

        char* FastDataPtrAt(size_t pos) {
            return Boost + pos;
        }

        ui8& At8(size_t pos) {
            return *(ui8*)DataPtrAt(pos);
        }

        ui64& At64(size_t pos) {
            return *(ui64*)DataPtrAt(pos * sizeof(ui64));
        }

        ui64& FastAt64(size_t pos) {
            return *(ui64*)FastDataPtrAt(pos * sizeof(ui64));
        }

        Iterator GetBegin() {
            return Begin;
        }

        void SetOffset(ui64 offset) {
            Offset = offset;
            if (BlockData) {
                Boost = BlockData - BlockBeginIndx - Offset;
            }
        }

        Iterator GetCurrent(size_t pos) {
            if (BlockBeginIndx > pos - Offset || BlockEndIndx <= pos - Offset) {
                UpdateCurrent(pos - Offset);
            }
            if (Current.ContiguousData() > Boost + pos) {
                Current -= Current.ContiguousData() - (Boost + pos);
            } else {
                Current += (Boost + pos) - Current.ContiguousData();
            }
            Y_ABORT_UNLESS(Current.ContiguousData() == Boost + pos);
            return Current;
        }

    private:

        char* UpdateCurrent(size_t pos) {
            if (pos >= BlockEndIndx) {
                while (pos >= BlockEndIndx) {
                    Current.AdvanceToNextContiguousBlock();
                    BlockBeginIndx = BlockEndIndx;
                    BlockEndIndx += Current.ContiguousSize();
                }
                Y_ABORT_UNLESS(BlockBeginIndx <= pos && pos < BlockEndIndx);
                BlockData = const_cast<char*>(Current.ContiguousData());
                Boost = BlockData - BlockBeginIndx - Offset;
                return BlockData + (pos - BlockBeginIndx);
            }

            if (pos < BlockBeginIndx - pos) {
                Current = Begin + pos;
            } else {
                Current -= Current.ChunkOffset();
                Current -= BlockBeginIndx - pos;
            }

            BlockData = const_cast<char*>(Current.ContiguousData()) - Current.ChunkOffset();
            if (BlockData == Begin.ContiguousData() - Begin.ChunkOffset()) {
                BlockData = const_cast<char*>(Begin.ContiguousData());
            }

            ui64 offset = Current.ContiguousData() - BlockData;
            Y_ABORT_UNLESS(pos >= offset);
            BlockBeginIndx = pos - offset;
            BlockEndIndx = pos + Current.ContiguousSize();
            Boost = BlockData - BlockBeginIndx - Offset;
            Y_ABORT_UNLESS(pos >= BlockBeginIndx && pos < BlockEndIndx);

            return BlockData + (pos - BlockBeginIndx);
        }
    };
};

// Part fragment, contains only some data
struct TPartFragment {
    TRope OwnedRope;
    ui64 Offset = 0; // Relative to part beginning
    ui64 Size = 0;
    ui64 PartSize = 0; // Full size of the part

    mutable TRopeHelpers::TRopeFastView FastViewer;

    TPartFragment() = default;

    TPartFragment(const TPartFragment& lhs) {
        OwnedRope = lhs.OwnedRope;
        Offset = lhs.Offset;
        Size = lhs.Size;
        PartSize = lhs.PartSize;
        FastViewer = TRopeHelpers::TRopeFastView(OwnedRope);
        FastViewer.SetOffset(Offset);
    }

    TPartFragment& operator=(const TPartFragment& lhs) {
        if (this == &lhs) {
            return *this;
        }
        OwnedRope = lhs.OwnedRope;
        Offset = lhs.Offset;
        Size = lhs.Size;
        PartSize = lhs.PartSize;
        FastViewer = TRopeHelpers::TRopeFastView(OwnedRope);
        FastViewer.SetOffset(Offset);
        return *this;
    }

    ui64 size() const {
        return PartSize;
    }

    void clear() {
        OwnedRope = TRope();
        Offset = 0;
        Size = 0;
        PartSize = 0;
        FastViewer = TRopeHelpers::TRopeFastView(OwnedRope);
    }

    void UninitializedOwnedWhole(ui64 size) {
        OwnedRope = TRopeHelpers::RopeUninitialized(size);
        FastViewer = TRopeHelpers::TRopeFastView(OwnedRope);
        Offset = 0;
        Size = size;
        PartSize = size;
    }

    void ResetToWhole(const TRope& whole) {
        OwnedRope = TRopeHelpers::RopeCopy(whole);
        FastViewer = TRopeHelpers::TRopeFastView(OwnedRope);
        Offset = 0;
        Size = whole.GetSize();
        PartSize = Size;
    }

    void ReferenceTo(const TRope& whole) {
        OwnedRope = whole;
        FastViewer = TRopeHelpers::TRopeFastView(OwnedRope);
        Offset = 0;
        Size = whole.GetSize();
        PartSize = Size;
    }

    void ReferenceTo(const TRope &piece, ui64 offset, ui64 size, ui64 partSize) {
        OwnedRope = piece;
        FastViewer = TRopeHelpers::TRopeFastView(OwnedRope);
        Offset = offset;
        Y_ABORT_UNLESS(size <= piece.GetSize());
        Size = size;
        Y_ABORT_UNLESS(offset + size <= partSize);
        PartSize = partSize;
        FastViewer.SetOffset(Offset);
    }

    char *GetDataAt(ui64 getOffset) const {
        Y_DEBUG_ABORT_UNLESS(Size);
        Y_DEBUG_ABORT_UNLESS(getOffset >= Offset, "%s", (TStringBuilder() << "get_offset# " << getOffset
                    << " Offset# " << Offset << " Size# " << Size << " capacity# " << OwnedRope.GetSize()).c_str());
        Y_DEBUG_ABORT_UNLESS(getOffset < Offset + Size, "%s", (TStringBuilder() << "get_offset# " << getOffset
                    << " Offset# " << Offset << " Size# " << Size << " capacity# " << OwnedRope.GetSize()).c_str());
        return FastViewer.DataPtrAt(getOffset);
    }

    char *GetDataAtSafe(ui64 getOffset) const {
        if (getOffset < Offset || getOffset >= Offset + Size) {
            return nullptr;
        }
        return FastViewer.DataPtrAt(getOffset);
    }

    ui64 MemoryConsumed() const {
        return OwnedRope.GetSize();
    }

    void ResetWithFullCopy() {
        OwnedRope = TRopeHelpers::RopeCopy(OwnedRope);
        FastViewer = TRopeHelpers::TRopeFastView(OwnedRope);
    }

    ui64 GetContiguousSize(ui64 pos) const {
        return FastViewer.GetContiguousSize(pos);
    }

    char* FastDataPtrAt(size_t pos) {
        return FastViewer.FastDataPtrAt(pos);
    }
};

struct TDataPartSet {
    ui64 FullDataSize;
    ui32 PartsMask;
    TStackVec<TPartFragment, 8> Parts;
    TPartFragment FullDataFragment;
    ui64 MemoryConsumed;
    bool IsFragment;

    TDataPartSet()
        : FullDataSize(0)
        , PartsMask(0)
        , MemoryConsumed(0)
        , IsFragment(false)
    {}

    bool Is8Aligned() const {
        for (ui32 i = 0; i < 8u; ++i) {
            if ((PartsMask & (1u << i)) && !TRopeHelpers::Is8Aligned(Parts[i].OwnedRope)) {
                return false;
            }
        }
        return true;
    }

    void ResetWithFullCopy() {
        for (size_t i = 0; i < Parts.size(); ++i) {
            Parts[i].ResetWithFullCopy();
        }
        FullDataFragment.ResetWithFullCopy();
    }
};

struct TPartOffsetRange {  // [Begin, End)
    ui64 Begin = 0;
    ui64 End = 0;

    ui64 WholeBegin = 0;
    ui64 WholeEnd = 0;

    ui64 AlignedBegin = 0;
    ui64 AlignedEnd = 0;

    // AlignedWholeEnd does not always exist because part are zero-padded
    ui64 AlignedWholeBegin = 0;

    bool IsEmpty() {
        return (End == 0);
    }

    void Reset() {
        Begin = 0;
        End = 0;

        WholeBegin = 0;
        WholeEnd = 0;

        AlignedBegin = 0;
        AlignedEnd = 0;

        AlignedWholeBegin = 0;
    }
};

struct TBlockSplitRange {
    ui64 BeginPartIdx = 0;
    ui64 EndPartIdx = 0;
    TStackVec<TPartOffsetRange, 8> PartRanges;
};

struct TErasureParameters;

struct TRopeErasureType {

    enum EErasureSpecies {
        ErasureNone = 0,
        ErasureMirror3 = 1,
        Erasure3Plus1Block = 2,
        Erasure3Plus1Stripe = 3, // Not implemented in TRope version of erasure

        Erasure4Plus2Block = 4,
        Erasure3Plus2Block = 5,
        Erasure4Plus2Stripe = 6, // Not implemented in TRope version of erasure
        Erasure3Plus2Stripe = 7, // Not implemented in TRope version of erasure

        ErasureMirror3Plus2 = 8,
        ErasureMirror3dc = 9,

        Erasure4Plus3Block = 10,
        Erasure4Plus3Stripe = 11, // Not implemented in TRope version of erasure
        Erasure3Plus3Block = 12,
        Erasure3Plus3Stripe = 13, // Not implemented in TRope version of erasure
        Erasure2Plus3Block = 14,
        Erasure2Plus3Stripe = 15, // Not implemented in TRope version of erasure

        Erasure2Plus2Block = 16,
        Erasure2Plus2Stripe = 17, // Not implemented in TRope version of erasure

        ErasureMirror3of4 = 18,

        ErasureSpeciesCount = 19
    };

    static const char *ErasureSpeciesToStr(EErasureSpecies es);

    enum EErasureFamily {
        ErasureMirror,
        ErasureParityStripe,
        ErasureParityBlock
    };

    enum ECrcMode {
        CrcModeNone = 0,
        CrcModeWholePart = 1
    };

    TRopeErasureType(EErasureSpecies s = ErasureNone)
        : ErasureSpecies(s)
    {}

    virtual ~TRopeErasureType() = default;
    TRopeErasureType(const TRopeErasureType &) = default;
    TRopeErasureType &operator =(const TRopeErasureType &) = default;

    EErasureSpecies GetErasure() const {
        return ErasureSpecies;
    }

    TString ToString() const {
        Y_ABORT_UNLESS((ui64)ErasureSpecies < ErasureSpeciesCount);
        return ErasureName[ErasureSpecies];
    }

    static TString ErasureSpeciesName(ui32 erasureSpecies) {
        if (erasureSpecies < ErasureSpeciesCount) {
            return ErasureName[erasureSpecies];
        }
        TStringStream str;
        str << "Unknown" << erasureSpecies;
        return str.Str();
    }

    static EErasureSpecies ErasureSpeciesByName(TString name) {
        for (ui32 species = 0; species < TRopeErasureType::ErasureSpeciesCount; ++species) {
            if (TRopeErasureType::ErasureName[species] == name) {
                return TRopeErasureType::EErasureSpecies(species);
            }
        }
        return TRopeErasureType::ErasureSpeciesCount;
    }

    ui32 ParityParts() const; // 4 + _2_
    ui32 DataParts() const; // _4_ + 2
    ui32 TotalPartCount() const; // _4_+_2_
    ui32 MinimalRestorablePartCount() const; // ? _4_ + 2
    ui32 MinimalBlockSize() const;
    // Size of user data contained in the part.
    ui64 PartUserSize(ui64 dataSize) const;
    // Size of the part including user data and crcs
    ui64 PartSize(ECrcMode crcMode, ui64 dataSize) const;
    ui32 Prime() const;

    void SplitData(ECrcMode crcMode, const TRope& buffer, TDataPartSet& outPartSet) const;

    void RestoreData(ECrcMode crcMode, TDataPartSet& partSet, TRope& outBuffer, bool restoreParts,
            bool restoreFullData, bool restoreParityParts) const;

    void RestoreData(ECrcMode crcMode, TDataPartSet& partSet, bool restoreParts, bool restoreFullData,
            bool restoreParityParts) const;

    void BlockSplitRange(ECrcMode crcMode, ui64 blobSize, ui64 wholeBegin, ui64 wholeEnd,
            TBlockSplitRange *outRange) const;

    static const std::array<TString, ErasureSpeciesCount> ErasureName;
protected:
    EErasureSpecies ErasureSpecies;
};

bool CheckCrcAtTheEnd(TRopeErasureType::ECrcMode crcMode, const TRope& buf);

} // NKikimr
} // NErasureRope

