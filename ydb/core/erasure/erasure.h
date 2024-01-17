#pragma once

#include <array>
#include <span>

#include <ydb/core/debug/valgrind_check.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/stream/str.h>
#include <util/generic/string.h>
#include <util/generic/bt_exception.h>
#include <util/string/builder.h>

#include <util/generic/list.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/library/actors/util/rope.h>

namespace NKikimr {

struct TDiff {
    TRcBuf Buffer;
    ui32 Offset = 0;
    bool IsXor = false;
    bool IsAligned = false;

    TDiff(const TString &buffer, ui32 offset, bool isXor, bool isAligned)
        : Buffer(buffer)
        , Offset(offset)
        , IsXor(isXor)
        , IsAligned(isAligned)
    {
    }

    TDiff(const TString &buffer, ui32 offset)
        : TDiff(buffer, offset, false, false)
    {
    }

    TDiff(const TRcBuf &buffer, ui32 offset, bool isXor, bool isAligned)
        : Buffer(buffer)
        , Offset(offset)
        , IsXor(isXor)
        , IsAligned(isAligned)
    {
    }

    TDiff(const TRcBuf &buffer, ui32 offset)
        : TDiff(buffer, offset, false, false)
    {
    }

    ui32 GetDiffLength() const {
        return (IsAligned ? Buffer.size() - Offset % sizeof(ui64) : Buffer.size());
    }

    const ui8* GetBufferBegin() const {
        return reinterpret_cast<const ui8*>((Buffer.data()));
    }

    const ui8* GetDataBegin() const {
        return GetBufferBegin() + (IsAligned ? Offset % sizeof(ui64) : 0);
    }
};

struct TPartDiff {
    TVector<TDiff> Diffs;
};

struct TPartDiffSet {
    TVector<TPartDiff> PartDiffs;
};

// Part fragment, contains only some data
struct TPartFragment {
    TRope OwnedString; // Used for ownership only
    char *Bytes = nullptr;
    ui64 Offset = 0; // Relative to part beginning
    ui64 Size = 0;
    ui64 PartSize = 0; // Full size of the part

    ui64 size() const {
        return PartSize;
    }

    void clear() {
        OwnedString.clear();
        Bytes = nullptr;
        Offset = 0;
        Size = 0;
        PartSize = 0;
    }

    void UninitializedOwnedWhole(ui64 size, ui64 headroom = 0, ui64 tailroom = 0) {
        OwnedString = TRope(TRcBuf::Uninitialized(size, headroom, tailroom));
        Bytes = OwnedString.UnsafeGetContiguousSpanMut().data();
        Offset = 0;
        Size = size;
        PartSize = size;
    }

    void ResetToWhole(const TRope &whole) {
        OwnedString = whole;
        Bytes = OwnedString.GetContiguousSpanMut().data();
        Offset = 0;
        Size = OwnedString.size();
        PartSize = Size;
    }

    void ReferenceTo(const TRope &whole) {
        Y_ABORT_UNLESS(whole.IsContiguous());
        OwnedString = whole;
        Bytes = OwnedString.UnsafeGetContiguousSpanMut().data();
        Offset = 0;
        Size = OwnedString.size();
        PartSize = Size;
    }

    void ReferenceTo(const TRope &piece, ui64 offset, ui64 size, ui64 partSize) {
        Y_ABORT_UNLESS(piece.IsContiguous());
        OwnedString = piece;
        Bytes = OwnedString.UnsafeGetContiguousSpanMut().data();
        Offset = offset;
        Y_ABORT_UNLESS(size <= piece.size());
        Size = size;
        Y_ABORT_UNLESS(offset + size <= partSize);
        PartSize = partSize;
    }

    void ReferenceTo(const TString &whole) {
        TRope rope(whole);
        ReferenceTo(rope);
    }

    void ReferenceTo(const TString &piece, ui64 offset, ui64 size, ui64 partSize) {
        TRope rope(piece);
        ReferenceTo(rope, offset, size, partSize);
    }


    char *GetDataAt(ui64 get_offset) const {
        Y_DEBUG_ABORT_UNLESS(Size);
        Y_DEBUG_ABORT_UNLESS(get_offset >= Offset, "%s", (TStringBuilder() << "get_offset# " << get_offset
                    << " Offset# " << Offset << " Size# " << Size << " capacity# " << OwnedString.capacity()).c_str());
        Y_DEBUG_ABORT_UNLESS(get_offset < Offset + Size, "%s", (TStringBuilder() << "get_offset# " << get_offset
                    << " Offset# " << Offset << " Size# " << Size << " capacity# " << OwnedString.capacity()).c_str());
        return Bytes + get_offset - Offset;
    }

    char *GetDataAtSafe(ui64 get_offset) const {
        if (get_offset < Offset || get_offset >= Offset + Size) {
            return nullptr;
        }
        return Bytes + get_offset - Offset;
    }

    ui64 MemoryConsumed() const {
        return OwnedString.capacity();
    }

    void Detach() {
        if(Bytes) {
            char *oldBytes = Bytes;
            char *oldData = OwnedString.UnsafeGetContiguousSpanMut().data();
            intptr_t bytesOffset = oldBytes - oldData;
            Bytes = OwnedString.GetContiguousSpanMut().data() + bytesOffset;
        }
    }
};

struct TDataPartSet {
    ui64 FullDataSize = 0;
    ui32 PartsMask = 0;
    TStackVec<TPartFragment, 8> Parts;
    TPartFragment FullDataFragment;
    ui64 MemoryConsumed = 0;
    bool IsFragment = false;

    // Incremental split KIKIMR-10794
    ui64 WholeBlocks = 0; // Blocks to be split (not including tail)
    ui64 CurBlockIdx = 0; // Blocks have been already split

    void Detach() {
        for (size_t i = 0; i < Parts.size(); ++i) {
            Parts[i].Detach();
        }
        FullDataFragment.Detach();
    }

    void StartSplit(ui64 wholeBlocks) {
        WholeBlocks = wholeBlocks;
    }

    bool IsSplitStarted() const {
        return WholeBlocks != 0;
    }

    bool IsSplitDone() const {
        // True if either:
        //  - split is not started
        //  - split is done
        return CurBlockIdx >= WholeBlocks;
    }

    void ResetSplit() {
        WholeBlocks = 0;
        CurBlockIdx = 0;
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

struct TErasureType {

    enum EErasureSpecies {
        ErasureNone = 0,
        ErasureMirror3 = 1,
        Erasure3Plus1Block = 2,
        Erasure3Plus1Stripe = 3,

        Erasure4Plus2Block = 4,
        Erasure3Plus2Block = 5,
        Erasure4Plus2Stripe = 6,
        Erasure3Plus2Stripe = 7,

        ErasureMirror3Plus2 = 8,
        ErasureMirror3dc = 9,

        Erasure4Plus3Block = 10,
        Erasure4Plus3Stripe = 11,
        Erasure3Plus3Block = 12,
        Erasure3Plus3Stripe = 13,
        Erasure2Plus3Block = 14,
        Erasure2Plus3Stripe = 15,

        Erasure2Plus2Block = 16,
        Erasure2Plus2Stripe = 17,

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

    TErasureType(EErasureSpecies s = ErasureNone)
        : ErasureSpecies(s)
    {}

    TErasureType(const TErasureType &) = default;
    TErasureType &operator =(const TErasureType &) = default;

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
        for (ui32 species = 0; species < TErasureType::ErasureSpeciesCount; ++species) {
            if (TErasureType::ErasureName[species] == name) {
                return TErasureType::EErasureSpecies(species);
            }
        }
        return TErasureType::ErasureSpeciesCount;
    }

    TErasureType::EErasureFamily ErasureFamily() const;
    ui32 ParityParts() const; // 4 + _2_
    ui32 DataParts() const; // _4_ + 2
    ui32 TotalPartCount() const; // _4_+_2_
    ui32 MinimalRestorablePartCount() const; // ? _4_ + 2
    ui32 MinimalBlockSize() const;
    // Size of user data contained in the part.
    ui64 PartUserSize(ui64 dataSize) const;
    // Size of the part including user data and crcs
    ui64 PartSize(ECrcMode crcMode, ui64 dataSize) const;
    ui64 SuggestDataSize(ECrcMode crcMode, ui64 partSize, bool roundDown) const;
    ui32 Prime() const;

    void SplitData(ECrcMode crcMode, TRope& buffer, TDataPartSet& outPartSet) const;
    void SplitData(ECrcMode crcMode, const TString& buffer, TDataPartSet& outPartSet) const {
        TRope rope(buffer);
        SplitData(crcMode, rope, outPartSet);
    }
    void IncrementalSplitData(ECrcMode crcMode, TRope& buffer, TDataPartSet& outPartSet) const;
    void IncrementalSplitData(ECrcMode crcMode, const TString& buffer, TDataPartSet& outPartSet) const {
        TRope rope(buffer);
        IncrementalSplitData(crcMode, rope, outPartSet);
    }

    void SplitDiffs(ECrcMode crcMode, ui32 dataSize, const TVector<TDiff> &diffs, TPartDiffSet& outDiffSet) const;
    void ApplyDiff(ECrcMode crcMode, ui8 *dst, const TVector<TDiff> &diffs) const;
    void MakeXorDiff(ECrcMode crcMode, ui32 dataSize, const ui8 *src, const TVector<TDiff> &inDiffs,
            TVector<TDiff> *outDiffs) const;
    void ApplyXorDiff(ECrcMode crcMode, ui32 dataSize, ui8 *dst,
            const TVector<TDiff> &diffs, ui8 fromPart, ui8 toPart) const;

    void RestoreData(ECrcMode crcMode, TDataPartSet& partSet, TRope& outBuffer, bool restoreParts,
            bool restoreFullData, bool restoreParityParts) const;
    void RestoreData(ECrcMode crcMode, TDataPartSet& partSet, bool restoreParts, bool restoreFullData,
            bool restoreParityParts) const;

    bool IsSinglePartRequest(ui32 fullDataSize, ui32 shift, ui32 size, ui32 &outPartIdx) const;
    bool IsPartialDataRequestPossible() const;
    bool IsUnknownFullDataSizePartialDataRequestPossible() const;
    void AlignPartialDataRequest(ui64 shift, ui64 size, ui64 fullDataSize, ui64 &outShift, ui64 &outSize) const;
    void BlockSplitRange(ECrcMode crcMode, ui64 blobSize, ui64 wholeBegin, ui64 wholeEnd,
            TBlockSplitRange *outRange) const;
    ui64 BlockSplitPartUsedSize(ui64 dataSize, ui32 partIdx) const;
    ui32 BlockSplitPartIndex(ui64 offset, ui64 dataSize, ui64 &outPartOffset) const;
    ui64 BlockSplitWholeOffset(ui64 dataSize, ui64 partIdx, ui64 offset) const;

    static const std::array<TString, ErasureSpeciesCount> ErasureName;
protected:
    EErasureSpecies ErasureSpecies;

    ui32 ColumnSize() const;
};

bool CheckCrcAtTheEnd(TErasureType::ECrcMode crcMode, const TContiguousSpan& buf);
bool CheckCrcAtTheEnd(TErasureType::ECrcMode crcMode, const TRope& rope);

struct TErasureSplitContext {
    ui32 MaxSizeAtOnce = 0;
    ui32 Offset = 0;

    static TErasureSplitContext Init(ui32 maxSizeAtOnce) { return {maxSizeAtOnce, 0}; }
};

bool ErasureSplit(TErasureType::ECrcMode crcMode, TErasureType erasure, const TRope& whole, std::span<TRope> parts,
    TErasureSplitContext *context = nullptr);

void ErasureRestore(TErasureType::ECrcMode crcMode, TErasureType erasure, ui32 fullSize, TRope *whole,
    std::span<TRope> parts, ui32 restoreMask, ui32 offset = 0, bool isFragment = false);

}
