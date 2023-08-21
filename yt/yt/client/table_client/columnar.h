#pragma once

#include "public.h"

#include <yt/yt/core/misc/range.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Returns the minimum number of bytes needed to store a bitmap with #bitCount bits.
i64 GetBitmapByteSize(i64 bitCount);

//! Builds validity bitmap from #dictionaryIndexes by replacing each zero with
//! 0-bit and each non-zero with 1-bit.
void BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TMutableRef dst);

//! Same as #BuildValidityBitmapFromDictionaryIndexesWithZeroNull but for RLE.
void BuildValidityBitmapFromRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRef dst);

//! Builds validity bitmap from #dictionaryIndexes by replacing each zero with
//! 1-byte and each non-zero with 0-byte.
//! The size of #dst must match the size of #dictionaryIndexes.
template <typename TByte>
void BuildNullBytemapFromDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TMutableRange<TByte> dst);

//! Same as #BuildNullBytmapFromDictionaryIndexesWithZeroNull but for RLE.
template <typename TByte>
void BuildNullBytemapFromRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<TByte> dst);

//! Copies dictionary indexes from #dictionaryIndexes to #dst subtracting one.
//! Zeroes are replaced by unspecified values.
//! The size of #dst must be exactly |endIndex - startIndex|.
void BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TMutableRange<ui32> dst);

//! Same as #BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull but for RLE.
void BuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<ui32> dst);

//! Writes a "iota"-like sequence (0, 1, 2, ...) to #dst indicating
//! RLE groups in accordance to #rleIndexes.
//! The size of #dst must be exactly |endIndex - startIndex|.
void BuildIotaDictionaryIndexesFromRleIndexes(
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<ui32> dst);

//! Counts the number of #indexes equal to zero.
i64 CountNullsInDictionaryIndexesWithZeroNull(TRange<ui32> indexes);

//! Same as #CountNullsInDictionaryIndexesWithZeroNull but for RLE.
i64 CountNullsInRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex);

//! Counts the number of 1-bits in range [#startIndex, #endIndex) in #bitmap.
i64 CountOnesInBitmap(
    TRef bitmap,
    i64 startIndex,
    i64 endIndex);

//! Same as #CountOnesInBitmap but for RLE.
i64 CountOnesInRleBitmap(
    TRef bitmap,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex);

//! Copies bits in range [#startIndex, #endIndex) from #bitmap to #dst (representing another bitmap).
//! #bitmap must be 8-byte aligned and its trailing qword must be readable.
//! The byte size of #dst be be enough to store |endIndex - startIndex| bits.
void CopyBitmapRangeToBitmap(
    TRef bitmap,
    i64 startIndex,
    i64 endIndex,
    TMutableRef dst);

//! Same as #CopyBitmapRangeToBitmap but inverts the bits.
void CopyBitmapRangeToBitmapNegated(
    TRef bitmap,
    i64 startIndex,
    i64 endIndex,
    TMutableRef dst);

//! Decodes bits in range [#startIndex, #endIndex) from #bitmap to #dst bytemap
//! (0 indicates |false|, 1 indicates |true|).
//! #bitmap must be 8-byte aligned and its trailing qword must be readable.
//! The size of #dst must be |endIndex - startIndex|.
template <typename TByte>
void DecodeBytemapFromBitmap(
    TRef bitmap,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<TByte> dst);

//! Decodes RLE #bitmap and inverts the bits.
//! The byte size of #dst be enough to store |endIndex - startIndex| bits.
void BuildValidityBitmapFromRleNullBitmap(
    TRef bitmap,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRef dst);

//! Decodes RLE #bitmap into #dst bytemap.
//! The size of #dst be |endIndex - startIndex|.
template <typename TByte>
void BuildNullBytemapFromRleNullBitmap(
    TRef bitmap,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<TByte> dst);

//! Decodes the starting offset of the #index-th string.
//! #index must be in range [0, N], where N is the size of #offsets.
i64 DecodeStringOffset(
    TRange<ui32> offsets,
    ui32 avgLength,
    i64 index);

//! Decodes the starting offset and the ending offset of the #index-th string.
//! #index must be in range [0, N), where N is the size of #offsets.
std::pair<i64, i64> DecodeStringRange(
    TRange<ui32> offsets,
    ui32 avgLength,
    i64 index);

//! Decodes start offsets of strings in [#startIndex, #endIndex)
//! range given by #offsets. The resulting array contains 32-bit offsets and
//! must be of size |endIndex - startIndex + 1| (the last element will indicate the
//! offset where the last string ends).
void DecodeStringOffsets(
    TRange<ui32> offsets,
    ui32 avgLength,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<ui32> dst);

//! Decodes start pointers and lengths in [#startIndex, #endIndex) row range.
//! The size of #strings and #lengths must be |endIndex - startIndex|.
void DecodeStringPointersAndLengths(
    TRange<ui32> offsets,
    ui32 avgLength,
    TRef stringData,
    TMutableRange<const char*> strings,
    TMutableRange<i32> lengths);

//! Computes the total length of RLE and dictionary-encoded strings in
//! a given row range [#startIndex, #endIndex).
i64 CountTotalStringLengthInRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TRange<i32> stringLengths,
    i64 startIndex,
    i64 endIndex);

//! Given #rleIndexes, translates #index in actual rowspace to
//! to the index in RLE rowspace.
i64 TranslateRleIndex(
    TRange<ui64> rleIndexes,
    i64 index);

//! A synonym for TranslateRleIndex.
i64 TranslateRleStartIndex(
    TRange<ui64> rleIndexes,
    i64 index);

//! Regards #index as the end (non-inclusive) index.
//! Equivalent to |TranslateRleIndex(index - 1) + 1| (unless index = 0).
i64 TranslateRleEndIndex(
    TRange<ui64> rleIndexes,
    i64 index);

//! A unversal decoder for dictionary-encoded and RLE integer vectors.
//! Values are first retrieved via #fetcher, then
//! pass through #DecodeIntegerValue and finally are forwarded
//! to #consumer.
template <
    class TFetcher,
    class TConsumer
>
void DecodeIntegerVector(
    i64 startIndex,
    i64 endIndex,
    ui64 baseValue,
    bool zigZagEncoded,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TFetcher fetcher,
    TConsumer consumer);

//! A unversal decoder for dictionary-encoded and RLE vectors of raw values.
//! Values are first retrieved via #fetcher and then are forwarded
//! to #consumer.
template <
    class T,
    class TFetcher,
    class TConsumer
>
void DecodeRawVector(
    i64 startIndex,
    i64 endIndex,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TFetcher fetcher,
    TConsumer consumer);

//! Decodes a single integer from YT chunk representation.
template <class T>
T DecodeIntegerValue(
    ui64 value,
    ui64 baseValue,
    bool zigZagEncoded);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define COLUMNAR_INL_H_
#include "columnar-inl.h"
#undef COLUMNAR_INL_H_
