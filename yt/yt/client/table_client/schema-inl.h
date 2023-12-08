#ifndef SCHEMA_INL_H_
#error "Direct inclusion of this file is not allowed, include schema.h"
// For the sake of sane code completion.
#include "schema.h"
#endif

#include <yt/yt/core/misc/numeric_helpers.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

inline TLegacyLockMask::TLegacyLockMask(TLegacyLockBitmap value)
    : Data_(value)
{ }

inline ELockType TLegacyLockMask::Get(int index) const
{
    YT_VERIFY(index >= 0);
    return ELockType((Data_ >> (BitsPerType * index)) & TypeMask);
}

inline void TLegacyLockMask::Set(int index, ELockType lock)
{
    YT_VERIFY(index >= 0);
    YT_VERIFY(lock <= MaxOldLockType);
    Data_ &= ~(TypeMask << (BitsPerType * index));
    Data_ |= static_cast<TLegacyLockBitmap>(lock) << (BitsPerType * index);
}

inline void TLegacyLockMask::Enrich(int columnCount)
{
    auto primaryLockType = Get(PrimaryLockIndex);
    auto maxLockType = primaryLockType;
    for (int index = 1; index < columnCount; ++index) {
        auto lockType = Get(index);
        Set(index, GetStrongestLock(primaryLockType, lockType));
        maxLockType = GetStrongestLock(maxLockType, lockType);
    }
    Set(PrimaryLockIndex, maxLockType);
}

inline TLegacyLockBitmap TLegacyLockMask::GetBitmap() const
{
    return Data_;
}

////////////////////////////////////////////////////////////////////////////////

inline TLockMask::TLockMask(TLockBitmap bitmap, int size)
    : Bitmap_(std::move(bitmap))
    , Size_(size)
{ }

inline ELockType TLockMask::Get(int index) const
{
    YT_VERIFY(index >= 0);
    auto wordIndex = index / LocksPerWord;
    if (wordIndex < std::ssize(Bitmap_)) {
        auto wordPosition = index & (LocksPerWord - 1);
        auto lock = (Bitmap_[wordIndex] >> (BitsPerType * wordPosition)) & LockMask;
        return CheckedEnumCast<ELockType>(lock);
    } else {
        return ELockType::None;
    }
}

inline void TLockMask::Set(int index, ELockType lock)
{
    YT_VERIFY(index >= 0);
    if (index >= Size_) {
        Reserve(index + 1);
    }

    auto& word = Bitmap_[index / LocksPerWord];
    auto wordPosition = index & (LocksPerWord - 1);

    word &= ~(LockMask << (BitsPerType * wordPosition));
    word |= static_cast<ui64>(lock) << (BitsPerType * wordPosition);
}

inline void TLockMask::Enrich(int size)
{
    if (size > Size_) {
        Reserve(size);
    }
    Size_ = size;

    auto primaryLockType = Get(PrimaryLockIndex);
    auto maxLockType = primaryLockType;
    for (int index = 1; index < size; ++index) {
        auto lockType = Get(index);
        Set(index, GetStrongestLock(primaryLockType, lockType));
        maxLockType = GetStrongestLock(maxLockType, lockType);
    }
    // TODO(gritukan): Do we really need both to promote all locks up to primary lock
    // and promote primary lock to strongest of all locks in mask?
    Set(PrimaryLockIndex, maxLockType);
}

inline int TLockMask::GetSize() const
{
    return Size_;
}

inline TLockBitmap TLockMask::GetBitmap() const
{
    return Bitmap_;
}

inline TLegacyLockMask TLockMask::ToLegacyMask() const
{
    TLegacyLockMask legacyMask;
    for (int index = 0; index < TLegacyLockMask::MaxCount; ++index) {
        legacyMask.Set(index, Get(index));
    }

    return legacyMask;
}

inline bool TLockMask::HasNewLocks() const
{
    for (int index = 0; index < Size_; ++index) {
        if (Get(index) > MaxOldLockType) {
            return true;
        }
    }

    return false;
}

inline bool TLockMask::IsNone() const
{
    for (int index = 0; index < GetSize(); ++index) {
        if (Get(index) != ELockType::None) {
            return false;
        }
    }

    return true;
}

inline void TLockMask::Reserve(int size)
{
    YT_VERIFY(size < MaxSize);

    int wordCount = DivCeil(size, LocksPerWord);
    if (wordCount > std::ssize(Bitmap_)) {
        Bitmap_.resize(wordCount);
    }

    Size_ = size;
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool operator < (ESchemaCompatibility lhs, ESchemaCompatibility rhs)
{
    return static_cast<int>(lhs) < static_cast<int>(rhs);
}

constexpr bool operator > (ESchemaCompatibility lhs, ESchemaCompatibility rhs)
{
    return rhs < lhs;
}

constexpr bool operator <= (ESchemaCompatibility lhs, ESchemaCompatibility rhs)
{
    return !(rhs < lhs);
}

constexpr bool operator >= (ESchemaCompatibility lhs, ESchemaCompatibility rhs)
{
    return !(lhs < rhs);
}

////////////////////////////////////////////////////////////////////////////////

inline size_t TTableSchemaHash::operator() (const TTableSchema& schema) const
{
    return THash<TTableSchema>()(schema);
}

inline size_t TTableSchemaHash::operator() (const TTableSchemaPtr& schema) const
{
    return THash<TTableSchema>()(*schema);
}

////////////////////////////////////////////////////////////////////////////////

inline bool TTableSchemaEquals::operator() (const TTableSchema& lhs, const TTableSchema& rhs) const
{
    return lhs == rhs;
}

inline bool TTableSchemaEquals::operator() (const TTableSchemaPtr& lhs, const TTableSchemaPtr& rhs) const
{
    return *lhs == *rhs;
}

inline bool TTableSchemaEquals::operator() (const TTableSchemaPtr& lhs, const TTableSchema& rhs) const
{
    return *lhs == rhs;
}

////////////////////////////////////////////////////////////////////////////////

inline size_t TCellTaggedTableSchemaHash::operator() (const TCellTaggedTableSchema& cellTaggedSchema) const
{
    return MultiHash(cellTaggedSchema.TableSchema, cellTaggedSchema.CellTag);
}

inline size_t TCellTaggedTableSchemaHash::operator() (const TCellTaggedTableSchemaPtr& cellTaggedSchemaPtr) const
{
    YT_ASSERT(cellTaggedSchemaPtr.TableSchema);
    return MultiHash(*cellTaggedSchemaPtr.TableSchema, cellTaggedSchemaPtr.CellTag);
}

////////////////////////////////////////////////////////////////////////////////

inline bool TCellTaggedTableSchemaEquals::operator() (const TCellTaggedTableSchema& lhs, const TCellTaggedTableSchema& rhs) const
{
    return lhs.TableSchema == rhs.TableSchema && lhs.CellTag == rhs.CellTag;
}

inline bool TCellTaggedTableSchemaEquals::operator() (const TCellTaggedTableSchemaPtr& lhs, const TCellTaggedTableSchemaPtr& rhs) const
{
    YT_ASSERT(lhs.TableSchema && rhs.TableSchema);
    return *lhs.TableSchema == *rhs.TableSchema && lhs.CellTag == rhs.CellTag;
}

inline bool TCellTaggedTableSchemaEquals::operator() (const TCellTaggedTableSchemaPtr& lhs, const TCellTaggedTableSchema& rhs) const
{
    YT_ASSERT(lhs.TableSchema);
    return *lhs.TableSchema == rhs.TableSchema && lhs.CellTag == rhs.CellTag;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
