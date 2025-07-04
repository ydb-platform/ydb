#pragma once

#include "defs.h"
#include "scheme_type_order.h"
#include "scheme_types_defs.h"

#include <util/generic/bitops.h>
#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/system/unaligned_mem.h>
#include <util/memory/pool.h>

#include <bit>
#include <type_traits>

namespace NKikimr {

static_assert(std::endian::native == std::endian::little, "TCell expects little endian architecture for data packing");

// Represents one element in a tuple
// Doesn't own the memory buffer that stores the actual value
// Small values (<= 14 bytes) are stored inline
struct TCell {
    template<typename T>
    using TStdLayout = std::enable_if_t<std::is_standard_layout<T>::value, T>;

public:
    // 14 bytes ensures parity with TUnboxedValuePod
    static constexpr size_t MaxInlineSize() { return 14; }
    static constexpr bool CanInline(size_t size) { return size <= MaxInlineSize(); }

private:
    // NotInline (low bit):
    //   0: IsInline() returns true
    //   1: IsInline() returns false
    // NotRefValue (high bit):
    //   0: data is TRefValue
    //   1: data is TInlineValue
    // Null values are IsInline() with a nullptr data pointer and zero size
    // TCell filled with zeroes is always a null value
    // This bit arrangment allows methods to check a single flag
    enum EKind : ui8 {
        // Both bits 0: null value
        KindNull = 0,
        // Low bit 1: ref value
        KindRefValue = 1,
        // High bit 1: inline value
        KindInlineValue = 2,
    };

    struct TRefValue {
        const char* Ptr;
        ui64 Size : 62;
        ui64 Kind : 2;
    };

    struct TInlineValue {
        char Data[15];
        ui8 Size : 6;
        ui8 Kind : 2;
    };

private:
    union {
        char Raw[16];
        TRefValue Ref;
        TInlineValue Inline;
    };

    constexpr ui8 GetKind() const noexcept {
        return ui8(Raw[15]) >> 6;
    }

    constexpr bool IsRefValue() const noexcept {
        return (GetKind() & 2) == 0;
    }

    constexpr bool HasInlineFlag() const noexcept {
        return (GetKind() & 1) == 0;
    }

public:
    TCell()
        : TCell(nullptr, 0)
    {}

    TCell(TArrayRef<const char> ref)
        : TCell(ref.data(), ref.size())
    {}

    TCell(const char* ptr, size_t size) {
        if (!ptr) {
            Y_ASSERT(size == 0);
            // All zeroes represents the null value
            ::memset(Raw, 0, 16);
        } else if (CanInline(size)) {
            // We use switch with constant size memcpy for better codegen
            switch (size) {
                case 15: ::memcpy(Inline.Data, ptr, 15); break;
                case 14: ::memcpy(Inline.Data, ptr, 14); break;
                case 13: ::memcpy(Inline.Data, ptr, 13); break;
                case 12: ::memcpy(Inline.Data, ptr, 12); break;
                case 11: ::memcpy(Inline.Data, ptr, 11); break;
                case 10: ::memcpy(Inline.Data, ptr, 10); break;
                case 9: ::memcpy(Inline.Data, ptr, 9); break;
                case 8: ::memcpy(Inline.Data, ptr, 8); break;
                case 7: ::memcpy(Inline.Data, ptr, 7); break;
                case 6: ::memcpy(Inline.Data, ptr, 6); break;
                case 5: ::memcpy(Inline.Data, ptr, 5); break;
                case 4: ::memcpy(Inline.Data, ptr, 4); break;
                case 3: ::memcpy(Inline.Data, ptr, 3); break;
                case 2: ::memcpy(Inline.Data, ptr, 2); break;
                case 1: ::memcpy(Inline.Data, ptr, 1); break;
                case 0: break;
                default: Y_ENSURE(false, "unreachable");
            }
            Inline.Size = size;
            Inline.Kind = KindInlineValue;
        } else {
            Y_ASSERT(size <= Max<ui32>());
            Ref.Ptr = ptr;
            Ref.Size = size;
            Ref.Kind = KindRefValue;
        }
    }

    explicit TCell(const TRawTypeValue* v)
        : TCell((const char*)v->Data(), v->Size())
    {}

    constexpr explicit operator bool() const {
        return !IsNull();
    }

    constexpr bool IsNull() const {
        return GetKind() == KindNull;
    }

    constexpr bool IsInline() const {
        return HasInlineFlag();
    }

    constexpr const char* InlineData() const {
        Y_ASSERT(IsInline());
        return Raw;
    }

    constexpr const char* Data() const {
        return IsRefValue() ? Ref.Ptr : Inline.Data;
    }

    constexpr ui32 Size() const {
        return IsRefValue() ? Ref.Size : Inline.Size;
    }

    TArrayRef<const char> AsRef() const noexcept
    {
        return { Data(), Size() };
    }

    TStringBuf AsBuf() const noexcept
    {
        return { Data(), Size() };
    }

    template<typename T, typename = TStdLayout<T>>
    T AsValue() const
    {
        Y_ENSURE(sizeof(T) == Size(), "AsValue<T>() type size" << sizeof(T) << " doesn't match TCell size " << Size());

        return ReadUnaligned<T>(Data());
    }

    template <typename T, typename = TStdLayout<T>>
    bool ToValue(T& value, TString& err) const {
        if (Y_UNLIKELY(sizeof(T) != Size())) {
            err = TStringBuilder() << "ToValue<T>() type size " << sizeof(T) << " doesn't match TCell size " << Size();
            return false;
        }

        value = ReadUnaligned<T>(Data());
        return true;
    }

    template <typename T, typename = TStdLayout<T>>
    bool ToStream(IOutputStream& out, TString& err) const {
        T value;
        if (!ToValue(value, err))
            return false;

        out << value;
        return true;
    }

    template <typename T, typename = TStdLayout<T>>
    static inline TCell Make(const T& val) noexcept {
        auto *ptr = static_cast<const char*>(static_cast<const void*>(&val));

        return TCell(ptr, sizeof(val));
    }

    void CopyDataInto(char* dst) const {
        if (IsRefValue()) {
            if (Ref.Size > 0) {
                ::memcpy(dst, Ref.Ptr, Ref.Size);
            }
        } else {
            // We use switch with constant size memcpy for better codegen
            switch (Inline.Size) {
                case 15: ::memcpy(dst, Inline.Data, 15); break;
                case 14: ::memcpy(dst, Inline.Data, 14); break;
                case 13: ::memcpy(dst, Inline.Data, 13); break;
                case 12: ::memcpy(dst, Inline.Data, 12); break;
                case 11: ::memcpy(dst, Inline.Data, 11); break;
                case 10: ::memcpy(dst, Inline.Data, 10); break;
                case 9: ::memcpy(dst, Inline.Data, 9); break;
                case 8: ::memcpy(dst, Inline.Data, 8); break;
                case 7: ::memcpy(dst, Inline.Data, 7); break;
                case 6: ::memcpy(dst, Inline.Data, 6); break;
                case 5: ::memcpy(dst, Inline.Data, 5); break;
                case 4: ::memcpy(dst, Inline.Data, 4); break;
                case 3: ::memcpy(dst, Inline.Data, 3); break;
                case 2: ::memcpy(dst, Inline.Data, 2); break;
                case 1: ::memcpy(dst, Inline.Data, 1); break;
                case 0: break;
                default: Y_ENSURE(false, "unreachable");
            }
        }
    }
};

static_assert(sizeof(TCell) == 16, "TCell must be 16 bytes");
using TCellsRef = TConstArrayRef<const TCell>;

inline size_t EstimateSize(TCellsRef cells) {
    size_t cellsSize = cells.size();

    size_t size = sizeof(TCell) * cellsSize;
    for (auto& cell : cells) {
        if (!cell.IsNull() && !cell.IsInline()) {
            const size_t cellSize = cell.Size();
            size += AlignUp(cellSize, size_t(4));
        }
    }

    return size;
}

struct TCellVectorsHash {
    using is_transparent = void;

    size_t operator()(TConstArrayRef<TCell> key) const;
};

struct TCellVectorsEquals {
    using is_transparent = void;

    bool operator()(TConstArrayRef<TCell> a, TConstArrayRef<TCell> b) const;
};

inline int CompareCellsAsByteString(const TCell& a, const TCell& b, bool isDescending) {
    const char* pa = a.Data();
    const char* pb = b.Data();
    size_t sza = a.Size();
    size_t szb = b.Size();
    int cmp = memcmp(pa, pb, sza < szb ? sza : szb);
    if (cmp != 0)
        return isDescending ? (cmp > 0 ? -1 : +1) : cmp; // N.B. cannot multiply, may overflow
    return sza == szb ? 0 : ((sza < szb) != isDescending ? -1 : 1);
}

// NULL is considered equal to another NULL and less than non-NULL
// ATTENTION!!! return value is int!! (NOT just -1,0,1)
inline int CompareTypedCells(const TCell& a, const TCell& b, const NScheme::TTypeInfoOrder& type) {
    using TPair = std::pair<ui64, ui64>;
    if (a.IsNull())
        return b.IsNull() ? 0 : -1;
    if (b.IsNull())
        return 1;

    switch (type.GetTypeId()) {

#define SIMPLE_TYPE_SWITCH(typeEnum, castType)      \
    case NKikimr::NScheme::NTypeIds::typeEnum:      \
    {                                               \
        Y_ASSERT(a.Size() == sizeof(castType));     \
        Y_ASSERT(b.Size() == sizeof(castType));     \
        castType va = ReadUnaligned<castType>((const castType*)a.InlineData()); \
        castType vb = ReadUnaligned<castType>((const castType*)b.InlineData()); \
        return va == vb ? 0 : ((va < vb) != type.IsDescending() ? -1 : 1);   \
    }

#define LARGER_TYPE_SWITCH(typeEnum, castType)      \
    case NKikimr::NScheme::NTypeIds::typeEnum:      \
    {                                               \
        castType va = ReadUnaligned<castType>((const castType*)a.Data()); \
        castType vb = ReadUnaligned<castType>((const castType*)b.Data()); \
        return va == vb ? 0 : ((va < vb) != type.IsDescending() ? -1 : 1);   \
    }

    SIMPLE_TYPE_SWITCH(Int8,   i8);
    SIMPLE_TYPE_SWITCH(Int16,  i16);
    SIMPLE_TYPE_SWITCH(Uint16, ui16);
    SIMPLE_TYPE_SWITCH(Int32,  i32);
    SIMPLE_TYPE_SWITCH(Uint32, ui32);
    SIMPLE_TYPE_SWITCH(Int64,  i64);
    SIMPLE_TYPE_SWITCH(Uint64, ui64);
    SIMPLE_TYPE_SWITCH(Byte,   ui8);
    SIMPLE_TYPE_SWITCH(Bool,   ui8);
    SIMPLE_TYPE_SWITCH(Double, double);
    SIMPLE_TYPE_SWITCH(Float,  float);
    LARGER_TYPE_SWITCH(PairUi64Ui64, TPair);
    SIMPLE_TYPE_SWITCH(Date,   ui16);
    SIMPLE_TYPE_SWITCH(Datetime,  ui32);
    SIMPLE_TYPE_SWITCH(Timestamp, ui64);
    SIMPLE_TYPE_SWITCH(Interval,  i64);
    SIMPLE_TYPE_SWITCH(Date32, i32);
    SIMPLE_TYPE_SWITCH(Datetime64, i64);
    SIMPLE_TYPE_SWITCH(Timestamp64, i64);
    SIMPLE_TYPE_SWITCH(Interval64, i64);

#undef SIMPLE_TYPE_SWITCH
#undef LARGER_TYPE_SWITCH

    case NKikimr::NScheme::NTypeIds::String:
    case NKikimr::NScheme::NTypeIds::String4k:
    case NKikimr::NScheme::NTypeIds::String2m:
    case NKikimr::NScheme::NTypeIds::Utf8:
    case NKikimr::NScheme::NTypeIds::Json:
    case NKikimr::NScheme::NTypeIds::Yson:
    // XXX: using memcmp is meaningless for both JsonDocument and Json
    case NKikimr::NScheme::NTypeIds::JsonDocument:
    case NKikimr::NScheme::NTypeIds::DyNumber:
    {
        return CompareCellsAsByteString(a, b, type.IsDescending());
    }

    case NKikimr::NScheme::NTypeIds::Uuid:
    {
        Y_ASSERT(a.Size() == 16);
        Y_ASSERT(b.Size() == 16);
        return CompareCellsAsByteString(a, b, type.IsDescending());
    }

    case NKikimr::NScheme::NTypeIds::Decimal:
    {
        Y_ASSERT(a.Size() == sizeof(std::pair<ui64, i64>));
        Y_ASSERT(b.Size() == sizeof(std::pair<ui64, i64>));
        std::pair<ui64, i64> va = ReadUnaligned<std::pair<ui64, i64>>((const std::pair<ui64, i64>*)a.Data());
        std::pair<ui64, i64> vb = ReadUnaligned<std::pair<ui64, i64>>((const std::pair<ui64, i64>*)b.Data());
        if (va.second == vb.second)
            return va.first == vb.first ? 0 : ((va.first < vb.first) != type.IsDescending() ? -1 : 1);
        return (va.second < vb.second) != type.IsDescending() ? -1 : 1;
    }

    case NKikimr::NScheme::NTypeIds::Pg:
    {
        auto typeDesc = type.GetPgTypeDesc();
        Y_ENSURE(typeDesc, "no pg type descriptor");
        int result = NPg::PgNativeBinaryCompare(a.Data(), a.Size(), b.Data(), b.Size(), typeDesc);
        return type.IsDescending() ? -result : result;
    }

    default:
        Y_ENSURE(false, "Unknown type");
    };

    return 0;
}

// ATTENTION!!! return value is int!! (NOT just -1,0,1)
template<class TTypeClass>
inline int CompareTypedCellVectors(const TCell* a, const TCell* b, const TTypeClass* type, const ui32 cnt) {
    for (ui32 i = 0; i < cnt; ++i) {
        int cmpRes = CompareTypedCells(a[i], b[i], type[i]);
        if (cmpRes != 0)
            return cmpRes;
    }
    return 0;
}

/// @warning Do not use this func to compare key with a range border. Partial key means it ends with Nulls here.
// ATTENTION!!! return value is int!! (NOT just -1,0,1)
template<class TTypeClass>
inline int CompareTypedCellVectors(const TCell* a, const TCell* b, const TTypeClass* type, const ui32 cnt_a, const ui32 cnt_b) {
    Y_ENSURE(cnt_b <= cnt_a);
    ui32 i = 0;
    for (; i < cnt_b; ++i) {
        int cmpRes = CompareTypedCells(a[i], b[i], type[i]);
        if (cmpRes != 0)
            return cmpRes;
    }
    for (; i < cnt_a; ++i) {
        if (!a[i].IsNull())
            return 1;
    }
    return 0;
}

// Bool with NULL semantics.
// Has operator<< for output to stream.
enum class ETriBool {
    False,
    True,
    Null,
};

inline ETriBool operator&&(ETriBool a, ETriBool b) {
    if (a == ETriBool::False || b == ETriBool::False) {
        return ETriBool::False;
    }
    if (a == ETriBool::Null || b == ETriBool::Null) {
        return ETriBool::Null;
    }
    return ETriBool::True;
}

inline ETriBool operator||(ETriBool a, ETriBool b) {
    if (a == ETriBool::True || b == ETriBool::True) {
        return ETriBool::True;
    }
    if (a == ETriBool::Null || b == ETriBool::Null) {
        return ETriBool::Null;
    }
    return ETriBool::False;
}

// Compare two TCell's for equality taking into account the NULL-value semantics.
// If one of the cells is NULL, the result of comparison is NULL.
// Else, the result is True when a == b and False when a != b.
inline ETriBool TypedCellsEqualWithNullSemantics(const TCell& a, const TCell& b, const NScheme::TTypeInfoOrder& type) {
    if (a.IsNull() || b.IsNull()) {
        return ETriBool::Null;
    }
    int res = CompareTypedCells(a, b, type);
    return res == 0 ? ETriBool::True : ETriBool::False;
}

// Compare two TCell vectors for equality taking into account the NULL-value semantics.
// If there is no NULL fields from both sides, the result is True when all the fields are equal and False otherwise.
// If all the fields that are NOT NULL are equal, but we have NULLs, the result is NULL.
// If we have two NOT NULL fields that are not equal, the result is False.
// See CompareWithNullSemantics test.
template<class TTypeClass>
inline ETriBool TypedCellVectorsEqualWithNullSemantics(const TCell* a, const TCell* b, const TTypeClass* type, const ui32 cnt) {
    ETriBool result = ETriBool::True;
    for (ui32 i = 0; i < cnt; ++i) {
        result = result && TypedCellsEqualWithNullSemantics(a[i], b[i], type[i]);
        if (result == ETriBool::False) {
            break;
        }
    }
    return result;
}

// TODO: use NYql ops when TCell and TUnboxedValuePod had merged
inline ui64 GetValueHash(NScheme::TTypeInfo info, const TCell& cell) {
    if (cell.IsNull())
        return 0;

    auto typeId = info.GetTypeId();
    const NYql::NProto::TypeIds yqlType = static_cast<NYql::NProto::TypeIds>(typeId);
    switch (yqlType) {
    case NYql::NProto::TypeIds::Bool:
        return ((*(const ui8 *)cell.Data()) == 0) ? THash<ui8>()((ui8)0) : THash<ui8>()((ui8)1);
    case NYql::NProto::TypeIds::Int8:
        return THash<i8>()(*(const i8*)cell.Data());
    case NYql::NProto::TypeIds::Uint8:
        return THash<ui8>()(*(const ui8*)cell.Data());
    case NYql::NProto::TypeIds::Int16:
        return THash<i16>()(*(const i16*)cell.Data());
    case NYql::NProto::TypeIds::Uint16:
        return THash<ui16>()(*(const ui16*)cell.Data());
    case NYql::NProto::TypeIds::Int32:
    case NYql::NProto::TypeIds::Date32:
        return THash<i32>()(ReadUnaligned<i32>((const i32*)cell.Data()));
    case NYql::NProto::TypeIds::Uint32:
        return THash<ui32>()(ReadUnaligned<ui32>((const ui32*)cell.Data()));
    case NYql::NProto::TypeIds::Int64:
    case NYql::NProto::TypeIds::Datetime64:
    case NYql::NProto::TypeIds::Timestamp64:
    case NYql::NProto::TypeIds::Interval64:
        return THash<i64>()(ReadUnaligned<i64>((const i64*)cell.Data()));
    case NYql::NProto::TypeIds::Uint64:
        return THash<ui64>()(ReadUnaligned<ui64>((const ui64*)cell.Data()));
    case NYql::NProto::TypeIds::Float:
        return THash<float>()(ReadUnaligned<float>((const float*)cell.Data()));
    case NYql::NProto::TypeIds::Double:
        return THash<double>()(ReadUnaligned<double>((const double*)cell.Data()));

    case NYql::NProto::TypeIds::Date:
        return THash<ui16>()(ReadUnaligned<ui16>((const ui16*)cell.Data()));
    case NYql::NProto::TypeIds::Datetime:
        return THash<ui32>()(ReadUnaligned<ui32>((const ui32*)cell.Data()));
    case NYql::NProto::TypeIds::Timestamp:
        return THash<ui32>()(ReadUnaligned<ui64>((const ui64*)cell.Data()));
    case NYql::NProto::TypeIds::Interval:
        return THash<ui32>()(ReadUnaligned<ui64>((const ui64*)cell.Data()));

    case NYql::NProto::TypeIds::String:
    case NYql::NProto::TypeIds::Utf8:
    case NYql::NProto::TypeIds::Yson:
    case NYql::NProto::TypeIds::Json:
    case NYql::NProto::TypeIds::Decimal:
    case NYql::NProto::TypeIds::JsonDocument:
    case NYql::NProto::TypeIds::DyNumber:
    case NYql::NProto::TypeIds::Uuid:
        return ComputeHash(TStringBuf{cell.Data(), cell.Size()});

    default:
        break;
    }

    if (typeId == NKikimr::NScheme::NTypeIds::Pg) {
        auto typeDesc = info.GetPgTypeDesc();
        Y_ENSURE(typeDesc, "no pg type descriptor");
        return NPg::PgNativeBinaryHash(cell.Data(), cell.Size(), typeDesc);
    }

    Y_ENSURE(false, "Type not supported for user columns: " << typeId);
    return 0;
}

// Only references a vector of cells and corresponding types
// Doesn't own the memory
struct TDbTupleRef {
    const NKikimr::NScheme::TTypeInfo* Types;
    const TCell* Columns;
    ui32 ColumnCount;

    TArrayRef<const TCell> Cells() const {
        return { Columns, ColumnCount };
    }

    TDbTupleRef(const NScheme::TTypeInfo* types = nullptr, const TCell* storage = nullptr, ui32 colCnt = 0)
        : Types(types)
        , Columns(storage)
        , ColumnCount(colCnt)
    {}
};

// An array of cells that owns its data and may be safely copied/moved
class TOwnedCellVec
    : public TConstArrayRef<TCell>
{
private:
    typedef TConstArrayRef<TCell> TCellVec;

    class TData : public TAtomicRefCount<TData> {
    public:
        const size_t DataSize;

        explicit TData(size_t size)
            : DataSize(size)
        {}

        void operator delete(TData* data, std::destroying_delete_t) noexcept;
    };

    struct TInit {
        TCellVec Cells;
        TIntrusivePtr<TData> Data;
    };

    TOwnedCellVec(TInit init) noexcept
        : TCellVec(std::move(init.Cells))
        , Data(std::move(init.Data))
    { }

    static TInit Allocate(TCellVec cells);

    static TInit AllocateFromSerialized(std::string_view data);

    TCellVec& CellVec() {
        return static_cast<TCellVec&>(*this);
    }

public:
    TOwnedCellVec() noexcept
        : TCellVec()
    { }

    explicit TOwnedCellVec(TCellVec cells)
        : TOwnedCellVec(Allocate(cells))
    { }

    static TOwnedCellVec Make(TCellVec cells) {
        return TOwnedCellVec(Allocate(cells));
    }

    static TOwnedCellVec FromSerialized(std::string_view data) {
        return TOwnedCellVec(AllocateFromSerialized(data));
    }

    TOwnedCellVec(const TOwnedCellVec& rhs) noexcept
        : TCellVec(rhs)
        , Data(rhs.Data)
    { }

    TOwnedCellVec(TOwnedCellVec&& rhs) noexcept
        : TCellVec(rhs)
        , Data(std::move(rhs.Data))
    {
        rhs.CellVec() = { };
    }

    TOwnedCellVec& operator=(const TOwnedCellVec& rhs) noexcept {
        if (Y_LIKELY(this != &rhs)) {
            Data = rhs.Data;
            CellVec() = rhs;
        }

        return *this;
    }

    TOwnedCellVec& operator=(TOwnedCellVec&& rhs) noexcept {
        if (Y_LIKELY(this != &rhs)) {
            Data = std::move(rhs.Data);
            CellVec() = rhs;
            rhs.CellVec() = { };
        }

        return *this;
    }

    size_t DataSize() const {
        return Data ? Data->DataSize : 0;
    }

private:
    TIntrusivePtr<TData> Data;
};

static_assert(std::is_nothrow_destructible_v<TOwnedCellVec>, "Expected TOwnedCellVec to be nothrow destructible");
static_assert(std::is_nothrow_copy_constructible_v<TOwnedCellVec>, "Expected TOwnedCellVec to be nothrow copy constructible");
static_assert(std::is_nothrow_move_constructible_v<TOwnedCellVec>, "Expected TOwnedCellVec to be nothrow move constructible");
static_assert(std::is_nothrow_default_constructible_v<TOwnedCellVec>, "Expected TOwnedCellVec to be nothrow default constructible");

// Used to store/load a vector of TCell in bytes array
// When loading from a buffer the cells will point to the buffer contents
class TSerializedCellVec {
public:
    TSerializedCellVec() = default;

    explicit TSerializedCellVec(TConstArrayRef<TCell> cells);

    explicit TSerializedCellVec(TString&& buf)
        : Buf(std::move(buf))
    {
        Y_ENSURE(DoTryParse());
    }

    explicit TSerializedCellVec(const TString& buf)
        : Buf(buf)
    {
        Y_ENSURE(DoTryParse());
    }

    TSerializedCellVec(TSerializedCellVec&& other)
    {
        *this = std::move(other);
    }

    TSerializedCellVec(const TSerializedCellVec& other)
    {
        *this = other;
    }

    TSerializedCellVec& operator=(TSerializedCellVec&& other)
    {
        if (this == &other)
            return *this;

        const char* prevPtr = other.Buf.data();
        Buf = std::move(other.Buf);
        if (Buf.data() != prevPtr) {
            // Data address changed, e.g. when TString is a small std::string
            Y_ENSURE(DoTryParse(), "Failed to re-parse TSerializedCellVec");
            other.Cells.clear();
        } else {
            // Data address unchanged, reuse parsed Cells
            Cells = std::move(other.Cells);
        }
        return *this;
    }

    TSerializedCellVec& operator=(const TSerializedCellVec& other)
    {
        if (this == &other)
            return *this;

        Buf = other.Buf;
        if (Buf.data() != other.Buf.data()) {
            // Data address changed, e.g. when TString is std::string
            Y_ENSURE(DoTryParse(), "Failed to re-parse TSerializedCellVec");
        } else {
            // Data address unchanged, reuse parsed Cells
            Cells = other.Cells;
        }
        return *this;
    }

    static bool TryParse(TString&& data, TSerializedCellVec& vec) {
        vec.Buf = std::move(data);
        return vec.DoTryParse();
    }

    static bool TryParse(const TString& data, TSerializedCellVec& vec) {
        vec.Buf = data;
        return vec.DoTryParse();
    }

    void Parse(TString&& buf) {
        Buf = std::move(buf);
        Y_ENSURE(DoTryParse());
    }

    void Parse(const TString& buf) {
        Buf = buf;
        Y_ENSURE(DoTryParse());
    }

    TConstArrayRef<TCell> GetCells() const {
        return Cells;
    }

    explicit operator bool() const
    {
        return !Cells.empty();
    }

    // read headers, assuming the buf is correct and append additional cells at the end
    static bool UnsafeAppendCells(TConstArrayRef<TCell> cells, TString& serializedCellVec);

    static void Serialize(TString& res, TConstArrayRef<TCell> cells);

    static TString Serialize(TConstArrayRef<TCell> cells);

    static size_t SerializedSize(TConstArrayRef<TCell> cells);

    static TCell ExtractCell(std::string_view data, size_t pos);

    const TString& GetBuffer() const { return Buf; }

    TString ReleaseBuffer() {
        Cells.clear();
        return std::move(Buf);
    }

private:
    bool DoTryParse();

private:
    TString Buf;
    TVector<TCell> Cells;
};

// Used to store/load a matrix of TCell in bytes array
// When loading from a buffer the cells will point to the buffer contents
class TSerializedCellMatrix {
public:
    TSerializedCellMatrix() = default;

    explicit TSerializedCellMatrix(TConstArrayRef<TCell> cells, ui32 rowCount, ui16 colCount);

    explicit TSerializedCellMatrix(TString&& buf)
        : Buf(std::move(buf))
    {
        Y_ENSURE(DoTryParse());
    }

    explicit TSerializedCellMatrix(const TString& buf)
        : Buf(buf)
    {
        Y_ENSURE(DoTryParse());
    }

    TSerializedCellMatrix(TSerializedCellMatrix&& other)
    {
        *this = std::move(other);
    }

    TSerializedCellMatrix(const TSerializedCellMatrix& other)
    {
        *this = other;
    }

    TSerializedCellMatrix& operator=(TSerializedCellMatrix&& other)
    {
        if (this == &other)
            return *this;

        const char* prevPtr = other.Buf.data();
        Buf = std::move(other.Buf);
        if (Buf.data() != prevPtr) {
            // Data address changed, e.g. when TString is a small std::string
            Y_ENSURE(DoTryParse(), "Failed to re-parse TSerializedCellMatrix");
            other.Cells.clear();
        } else {
            // Data address unchanged, reuse parsed cells
            Cells = std::move(other.Cells);
            RowCount = other.RowCount;
            ColCount = other.ColCount;
        }
        other.RowCount = 0;
        other.ColCount = 0;
        return *this;
    }

    TSerializedCellMatrix& operator=(const TSerializedCellMatrix& other)
    {
        if (this == &other)
            return *this;

        Buf = other.Buf;
        if (Buf.data() != other.Buf.data()) {
            // Data address changed, e.g. when TString is std::string
            Y_ENSURE(DoTryParse(), "Failed to re-parse TSerializedCellMatrix");
        } else {
            // Data address unchanged, reuse parsed cells
            Cells = other.Cells;
            RowCount = other.RowCount;
            ColCount = other.ColCount;
        }
        return *this;
    }

    static bool TryParse(TString&& data, TSerializedCellMatrix& vec) {
        vec.Buf = std::move(data);
        return vec.DoTryParse();
    }

    static bool TryParse(const TString& data, TSerializedCellMatrix& vec) {
        vec.Buf = data;
        return vec.DoTryParse();
    }

    void Parse(TString&& buf) {
        Buf = std::move(buf);
        Y_ENSURE(DoTryParse());
    }

    void Parse(const TString& buf) {
        Buf = buf;
        Y_ENSURE(DoTryParse());
    }

    TConstArrayRef<TCell> GetCells() const { return Cells; }
    const TCell& GetCell(ui32 row, ui16 column) const;

    ui32 GetRowCount() const { return RowCount; }
    ui16 GetColCount() const { return ColCount; }

    static size_t CalcIndex(ui32 row, ui16 column, ui16 columnCount) { return row * columnCount + column; }
    size_t CalcIndex(ui32 row, ui16 column) const { return CalcIndex(row, column, ColCount); }

    void GetSubmatrix(ui32 firstRow, ui32 lastRow, ui16 firstColumn, ui16 lastColumn, TVector<TCell>& resultCells) const;

    static void Serialize(TString& res, TConstArrayRef<TCell> cells, ui32 rowCount, ui16 colCount);

    static TString Serialize(TConstArrayRef<TCell> cells, ui32 rowCount, ui16 colCount);

    const TString& GetBuffer() const {
        return Buf;
    }

    TString ReleaseBuffer() {
        Cells.clear();
        return std::move(Buf);
    }

private:
    bool DoTryParse();

private:
    TString Buf;
    TVector<TCell> Cells;
    ui32 RowCount;
    ui16 ColCount;
};

class TCellsStorage
{
public:
    TCellsStorage() = default;

    inline TConstArrayRef<TCell> GetCells() const {
        return Cells;
    }

    void Reset(TArrayRef<const TCell> cells);

private:
    TArrayRef<TCell> Cells;
    std::vector<char> CellsData;
};

class TOwnedCellVecBatch {
public:
    TOwnedCellVecBatch();

    TOwnedCellVecBatch(std::unique_ptr<TMemoryPool> pool);

    TOwnedCellVecBatch(const TOwnedCellVecBatch& rhs) = delete;

    TOwnedCellVecBatch & operator=(const TOwnedCellVecBatch& rhs) = delete;

    TOwnedCellVecBatch(TOwnedCellVecBatch&& rhs) = default;

    TOwnedCellVecBatch & operator=(TOwnedCellVecBatch&& rhs) = default;

    size_t Append(TConstArrayRef<TCell> cells);

    size_t Size() const {
        return CellVectors.size();
    }

    bool Empty() const {
        return CellVectors.empty();
    }

    bool empty() const {
        return CellVectors.empty();
    }

    using iterator = TVector<TConstArrayRef<TCell>>::iterator;
    using const_iterator = TVector<TConstArrayRef<TCell>>::const_iterator;

    iterator begin() {
        return CellVectors.begin();
    }

    iterator end() {
        return CellVectors.end();
    }

    const_iterator begin() const {
        return CellVectors.begin();
    }

    const_iterator end() const {
        return CellVectors.end();
    }

    const_iterator cbegin() {
        return CellVectors.cbegin();
    }

    const_iterator cend() {
        return CellVectors.cend();
    }

    TConstArrayRef<TCell> operator[](size_t index) const {
        return CellVectors[index];
    }

    TConstArrayRef<TCell> front() const {
        return CellVectors.front();
    }

    TConstArrayRef<TCell> back() const {
        return CellVectors.back();
    }

private:
    static constexpr size_t InitialPoolSize = 1ULL << 16;

    std::unique_ptr<TMemoryPool> Pool;
    TVector<TConstArrayRef<TCell>> CellVectors;
};

void DbgPrintValue(TString&, const TCell&, NScheme::TTypeInfo typeInfo);
TString DbgPrintCell(const TCell& r, NScheme::TTypeInfo typeInfo, const NScheme::TTypeRegistry& typeRegistry);
TString DbgPrintTuple(const TDbTupleRef& row, const NScheme::TTypeRegistry& typeRegistry);

size_t GetCellMatrixHeaderSize();
size_t GetCellHeaderSize();

}
