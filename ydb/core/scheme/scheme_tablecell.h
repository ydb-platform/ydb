#pragma once

#include "defs.h"
#include "scheme_type_id.h"
#include "scheme_type_order.h"
#include "scheme_types_defs.h"

#include <util/generic/bitops.h>
#include <util/generic/hash.h>
#include <util/system/unaligned_mem.h>

#include <type_traits>

namespace NKikimr {

#pragma pack(push,4)
// Represents one element in a tuple
// Doesn't own the memory buffer that stores the actual value
// Small values (<= 8 bytes) are stored inline
struct TCell {
    template<typename T>
    using TStdLayout = std::enable_if_t<std::is_standard_layout<T>::value, T>;

private:
    ui32 DataSize_ : 30;
    ui32 IsInline_ : 1;
    ui32 IsNull_   : 1;
    union {
        i64 IntVal;
        const char* Ptr;
        double DoubleVal;
        float FloatVal;
        char Bytes[8];
    };

public:
    TCell()
        : TCell(nullptr, 0)
    {}

    TCell(TArrayRef<const char> ref)
        : TCell(ref.begin(), ui32(ref.size()))
    {
        Y_VERIFY(ref.size() < Max<ui32>(), " Too large blob size for TCell");
    }

    TCell(const char* ptr, ui32 sz)
        : DataSize_(sz)
        , IsInline_(0)
        , IsNull_(ptr == nullptr)
        , Ptr(ptr)
    {
        Y_VERIFY_DEBUG(ptr || sz == 0);
        if (CanInline(sz)) {
            IsInline_ = 1;
            IntVal = 0;
            if (ptr)
                memcpy(&IntVal, ptr, sz);
        }
    }

    explicit TCell(const TRawTypeValue* v)
        : TCell((const char*)v->Data(), v->Size())
    {}

    explicit operator bool() const
    {
        return !IsNull();
    }

    bool IsInline() const       { return IsInline_; }
    bool IsNull() const         { return IsNull_; }
    ui32 Size() const           { return DataSize_; }

    TArrayRef<const char> AsRef() const noexcept
    {
        return { Data(), Size() };
    }

    TStringBuf AsBuf() const noexcept
    {
        return { Data(), Size() };
    }

    template<typename T, typename = TStdLayout<T>>
    T AsValue() const noexcept
    {
        Y_VERIFY(sizeof(T) == Size(), "AsValue<T>() type doesn't match TCell");

        return ReadUnaligned<T>(Data());
    }

    template<typename T, typename = TStdLayout<T>>
    static inline TCell Make(const T &val) noexcept
    {
        auto *ptr = static_cast<const char*>(static_cast<const void*>(&val));

        return TCell{ ptr, sizeof(val) };
    }

#if 1
    // Optimization to store small values (<= 8 bytes) inplace
    static constexpr bool CanInline(ui32 sz) { return sz <= 8; }
    static constexpr size_t MaxInlineSize() { return 8; }
    const char* InlineData() const                  { Y_VERIFY_DEBUG(IsInline_); return IsNull_ ? nullptr : (char*)&IntVal; }
    const char* Data() const                        { return IsNull_ ? nullptr : (IsInline_ ? (char*)&IntVal : Ptr); }
#else
    // Non-inlinable version for perf comparisons
    static bool CanInline(ui32)                     { return false; }
    const char* InlineData() const                  { Y_VERIFY_DEBUG(!IsInline_); return Ptr; }
    const char* Data() const                        { Y_VERIFY_DEBUG(!IsInline_); return Ptr; }
#endif
};

#pragma pack(pop)

static_assert(sizeof(TCell) == 12, "TCell must be 12 bytes");
using TCellsRef = TConstArrayRef<const TCell>;


// NULL is considered equal to another NULL and less than non-NULL
// ATTENTION!!! return value is int!! (NOT just -1,0,1)
inline int CompareTypedCells(const TCell& a, const TCell& b, NScheme::TTypeInfoOrder type) {
    using TPair = std::pair<ui64, ui64>;
    if (a.IsNull())
        return b.IsNull() ? 0 : -1;
    if (b.IsNull())
        return 1;

    switch (type.GetTypeId()) {

#define SIMPLE_TYPE_SWITCH(typeEnum, castType)      \
    case NKikimr::NScheme::NTypeIds::typeEnum:      \
    {                                               \
        Y_VERIFY_DEBUG(a.IsInline());                      \
        Y_VERIFY_DEBUG(b.IsInline());                      \
        castType va = ReadUnaligned<castType>((const castType*)a.InlineData()); \
        castType vb = ReadUnaligned<castType>((const castType*)b.InlineData()); \
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
    SIMPLE_TYPE_SWITCH(PairUi64Ui64,  TPair);
    SIMPLE_TYPE_SWITCH(Date,   ui16);
    SIMPLE_TYPE_SWITCH(Datetime,  ui32);
    SIMPLE_TYPE_SWITCH(Timestamp, ui64);
    SIMPLE_TYPE_SWITCH(Interval,  i64);

#undef SIMPLE_TYPE_SWITCH

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
        const char* pa = (const char*)a.Data();
        const char* pb = (const char*)b.Data();
        size_t sza = a.Size();
        size_t szb = b.Size();
        int cmp = memcmp(pa, pb, sza < szb ? sza : szb);
        if (cmp != 0)
            return type.IsDescending() ? (cmp > 0 ? -1 : +1) : cmp; // N.B. cannot multiply, may overflow
        return sza == szb ? 0 : ((sza < szb) != type.IsDescending() ? -1 : 1);
    }

    case NKikimr::NScheme::NTypeIds::Decimal:
    {
        Y_VERIFY_DEBUG(a.Size() == sizeof(std::pair<ui64, i64>));
        Y_VERIFY_DEBUG(b.Size() == sizeof(std::pair<ui64, i64>));
        std::pair<ui64, i64> va = ReadUnaligned<std::pair<ui64, i64>>((const std::pair<ui64, i64>*)a.Data());
        std::pair<ui64, i64> vb = ReadUnaligned<std::pair<ui64, i64>>((const std::pair<ui64, i64>*)b.Data());
        if (va.second == vb.second)
            return va.first == vb.first ? 0 : ((va.first < vb.first) != type.IsDescending() ? -1 : 1);
        return (va.second < vb.second) != type.IsDescending() ? -1 : 1;
    }

    case NKikimr::NScheme::NTypeIds::Pg:
    {
        auto typeDesc = type.GetTypeDesc();
        Y_VERIFY(typeDesc, "no pg type descriptor");
        int result = NPg::PgNativeBinaryCompare(a.Data(), a.Size(), b.Data(), b.Size(), typeDesc);
        return type.IsDescending() ? -result : result;
    }

    default:
        Y_VERIFY_DEBUG(false, "Unknown type");
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
    Y_VERIFY_DEBUG(cnt_b <= cnt_a);
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
        return THash<i32>()(ReadUnaligned<i32>((const i32*)cell.Data()));
    case NYql::NProto::TypeIds::Uint32:
        return THash<ui32>()(ReadUnaligned<ui32>((const ui32*)cell.Data()));
    case NYql::NProto::TypeIds::Int64:
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
        return ComputeHash(TStringBuf{cell.Data(), cell.Size()});

    default:
        break;
    }

    if (typeId == NKikimr::NScheme::NTypeIds::Pg) {
        auto typeDesc = info.GetTypeDesc();
        Y_VERIFY(typeDesc, "no pg type descriptor");
        return NPg::PgNativeBinaryHash(cell.Data(), cell.Size(), typeDesc);
    }

    Y_VERIFY_DEBUG(false, "Type not supported for user columns: %d", typeId);
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
        TData() = default;

        void operator delete(void* mem) noexcept;
    };

    struct TInit {
        TCellVec Cells;
        TIntrusivePtr<TData> Data;
        size_t DataSize;
    };

    TOwnedCellVec(TInit init) noexcept
        : TCellVec(std::move(init.Cells))
        , Data(std::move(init.Data))
        , DataSize_(init.DataSize)
    { }

    static TInit Allocate(TCellVec cells);

    TCellVec& CellVec() {
        return static_cast<TCellVec&>(*this);
    }

public:
    TOwnedCellVec() noexcept
        : TCellVec()
        , DataSize_(0)
    { }

    explicit TOwnedCellVec(TCellVec cells)
        : TOwnedCellVec(Allocate(cells))
    { }

    static TOwnedCellVec Make(TCellVec cells) {
        return TOwnedCellVec(Allocate(cells));
    }

    TOwnedCellVec(const TOwnedCellVec& rhs) noexcept
        : TCellVec(rhs)
        , Data(rhs.Data)
        , DataSize_(rhs.DataSize_)
    { }

    TOwnedCellVec(TOwnedCellVec&& rhs) noexcept
        : TCellVec(rhs)
        , Data(std::move(rhs.Data))
        , DataSize_(rhs.DataSize_)
    {
        rhs.CellVec() = { };
        rhs.DataSize_ = 0;
    }

    TOwnedCellVec& operator=(const TOwnedCellVec& rhs) noexcept {
        if (Y_LIKELY(this != &rhs)) {
            Data = rhs.Data;
            DataSize_ = rhs.DataSize_;
            CellVec() = rhs;
        }

        return *this;
    }

    TOwnedCellVec& operator=(TOwnedCellVec&& rhs) noexcept {
        if (Y_LIKELY(this != &rhs)) {
            Data = std::move(rhs.Data);
            DataSize_ = rhs.DataSize_;
            CellVec() = rhs;
            rhs.CellVec() = { };
            rhs.DataSize_ = 0;
        }

        return *this;
    }

    size_t DataSize() const {
        return DataSize_;
    }

private:
    TIntrusivePtr<TData> Data;
    size_t DataSize_;
};

static_assert(std::is_nothrow_destructible_v<TOwnedCellVec>, "Expected TOwnedCellVec to be nothrow destructible");
static_assert(std::is_nothrow_copy_constructible_v<TOwnedCellVec>, "Expected TOwnedCellVec to be nothrow copy constructible");
static_assert(std::is_nothrow_move_constructible_v<TOwnedCellVec>, "Expected TOwnedCellVec to be nothrow move constructible");
static_assert(std::is_nothrow_default_constructible_v<TOwnedCellVec>, "Expected TOwnedCellVec to be nothrow default constructible");

// Used to store/load a vector of TCell in bytes array
// When loading from a buffer the cells will point to the buffer contents
class TSerializedCellVec {
public:
    explicit TSerializedCellVec(TConstArrayRef<TCell> cells);

    explicit TSerializedCellVec(const TString& buf)
    {
        Parse(buf);
    }

    TSerializedCellVec() = default;

    TSerializedCellVec(const TSerializedCellVec &other)
        : Buf(other.Buf)
        , Cells(other.Cells)
    {
        Y_VERIFY(Buf.data() == other.Buf.data(), "Buffer must be shared");
    }

    TSerializedCellVec(TSerializedCellVec &&other)
    {
        *this = std::move(other);
    }

    TSerializedCellVec &operator=(const TSerializedCellVec &other)
    {
        if (this == &other)
            return *this;

        TSerializedCellVec tmp(other);
        *this = std::move(tmp);
        return *this;
    }

    TSerializedCellVec &operator=(TSerializedCellVec &&other)
    {
        if (this == &other)
            return *this;

        const char* otherPtr = other.Buf.data();
        Buf = std::move(other.Buf);
        Y_VERIFY(Buf.data() == otherPtr, "Buffer address must not change");
        Cells = std::move(other.Cells);
        return *this;
    }

    static bool TryParse(const TString& data, TSerializedCellVec& vec) {
        return vec.DoTryParse(data);
    }

    void Parse(const TString &buf) {
        Y_VERIFY(DoTryParse(buf));
    }

    TConstArrayRef<TCell> GetCells() const {
        return Cells;
    }

    static void Serialize(TString& res, TConstArrayRef<TCell> cells);

    static TString Serialize(TConstArrayRef<TCell> cells);

    const TString &GetBuffer() const { return Buf; }

    TString ReleaseBuffer() {
        Cells.clear();
        return std::move(Buf);
    }

private:
    bool DoTryParse(const TString& data);

private:
    TString Buf;
    TVector<TCell> Cells;
};

void DbgPrintValue(TString&, const TCell&, NScheme::TTypeInfo typeInfo);
TString DbgPrintCell(const TCell& r, NScheme::TTypeInfo typeInfo, const NScheme::TTypeRegistry& typeRegistry);
TString DbgPrintTuple(const TDbTupleRef& row, const NScheme::TTypeRegistry& typeRegistry);

}
