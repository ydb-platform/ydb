#pragma once

#include "public.h"
#include "row_base.h"
#include "unversioned_value.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt_proto/yt/core/misc/proto/guid.pb.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

extern const TString SerializedNullRow;

////////////////////////////////////////////////////////////////////////////////

class TUnversionedOwningValue
{
public:
    TUnversionedOwningValue() = default;

    TUnversionedOwningValue(TUnversionedOwningValue&& other) noexcept
    {
        std::swap(Value_, other.Value_);
    }

    TUnversionedOwningValue(const TUnversionedOwningValue& other)
    {
        Assign(other.Value_);
    }

    TUnversionedOwningValue(const TUnversionedValue& other)
    {
        Assign(other);
    }

    ~TUnversionedOwningValue()
    {
        Clear();
    }

    operator TUnversionedValue() const
    {
        return Value_;
    }

    TUnversionedOwningValue& operator=(TUnversionedOwningValue other) noexcept
    {
        std::swap(Value_, other.Value_);
        return *this;
    }

    void Clear()
    {
        if (IsStringLikeType(Value_.Type)) {
            delete[] Value_.Data.String;
        }
        Value_.Type = EValueType::TheBottom;
        Value_.Length = 0;
    }

    //! Provides mutable access to the string data.
    char* GetMutableString()
    {
        YT_VERIFY(IsStringLikeType(Value_.Type));
        // NB: it is correct to use `const_cast` here to modify the stored string
        // because initially it's allocated as a non-const `char*`.
        return const_cast<char*>(Value_.Data.String);
    }

    EValueType Type() const
    {
        return Value_.Type;
    }

private:
    TUnversionedValue Value_{
        .Id = 0,
        .Type = EValueType::TheBottom,
        .Flags = {},
        .Length = 0,
        .Data = {},
    };

    void Assign(const TUnversionedValue& other)
    {
        Value_ = other;
        if (IsStringLikeType(Value_.Type)) {
            auto newString = new char[Value_.Length];
            ::memcpy(newString, Value_.Data.String, Value_.Length);
            Value_.Data.String = newString;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Debug printer for Gtest unittests.
inline void PrintTo(const TUnversionedOwningValue& value, std::ostream* os)
{
    PrintTo(static_cast<TUnversionedValue>(value), os);
}

inline TUnversionedValue MakeUnversionedSentinelValue(EValueType type, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeSentinelValue<TUnversionedValue>(type, id, flags);
}

inline TUnversionedValue MakeUnversionedNullValue(int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeNullValue<TUnversionedValue>(id, flags);
}

inline TUnversionedValue MakeUnversionedInt64Value(i64 value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeInt64Value<TUnversionedValue>(value, id, flags);
}

inline TUnversionedValue MakeUnversionedUint64Value(ui64 value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeUint64Value<TUnversionedValue>(value, id, flags);
}

inline TUnversionedValue MakeUnversionedDoubleValue(double value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeDoubleValue<TUnversionedValue>(value, id, flags);
}

inline TUnversionedValue MakeUnversionedBooleanValue(bool value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeBooleanValue<TUnversionedValue>(value, id, flags);
}

inline TUnversionedValue MakeUnversionedStringLikeValue(EValueType valueType, TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeStringLikeValue<TUnversionedValue>(valueType, value, id, flags);
}

inline TUnversionedValue MakeUnversionedStringValue(TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeStringValue<TUnversionedValue>(value, id, flags);
}

inline TUnversionedValue MakeUnversionedAnyValue(TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeAnyValue<TUnversionedValue>(value, id, flags);
}

inline TUnversionedValue MakeUnversionedCompositeValue(TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeCompositeValue<TUnversionedValue>(value, id, flags);
}

inline TUnversionedValue MakeUnversionedValueHeader(EValueType type, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeSentinelValue<TUnversionedValue>(type, id, flags);
}

////////////////////////////////////////////////////////////////////////////////

struct TUnversionedRowHeader
{
    ui32 Count;
    ui32 Capacity;
};

static_assert(
    sizeof(TUnversionedRowHeader) == 8,
    "TUnversionedRowHeader has to be exactly 8 bytes.");

////////////////////////////////////////////////////////////////////////////////

inline size_t GetDataWeight(EValueType type)
{
    switch (type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            return 0;

        case EValueType::Int64:
            return sizeof(i64);

        case EValueType::Uint64:
            return sizeof(ui64);

        case EValueType::Double:
            return sizeof(double);

        case EValueType::Boolean:
            return 1;

        default:
            YT_ABORT();
    }
}

inline size_t GetDataWeight(const TUnversionedValue& value)
{
    if (IsStringLikeType(value.Type)) {
        return value.Length;
    } else {
        return GetDataWeight(value.Type);
    }
}

size_t EstimateRowValueSize(const TUnversionedValue& value, bool isInlineHunkValue = false);
size_t WriteRowValue(char* output, const TUnversionedValue& value, bool isInlineHunkValue = false);
size_t ReadRowValue(const char* input, TUnversionedValue* value);

void Save(TStreamSaveContext& context, const TUnversionedValue& value);
void Load(TStreamLoadContext& context, TUnversionedValue& value, TChunkedMemoryPool* pool);

//! Ternary comparison predicate for TUnversionedValue-s.
//! Returns zero, positive or negative value depending on the outcome.
//! Note that this ignores flags.
int CompareRowValues(const TUnversionedValue& lhs, const TUnversionedValue& rhs);

//! Derived comparison operators.
//! Note that these ignore flags.
bool operator == (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator <= (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator <  (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator >= (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator >  (const TUnversionedValue& lhs, const TUnversionedValue& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Ternary comparison predicate for value ranges.
//! Note that this ignores aggregate flags.
int CompareValueRanges(
    TUnversionedValueRange lhs,
    TUnversionedValueRange rhs);

//! Ternary comparison predicate for TUnversionedRow-s stripped to a given number of
//! (leading) values.
//! Note that this ignores aggregate flags.
int CompareRows(
    TUnversionedRow lhs,
    TUnversionedRow rhs,
    ui32 prefixLength = std::numeric_limits<ui32>::max());

//! Derived comparison operators.
//! Note that these ignore aggregate flags.
bool operator == (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator <= (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator <  (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator >= (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator >  (TUnversionedRow lhs, TUnversionedRow rhs);

//! Computes FarmHash forever-fixed fingerprint for a range of values.
TFingerprint GetFarmFingerprint(TUnversionedValueRange range);

//! Computes FarmHash forever-fixed fingerprint for an unversioned row.
TFingerprint GetFarmFingerprint(TUnversionedRow row);

//! Returns the number of bytes needed to store an unversioned row (not including string data).
size_t GetUnversionedRowByteSize(ui32 valueCount);

//! Returns the storage-invariant data weight of a given row.
size_t GetDataWeight(TUnversionedRow row);

size_t GetDataWeight(TRange<TUnversionedRow> rows);

////////////////////////////////////////////////////////////////////////////////

//! A row with unversioned data.
/*!
 *  A lightweight wrapper around |TUnversionedRowHeader*|.
 *
 *  Provides access to a sequence of unversioned values.
 *  If data is schemaful then the positions of values must exactly match their ids.
 *
 *  Memory layout:
 *  1) TUnversionedRowHeader
 *  2) TUnversionedValue per each value (#TUnversionedRowHeader::Count)
 */
class TUnversionedRow
{
public:
    TUnversionedRow() = default;

    explicit TUnversionedRow(const TUnversionedRowHeader* header)
        : Header_(header)
    { }

    explicit TUnversionedRow(TTypeErasedRow erased)
        : Header_(reinterpret_cast<const TUnversionedRowHeader*>(erased.OpaqueHeader))
    { }

    explicit operator bool() const
    {
        return Header_ != nullptr;
    }

    TTypeErasedRow ToTypeErasedRow() const
    {
        return {Header_};
    }

    const TUnversionedRowHeader* GetHeader() const
    {
        return Header_;
    }

    const TUnversionedValue* Begin() const
    {
        return reinterpret_cast<const TUnversionedValue*>(Header_ + 1);
    }

    const TUnversionedValue* End() const
    {
        return Begin() + GetCount();
    }

    TUnversionedValueRange Elements() const
    {
        return {Begin(), End()};
    }

    TUnversionedValueRange FirstNElements(int count) const
    {
        YT_ASSERT(count <= static_cast<int>(GetCount()));
        return {Begin(), Begin() + count};
    }

    const TUnversionedValue& operator[] (int index) const
    {
        YT_ASSERT(index >= 0 && static_cast<ui32>(index) < GetCount());
        return Begin()[index];
    }

    ui32 GetCount() const
    {
        return Header_ ? Header_->Count : 0u;
    }

    // STL interop.
    const TUnversionedValue* begin() const
    {
        return Begin();
    }

    const TUnversionedValue* end() const
    {
        return End();
    }

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

private:
    const TUnversionedRowHeader* Header_ = nullptr;
};

static_assert(
    sizeof(TUnversionedRow) == sizeof(intptr_t),
    "TUnversionedRow size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

//! Checks that #value type is compatible with the schema column type.
/*
 * If #typeAnyAcceptsAllValues is false columns of type EValueType::ANY accept only values of same type.
 * If #typeAnyAcceptsAllValues is true then columns of type EValueType::ANY accept all values.
 *
 * \note Function throws an error if column has `Any' type and value has `non-Any' type.
 */
void ValidateValueType(
    const TUnversionedValue& value,
    const TTableSchema& schema,
    int schemaId,
    bool typeAnyAcceptsAllValues,
    bool ignoreRequired = false,
    bool validateAnyIsValidYson = false);
void ValidateValueType(
    const TUnversionedValue& value,
    const TColumnSchema& columnSchema,
    bool typeAnyAcceptsAllValues,
    bool ignoreRequired = false,
    bool validateAnyIsValidYson = false);

//! Checks that #value is allowed to appear in static tables' data. Throws on failure.
void ValidateStaticValue(const TUnversionedValue& value);

//! Checks that #value is allowed to appear in dynamic tables' data. Throws on failure.
void ValidateDataValue(const TUnversionedValue& value);

//! Checks that #value is allowed to appear in dynamic tables' keys. Throws on failure.
void ValidateKeyValue(const TUnversionedValue& value);

//! Checks that #count represents an allowed number of values in a row. Throws on failure.
void ValidateRowValueCount(int count);

//! Checks that #count represents an allowed number of components in a key. Throws on failure.
void ValidateKeyColumnCount(int count);

//! Checks that #count represents an allowed number of rows in a rowset. Throws on failure.
void ValidateRowCount(int count);

//! Checks that #row is a valid client-side data row. Throws on failure.
/*!
 *  Value ids in the row are first mapped via #idMapping.
 *  The row must obey the following properties:
 *  1. Its value count must pass #ValidateRowValueCount checks.
 *  2. It must contain all key components (values with ids in range [0, #schema.GetKeyColumnCount() - 1]).
 *  3. Value types must either be null or match those given in #schema.
 *  4. For values marked with #TUnversionedValue::Aggregate flag, the corresponding columns in #schema must
 *  be aggregating.
 *  5. Versioned values must be sorted by |id| (in ascending order) and then by |timestamp| (in descending order).
 *  6. At least one non-key column must be specified.
 */
void ValidateClientDataRow(
    TUnversionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable,
    std::optional<int> tabletIndexColumnId = std::nullopt,
    bool allowMissingKeyColumns = false);

//! Checks that #row contains no duplicate non-key columns and that all required columns are present. Skip values that map to negative ids via #idMapping.
/*! It is assumed that ValidateClientDataRow was called before. */
void ValidateDuplicateAndRequiredValueColumns(
    TUnversionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping);

//! Checks that #row contains write lock for non-key columns and returns true if any non-key columns encountered.
bool ValidateNonKeyColumnsAgainstLock(
    TUnversionedRow row,
    const TLockMask& locks,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable,
    const std::vector<int>& columnIndexToLockIndex);

//! Checks that #key is a valid client-side key. Throws on failure.
/*! The components must pass #ValidateKeyValue check. */
void ValidateClientKey(TLegacyKey key);

//! Checks that #key is a valid client-side key. Throws on failure.
/*! The key must obey the following properties:
 *  1. It cannot be null.
 *  2. It must contain exactly #schema.GetKeyColumnCount() components.
 *  3. Value ids must be a permutation of {0, ..., #schema.GetKeyColumnCount() - 1}.
 *  4. Value types must either be null of match those given in schema.
 */
void ValidateClientKey(
    TLegacyKey key,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable);

//! Checks if #timestamp is sane and can be used for data.
//! Allows timestamps in range [MinTimestamp, MaxTimestamp] plus some sentinels
//! (SyncLastCommittedTimestamp and AsyncLastCommittedTimestamp).
void ValidateReadTimestamp(TTimestamp timestamp);

//! Checks if #timestamp is sane and can be used for replica synchronization.
//! Allows timestamps in range [MinTimestamp, MaxTimestamp].
void ValidateGetInSyncReplicasTimestamp(TTimestamp timestamp);

//! Checks if #timestamp is sane and can be used for writing (versioned) data.
//! Allows timestamps in range [MinTimestamp, MaxTimestamp].
void ValidateWriteTimestamp(TTimestamp timestamp);

//! An internal helper used by validators.
int ApplyIdMapping(
    const TUnversionedValue& value,
    const TNameTableToSchemaIdMapping* idMapping);

//! Returns the successor of |key|, i.e. the key obtained from |key|
//! by appending a |EValueType::Min| sentinel.
TLegacyOwningKey GetKeySuccessor(TLegacyKey key);
TLegacyKey GetKeySuccessor(TLegacyKey key, const TRowBufferPtr& rowBuffer);

//! Returns the successor of |key| trimmed to a given length, i.e. the key
//! obtained by trimming |key| to |prefixLength| and appending
//! a |EValueType::Max| sentinel.
TLegacyOwningKey GetKeyPrefixSuccessor(TLegacyKey key, ui32 prefixLength);
TLegacyKey GetKeyPrefixSuccessor(TLegacyKey key, ui32 prefixLength, const TRowBufferPtr& rowBuffer);

//! Returns key of a strict length (either trimmed key or widened key)
TLegacyKey GetStrictKey(TLegacyKey key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);
TLegacyKey GetStrictKeySuccessor(TLegacyKey key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);

//! If #key has more than #prefixLength values then trims it this limit.
TLegacyOwningKey GetKeyPrefix(TLegacyKey key, ui32 prefixLength);
TLegacyKey GetKeyPrefix(TLegacyKey key, ui32 prefixLength, const TRowBufferPtr& rowBuffer);

//! Makes a new, wider key padded with given sentinel values.
TLegacyOwningKey WidenKey(const TLegacyOwningKey& key, ui32 keyColumnCount, EValueType sentinelType = EValueType::Null);
TLegacyKey WidenKey(const TLegacyKey& key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);

//! Makes the key wider by appending given sentinel values up to |keyColumnCount| length
//! and then appends a |EValueType::Max| sentinel.
TLegacyOwningKey WidenKeySuccessor(const TLegacyOwningKey& key, ui32 keyColumnCount, EValueType sentinelType = EValueType::Null);
TLegacyKey WidenKeySuccessor(const TLegacyKey& key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);

//! Takes prefix of a key and makes it wider.
TLegacyOwningKey WidenKeyPrefix(const TLegacyOwningKey& key, ui32 prefixLength, ui32 keyColumnCount, EValueType sentinelType = EValueType::Null);
TLegacyKey WidenKeyPrefix(TLegacyKey key, ui32 prefixLength, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);

//! Returns the key with no components.
const TLegacyOwningKey EmptyKey();

//! Returns the key with a single |Min| component.
const TLegacyOwningKey MinKey();

//! Returns the key with a single |Max| component.
const TLegacyOwningKey MaxKey();

//! Compares two keys, |a| and |b|, and returns a smaller one.
//! Ties are broken in favour of the first argument.
const TLegacyOwningKey& ChooseMinKey(const TLegacyOwningKey& a, const TLegacyOwningKey& b);

//! Compares two keys, |a| and |b|, and returns a bigger one.
//! Ties are broken in favour of the first argument.
const TLegacyOwningKey& ChooseMaxKey(const TLegacyOwningKey& a, const TLegacyOwningKey& b);

TString SerializeToString(TUnversionedRow row);
TString SerializeToString(TUnversionedValueRange range);

void ToProto(TProtobufString* protoRow, TUnversionedRow row);
void ToProto(TProtobufString* protoRow, const TUnversionedOwningRow& row);
void ToProto(TProtobufString* protoRow, TUnversionedValueRange range);
void ToProto(TProtobufString* protoRow, const TRange<TUnversionedOwningValue>& values);

void FromProto(TUnversionedOwningRow* row, const TProtobufString& protoRow, std::optional<int> nullPaddingWidth = {});
void FromProto(TUnversionedRow* row, const TProtobufString& protoRow, const TRowBufferPtr& rowBuffer);
void FromProto(std::vector<TUnversionedOwningValue>* values, const TProtobufString& protoRow);

void ToBytes(TString* bytes, const TUnversionedOwningRow& row);

void FromBytes(TUnversionedOwningRow* row, TStringBuf bytes);

void Serialize(const TUnversionedValue& value, NYson::IYsonConsumer* consumer, bool anyAsRaw = false);
void Serialize(TLegacyKey key, NYson::IYsonConsumer* consumer);
void Serialize(const TLegacyOwningKey& key, NYson::IYsonConsumer* consumer);

void Deserialize(TLegacyOwningKey& key, NYTree::INodePtr node);
void Deserialize(TLegacyOwningKey& key, NYson::TYsonPullParserCursor* cursor);

size_t GetYsonSize(const TUnversionedValue& value);
size_t WriteYson(char* buffer, const TUnversionedValue& unversionedValue);

//! Debug printers for Gtest unittests.
void PrintTo(const TUnversionedOwningRow& key, ::std::ostream* os);
void PrintTo(const TUnversionedRow& value, ::std::ostream* os);

TLegacyOwningKey RowToKey(
    const NTableClient::TTableSchema& schema,
    TUnversionedRow row);

//! Constructs a shared range of rows from a non-shared one.
/*!
 *  The values contained in the rows are also captured.
 *  The underlying storage allocation has just the right size to contain the captured
 *  data and is marked with #tagCookie. The size of allocated space is returned via
 *  second value.
 */
std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRows(
    TRange<TUnversionedRow> rows,
    TRefCountedTypeCookie tagCookie);

std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRows(
    TRange<TUnversionedOwningRow> rows,
    TRefCountedTypeCookie tagCookie);

template <class TTag, class TRow>
std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRows(TRange<TRow> rows)
{
    return CaptureRows(rows, GetRefCountedTypeCookie<TTag>());
}

////////////////////////////////////////////////////////////////////////////////

//! A variant of TUnversionedRow that enables mutating access to its content.
class TMutableUnversionedRow
    : public TUnversionedRow
{
public:
    TMutableUnversionedRow() = default;

    explicit TMutableUnversionedRow(TUnversionedRowHeader* header)
        : TUnversionedRow(header)
    { }

    explicit TMutableUnversionedRow(TTypeErasedRow row)
        : TUnversionedRow(reinterpret_cast<const TUnversionedRowHeader*>(row.OpaqueHeader))
    { }

    static TMutableUnversionedRow Allocate(
        TChunkedMemoryPool* pool,
        ui32 valueCount);

    static TMutableUnversionedRow Create(
        void* buffer,
        ui32 valueCount);

    TUnversionedRowHeader* GetHeader()
    {
        return const_cast<TUnversionedRowHeader*>(TUnversionedRow::GetHeader());
    }

    TUnversionedValue* Begin()
    {
        return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1);
    }

    TUnversionedValue* End()
    {
        return Begin() + GetCount();
    }

    const TUnversionedValue* Begin() const
    {
        return reinterpret_cast<const TUnversionedValue*>(TUnversionedRow::GetHeader() + 1);
    }

    const TUnversionedValue* End() const
    {
        return Begin() + GetCount();
    }

    TMutableUnversionedValueRange Elements()
    {
        return {Begin(), End()};
    }

    TMutableUnversionedValueRange FirstNElements(int count)
    {
        YT_ASSERT(count <= static_cast<int>(GetCount()));
        return {Begin(), Begin() + count};
    }

    void SetCount(ui32 count)
    {
        YT_ASSERT(count <= GetHeader()->Capacity);
        GetHeader()->Count = count;
    }

    void PushBack(TUnversionedValue value)
    {
        ui32 count = GetCount();
        SetCount(count + 1);
        Begin()[count] = value;
    }

    TUnversionedValue& operator[] (ui32 index)
    {
        YT_ASSERT(index < GetHeader()->Count);
        return Begin()[index];
    }

    // STL interop.
    TUnversionedValue* begin()
    {
        return Begin();
    }

    TUnversionedValue* end()
    {
        return End();
    }

    const TUnversionedValue* begin() const
    {
        return Begin();
    }

    const TUnversionedValue* end() const
    {
        return End();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! An owning variant of TUnversionedRow.
/*!
 *  Instances of TUnversionedOwningRow are lightweight handles.
 *  Fixed part is stored in shared ref-counted blobs.
 *  Variable part is stored in shared strings.
 */
class TUnversionedOwningRow
{
public:
    TUnversionedOwningRow() = default;

    TUnversionedOwningRow(TUnversionedValueRange range)
    {
        Init(range);
    }

    explicit TUnversionedOwningRow(TUnversionedRow other)
    {
        if (other) {
            Init(other.Elements());
        }
    }

    TUnversionedOwningRow(const TUnversionedOwningRow& other)
        : RowData_(other.RowData_)
        , StringData_(other.StringData_)
    { }

    TUnversionedOwningRow(TUnversionedOwningRow&& other)
        : RowData_(std::move(other.RowData_))
        , StringData_(std::move(other.StringData_))
    { }

    explicit operator bool() const
    {
        return static_cast<bool>(RowData_);
    }

    operator TUnversionedRow() const
    {
        return Get();
    }

    TUnversionedRow Get() const
    {
        return TUnversionedRow(GetHeader());
    }

    const TUnversionedValue* Begin() const
    {
        const auto* header = GetHeader();
        return header ? reinterpret_cast<const TUnversionedValue*>(header + 1) : nullptr;
    }

    const TUnversionedValue* End() const
    {
        return Begin() + GetCount();
    }

    TUnversionedValueRange Elements() const
    {
        return {Begin(), End()};
    }

    TUnversionedValueRange FirstNElements(int count) const
    {
        YT_ASSERT(count <= static_cast<int>(GetCount()));
        return {Begin(), Begin() + count};
    }

    const TUnversionedValue& operator[] (int index) const
    {
        YT_ASSERT(index >= 0 && index < GetCount());
        return Begin()[index];
    }

    int GetCount() const
    {
        const auto* header = GetHeader();
        return header ? static_cast<int>(header->Count) : 0;
    }

    size_t GetSpaceUsed() const
    {
        size_t size = 0;
        if (StringData_) {
            size += StringData_.GetHolder()->GetTotalByteSize().value_or(StringData_.Size());
        }
        if (RowData_) {
            size += RowData_.GetHolder()->GetTotalByteSize().value_or(RowData_.Size());
        }
        return size;
    }

    friend void swap(TUnversionedOwningRow& lhs, TUnversionedOwningRow& rhs)
    {
        using std::swap;

        swap(lhs.RowData_, rhs.RowData_);
        swap(lhs.StringData_, rhs.StringData_);
    }

    TUnversionedOwningRow& operator=(const TUnversionedOwningRow& other)
    {
        RowData_ = other.RowData_;
        StringData_ = other.StringData_;
        return *this;
    }

    TUnversionedOwningRow& operator=(TUnversionedOwningRow&& other)
    {
        RowData_ = std::move(other.RowData_);
        StringData_ = std::move(other.StringData_);
        return *this;
    }

    // STL interop.
    const TUnversionedValue* begin() const
    {
        return Begin();
    }

    const TUnversionedValue* end() const
    {
        return End();
    }

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    friend TLegacyOwningKey GetKeySuccessorImpl(const TLegacyOwningKey& key, int prefixLength, EValueType sentinelType);
    friend TUnversionedOwningRow DeserializeFromString(TString&& data, std::optional<int> nullPaddingWidth);

    friend class TUnversionedOwningRowBuilder;

    TSharedMutableRef RowData_; // TRowHeader plus TValue-s
    TSharedRef StringData_;     // Holds string data


    TUnversionedOwningRow(TSharedMutableRef&& rowData, TSharedRef&& stringData)
        : RowData_(std::move(rowData))
        , StringData_(std::move(stringData))
    { }

    void Init(TUnversionedValueRange range);

    TUnversionedRowHeader* GetHeader()
    {
        return RowData_ ? reinterpret_cast<TUnversionedRowHeader*>(RowData_.Begin()) : nullptr;
    }

    const TUnversionedRowHeader* GetHeader() const
    {
        return RowData_ ? reinterpret_cast<const TUnversionedRowHeader*>(RowData_.Begin()) : nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TUnversionedRow instances.
//! Only row values are kept, strings are only referenced.
class TUnversionedRowBuilder
{
public:
    static const int DefaultValueCapacity = 8;

    explicit TUnversionedRowBuilder(int initialValueCapacity = DefaultValueCapacity);

    int AddValue(const TUnversionedValue& value);
    TMutableUnversionedRow GetRow();
    void Reset();

private:
    static const int DefaultBlobCapacity =
        sizeof(TUnversionedRowHeader) +
        DefaultValueCapacity * sizeof(TUnversionedValue);

    TCompactVector<char, DefaultBlobCapacity> RowData_;

    TUnversionedRowHeader* GetHeader();
    TUnversionedValue* GetValue(ui32 index);
};

////////////////////////////////////////////////////////////////////////////////

struct TOwningRowTag
{ };

//! A helper used for constructing TUnversionedOwningRow instances.
//! Keeps both row values and strings.
class TUnversionedOwningRowBuilder
{
public:
    static const int DefaultValueCapacity = 16;

    explicit TUnversionedOwningRowBuilder(int initialValueCapacity = DefaultValueCapacity);

    int AddValue(const TUnversionedValue& value);
    TUnversionedValue* BeginValues();
    TUnversionedValue* EndValues();

    TUnversionedOwningRow FinishRow();

private:
    const int InitialValueCapacity_;

    TBlob RowData_{GetRefCountedTypeCookie<TOwningRowTag>()};
    TBlob StringData_{GetRefCountedTypeCookie<TOwningRowTag>()};

    TUnversionedRowHeader* GetHeader();
    TUnversionedValue* GetValue(ui32 index);
    void Reset();
};

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> MakeSingletonRowRange(
    TLegacyKey lowerBound,
    TLegacyKey upperBound,
    TRowBufferPtr rowBuffer = nullptr);

TKeyRef ToKeyRef(TUnversionedRow row);
TKeyRef ToKeyRef(TUnversionedRow row, int prefixLength);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TUnversionedValueRange values, TStringBuf format);
void FormatValue(TStringBuilderBase* builder, TUnversionedRow row, TStringBuf format);
void FormatValue(TStringBuilderBase* builder, TMutableUnversionedRow row, TStringBuf format);
void FormatValue(TStringBuilderBase* builder, const TUnversionedOwningRow& row, TStringBuf format);

TString ToString(TUnversionedRow row, bool valuesOnly = false);
TString ToString(TMutableUnversionedRow row, bool valuesOnly = false);
TString ToString(const TUnversionedOwningRow& row, bool valuesOnly = false);

////////////////////////////////////////////////////////////////////////////////

// NB: Hash function may change in future. Use fingerprints for stability.
struct TDefaultUnversionedValueRangeHash
{
    size_t operator()(TUnversionedValueRange range) const;
};

struct TDefaultUnversionedValueRangeEqual
{
    bool operator()(TUnversionedValueRange lhs, TUnversionedValueRange rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

// NB: Hash function may change in future. Use fingerprints for stability.
struct TDefaultUnversionedRowHash
{
    size_t operator()(TUnversionedRow row) const;
};

struct TDefaultUnversionedRowEqual
{
    bool operator()(TUnversionedRow lhs, TUnversionedRow rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TBitwiseUnversionedValueRangeHash
{
    size_t operator()(TUnversionedValueRange range) const;
};

struct TBitwiseUnversionedValueRangeEqual
{
    bool operator()(TUnversionedValueRange lhs, TUnversionedValueRange rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TBitwiseUnversionedRowHash
{
    size_t operator()(TUnversionedRow row) const;
};

struct TBitwiseUnversionedRowEqual
{
    bool operator()(TUnversionedRow lhs, TUnversionedRow rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

//! A hasher for TUnversionedRow.
template <>
struct THash<NYT::NTableClient::TUnversionedRow>
{
    inline size_t operator()(NYT::NTableClient::TUnversionedRow row) const
    {
        return NYT::NTableClient::TDefaultUnversionedRowHash()(row);
    }
};

template <class T>
    requires std::derived_from<std::remove_cvref_t<T>, NYT::NTableClient::TUnversionedRow>
struct NYT::TFormatArg<T>
    : public NYT::TFormatArgBase
{
    static constexpr auto FlagSpecifiers
        = TFormatArgBase::ExtendFlags</*Hot*/ true, 1, std::array{'k'}>();
};

template <class T>
    requires std::derived_from<std::remove_cvref_t<T>, NYT::NTableClient::TUnversionedValueRange>
struct NYT::TFormatArg<T>
    : public NYT::TFormatArgBase
{
    static constexpr auto FlagSpecifiers
        = TFormatArgBase::ExtendFlags</*Hot*/ true, 1, std::array{'k'}>();
};
