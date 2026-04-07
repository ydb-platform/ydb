#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/range.h>

#include <util/digest/multi.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

constexpr int PrimaryLockIndex = 0;

DEFINE_ENUM(ELockType,
    ((None)         (0))
    ((SharedWeak)   (1))
    ((SharedStrong) (2))
    ((Exclusive)    (3))
    ((SharedWrite)  (4))
);

// COMPAT(gritukan)
constexpr ELockType MaxOldLockType = ELockType::Exclusive;

bool IsReadLock(ELockType lock);
bool IsWriteLock(ELockType lock);

ELockType GetStrongestLock(ELockType lhs, ELockType rhs);

////////////////////////////////////////////////////////////////////////////////

class TLegacyLockMask
{
public:
    explicit TLegacyLockMask(TLegacyLockBitmap value = 0);

    ELockType Get(int index) const;
    void Set(int index, ELockType lock);

    void Enrich(int columnCount);

    TLegacyLockBitmap GetBitmap() const;

    TLegacyLockMask(const TLegacyLockMask& other) = default;
    TLegacyLockMask& operator=(const TLegacyLockMask& other) = default;

    static constexpr int BitsPerType = 2;
    static constexpr TLegacyLockBitmap TypeMask = (1 << BitsPerType) - 1;
    static constexpr int MaxCount = 8 * sizeof(TLegacyLockBitmap) / BitsPerType;

private:
    TLegacyLockBitmap Data_;
};

////////////////////////////////////////////////////////////////////////////////

class TLockMask
{
public:
    TLockMask() = default;

    TLockMask(TLockBitmap bitmap, int size);

    ELockType Get(int index) const;
    void Set(int index, ELockType lock);

    void Enrich(int size);

    int GetSize() const;
    TLockBitmap GetBitmap() const;

    // COMPAT(gritukan)
    TLegacyLockMask ToLegacyMask() const;
    bool HasNewLocks() const;

    // NB: Has linear complexity.
    bool IsNone() const;

    static constexpr int BitsPerType = 4;
    static_assert(static_cast<int>(TEnumTraits<ELockType>::GetMaxValue()) < (1 << BitsPerType));

    static constexpr ui64 LockMask = (1 << BitsPerType) - 1;

    static constexpr int LocksPerWord = 8 * sizeof(TLockBitmap::value_type) / BitsPerType;
    static_assert(IsPowerOf2(LocksPerWord));

    // Size of the lock mask should fit into ui16 for wire protocol.
    static constexpr int MaxSize = (1 << 16) - 1;

private:
    TLockBitmap Bitmap_;
    int Size_ = 0;

    void Reserve(int size);
};

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TLockMask& lhs, const TLockMask& rhs);

TLockMask MaxMask(TLockMask lhs, TLockMask rhs);

void ToProto(NTabletClient::NProto::TLockMask* protoLockMask, const TLockMask& lockMask);
void FromProto(TLockMask* lockMask, const NTabletClient::NProto::TLockMask& protoLockMask);

////////////////////////////////////////////////////////////////////////////////

class TColumnSchema
{
public:
    // Keep in sync with hasher below.
    DEFINE_BYREF_RO_PROPERTY(TColumnStableName, StableName);
    DEFINE_BYREF_RO_PROPERTY(std::string, Name);
    DEFINE_BYREF_RO_PROPERTY(TLogicalTypePtr, LogicalType);
    DEFINE_BYREF_RO_PROPERTY(std::optional<ESortOrder>, SortOrder);
    DEFINE_BYREF_RO_PROPERTY(std::optional<std::string>, Lock);
    DEFINE_BYREF_RO_PROPERTY(std::optional<std::string>, Expression);
    DEFINE_BYREF_RO_PROPERTY(std::optional<bool>, Materialized);
    DEFINE_BYREF_RO_PROPERTY(std::optional<std::string>, Aggregate);
    DEFINE_BYREF_RO_PROPERTY(std::optional<std::string>, Group);
    DEFINE_BYREF_RO_PROPERTY(bool, Required);
    DEFINE_BYREF_RO_PROPERTY(std::optional<i64>, MaxInlineHunkSize);

public:
    TColumnSchema();
    TColumnSchema(
        const std::string& name,
        EValueType type,
        std::optional<ESortOrder> sortOrder = {});
    TColumnSchema(
        const std::string& name,
        ESimpleLogicalValueType type,
        std::optional<ESortOrder> sortOrder = {});

    TColumnSchema(
        const std::string& name,
        TLogicalTypePtr type,
        std::optional<ESortOrder> sortOrder = {});

    TColumnSchema(const TColumnSchema&) = default;
    TColumnSchema(TColumnSchema&&) = default;

    TColumnSchema& operator=(const TColumnSchema&) = default;
    TColumnSchema& operator=(TColumnSchema&&) = default;

    TColumnSchema& SetStableName(const TColumnStableName& stableName);
    TColumnSchema& SetName(const std::string& name);
    TColumnSchema& SetLogicalType(TLogicalTypePtr valueType);
    TColumnSchema& SetSimpleLogicalType(ESimpleLogicalValueType type);
    TColumnSchema& SetSortOrder(std::optional<ESortOrder> value);
    TColumnSchema& SetLock(const std::optional<std::string>& value);
    TColumnSchema& SetExpression(const std::optional<std::string>& value);
    TColumnSchema& SetMaterialized(std::optional<bool> value);
    TColumnSchema& SetAggregate(const std::optional<std::string>& value);
    TColumnSchema& SetGroup(const std::optional<std::string>& value);
    TColumnSchema& SetRequired(bool value);
    TColumnSchema& SetMaxInlineHunkSize(std::optional<i64> value);

    EValueType GetWireType() const;

    i64 GetMemoryUsage() const;
    i64 GetMemoryUsage(i64 threshold) const;

    // Check if column has plain old v1 type.
    bool IsOfV1Type() const;

    // Check if column has specified v1 type.
    bool IsOfV1Type(ESimpleLogicalValueType type) const;

    ESimpleLogicalValueType CastToV1Type() const;

    bool IsRenamed() const;
    std::string GetDiagnosticNameString() const;

private:
    ESimpleLogicalValueType V1Type_;
    EValueType WireType_;
    bool IsOfV1Type_;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TColumnSchema& schema, TStringBuf spec);

void Serialize(const TColumnSchema& schema, NYson::IYsonConsumer* consumer);
void Serialize(const TColumnSchema& schema, std::optional<std::string> constraint, NYson::IYsonConsumer* consumer);

void ToProto(NProto::TColumnSchema* protoSchema, const TColumnSchema& schema);
void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema);

void PrintTo(const TColumnSchema& columnSchema, std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

struct TDeletedColumn
{
    TDeletedColumn() = default;
    explicit TDeletedColumn(TColumnStableName stableName);

    DEFINE_BYREF_RW_PROPERTY(TColumnStableName, StableName);
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDeletedColumn& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TDeletedColumn& schema, NYTree::INodePtr node);
void Deserialize(TDeletedColumn& schema, NYson::TYsonPullParserCursor* cursor);

void ToProto(NProto::TDeletedColumn* protoSchema, const TDeletedColumn& schema);
void FromProto(TDeletedColumn* schema, const NProto::TDeletedColumn& protoSchema);

////////////////////////////////////////////////////////////////////////////////

class TTableSchema final
{
public:
    class TNameMapping
    {
    public:
        explicit TNameMapping(const TTableSchema& schema);

        bool IsDeleted(const TColumnStableName& stableName) const;
        std::string StableNameToName(const TColumnStableName& stableName) const;
        TColumnStableName NameToStableName(TStringBuf name) const;

    private:
        const TTableSchema& Schema_;
    };

    struct TSystemColumnOptions
    {
        bool EnableTableIndex;
        bool EnableRowIndex;
        bool EnableRangeIndex;
    };

public:
    const std::vector<TColumnSchema>& Columns() const;
    const std::vector<TDeletedColumn>& DeletedColumns() const;

    //! Strict schema forbids columns not specified in the schema.
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Strict, false);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(UniqueKeys, false);
    DEFINE_BYVAL_RO_PROPERTY(ETableSchemaModification, SchemaModification, ETableSchemaModification::None);

    //! Constructs an empty non-strict schema.
    TTableSchema() = default;

    TTableSchema(TTableSchema&&) = default;
    TTableSchema& operator=(TTableSchema&&) = default;
    TTableSchema(const TTableSchema&) = default;
    TTableSchema& operator=(const TTableSchema&) = default;

    //! Constructs a schema with given columns and strictness flag.
    //! No validation is performed.
    explicit TTableSchema(
        std::vector<TColumnSchema> columns,
        bool strict = true,
        bool uniqueKeys = false,
        ETableSchemaModification schemaModification = ETableSchemaModification::None,
        std::vector<TDeletedColumn> deletedColumns = {});

    const TColumnSchema* FindColumnByStableName(const TColumnStableName& stableName) const;
    const TDeletedColumn* FindDeletedColumn(const TColumnStableName& stableName) const;

    int GetColumnIndex(const TColumnSchema& column) const;

    int GetColumnIndex(TStringBuf name) const;
    int GetColumnIndexOrThrow(TStringBuf name) const;

    TNameMapping GetNameMapping() const;

    const TColumnSchema* FindColumn(TStringBuf name) const;
    const TColumnSchema& GetColumn(TStringBuf name) const;
    const TColumnSchema& GetColumnOrThrow(TStringBuf name) const;
    std::vector<std::string> GetColumnNames() const;

    TTableSchemaPtr Filter(
        const TColumnFilter& columnFilter,
        bool discardSortOrder = false) const;
    TTableSchemaPtr Filter(
        const THashSet<std::string>& columnNames,
        bool discardSortOrder = false) const;
    TTableSchemaPtr Filter(
        const std::optional<std::vector<std::string>>& columnNames,
        bool discardSortOrder = false) const;

    bool HasMaterializedComputedColumns() const;
    bool HasNonMaterializedComputedColumns() const;
    bool HasComputedColumns() const;
    bool HasAggregateColumns() const;
    bool HasHunkColumns() const;
    bool HasTimestampColumn() const;
    bool HasTtlColumn() const;
    bool IsSorted() const;
    bool HasRenamedColumns() const;
    bool IsEmpty() const;

    //! Checks if the first `keyColumnCount` columns
    //! (or all if not specified) are suitable for codegen comparison.
    bool IsCGComparatorApplicable(std::optional<int> keyColumnCount = std::nullopt) const;

    std::optional<int> GetTtlColumnIndex() const;

    std::vector<TColumnStableName> GetKeyColumnStableNames() const;
    TKeyColumns GetKeyColumnNames() const;
    TKeyColumns GetKeyColumns() const;

    int GetColumnCount() const;
    int GetKeyColumnCount() const;
    int GetValueColumnCount() const;
    std::vector<TColumnStableName> GetColumnStableNames() const;
    const THunkColumnIds& GetHunkColumnIds() const;

    TSortColumns GetSortColumns(const std::optional<TNameMapping>& nameMapping = std::nullopt) const;

    bool HasNontrivialSchemaModification() const;

    //! Constructs a non-strict schema from #keyColumns assigning all components EValueType::Any type.
    //! #keyColumns could be empty, in which case an empty non-strict schema is returned.
    //! The resulting schema is validated.
    static TTableSchemaPtr FromKeyColumns(const TKeyColumns& keyColumns);

    //! Same as above, but infers key column sort orders from #sortColumns.
    static TTableSchemaPtr FromSortColumns(const TSortColumns& sortColumns);

    //! Returns schema with `UniqueKeys' set to given value.
    TTableSchemaPtr SetUniqueKeys(bool uniqueKeys) const;

    //! Returns schema with `SchemaModification' set to given value.
    TTableSchemaPtr SetSchemaModification(ETableSchemaModification schemaModification) const;

    //! For sorted tables, return the current schema as-is.
    //! For ordered tables, prepends the current schema with |(tablet_index, row_index)| key columns.
    TTableSchemaPtr ToQuery() const;

    //! Appends |$table_index|, |$row_index| and/or |$range_index|, based on the options.
    TTableSchemaPtr WithSystemColumns(const TSystemColumnOptions& options) const;

    //! For sorted tables, return the current schema without computed columns.
    //! For ordered tables, prepends the current schema with |(tablet_index)| key column
    //! but without |$timestamp| column, if any.
    TTableSchemaPtr ToWrite() const;

    //! For sorted tables, return the current schema.
    //! For ordered tables, prepends the current schema with |(tablet_index, sequence_number)| key column.
    TTableSchemaPtr ToWriteViaQueueProducer() const;

    //! For sorted tables, return the current schema.
    //! For ordered tables, prepends the current schema with |(tablet_index)| key column.
    TTableSchemaPtr WithTabletIndex() const;

    //! Returns the current schema without |(tablet_index, row_index)| columns.
    TTableSchemaPtr ToCreate() const;

    //! Returns the current schema as-is.
    //! For ordered tables, prepends the current schema with |(tablet_index)| key column.
    TTableSchemaPtr ToVersionedWrite() const;

    //! For sorted tables, returns the non-computed key columns.
    //! For ordered tables, returns an empty schema.
    TTableSchemaPtr ToLookup() const;

    //! For sorted tables, returns the non-computed key columns.
    //! For ordered tables, returns an empty schema.
    TTableSchemaPtr ToDelete() const;

    //! For sorted tables, returns the non-computed key columns.
    //! For ordered tables, returns an empty schema.
    TTableSchemaPtr ToLock() const;

    //! Returns just the key columns.
    TTableSchemaPtr ToKeys() const;

    //! Returns the schema with UniqueKeys set to |true|.
    TTableSchemaPtr ToUniqueKeys() const;

    //! Returns the schema with all column attributes unset except
    //! StableName, Name, Type and Required.
    TTableSchemaPtr ToStrippedColumnAttributes() const;

    //! Returns the schema with all column attributes unset except
    //! StableName, Name, Type, Required and SortOrder.
    TTableSchemaPtr ToSortedStrippedColumnAttributes() const;

    //! Returns (possibly reordered) schema sorted by column names.
    TTableSchemaPtr ToCanonical() const;

    //! Returns (possibly reordered) schema with set key columns.
    TTableSchemaPtr ToSorted(const TKeyColumns& keyColumns) const;
    TTableSchemaPtr ToSorted(const TSortColumns& sortColumns) const;

    //! Only applies to sorted replicated tables.
    //! Returns the ordered schema used in replication logs.
    TTableSchemaPtr ToReplicationLog() const;

    //! Only applies to sorted dynamic tables.
    //! Returns the static schema used for unversioned updates from bulk insert.
    //! Key columns remain unchanged. Additional column |($change_type)| is prepended.
    //! Each value column |name| is replaced with two columns |($value:name)| and |($flags:name)|.
    //! If |sorted| is |false|, sort order is removed from key columns.
    TTableSchemaPtr ToUnversionedUpdate(bool sorted = true) const;

    TTableSchemaPtr ToModifiedSchema(ETableSchemaModification schemaModification) const;

    TComparator ToComparator(TCallback<TUUComparerSignature> cgComparator = {}) const;

    TKeyColumnTypes GetKeyColumnTypes() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    i64 GetMemoryUsage() const;
    i64 GetMemoryUsage(i64 threshold) const;

private:
    struct TColumnInfo
    {
        TColumnInfo(std::vector<TColumnSchema> columns, std::vector<TDeletedColumn> deletedColumns)
            : Columns(std::move(columns))
            , DeletedColumns(std::move(deletedColumns))
        { }

        std::vector<TColumnSchema> Columns;
        std::vector<TDeletedColumn> DeletedColumns;
    };

    std::shared_ptr<const TColumnInfo> ColumnInfo_;
    int KeyColumnCount_ = 0;
    bool HasMaterializedComputedColumns_ = false;
    bool HasNonMaterializedComputedColumns_ = false;
    bool HasAggregateColumns_ = false;
    THunkColumnIds HunkColumnsIds_;

    THashMap<TStringBuf, int> StableNameToColumnIndex_;
    THashMap<TStringBuf, int> NameToColumnIndex_;
    THashMap<TStringBuf, int> StableNameToDeletedColumnIndex_;
};

DEFINE_REFCOUNTED_TYPE(TTableSchema)

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TTableSchema& schema, TStringBuf spec);
void FormatValue(TStringBuilderBase* builder, const TTableSchemaPtr& schema, TStringBuf spec);

//! Returns serialized NTableClient.NProto.TTableSchemaExt.
std::string SerializeToWireProto(const TTableSchema& schema);
std::string SerializeToWireProto(const TTableSchemaPtr& schema);

void DeserializeFromWireProto(TTableSchemaPtr* schema, const std::string& serializedProto);

void Serialize(const TTableSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchema& schema, NYTree::INodePtr node);
void Deserialize(TTableSchema& schema, NYson::TYsonPullParserCursor* cursor);

void Serialize(const TTableSchemaPtr& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchemaPtr& schema, NYTree::INodePtr node);
void Deserialize(TTableSchemaPtr& schema, NYson::TYsonPullParserCursor* cursor);

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema);
void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema);
void FromProto(
    TTableSchema* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& keyColumnsExt);

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchemaPtr& schema);
void FromProto(TTableSchemaPtr* schema, const NProto::TTableSchemaExt& protoSchema);
void FromProto(
    TTableSchemaPtr* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& keyColumnsExt);

void PrintTo(const TTableSchema& tableSchema, std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaTruncatedFormatter
{
public:
    // NB: #schema is allowed to be |nullptr|.
    TTableSchemaTruncatedFormatter(const TTableSchemaPtr& schema, i64 memoryThreshold);

    void operator()(TStringBuilderBase* builder) const;

private:
    const TTableSchema* const Schema_ = nullptr;
    const i64 Threshold_ = 0;
};

TFormatterWrapper<TTableSchemaTruncatedFormatter> MakeTableSchemaTruncatedFormatter(
    const TTableSchemaPtr& schema,
    i64 memoryThreshold);

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs);

bool operator==(const TDeletedColumn& lhs, const TDeletedColumn& rhs);

bool operator==(const TTableSchema& lhs, const TTableSchema& rhs);

// Compat function for https://st.yandex-team.ru/YT-10668 workaround.
bool IsEqualIgnoringRequiredness(const TTableSchema& lhs, const TTableSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

std::vector<TColumnStableName> MapNamesToStableNames(
    const TTableSchema& schema,
    const std::vector<std::string>& names,
    std::optional<TStringBuf> missingColumnReplacement = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

struct TNestedColumn
{
    TStringBuf NestedTableName;
    bool IsKey;
    TStringBuf Aggregate;
};

std::optional<TNestedColumn> TryParseNestedAggregate(TStringBuf description);

EValueType GetNestedColumnElementType(const TLogicalType* logicalType);

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns);

void ValidateDynamicTableKeyColumnCount(int count);

void ValidateColumnName(const std::string& name);

////////////////////////////////////////////////////////////////////////////////

struct TSchemaValidationOptions
{
    bool AllowUnversionedUpdateColumns = false;
    bool AllowTimestampColumns = false;
    bool AllowOperationColumns = false;
};

void ValidateColumnSchema(
    const TColumnSchema& columnSchema,
    bool isTableSorted = false,
    bool isTableDynamic = false,
    const TSchemaValidationOptions& options = {});

void ValidateTableSchema(
    const TTableSchema& schema,
    bool isTableDynamic = false,
    const TSchemaValidationOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

void ValidateNoDescendingSortOrder(const TTableSchema& schema);
void ValidateNoDescendingSortOrder(
    const std::vector<ESortOrder>& sortOrders,
    const TKeyColumns& keyColumns);

void ValidateNoRenamedColumns(const TTableSchema& schema);

void ValidateColumnUniqueness(const TTableSchema& schema);

void ValidatePivotKey(
    TUnversionedRow pivotKey,
    const TTableSchema& schema,
    TStringBuf keyType = "pivot",
    bool validateRequired = false);

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, int> GetLocksMapping(
    const NTableClient::TTableSchema& schema,
    bool fullAtomicity,
    std::vector<int>* columnIndexToLockIndex = nullptr,
    std::vector<std::string>* lockIndexToName = nullptr);

TLockMask GetLockMask(
    const NTableClient::TTableSchema& schema,
    bool fullAtomicity,
    const std::vector<std::string>& locks,
    ELockType lockType = ELockType::SharedWeak);

////////////////////////////////////////////////////////////////////////////////

// NB: Need to place this into NProto for ADL to work properly since TKeyColumns is std::vector.
namespace NProto {

void ToProto(NProto::TKeyColumnsExt* protoKeyColumns, const TKeyColumns& keyColumns);
void FromProto(TKeyColumns* keyColumns, const NProto::TKeyColumnsExt& protoKeyColumns);

void ToProto(TColumnFilter* protoColumnFilter, const NTableClient::TColumnFilter& columnFilter);
void FromProto(NTableClient::TColumnFilter* columnFilter, const TColumnFilter& protoColumnFilter);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

// Incompatible < RequireValidation < FullyCompatible
constexpr bool operator<(ESchemaCompatibility lhs, ESchemaCompatibility rhs);
constexpr bool operator<=(ESchemaCompatibility lhs, ESchemaCompatibility rhs);
constexpr bool operator>(ESchemaCompatibility lhs, ESchemaCompatibility rhs);
constexpr bool operator>=(ESchemaCompatibility lhs, ESchemaCompatibility rhs);

////////////////////////////////////////////////////////////////////////////////

struct TTableSchemaHash
{
    size_t operator()(const TTableSchema& schema) const;
    size_t operator()(const TTableSchemaPtr& schema) const;
};

struct TTableSchemaEquals
{
    bool operator()(const TTableSchema& lhs, const TTableSchema& rhs) const;
    bool operator()(const TTableSchemaPtr& lhs, const TTableSchemaPtr& rhs) const;
    bool operator()(const TTableSchemaPtr& lhs, const TTableSchema& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NTableClient::TColumnStableName>
{
    size_t operator()(const NYT::NTableClient::TColumnStableName& stableName) const;
};

template <>
struct THash<NYT::NTableClient::TDeletedColumn>
{
    size_t operator()(const NYT::NTableClient::TDeletedColumn& deletedColumn) const;
};

template <>
struct THash<NYT::NTableClient::TColumnSchema>
{
    size_t operator()(const NYT::NTableClient::TColumnSchema& columnSchema) const;
};

template <>
struct THash<NYT::NTableClient::TTableSchema>
{
    size_t operator()(const NYT::NTableClient::TTableSchema& tableSchema) const;
};

////////////////////////////////////////////////////////////////////////////////

#define SCHEMA_INL_H_
#include "schema-inl.h"
#undef SCHEMA_INL_H_
