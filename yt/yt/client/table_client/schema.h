#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

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
    TLegacyLockMask& operator= (const TLegacyLockMask& other) = default;

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

bool operator == (const TLockMask& lhs, const TLockMask& rhs);

TLockMask MaxMask(TLockMask lhs, TLockMask rhs);

////////////////////////////////////////////////////////////////////////////////

class TColumnSchema
{
public:
    // Keep in sync with hasher below.
    DEFINE_BYREF_RO_PROPERTY(TColumnStableName, StableName);
    DEFINE_BYREF_RO_PROPERTY(TString, Name);
    DEFINE_BYREF_RO_PROPERTY(TLogicalTypePtr, LogicalType);
    DEFINE_BYREF_RO_PROPERTY(std::optional<ESortOrder>, SortOrder);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Lock);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Expression);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Aggregate);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Group);
    DEFINE_BYREF_RO_PROPERTY(bool, Required);
    DEFINE_BYREF_RO_PROPERTY(std::optional<i64>, MaxInlineHunkSize);

public:
    TColumnSchema();
    TColumnSchema(
        TString name,
        EValueType type,
        std::optional<ESortOrder> sortOrder = {});
    TColumnSchema(
        TString name,
        ESimpleLogicalValueType type,
        std::optional<ESortOrder> sortOrder = {});

    TColumnSchema(
        TString name,
        TLogicalTypePtr type,
        std::optional<ESortOrder> sortOrder = {});

    TColumnSchema(const TColumnSchema&) = default;
    TColumnSchema(TColumnSchema&&) = default;

    TColumnSchema& operator=(const TColumnSchema&) = default;
    TColumnSchema& operator=(TColumnSchema&&) = default;

    TColumnSchema& SetStableName(TColumnStableName stableName);
    TColumnSchema& SetName(TString name);
    TColumnSchema& SetLogicalType(TLogicalTypePtr valueType);
    TColumnSchema& SetSimpleLogicalType(ESimpleLogicalValueType type);
    TColumnSchema& SetSortOrder(std::optional<ESortOrder> value);
    TColumnSchema& SetLock(std::optional<TString> value);
    TColumnSchema& SetExpression(std::optional<TString> value);
    TColumnSchema& SetAggregate(std::optional<TString> value);
    TColumnSchema& SetGroup(std::optional<TString> value);
    TColumnSchema& SetRequired(bool value);
    TColumnSchema& SetMaxInlineHunkSize(std::optional<i64> value);

    EValueType GetWireType() const;

    i64 GetMemoryUsage() const;

    // Check if column has plain old v1 type.
    bool IsOfV1Type() const;

    // Check if column has specified v1 type.
    bool IsOfV1Type(ESimpleLogicalValueType type) const;

    ESimpleLogicalValueType CastToV1Type() const;

    bool IsRenamed() const;
    TString GetDiagnosticNameString() const;

private:
    ESimpleLogicalValueType V1Type_;
    EValueType WireType_;
    bool IsOfV1Type_;
};

////////////////////////////////////////////////////////////////////////////////

class TDeletedColumn
{
public:
    TDeletedColumn();
    explicit TDeletedColumn(TColumnStableName stableName);

    DEFINE_BYREF_RO_PROPERTY(TColumnStableName, StableName);
    TDeletedColumn& SetStableName(TColumnStableName stableName);
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TColumnSchema& schema, TStringBuf spec);

void Serialize(const TColumnSchema& schema, NYson::IYsonConsumer* consumer);

void ToProto(NProto::TColumnSchema* protoSchema, const TColumnSchema& schema);
void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema);

void ToProto(NProto::TDeletedColumn* protoSchema, const TDeletedColumn& schema);
void FromProto(TDeletedColumn* schema, const NProto::TDeletedColumn& protoSchema);

void PrintTo(const TColumnSchema& columnSchema, std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

class TTableSchema final
{
public:
    class TNameMapping
    {
    public:
        explicit TNameMapping(const TTableSchema& schema);

        bool IsDeleted(const TColumnStableName& stableName) const;
        TString StableNameToName(const TColumnStableName& stableName) const;
        TColumnStableName NameToStableName(TStringBuf name) const;

    private:
        const TTableSchema& Schema_;
    };

public:
    const std::vector<TColumnSchema>& Columns() const;
    const std::vector<TDeletedColumn>& DeletedColumns() const;

    //! Strict schema forbids columns not specified in the schema.
    DEFINE_BYVAL_RO_PROPERTY(bool, Strict, false);
    DEFINE_BYVAL_RO_PROPERTY(bool, UniqueKeys, false);
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
    std::vector<TString> GetColumnNames() const;

    TTableSchemaPtr Filter(
        const TColumnFilter& columnFilter,
        bool discardSortOrder = false) const;
    TTableSchemaPtr Filter(
        const THashSet<TString>& columnNames,
        bool discardSortOrder = false) const;
    TTableSchemaPtr Filter(
        const std::optional<std::vector<TString>>& columnNames,
        bool discardSortOrder = false) const;

    bool HasComputedColumns() const;
    bool HasAggregateColumns() const;
    bool HasHunkColumns() const;
    bool HasTimestampColumn() const;
    bool IsSorted() const;
    bool IsUniqueKeys() const;
    bool HasRenamedColumns() const;
    bool IsEmpty() const;

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

    //! For sorted tables, return the current schema without computed columns.
    //! For ordered tables, prepends the current schema with |(tablet_index)| key column
    //! but without |$timestamp| column, if any.
    TTableSchemaPtr ToWrite() const;

    //! For sorted tables, return the current schema
    //! For ordered tables, prepends the current schema with |(tablet_index)| key column.
    TTableSchemaPtr WithTabletIndex() const;

    //! Returns the current schema as-is.
    //! For ordered tables, prepends the current schema with |(tablet_index)| key column.
    TTableSchemaPtr ToVersionedWrite() const;

    //! For sorted tables, returns the non-computed key columns.
    //! For ordered tables, returns an empty schema.
    TTableSchemaPtr ToLookup() const;

    //! For sorted tables, returns the non-computed key columns.
    //! For ordered tables, returns an empty schema.
    TTableSchemaPtr ToDelete() const;

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

    TComparator ToComparator() const;

    TKeyColumnTypes GetKeyColumnTypes() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    i64 GetMemoryUsage() const;

private:
    struct TColumnInfo
    {
        TColumnInfo(std::vector<TColumnSchema> columns, std::vector<TDeletedColumn> deletedColumns)
            : Columns(std::move(columns))
            , DeletedColumns(std::move(deletedColumns))
        {
        }

        std::vector<TColumnSchema> Columns;
        std::vector<TDeletedColumn> DeletedColumns;
    };


    std::shared_ptr<const TColumnInfo> ColumnInfo_;
    int KeyColumnCount_ = 0;
    bool HasComputedColumns_ = false;
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

TString ToString(const TTableSchema& schema);
TString ToString(const TTableSchemaPtr& schema);

//! Returns serialized NTableClient.NProto.TTableSchemaExt.
TString SerializeToWireProto(const TTableSchemaPtr& schema);

void DeserializeFromWireProto(TTableSchemaPtr* schema, const TString& serializedProto);

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

bool operator == (const TColumnSchema& lhs, const TColumnSchema& rhs);

bool operator == (const TDeletedColumn& lhs, const TDeletedColumn& rhs);

bool operator == (const TTableSchema& lhs, const TTableSchema& rhs);

// Compat function for https://st.yandex-team.ru/YT-10668 workaround.
bool IsEqualIgnoringRequiredness(const TTableSchema& lhs, const TTableSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf NonexistentColumnName = "$__YT_NONEXISTENT_COLUMN_NAME__";

std::vector<TColumnStableName> MapNamesToStableNames(
    const TTableSchema& schema,
    std::vector<TString> names,
    const std::optional<TStringBuf>& missingColumnReplacement = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns);

void ValidateColumnName(const TString& name);

void ValidateColumnSchema(
    const TColumnSchema& columnSchema,
    bool isTableSorted = false,
    bool isTableDynamic = false,
    bool allowUnversionedUpdateColumns = false);

void ValidateTableSchema(
    const TTableSchema& schema,
    bool isTableDynamic = false,
    bool allowUnversionedUpdateColumns = false);

void ValidateNoDescendingSortOrder(const TTableSchema& schema);

void ValidateNoRenamedColumns(const TTableSchema& schema);

void ValidateColumnUniqueness(const TTableSchema& schema);

void ValidatePivotKey(
    TUnversionedRow pivotKey,
    const TTableSchema& schema,
    TStringBuf keyType = "pivot",
    bool validateRequired = false);

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, int> GetLocksMapping(
    const NTableClient::TTableSchema& schema,
    bool fullAtomicity,
    std::vector<int>* columnIndexToLockIndex = nullptr,
    std::vector<TString>* lockIndexToName = nullptr);

TLockMask GetLockMask(
    const NTableClient::TTableSchema& schema,
    bool fullAtomicity,
    const std::vector<TString>& locks,
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
constexpr bool operator < (ESchemaCompatibility lhs, ESchemaCompatibility rhs);
constexpr bool operator <= (ESchemaCompatibility lhs, ESchemaCompatibility rhs);
constexpr bool operator > (ESchemaCompatibility lhs, ESchemaCompatibility rhs);
constexpr bool operator >= (ESchemaCompatibility lhs, ESchemaCompatibility rhs);

////////////////////////////////////////////////////////////////////////////////

struct TTableSchemaHash
{
    size_t operator() (const TTableSchema& schema) const;
    size_t operator() (const TTableSchemaPtr& schema) const;
};

struct TTableSchemaEquals
{
    bool operator() (const TTableSchema& lhs, const TTableSchema& rhs) const;
    bool operator() (const TTableSchemaPtr& lhs, const TTableSchemaPtr& rhs) const;
    bool operator() (const TTableSchemaPtr& lhs, const TTableSchema& rhs) const;
};

struct TCellTaggedTableSchema
{
    TCellTaggedTableSchema(TTableSchema tableSchema, NObjectClient::TCellTag cellTag);

    TTableSchema TableSchema;
    NObjectClient::TCellTag CellTag;
};

struct TCellTaggedTableSchemaPtr
{
    TCellTaggedTableSchemaPtr(TTableSchemaPtr tableSchema, NObjectClient::TCellTag cellTag);

    TTableSchemaPtr TableSchema;
    NObjectClient::TCellTag CellTag;
};

////////////////////////////////////////////////////////////////////////////////

struct TCellTaggedTableSchemaHash
{
    size_t operator() (const TCellTaggedTableSchema& cellTaggedSchema) const;
    size_t operator() (const TCellTaggedTableSchemaPtr& cellTaggedSchemaPtr) const;
};

struct TCellTaggedTableSchemaEquals
{
    bool operator() (const TCellTaggedTableSchema& lhs, const TCellTaggedTableSchema& rhs) const;
    bool operator() (const TCellTaggedTableSchemaPtr& lhs, const TCellTaggedTableSchemaPtr& rhs) const;
    bool operator() (const TCellTaggedTableSchemaPtr& lhs, const TCellTaggedTableSchema& rhs) const;
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
