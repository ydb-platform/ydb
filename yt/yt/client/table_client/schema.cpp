#include "schema.h"

#include "column_sort_schema.h"
#include "comparator.h"
#include "logical_type.h"
#include "unversioned_row.h"

#include <optional>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/client/table_client/schema_serialization_helpers.h>

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt_proto/yt/client/tablet_client/proto/lock_mask.pb.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NYson;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

int GetLockPriority(ELockType lockType)
{
    switch (lockType) {
        case ELockType::None:
            return 0;
        case ELockType::SharedWeak:
            return 1;
        case ELockType::SharedStrong:
            return 2;
        case ELockType::SharedWrite:
            return 3;
        case ELockType::Exclusive:
            return 4;
        default:
            YT_ABORT();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsReadLock(ELockType lock)
{
    return lock == ELockType::SharedWeak || lock == ELockType::SharedStrong;
}

bool IsWriteLock(ELockType lock)
{
    return lock == ELockType::Exclusive || lock == ELockType::SharedWrite;
}

ELockType GetStrongestLock(ELockType lhs, ELockType rhs)
{
    if (lhs == ELockType::None) {
        return rhs;
    }

    if (rhs == ELockType::None) {
        return lhs;
    }

    if (IsReadLock(lhs) && IsWriteLock(rhs) ||
        IsReadLock(rhs) && IsWriteLock(lhs))
    {
        return ELockType::Exclusive;
    }

    return GetLockPriority(lhs) > GetLockPriority(rhs) ? lhs : rhs;
}

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TLockMask& lhs, const TLockMask& rhs)
{
    int lockCount = std::max(lhs.GetSize(), rhs.GetSize());
    for (int index = 0; index < lockCount; ++index) {
        if (lhs.Get(index) != rhs.Get(index)) {
            return false;
        }
    }

    return true;
}

TLockMask MaxMask(TLockMask lhs, TLockMask rhs)
{
    int size = std::max<int>(lhs.GetSize(), rhs.GetSize());
    for (int index = 0; index < size; ++index) {
        lhs.Set(index, GetStrongestLock(lhs.Get(index), rhs.Get(index)));
    }

    return lhs;
}

void ToProto(NTabletClient::NProto::TLockMask* protoLockMask, const TLockMask& lockMask)
{
    auto size = lockMask.GetSize();
    YT_VERIFY(size <= TLockMask::MaxSize);

    protoLockMask->set_size(size);

    const auto& bitmap = lockMask.GetBitmap();
    auto wordCount = DivCeil(size, TLockMask::LocksPerWord);
    YT_VERIFY(std::ssize(bitmap) >= wordCount);

    protoLockMask->clear_bitmap();
    for (int index = 0; index < wordCount; ++index) {
        protoLockMask->add_bitmap(bitmap[index]);
    }
}

void FromProto(TLockMask* lockMask, const NTabletClient::NProto::TLockMask& protoLockMask)
{
    auto size = protoLockMask.size();
    auto wordCount = DivCeil<int>(size, TLockMask::LocksPerWord);

    TLockBitmap bitmap;
    bitmap.reserve(wordCount);
    for (int index = 0; index < wordCount; ++index) {
        bitmap.push_back(protoLockMask.bitmap(index));
    }

    *lockMask = TLockMask(bitmap, size);
}

////////////////////////////////////////////////////////////////////////////////

TColumnSchema::TColumnSchema()
    : TColumnSchema(
        TString(),
        NullLogicalType(),
        std::nullopt)
{ }

TColumnSchema::TColumnSchema(
    TString name,
    EValueType type,
    std::optional<ESortOrder> sortOrder)
    : TColumnSchema(
        std::move(name),
        MakeLogicalType(GetLogicalType(type), /*required*/ false),
        sortOrder)
{ }

TColumnSchema::TColumnSchema(
    TString name,
    ESimpleLogicalValueType type,
    std::optional<ESortOrder> sortOrder)
    : TColumnSchema(
        std::move(name),
        MakeLogicalType(type, /*required*/ false),
        sortOrder)
{ }

TColumnSchema::TColumnSchema(
    TString name,
    TLogicalTypePtr type,
    std::optional<ESortOrder> sortOrder)
    : StableName_(name)
    , Name_(name)
    , SortOrder_(sortOrder)
{
    SetLogicalType(std::move(type));
}

TColumnSchema& TColumnSchema::SetStableName(TColumnStableName value)
{
    StableName_ = std::move(value);
    return *this;
}

TColumnSchema& TColumnSchema::SetName(TString value)
{
    Name_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetSortOrder(std::optional<ESortOrder> value)
{
    SortOrder_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetLock(std::optional<TString> value)
{
    Lock_ = std::move(value);
    return *this;
}

TColumnSchema& TColumnSchema::SetGroup(std::optional<TString> value)
{
    Group_ = std::move(value);
    return *this;
}

TColumnSchema& TColumnSchema::SetExpression(std::optional<TString> value)
{
    Expression_ = std::move(value);
    return *this;
}

TColumnSchema& TColumnSchema::SetAggregate(std::optional<TString> value)
{
    Aggregate_ = std::move(value);
    return *this;
}

TColumnSchema& TColumnSchema::SetRequired(bool value)
{
    if (!IsOfV1Type_) {
        THROW_ERROR_EXCEPTION("Cannot set required flag for non-v1 typed column");
    }
    SetLogicalType(MakeLogicalType(V1Type_, value));
    return *this;
}

TColumnSchema& TColumnSchema::SetMaxInlineHunkSize(std::optional<i64> value)
{
    MaxInlineHunkSize_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetLogicalType(TLogicalTypePtr type)
{
    LogicalType_ = std::move(type);
    WireType_ = NTableClient::GetWireType(LogicalType_);
    IsOfV1Type_ = IsV1Type(LogicalType_);
    std::tie(V1Type_, Required_) = NTableClient::CastToV1Type(LogicalType_);
    return *this;
}

TColumnSchema& TColumnSchema::SetSimpleLogicalType(ESimpleLogicalValueType type)
{
    SetLogicalType(MakeLogicalType(type, /*required*/ false));
    return *this;
}

EValueType TColumnSchema::GetWireType() const
{
    return WireType_;
}

i64 TColumnSchema::GetMemoryUsage() const
{
    return
        sizeof(TColumnSchema) +
        StableName_.Underlying().size() +
        Name_.size() +
        (LogicalType_ ? LogicalType_->GetMemoryUsage() : 0) +
        (Lock_ ? Lock_->size() : 0) +
        (Expression_ ? Expression_->size() : 0) +
        (Aggregate_ ? Aggregate_->size() : 0) +
        (Group_ ? Group_->size() : 0);
}

bool TColumnSchema::IsOfV1Type() const
{
    return IsOfV1Type_;
}

bool TColumnSchema::IsOfV1Type(ESimpleLogicalValueType type) const
{
    return IsOfV1Type_ && V1Type_ == type;
}

ESimpleLogicalValueType TColumnSchema::CastToV1Type() const
{
    return V1Type_;
}

bool TColumnSchema::IsRenamed() const
{
    return Name() != StableName().Underlying();
}

TString TColumnSchema::GetDiagnosticNameString() const
{
    if (IsRenamed()) {
        return Format("%Qv (stable name %Qv)", Name(), StableName().Underlying());
    } else {
        return Format("%Qv", Name());
    }
}

////////////////////////////////////////////////////////////////////////////////

TDeletedColumn::TDeletedColumn()
{
}

TDeletedColumn::TDeletedColumn(TColumnStableName stableName)
    : StableName_(stableName)
{
}

TDeletedColumn& TDeletedColumn::SetStableName(TColumnStableName value)
{
    StableName_ = std::move(value);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TColumnSchema& schema, TStringBuf /*spec*/)
{
    builder->AppendChar('{');

    builder->AppendFormat("name=%Qv", schema.Name());

    if (schema.IsRenamed()) {
        builder->AppendFormat("; stable_name=%Qv", schema.StableName());
    }

    if (const auto& logicalType = schema.LogicalType()) {
        builder->AppendFormat("; type=%v", *logicalType);
    }

    if (const auto& sortOrder = schema.SortOrder()) {
        builder->AppendFormat("; sort_order=%v", *sortOrder);
    }

    if (const auto& lock = schema.Lock()) {
        builder->AppendFormat("; lock=%v", *lock);
    }

    if (const auto& expression = schema.Expression()) {
        builder->AppendFormat("; expression=%Qv", *expression);
    }

    if (const auto& aggregate = schema.Aggregate()) {
        builder->AppendFormat("; aggregate=%v", *aggregate);
    }

    if (const auto& group = schema.Group()) {
        builder->AppendFormat("; group=%v", *group);
    }

    builder->AppendFormat("; physical_type=%v", CamelCaseToUnderscoreCase(ToString(schema.CastToV1Type())));

    builder->AppendFormat("; required=%v", schema.Required());

    if (auto maxInlineHunkSize = schema.MaxInlineHunkSize()) {
        builder->AppendFormat("; max_inline_hunk_size=%v", *maxInlineHunkSize);
    }

    builder->AppendChar('}');
}

void FormatValue(TStringBuilderBase* builder, const TDeletedColumn& schema, TStringBuf spec)
{
    builder->AppendChar('{');

    FormatValue(builder, schema.StableName(), spec);
    builder->AppendFormat("; deleted=true");

    builder->AppendChar('}');
}

void ToProto(NProto::TColumnSchema* protoSchema, const TColumnSchema& schema)
{
    protoSchema->set_name(schema.Name());
    if (schema.IsRenamed()) {
        protoSchema->set_stable_name(schema.StableName().Underlying());
    }

    protoSchema->set_type(static_cast<int>(GetPhysicalType(schema.CastToV1Type())));

    if (schema.IsOfV1Type()) {
        protoSchema->set_simple_logical_type(static_cast<int>(schema.CastToV1Type()));
    } else {
        protoSchema->clear_simple_logical_type();
    }
    if (schema.Required()) {
        protoSchema->set_required(true);
    } else {
        protoSchema->clear_required();
    }
    ToProto(protoSchema->mutable_logical_type(), schema.LogicalType());
    if (schema.Lock()) {
        protoSchema->set_lock(*schema.Lock());
    } else {
        protoSchema->clear_lock();
    }
    if (schema.Expression()) {
        protoSchema->set_expression(*schema.Expression());
    } else {
        protoSchema->clear_expression();
    }
    if (schema.Aggregate()) {
        protoSchema->set_aggregate(*schema.Aggregate());
    } else {
        protoSchema->clear_aggregate();
    }
    if (schema.SortOrder()) {
        protoSchema->set_sort_order(static_cast<int>(*schema.SortOrder()));
    } else {
        protoSchema->clear_sort_order();
    }
    if (schema.Group()) {
        protoSchema->set_group(*schema.Group());
    } else {
        protoSchema->clear_group();
    }
    if (schema.MaxInlineHunkSize()) {
        protoSchema->set_max_inline_hunk_size(*schema.MaxInlineHunkSize());
    } else {
        protoSchema->clear_max_inline_hunk_size();
    }
}

void ToProto(NProto::TDeletedColumn* protoSchema, const TDeletedColumn& schema)
{
    protoSchema->set_stable_name(schema.StableName().Underlying());
}

void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema)
{
    schema->SetName(protoSchema.name());
    schema->SetStableName(protoSchema.has_stable_name()
        ? TColumnStableName(protoSchema.stable_name())
        : TColumnStableName(protoSchema.name()));

    if (protoSchema.has_logical_type()) {
        TLogicalTypePtr logicalType;
        FromProto(&logicalType, protoSchema.logical_type());
        schema->SetLogicalType(logicalType);
    } else if (protoSchema.has_simple_logical_type()) {
        schema->SetLogicalType(
            MakeLogicalType(
                CheckedEnumCast<ESimpleLogicalValueType>(protoSchema.simple_logical_type()),
                protoSchema.required()));
    } else {
        auto physicalType = CheckedEnumCast<EValueType>(protoSchema.type());
        schema->SetLogicalType(MakeLogicalType(GetLogicalType(physicalType), protoSchema.required()));
    }

    schema->SetLock(protoSchema.has_lock() ? std::make_optional(protoSchema.lock()) : std::nullopt);
    schema->SetExpression(protoSchema.has_expression() ? std::make_optional(protoSchema.expression()) : std::nullopt);
    schema->SetAggregate(protoSchema.has_aggregate() ? std::make_optional(protoSchema.aggregate()) : std::nullopt);
    schema->SetSortOrder(protoSchema.has_sort_order() ? std::make_optional(CheckedEnumCast<ESortOrder>(protoSchema.sort_order())) : std::nullopt);
    schema->SetGroup(protoSchema.has_group() ? std::make_optional(protoSchema.group()) : std::nullopt);
    schema->SetMaxInlineHunkSize(protoSchema.has_max_inline_hunk_size() ? std::make_optional(protoSchema.max_inline_hunk_size()) : std::nullopt);
}

void FromProto(TDeletedColumn* schema, const NProto::TDeletedColumn& protoSchema)
{
    schema->SetStableName(TColumnStableName{protoSchema.stable_name()});
}

void PrintTo(const TColumnSchema& columnSchema, std::ostream* os)
{
    *os << Format("%v", columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

TTableSchema::TNameMapping::TNameMapping(const TTableSchema& schema)
    : Schema_(schema)
{ }

bool TTableSchema::TNameMapping::IsDeleted(const TColumnStableName& stableName) const
{
    return Schema_.FindDeletedColumn(stableName) != nullptr;
}

TString TTableSchema::TNameMapping::StableNameToName(const TColumnStableName& stableName) const
{
    auto* column = Schema_.FindColumnByStableName(stableName);
    if (!column) {
        if (Schema_.GetStrict()) {
            THROW_ERROR_EXCEPTION("No column with stable name %Qv in strict schema", stableName);
        }
        return stableName.Underlying();
    }
    return column->Name();
}

TColumnStableName TTableSchema::TNameMapping::NameToStableName(TStringBuf name) const
{
    auto* column = Schema_.FindColumn(name);
    if (!column) {
        if (Schema_.GetStrict()) {
            THROW_ERROR_EXCEPTION("No column with name %Qv in strict schema", name);
        }
        return TColumnStableName(TString(name));
    }
    return column->StableName();
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TColumnSchema>& TTableSchema::Columns() const
{
    if (ColumnInfo_) [[likely]] {
        return ColumnInfo_->Columns;
    } else {
        static const std::vector<TColumnSchema> empty;
        return empty;
    }
}

const std::vector<TDeletedColumn>& TTableSchema::DeletedColumns() const
{
    if (ColumnInfo_) [[likely]] {
        return ColumnInfo_->DeletedColumns;
    } else {
        static std::vector<TDeletedColumn> empty;
        return empty;
    }
}

TTableSchema::TTableSchema(
    std::vector<TColumnSchema> columns,
    bool strict,
    bool uniqueKeys,
    ETableSchemaModification schemaModification,
    std::vector<TDeletedColumn> deletedColumns)
    : Strict_(strict)
    , UniqueKeys_(uniqueKeys)
    , SchemaModification_(schemaModification)
    , ColumnInfo_(std::make_shared<const TColumnInfo>(std::move(columns), std::move(deletedColumns)))
{
    const auto& info = *ColumnInfo_;
    for (int index = 0; index < std::ssize(Columns()); ++index) {
        const auto& column = info.Columns[index];
        if (column.SortOrder()) {
            ++KeyColumnCount_;
        }
        if (column.Expression()) {
            HasComputedColumns_ = true;
        }
        if (column.Aggregate()) {
            HasAggregateColumns_ = true;
        }
        if (column.MaxInlineHunkSize()) {
            HunkColumnsIds_.push_back(index);
        }
        // NB(levysotsky): We ignore duplicates in both maps, they will be
        // accounted for in consequent validation.
        NameToColumnIndex_.emplace(column.Name(), index);
        StableNameToColumnIndex_.emplace(column.StableName().Underlying(), index);
    }
    for (int index = 0; index < std::ssize(DeletedColumns()); ++index) {
        const auto& deletedColumn = info.DeletedColumns[index];
        StableNameToDeletedColumnIndex_.emplace(deletedColumn.StableName().Underlying(), index);
    }
}

const TColumnSchema* TTableSchema::FindColumnByStableName(const TColumnStableName& stableName) const
{
    auto it = StableNameToColumnIndex_.find(stableName.Underlying());
    if (it == StableNameToColumnIndex_.end()) {
        return nullptr;
    }
    return &Columns()[it->second];
}

const TDeletedColumn* TTableSchema::FindDeletedColumn(const TColumnStableName& stableName) const
{
    auto it = StableNameToDeletedColumnIndex_.find(stableName.Underlying());
    if (it == StableNameToDeletedColumnIndex_.end()) {
        return nullptr;
    }
    return &DeletedColumns()[it->second];
}

const TColumnSchema* TTableSchema::FindColumn(TStringBuf name) const
{
    auto it = NameToColumnIndex_.find(name);
    if (it == NameToColumnIndex_.end()) {
        return nullptr;
    }
    return &Columns()[it->second];
}

const TColumnSchema& TTableSchema::GetColumn(TStringBuf name) const
{
    auto* column = FindColumn(name);
    YT_VERIFY(column);
    return *column;
}

const TColumnSchema& TTableSchema::GetColumnOrThrow(TStringBuf name) const
{
    auto* column = FindColumn(name);
    if (!column) {
        THROW_ERROR_EXCEPTION("Missing schema column with name %Qv", name);
    }
    return *column;
}

int TTableSchema::GetColumnIndex(const TColumnSchema& column) const
{
    auto begin = Columns().data();
    auto end = begin + Columns().size();
    YT_VERIFY(begin <= &column && &column < end);

    return &column - begin;
}

int TTableSchema::GetColumnIndex(TStringBuf name) const
{
    return GetColumnIndex(GetColumn(name));
}

int TTableSchema::GetColumnIndexOrThrow(TStringBuf name) const
{
    return GetColumnIndex(GetColumnOrThrow(name));
}

TTableSchema::TNameMapping TTableSchema::GetNameMapping() const
{
    return TNameMapping(*this);
}

TTableSchemaPtr TTableSchema::Filter(const TColumnFilter& columnFilter, bool discardSortOrder) const
{
    int newKeyColumnCount = 0;
    std::vector<TColumnSchema> columns;
    if (columnFilter.IsUniversal()) {
        if (!discardSortOrder) {
            return New<TTableSchema>(*this);
        } else {
            columns = Columns();
            for (auto& column : columns) {
                column.SetSortOrder(std::nullopt);
            }
        }
    } else {
        bool inKeyColumns = !discardSortOrder;
        const auto& schemaColumns = Columns();
        for (int id : columnFilter.GetIndexes()) {
            if (id < 0 || id >= std::ssize(schemaColumns)) {
                THROW_ERROR_EXCEPTION("Invalid column during schema filtering: expected in range [0, %v), got %v",
                    schemaColumns.size(),
                    id);
            }

            if (id != std::ssize(columns) || !schemaColumns[id].SortOrder()) {
                inKeyColumns = false;
            }

            columns.push_back(schemaColumns[id]);

            if (!inKeyColumns) {
                columns.back().SetSortOrder(std::nullopt);
            }

            if (columns.back().SortOrder()) {
                ++newKeyColumnCount;
            }
        }
    }

    return New<TTableSchema>(
        std::move(columns),
        Strict_,
        UniqueKeys_ && (newKeyColumnCount == GetKeyColumnCount()),
        ETableSchemaModification::None,
        DeletedColumns());
}

TTableSchemaPtr TTableSchema::Filter(const THashSet<TString>& columnNames, bool discardSortOrder) const
{
    TColumnFilter::TIndexes indexes;
    for (const auto& column : Columns()) {
        if (columnNames.find(column.Name()) != columnNames.end()) {
            indexes.push_back(GetColumnIndex(column));
        }
    }
    return Filter(TColumnFilter(std::move(indexes)), discardSortOrder);
}

TTableSchemaPtr TTableSchema::Filter(const std::optional<std::vector<TString>>& columnNames, bool discardSortOrder) const
{
    if (!columnNames) {
        return Filter(TColumnFilter(), discardSortOrder);
    }

    return Filter(THashSet<TString>(columnNames->begin(), columnNames->end()), discardSortOrder);
}

bool TTableSchema::HasComputedColumns() const
{
    return HasComputedColumns_;
}

bool TTableSchema::HasAggregateColumns() const
{
    return HasAggregateColumns_;
}

bool TTableSchema::HasHunkColumns() const
{
    return !HunkColumnsIds_.empty();
}

bool TTableSchema::HasTimestampColumn() const
{
    return FindColumn(TimestampColumnName);
}

bool TTableSchema::HasTtlColumn() const
{
    return FindColumn(TtlColumnName);
}

bool TTableSchema::IsSorted() const
{
    return KeyColumnCount_ > 0;
}

bool TTableSchema::IsUniqueKeys() const
{
    return UniqueKeys_;
}

bool TTableSchema::HasRenamedColumns() const
{
    return std::any_of(Columns().begin(), Columns().end(), [] (const TColumnSchema& column) {
        return column.IsRenamed();
    });
}

bool TTableSchema::IsEmpty() const
{
    return Columns().empty();
}

bool TTableSchema::IsCGComparatorApplicable() const
{
    if (GetKeyColumnCount() > MaxKeyColumnCountInDynamicTable) {
        return false;
    }

    auto keyTypes = GetKeyColumnTypes();
    return std::none_of(keyTypes.begin(), keyTypes.end(), [] (auto type) {
        return type == EValueType::Any;
    });
}

std::optional<int> TTableSchema::GetTtlColumnIndex() const
{
    auto* column = FindColumn(TtlColumnName);
    if (!column) {
        return std::nullopt;
    }
    return GetColumnIndex(*column);
}

TKeyColumns TTableSchema::GetKeyColumnNames() const
{
    TKeyColumns keyColumns;
    for (const auto& column : Columns()) {
        if (column.SortOrder()) {
            keyColumns.push_back(column.Name());
        }
    }
    return keyColumns;
}

std::vector<TColumnStableName> TTableSchema::GetKeyColumnStableNames() const
{
    std::vector<TColumnStableName> keyColumns;
    for (const auto& column : Columns()) {
        if (column.SortOrder()) {
            keyColumns.push_back(column.StableName());
        }
    }
    return keyColumns;
}

TKeyColumns TTableSchema::GetKeyColumns() const
{
    return GetKeyColumnNames();
}

int TTableSchema::GetColumnCount() const
{
    return ssize(Columns());
}

std::vector<TString> TTableSchema::GetColumnNames() const
{
    if (!ColumnInfo_) {
        return std::vector<TString>();
    }
    std::vector<TString> result;
    const auto& info = *ColumnInfo_;
    result.reserve(info.Columns.size());
    for (const auto& column : info.Columns) {
        result.push_back(column.Name());
    }
    return result;
}

std::vector<TColumnStableName> TTableSchema::GetColumnStableNames() const
{
    if (!ColumnInfo_) {
        return std::vector<TColumnStableName>();
    }
    std::vector<TColumnStableName> result;
    const auto& info = *ColumnInfo_;
    result.reserve(info.Columns.size());
    for (const auto& column : info.Columns) {
        result.push_back(column.StableName());
    }
    return result;
}

const THunkColumnIds& TTableSchema::GetHunkColumnIds() const
{
    return HunkColumnsIds_;
}

std::vector<TColumnStableName> MapNamesToStableNames(
    const TTableSchema& schema,
    std::vector<TString> names,
    const std::optional<TStringBuf>& missingColumnReplacement)
{
    std::vector<TColumnStableName> stableNames;
    stableNames.reserve(names.size());
    for (const auto& name : names) {
        const auto* column = schema.FindColumn(name);
        if (column) {
            stableNames.push_back(column->StableName());
        } else if (!schema.GetStrict()) {
            stableNames.push_back(TColumnStableName(name));
        } else if (missingColumnReplacement) {
            stableNames.push_back(TColumnStableName(TString(*missingColumnReplacement)));
        } else {
            THROW_ERROR_EXCEPTION("Column %Qv is missing in strict schema",
                name);
        }
    }
    return stableNames;
}

int TTableSchema::GetKeyColumnCount() const
{
    return KeyColumnCount_;
}

int TTableSchema::GetValueColumnCount() const
{
    return GetColumnCount() - GetKeyColumnCount();
}

TSortColumns TTableSchema::GetSortColumns(const std::optional<TNameMapping>& nameMapping) const
{
    auto actualNameMapping = nameMapping
        ? *nameMapping
        : GetNameMapping();

    TSortColumns sortColumns;
    sortColumns.reserve(GetKeyColumnCount());

    for (const auto& column : Columns()) {
        if (column.SortOrder()) {
            const auto& name = actualNameMapping.StableNameToName(column.StableName());
            sortColumns.push_back(TColumnSortSchema{
                .Name = TString(name),
                .SortOrder = *column.SortOrder(),
            });
        }
    }

    return sortColumns;
}

TTableSchemaPtr TTableSchema::SetUniqueKeys(bool uniqueKeys) const
{
    auto schema = *this;
    schema.UniqueKeys_ = uniqueKeys;
    return New<TTableSchema>(std::move(schema));
}

TTableSchemaPtr TTableSchema::SetSchemaModification(ETableSchemaModification schemaModification) const
{
    auto schema = *this;
    schema.SchemaModification_ = schemaModification;
    return New<TTableSchema>(std::move(schema));
}

bool TTableSchema::HasNontrivialSchemaModification() const
{
    return GetSchemaModification() != ETableSchemaModification::None;
}

TTableSchemaPtr TTableSchema::FromKeyColumns(const TKeyColumns& keyColumns)
{
    TTableSchema schema;
    std::vector<TColumnSchema> columns;
    for (const auto& columnName : keyColumns) {
        columns.push_back(
            TColumnSchema(columnName, ESimpleLogicalValueType::Any)
                .SetSortOrder(ESortOrder::Ascending));
    }
    schema.ColumnInfo_ = std::make_shared<const TColumnInfo>(
        std::move(columns),
        std::vector<TDeletedColumn>{});
    schema.KeyColumnCount_ = keyColumns.size();
    ValidateTableSchema(schema);
    return New<TTableSchema>(std::move(schema));
}

TTableSchemaPtr TTableSchema::FromSortColumns(const TSortColumns& sortColumns)
{
    std::vector<TColumnSchema> columns;
    for (const auto& sortColumn : sortColumns) {
        columns.push_back(
            TColumnSchema(sortColumn.Name, ESimpleLogicalValueType::Any)
                .SetSortOrder(sortColumn.SortOrder));
    }

    auto schema = New<TTableSchema>(std::move(columns), /*strict*/ false);
    ValidateTableSchema(*schema);
    return schema;
}

TTableSchemaPtr TTableSchema::ToQuery() const
{
    if (!ColumnInfo_) {
        return New<TTableSchema>(
            std::vector<TColumnSchema>(),
            true,  /*strict*/
            false,  /*uniqueKeys*/
            ETableSchemaModification::None,
            std::vector<TDeletedColumn>());
    }
    const auto& info = *ColumnInfo_;
    if (IsSorted()) {
        return New<TTableSchema>(*this);
    } else {
        std::vector<TColumnSchema> columns {
            TColumnSchema(TabletIndexColumnName, ESimpleLogicalValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema(RowIndexColumnName, ESimpleLogicalValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
        };
        columns.insert(columns.end(), info.Columns.begin(), info.Columns.end());
        return New<TTableSchema>(std::move(columns), true, false,
            ETableSchemaModification::None, DeletedColumns());
    }
}

TTableSchemaPtr TTableSchema::ToWriteViaQueueProducer() const
{
    std::vector<TColumnSchema> columns;
    if (IsSorted()) {
        for (const auto& column : Columns()) {
            if (!column.Expression()) {
                columns.push_back(column);
            }
        }
    } else {
        columns.push_back(TColumnSchema(TabletIndexColumnName, ESimpleLogicalValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending));
        columns.push_back(TColumnSchema(SequenceNumberColumnName, ESimpleLogicalValueType::Int64));
        for (const auto& column : Columns()) {
            if (column.StableName().Underlying() != TimestampColumnName &&
                column.StableName().Underlying() != CumulativeDataWeightColumnName)
            {
                columns.push_back(column);
            }
        }
    }
    return New<TTableSchema>(
        std::move(columns),
        Strict_,
        UniqueKeys_,
        ETableSchemaModification::None,
        DeletedColumns());
}

TTableSchemaPtr TTableSchema::ToWrite() const
{
    std::vector<TColumnSchema> columns;
    if (IsSorted()) {
        for (const auto& column : Columns()) {
            if (!column.Expression()) {
                columns.push_back(column);
            }
        }
    } else {
        columns.push_back(TColumnSchema(TabletIndexColumnName, ESimpleLogicalValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending));
        for (const auto& column : Columns()) {
            if (column.StableName().Underlying() != TimestampColumnName &&
                column.StableName().Underlying() != CumulativeDataWeightColumnName)
            {
                columns.push_back(column);
            }
        }
    }
    return New<TTableSchema>(
        std::move(columns),
        Strict_,
        UniqueKeys_,
        ETableSchemaModification::None,
        DeletedColumns());
}

TTableSchemaPtr TTableSchema::WithTabletIndex() const
{
    if (IsSorted()) {
        return New<TTableSchema>(*this);
    } else {
        auto columns = Columns();
        // XXX: Is it ok? $tablet_index is usually a key column.
        columns.push_back(TColumnSchema(TabletIndexColumnName, ESimpleLogicalValueType::Int64));
        return New<TTableSchema>(
            std::move(columns),
            Strict_,
            UniqueKeys_,
            ETableSchemaModification::None,
            DeletedColumns());
    }
}

TTableSchemaPtr TTableSchema::ToVersionedWrite() const
{
    if (IsSorted()) {
        return New<TTableSchema>(*this);
    } else {
        auto columns = Columns();
        columns.insert(columns.begin(), TColumnSchema(TabletIndexColumnName, ESimpleLogicalValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending));
        return New<TTableSchema>(
            std::move(columns),
            Strict_,
            UniqueKeys_,
            ETableSchemaModification::None,
            DeletedColumns());
    }
}

TTableSchemaPtr TTableSchema::ToLookup() const
{
    std::vector<TColumnSchema> columns;
    for (const auto& column : Columns()) {
        if (column.SortOrder() && !column.Expression()) {
            columns.push_back(column);
        }
    }
    return New<TTableSchema>(
        std::move(columns),
        Strict_,
        UniqueKeys_,
        ETableSchemaModification::None,
        DeletedColumns());
}

TTableSchemaPtr TTableSchema::ToDelete() const
{
    return ToLookup();
}

TTableSchemaPtr TTableSchema::ToLock() const
{
    return ToLookup();
}

TTableSchemaPtr TTableSchema::ToKeys() const
{
    if (!ColumnInfo_) {
        return New<TTableSchema>(
            std::vector<TColumnSchema>(),
            Strict_,
            UniqueKeys_,
            ETableSchemaModification::None,
            std::vector<TDeletedColumn>());
    }
    const auto& info = *ColumnInfo_;
    std::vector<TColumnSchema> columns(info.Columns.begin(), info.Columns.begin() + KeyColumnCount_);
    return New<TTableSchema>(
        std::move(columns),
        Strict_,
        UniqueKeys_,
        ETableSchemaModification::None,
        info.DeletedColumns);
}

TTableSchemaPtr TTableSchema::ToUniqueKeys() const
{
    if (!ColumnInfo_) {
        return New<TTableSchema>(
            std::vector<TColumnSchema>(),
            Strict_,
            true,  /*uniqueKeys*/
            ETableSchemaModification::None,
            std::vector<TDeletedColumn>());
    }
    const auto& info = *ColumnInfo_;
    return New<TTableSchema>(
        info.Columns,
        Strict_,
        /*uniqueKeys*/ true,
        ETableSchemaModification::None,
        info.DeletedColumns);
}

TTableSchemaPtr TTableSchema::ToStrippedColumnAttributes() const
{
    if (!ColumnInfo_) {
        return New<TTableSchema>(
            std::vector<TColumnSchema>(),
            Strict_,
            false,  /*uniqueKeys*/
            ETableSchemaModification::None,
            std::vector<TDeletedColumn>());
    }
    const auto& info = *ColumnInfo_;
    std::vector<TColumnSchema> strippedColumns;
    for (const auto& column : info.Columns) {
        auto& strippedColumn = strippedColumns.emplace_back(column.Name(), column.LogicalType());
        strippedColumn.SetStableName(column.StableName());
    }
    return New<TTableSchema>(
        std::move(strippedColumns),
        Strict_,
        /*uniqueKeys*/ false,
        ETableSchemaModification::None,
        info.DeletedColumns);
}

TTableSchemaPtr TTableSchema::ToSortedStrippedColumnAttributes() const
{
    if (!ColumnInfo_) {
        return New<TTableSchema>(
            std::vector<TColumnSchema>(),
            Strict_,
            UniqueKeys_,  /*uniqueKeys*/
            ETableSchemaModification::None,
            std::vector<TDeletedColumn>());
    }
    const auto& info = *ColumnInfo_;
    std::vector<TColumnSchema> strippedColumns;
    for (const auto& column : info.Columns) {
        auto& strippedColumn = strippedColumns.emplace_back(column.Name(), column.LogicalType(), column.SortOrder());
        strippedColumn.SetStableName(column.StableName());
    }
    return New<TTableSchema>(
        std::move(strippedColumns),
        Strict_,
        UniqueKeys_,
        ETableSchemaModification::None,
        info.DeletedColumns);
}

TTableSchemaPtr TTableSchema::ToCanonical() const
{
    if (!ColumnInfo_) {
        return New<TTableSchema>(
            std::vector<TColumnSchema>(),
            Strict_,
            UniqueKeys_,  /*uniqueKeys*/
            ETableSchemaModification::None,
            std::vector<TDeletedColumn>());
    }
    const auto& info = *ColumnInfo_;
    auto columns = info.Columns;
    std::sort(
        columns.begin() + KeyColumnCount_,
        columns.end(),
        [] (const TColumnSchema& lhs, const TColumnSchema& rhs) {
            return lhs.Name() < rhs.Name();
        });
    return New<TTableSchema>(
        columns,
        Strict_,
        UniqueKeys_,
        ETableSchemaModification::None,
        info.DeletedColumns);
}

TTableSchemaPtr TTableSchema::ToSorted(const TKeyColumns& keyColumns) const
{
    TSortColumns sortColumns;
    sortColumns.reserve(keyColumns.size());
    for (const auto& keyColumn : keyColumns) {
        sortColumns.push_back(TColumnSortSchema{
            .Name = keyColumn,
            .SortOrder = ESortOrder::Ascending
        });
    }

    return TTableSchema::ToSorted(sortColumns);
}

TTableSchemaPtr TTableSchema::ToSorted(const TSortColumns& sortColumns) const
{
    int oldKeyColumnCount = 0;
    auto columns = Columns();
    for (int index = 0; index < std::ssize(sortColumns); ++index) {
        auto it = std::find_if(
            columns.begin() + index,
            columns.end(),
            [&] (const TColumnSchema& column) {
                return column.Name() == sortColumns[index].Name;
            });

        if (it == columns.end()) {
            if (Strict_) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::IncompatibleKeyColumns,
                    "Column %Qv is not found in strict schema",
                    sortColumns[index].Name)
                    << TErrorAttribute("schema", *this)
                    << TErrorAttribute("sort_columns", sortColumns);
            } else {
                columns.push_back(TColumnSchema(sortColumns[index].Name, EValueType::Any));
                it = columns.end();
                --it;
            }
        }

        if (it->SortOrder()) {
            ++oldKeyColumnCount;
        }

        std::swap(columns[index], *it);
        columns[index].SetSortOrder(sortColumns[index].SortOrder);
    }

    auto uniqueKeys = UniqueKeys_ && oldKeyColumnCount == GetKeyColumnCount();

    for (auto it = columns.begin() + sortColumns.size(); it != columns.end(); ++it) {
        it->SetSortOrder(std::nullopt);
    }

    return New<TTableSchema>(
        std::move(columns),
        Strict_,
        uniqueKeys,
        GetSchemaModification(),
        DeletedColumns());
}

TTableSchemaPtr TTableSchema::ToReplicationLog() const
{
    const auto& schemaColumns = Columns();
    std::vector<TColumnSchema> columns;
    columns.push_back(TColumnSchema(TimestampColumnName, ESimpleLogicalValueType::Uint64));
    if (IsSorted()) {
        columns.push_back(TColumnSchema(TReplicationLogTable::ChangeTypeColumnName, ESimpleLogicalValueType::Int64));
        for (const auto& column : schemaColumns) {
            if (column.SortOrder()) {
                columns.push_back(
                    TColumnSchema(
                        TReplicationLogTable::KeyColumnNamePrefix + column.Name(),
                        column.LogicalType()));
            } else {
                columns.push_back(
                    TColumnSchema(
                        TReplicationLogTable::ValueColumnNamePrefix + column.Name(),
                        MakeOptionalIfNot(column.LogicalType())));
                columns.push_back(
                    TColumnSchema(TReplicationLogTable::FlagsColumnNamePrefix + column.Name(), ESimpleLogicalValueType::Uint64));
            }
        }
    } else {
        for (const auto& column : schemaColumns) {
            columns.push_back(
                TColumnSchema(
                    TReplicationLogTable::ValueColumnNamePrefix + column.Name(),
                    MakeOptionalIfNot(column.LogicalType()))
                .SetMaxInlineHunkSize(column.MaxInlineHunkSize()));
        }
        columns.push_back(TColumnSchema(TReplicationLogTable::ValueColumnNamePrefix + TabletIndexColumnName, ESimpleLogicalValueType::Int64));
    }
    return New<TTableSchema>(
        std::move(columns),
        /* strict */ true,
        /* uniqueKeys */ false,
        ETableSchemaModification::None,
        DeletedColumns());
}

TTableSchemaPtr TTableSchema::ToUnversionedUpdate(bool sorted) const
{
    YT_VERIFY(IsSorted());
    const auto& info = *ColumnInfo_;

    std::vector<TColumnSchema> columns;
    columns.reserve(GetKeyColumnCount() + 1 + GetValueColumnCount() * 2);

    // Keys.
    for (int columnIndex = 0; columnIndex < GetKeyColumnCount(); ++columnIndex) {
        auto column = Columns()[columnIndex];
        if (!sorted) {
            column.SetSortOrder(std::nullopt);
        }
        columns.push_back(column);
    }

    // Modification type.
    columns.emplace_back(
        TUnversionedUpdateSchema::ChangeTypeColumnName,
        MakeLogicalType(ESimpleLogicalValueType::Uint64, /*required*/ true));

    // Values.
    for (int columnIndex = GetKeyColumnCount(); columnIndex < GetColumnCount(); ++columnIndex) {
        const auto& column = info.Columns[columnIndex];
        YT_VERIFY(!column.SortOrder());
        columns.emplace_back(
            TUnversionedUpdateSchema::ValueColumnNamePrefix + column.Name(),
            MakeOptionalIfNot(column.LogicalType()));
        columns.emplace_back(
            TUnversionedUpdateSchema::FlagsColumnNamePrefix + column.Name(),
            MakeLogicalType(ESimpleLogicalValueType::Uint64, /*required*/ false));
    }

    return New<TTableSchema>(
        std::move(columns),
        /*strict*/ true,
        /*uniqueKeys*/ sorted,
        ETableSchemaModification::None,
        info.DeletedColumns);
}

TTableSchemaPtr TTableSchema::ToModifiedSchema(ETableSchemaModification schemaModification) const
{
    if (HasNontrivialSchemaModification()) {
        THROW_ERROR_EXCEPTION("Cannot apply schema modification because schema is already modified")
            << TErrorAttribute("existing_modification", GetSchemaModification())
            << TErrorAttribute("requested_modification", schemaModification);
    }
    YT_VERIFY(GetSchemaModification() == ETableSchemaModification::None);

    switch (schemaModification) {
        case ETableSchemaModification::None:
            return New<TTableSchema>(*this);

        case ETableSchemaModification::UnversionedUpdate:
            return ToUnversionedUpdate(/*sorted*/ true)
                ->SetSchemaModification(schemaModification);

        case ETableSchemaModification::UnversionedUpdateUnsorted:
            return ToUnversionedUpdate(/*sorted*/ false)
                ->SetSchemaModification(schemaModification);

        default:
            YT_ABORT();
    }
}

TComparator TTableSchema::ToComparator(TCallback<TUUComparerSignature> cgComparator) const
{
    std::vector<ESortOrder> sortOrders;
    if (ColumnInfo_) {
        const auto& info = *ColumnInfo_;
        sortOrders.resize(KeyColumnCount_);
        for (int index = 0; index < KeyColumnCount_; ++index) {
            YT_VERIFY(info.Columns[index].SortOrder());
            sortOrders[index] = *info.Columns[index].SortOrder();
        }
    }

    return TComparator(std::move(sortOrders), std::move(cgComparator));
}

void TTableSchema::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, ToProto<NTableClient::NProto::TTableSchemaExt>(*this));
}

void TTableSchema::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    auto protoSchema = NYT::Load<NTableClient::NProto::TTableSchemaExt>(context);
    *this = FromProto<TTableSchema>(protoSchema);
}

i64 TTableSchema::GetMemoryUsage() const
{
    i64 usage = sizeof(TTableSchema);
    for (const auto& column : Columns()) {
        usage += column.GetMemoryUsage();
    }
    return usage;
}

TKeyColumnTypes TTableSchema::GetKeyColumnTypes() const
{
    TKeyColumnTypes result(KeyColumnCount_);
    for (int index = 0; index < KeyColumnCount_; ++index) {
        result[index] = Columns()[index].GetWireType();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TTableSchema& schema, TStringBuf /*spec*/)
{
    builder->AppendFormat("<strict=%v;unique_keys=%v", schema.GetStrict(), schema.GetUniqueKeys());
    if (schema.HasNontrivialSchemaModification()) {
        builder->AppendFormat(";schema_modification=%v", schema.GetSchemaModification());
    }
    builder->AppendChar('>');
    builder->AppendChar('[');
    bool first = true;
    for (const auto& column : schema.Columns()) {
        if (!first) {
            builder->AppendString(TStringBuf("; "));
        }
        builder->AppendFormat("%v", column);
        first = false;
    }
    for (const auto& deletedColumn : schema.DeletedColumns()) {
        if (!first) {
            builder->AppendString(TStringBuf("; "));
        }
        builder->AppendFormat("%v", deletedColumn);
        first = false;
    }
    builder->AppendChar(']');
}

void FormatValue(TStringBuilderBase* builder, const TTableSchemaPtr& schema, TStringBuf spec)
{
    if (schema) {
        FormatValue(builder, *schema, spec);
    } else {
        builder->AppendString(TStringBuf("<null>"));
    }
}

TString SerializeToWireProto(const TTableSchemaPtr& schema)
{
    NTableClient::NProto::TTableSchemaExt protoSchema;
    ToProto(&protoSchema, schema);
    return protoSchema.SerializeAsString();
}

void DeserializeFromWireProto(TTableSchemaPtr* schema, const TString& serializedProto)
{
    NTableClient::NProto::TTableSchemaExt protoSchema;
    if (!protoSchema.ParseFromString(serializedProto)) {
        THROW_ERROR_EXCEPTION("Failed to deserialize table schema from wire proto");
    }
    FromProto(schema, protoSchema);
}

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema)
{
    ToProto(protoSchema->mutable_columns(), schema.Columns());
    ToProto(protoSchema->mutable_deleted_columns(), schema.DeletedColumns());
    protoSchema->set_strict(schema.GetStrict());
    protoSchema->set_unique_keys(schema.GetUniqueKeys());
    protoSchema->set_schema_modification(static_cast<int>(schema.GetSchemaModification()));
}

void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema)
{
    *schema = TTableSchema(
        FromProto<std::vector<TColumnSchema>>(protoSchema.columns()),
        protoSchema.strict(),
        protoSchema.unique_keys(),
        CheckedEnumCast<ETableSchemaModification>(protoSchema.schema_modification()),
        FromProto<std::vector<TDeletedColumn>>(protoSchema.deleted_columns()));
}

void FromProto(
    TTableSchema* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& protoKeyColumns)
{
    auto columns = FromProto<std::vector<TColumnSchema>>(protoSchema.columns());
    for (int columnIndex = 0; columnIndex < protoKeyColumns.names_size(); ++columnIndex) {
        auto& columnSchema = columns[columnIndex];
        YT_VERIFY(columnSchema.Name() == protoKeyColumns.names(columnIndex));
        // TODO(gritukan): YT-14155
        if (!columnSchema.SortOrder()) {
            columnSchema.SetSortOrder(ESortOrder::Ascending);
        }
    }
    for (int columnIndex = protoKeyColumns.names_size(); columnIndex < std::ssize(columns); ++columnIndex) {
        auto& columnSchema = columns[columnIndex];
        YT_VERIFY(!columnSchema.SortOrder());
    }

    auto deletedColumns = FromProto<std::vector<TDeletedColumn>>(protoSchema.deleted_columns());

    *schema = TTableSchema(
        std::move(columns),
        protoSchema.strict(),
        protoSchema.unique_keys(),
        ETableSchemaModification::None,
        deletedColumns);
}

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchemaPtr& schema)
{
    ToProto(protoSchema, *schema);
}

void FromProto(TTableSchemaPtr* schema, const NProto::TTableSchemaExt& protoSchema)
{
    *schema = New<TTableSchema>();
    FromProto(schema->Get(), protoSchema);
}

void FromProto(
    TTableSchemaPtr* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& keyColumnsExt)
{
    *schema = New<TTableSchema>();
    FromProto(schema->Get(), protoSchema, keyColumnsExt);
}

void PrintTo(const TTableSchema& tableSchema, std::ostream* os)
{
    *os << Format("%v", tableSchema);
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return
        lhs.StableName() == rhs.StableName() &&
        lhs.Name() == rhs.Name() &&
        *lhs.LogicalType() == *rhs.LogicalType() &&
        lhs.Required() == rhs.Required() &&
        lhs.SortOrder() == rhs.SortOrder() &&
        lhs.Lock() == rhs.Lock() &&
        lhs.Expression() == rhs.Expression() &&
        lhs.Aggregate() == rhs.Aggregate() &&
        lhs.Group() == rhs.Group() &&
        lhs.MaxInlineHunkSize() == rhs.MaxInlineHunkSize();
}

bool operator==(const TDeletedColumn& lhs, const TDeletedColumn& rhs)
{
    return lhs.StableName() == rhs.StableName();
}

bool operator==(const TTableSchema& lhs, const TTableSchema& rhs)
{
    return
        lhs.Columns() == rhs.Columns() &&
        lhs.GetStrict() == rhs.GetStrict() &&
        lhs.GetUniqueKeys() == rhs.GetUniqueKeys() &&
        lhs.GetSchemaModification() == rhs.GetSchemaModification() &&
        lhs.DeletedColumns() == rhs.DeletedColumns();
}

// Compat code for https://st.yandex-team.ru/YT-10668 workaround.
bool IsEqualIgnoringRequiredness(const TTableSchema& lhs, const TTableSchema& rhs)
{
    auto dropRequiredness = [] (const TTableSchema& schema) {
        std::vector<TColumnSchema> resultColumns;
        for (auto column : schema.Columns()) {
            if (column.LogicalType()->GetMetatype() == ELogicalMetatype::Optional) {
                column.SetLogicalType(column.LogicalType()->AsOptionalTypeRef().GetElement());
            }
            resultColumns.emplace_back(column);
        }
        return TTableSchema(resultColumns, schema.GetStrict(), schema.GetUniqueKeys());
    };
    return dropRequiredness(lhs) == dropRequiredness(rhs);
}

////////////////////////////////////////////////////////////////////////////////

// Parses description of the following form nested_key.name or nested_value.name or nested_value.name.sum
std::optional<TNestedColumn> TryParseNestedAggregate(TStringBuf description)
{
    if (!description.StartsWith("nested")) {
        return std::nullopt;
    }

    const char* ptr = description.data();
    const char* ptrEnd = ptr + description.size();

    auto skipSpace = [&] () {
        while (ptr != ptrEnd) {
            if (!IsSpace(*ptr)) {
                break;
            }

            ++ptr;
        }
    };

    auto parseName = [&] () {
        auto start = ptr;
        while (ptr != ptrEnd) {
            auto ch = *ptr;
            if (!isalpha(ch) && !isdigit(ch) && ch != '_' && ch != '-' && ch != '%' && ch != '.') {
                break;
            }

            ++ptr;
        }

        return TStringBuf(start, ptr);
    };

    auto parseToken = [&] (TStringBuf token) {
        if (TStringBuf(ptr, ptrEnd).StartsWith(token)) {
            ptr += token.size();
            return true;
        }

        return false;
    };

    auto throwError = [&] (TStringBuf message) {
        int location = ptr - description.data();

        THROW_ERROR_EXCEPTION("Error while parsing nested aggregate description: %v", message)
            << TErrorAttribute("position", Format("%v", location))
            << TErrorAttribute("description", description);
    };

    auto nestedFunction = parseName();

    skipSpace();

    if (!parseToken("(")) {
        throwError("expected \"(\"");
    }

    if (nestedFunction == "nested_key") {
        skipSpace();
        auto nestedTableName = parseName();
        skipSpace();

        if (parseToken(")")) {
            return TNestedColumn{
                .NestedTableName = nestedTableName,
                .IsKey = true
            };
        }

        throwError("expected \")\"");
    } else if (nestedFunction == "nested_value") {
        skipSpace();
        auto nestedTableName = parseName();
        skipSpace();

        if (parseToken(")")) {
            return TNestedColumn{
                .NestedTableName = nestedTableName,
                .IsKey = false
            };
        }

        if (parseToken(",")) {
            skipSpace();
            auto aggregateFunction = parseName();
            skipSpace();

            if (parseToken(")")) {
                return TNestedColumn{
                    .NestedTableName = nestedTableName,
                    .IsKey = false,
                    .Aggregate = aggregateFunction
                };
            }

            throwError("expected \")\"");
        }

        throwError("expected \")\" or \",\" ");
    }

    THROW_ERROR_EXCEPTION("Error while parsing nested aggregate description. Expected nested_key or nested_value");
}

EValueType GetNestedColumnElementType(const TLogicalType* logicalType)
{
    if (logicalType->GetMetatype() == ELogicalMetatype::Optional) {
        logicalType = logicalType->GetElement().Get();
    }

    if (logicalType->GetMetatype() != ELogicalMetatype::List) {
        THROW_ERROR_EXCEPTION("Invalid nested column type %Qv",
            *logicalType);
    }

    return GetWireType(logicalType->GetElement());
}

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns)
{
    ValidateKeyColumnCount(keyColumns.size());

    THashSet<TString> names;
    for (const auto& name : keyColumns) {
        if (!names.insert(name).second) {
            THROW_ERROR_EXCEPTION("Duplicate key column name %Qv",
                name);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateDynamicTableKeyColumnCount(int count)
{
    THROW_ERROR_EXCEPTION_IF(count > MaxKeyColumnCountInDynamicTable,
        "Too many key columns: expected <= %v, got %v",
        MaxKeyColumnCountInDynamicTable,
        count);
}

////////////////////////////////////////////////////////////////////////////////

void ValidateSystemColumnSchema(
    const TColumnSchema& columnSchema,
    bool isTableSorted,
    bool allowUnversionedUpdateColumns,
    bool allowTimestampColumns)
{
    static const auto allowedSortedTablesSystemColumns = THashMap<TString, ESimpleLogicalValueType>{
        {EmptyValueColumnName, ESimpleLogicalValueType::Int64},
        {TtlColumnName, ESimpleLogicalValueType::Uint64},
    };

    static const auto allowedOrderedTablesSystemColumns = THashMap<TString, ESimpleLogicalValueType>{
        {TimestampColumnName, ESimpleLogicalValueType::Uint64},
        {CumulativeDataWeightColumnName, ESimpleLogicalValueType::Int64},
    };

    auto validateType = [&] (ESimpleLogicalValueType expected) {
        if (!columnSchema.IsOfV1Type(expected)) {
            THROW_ERROR_EXCEPTION("Invalid type of column %Qv: expected %Qlv, got %Qlv",
                columnSchema.GetDiagnosticNameString(),
                expected,
                *columnSchema.LogicalType());
        }
    };

    const auto& name = columnSchema.Name();

    if (columnSchema.IsRenamed()) {
        THROW_ERROR_EXCEPTION("System column schema must have equal name and stable name")
            << TErrorAttribute("name", name)
            << TErrorAttribute("stable_name", columnSchema.StableName().Underlying());
    }

    const auto& allowedSystemColumns = isTableSorted
        ? allowedSortedTablesSystemColumns
        : allowedOrderedTablesSystemColumns;
    auto it = allowedSystemColumns.find(name);

    // Ordinary system column.
    if (it != allowedSystemColumns.end()) {
        validateType(it->second);
        return;
    }

    if (allowUnversionedUpdateColumns) {
        // Unversioned update schema system column.
        if (name == TUnversionedUpdateSchema::ChangeTypeColumnName) {
            validateType(ESimpleLogicalValueType::Uint64);
            return;
        } else if (name.StartsWith(TUnversionedUpdateSchema::FlagsColumnNamePrefix)) {
            validateType(ESimpleLogicalValueType::Uint64);
            return;
        } else if (name.StartsWith(TUnversionedUpdateSchema::ValueColumnNamePrefix)) {
            // Value can have any type.
            return;
        }
    }

    if (allowTimestampColumns) {
        if (name.StartsWith(TimestampColumnPrefix)) {
            validateType(ESimpleLogicalValueType::Uint64);
            return;
        }
    }

    // Unexpected system column.
    THROW_ERROR_EXCEPTION("System column name %Qv is not allowed here",
        name);
}

void ValidateColumnName(const TString& name)
{
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Column name cannot be empty");
    }

    if (name.size() > MaxColumnNameLength) {
        THROW_ERROR_EXCEPTION("Column name %Qv is longer than maximum allowed: %v > %v",
            name,
            name.size(),
            MaxColumnNameLength);
    }
}

void ValidateColumnSchema(
    const TColumnSchema& columnSchema,
    bool isTableSorted,
    bool isTableDynamic,
    bool allowUnversionedUpdateColumns,
    bool allowTimestampColumns)
{
    static const auto allowedAggregates = THashSet<TString>{
        "sum",
        "min",
        "max",
        "first",
        "xdelta",
        "_yt_stored_replica_set",
        "_yt_last_seen_replica_set",
        "dict_sum",
    };

    static const auto allowedNestedAggregates = THashSet<TString>{
        "sum",
        "max"
    };

    try {
        const auto& stableName = columnSchema.StableName();
        ValidateColumnName(stableName.Underlying());

        const auto& name = columnSchema.Name();
        ValidateColumnName(name);

        if (stableName.Underlying().StartsWith(SystemColumnNamePrefix) || name.StartsWith(SystemColumnNamePrefix)) {
            ValidateSystemColumnSchema(
                columnSchema,
                isTableSorted,
                allowUnversionedUpdateColumns,
                allowTimestampColumns);
        }

        {
            TComplexTypeFieldDescriptor descriptor(name, columnSchema.LogicalType());
            ValidateLogicalType(descriptor, MaxSchemaDepth);
        }

        if (!IsComparable(columnSchema.LogicalType()) &&
            columnSchema.SortOrder() &&
            !columnSchema.IsOfV1Type(ESimpleLogicalValueType::Any))
        {
            THROW_ERROR_EXCEPTION("Key column cannot be of %Qv type",
                *columnSchema.LogicalType());
        }

        if (*DetagLogicalType(columnSchema.LogicalType()) == *SimpleLogicalType(ESimpleLogicalValueType::Any)) {
            THROW_ERROR_EXCEPTION("Column of type %Qlv cannot be required",
                ESimpleLogicalValueType::Any);
        }

        if (columnSchema.Lock()) {
            if (columnSchema.Lock()->empty()) {
                THROW_ERROR_EXCEPTION("Column lock name cannot be empty");
            }
            if (columnSchema.Lock()->size() > MaxColumnLockLength) {
                THROW_ERROR_EXCEPTION("Column lock name is longer than maximum allowed: %v > %v",
                    columnSchema.Lock()->size(),
                    MaxColumnLockLength);
            }
            if (columnSchema.SortOrder()) {
                THROW_ERROR_EXCEPTION("Column lock cannot be set on a key column");
            }
        }

        if (columnSchema.Group()) {
            if (columnSchema.Group()->empty()) {
                THROW_ERROR_EXCEPTION("Column group should either be unset or be non-empty");
            }
            if (columnSchema.Group()->size() > MaxColumnGroupLength) {
                THROW_ERROR_EXCEPTION("Column group name is longer than maximum allowed: %v > %v",
                    columnSchema.Group()->size(),
                    MaxColumnGroupLength);
            }
        }

        ValidateSchemaValueType(columnSchema.GetWireType());

        if (columnSchema.Expression() && !columnSchema.SortOrder() && isTableDynamic) {
            THROW_ERROR_EXCEPTION("Non-key column cannot be computed");
        }

        if (columnSchema.Aggregate() && columnSchema.SortOrder()) {
            THROW_ERROR_EXCEPTION("Key column cannot be aggregated");
        }

        if (columnSchema.Aggregate()) {
            auto aggregateName = *columnSchema.Aggregate();

            if (auto nested = TryParseNestedAggregate(aggregateName)) {
                if (nested->NestedTableName.empty()) {
                    THROW_ERROR_EXCEPTION("Invalid nested table name for aggregate %Qv",
                        aggregateName);
                }

                GetNestedColumnElementType(columnSchema.LogicalType().Get());

                if (!nested->IsKey) {
                    auto aggregateName = nested->Aggregate;
                    if (!aggregateName.empty() && allowedNestedAggregates.find(aggregateName) == allowedNestedAggregates.end()) {
                        THROW_ERROR_EXCEPTION("Invalid aggregate function %Qv",
                            aggregateName);
                    }
                }
            } else if (allowedAggregates.find(aggregateName) == allowedAggregates.end()) {
                THROW_ERROR_EXCEPTION("Invalid aggregate function %Qv",
                    aggregateName);
            }
        }

        if (columnSchema.Expression() && columnSchema.Required()) {
            THROW_ERROR_EXCEPTION("Computed column cannot be required");
        }

        if (columnSchema.MaxInlineHunkSize()) {
            if (columnSchema.MaxInlineHunkSize() <= 0) {
                THROW_ERROR_EXCEPTION("Max inline hunk size must be positive");
            }
            if (!IsStringLikeType(columnSchema.GetWireType())) {
                THROW_ERROR_EXCEPTION("Max inline hunk size can only be set for string-like columns, not %Qlv",
                    columnSchema.GetWireType());
            }
            if (columnSchema.SortOrder()) {
                THROW_ERROR_EXCEPTION("Max inline hunk size cannot be set for key column");
            }
            if (columnSchema.Aggregate()) {
                THROW_ERROR_EXCEPTION("Max inline hunk size cannot be set for aggregate column");
            }
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating schema of column %v",
            columnSchema.GetDiagnosticNameString())
            << ex;
    }
}

void ValidateDynamicTableConstraints(const TTableSchema& schema)
{
    if (!schema.GetStrict()) {
        THROW_ERROR_EXCEPTION("\"strict\" cannot be \"false\" for a dynamic table");
    }

    if (schema.IsSorted() && !schema.GetUniqueKeys()) {
        THROW_ERROR_EXCEPTION("\"unique_keys\" cannot be \"false\" for a sorted dynamic table");
    }

    if (schema.GetKeyColumnCount() == std::ssize(schema.Columns())) {
        THROW_ERROR_EXCEPTION("There must be at least one non-key column");
    }

    ValidateDynamicTableKeyColumnCount(schema.GetKeyColumnCount());

    for (const auto& column : schema.Columns()) {
        try {
            auto logicalType = column.LogicalType();
            if (column.SortOrder() && !column.IsOfV1Type() &&
                logicalType->GetMetatype() != ELogicalMetatype::List &&
                logicalType->GetMetatype() != ELogicalMetatype::Tuple)
            {
                THROW_ERROR_EXCEPTION("Dynamic table cannot have key column of type %Qv",
                    *logicalType);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error validating column %v in dynamic table schema",
                column.GetDiagnosticNameString())
                << ex;
        }
    }
}

//! Validates that there are no duplicates among the column names.
void ValidateColumnUniqueness(const TTableSchema& schema)
{
    THashSet<TStringBuf> columnNames;
    THashSet<TStringBuf> columnStableNames;
    for (const auto& column : schema.Columns()) {
        if (!columnStableNames.insert(column.StableName().Underlying()).second) {
            THROW_ERROR_EXCEPTION("Duplicate column stable name %Qv in table schema",
                column.StableName());
        }
        if (!columnNames.insert(column.Name()).second) {
            THROW_ERROR_EXCEPTION("Duplicate column name %Qv in table schema",
                column.Name());
        }
    }
    for (const auto& deletedColumn : schema.DeletedColumns()) {
        if (!columnStableNames.insert(deletedColumn.StableName().Underlying()).second) {
            THROW_ERROR_EXCEPTION("Duplicate column stable name %Qv in table schema",
                deletedColumn.StableName());
        }
    }
}

void ValidatePivotKey(
    TUnversionedRow pivotKey,
    const TTableSchema& schema,
    TStringBuf keyType,
    bool validateRequired)
{
    if (static_cast<int>(pivotKey.GetCount()) > schema.GetKeyColumnCount()) {
        auto titleKeyType = TString(keyType);
        titleKeyType.to_title();
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation, "%v key must form a prefix of key", titleKeyType);
    }

    for (int index = 0; index < static_cast<int>(pivotKey.GetCount()); ++index) {
        if (pivotKey[index].Type != EValueType::Null && pivotKey[index].Type != schema.Columns()[index].GetWireType()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SchemaViolation,
                "Mismatched type of column %v in %v key: expected %Qlv, found %Qlv",
                schema.Columns()[index].GetDiagnosticNameString(),
                keyType,
                schema.Columns()[index].GetWireType(),
                pivotKey[index].Type);
        }
        if (validateRequired && pivotKey[index].Type == EValueType::Null && !schema.Columns()[index].LogicalType()->IsNullable()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SchemaViolation,
                "Unexpected null for required column %v in %v key",
                schema.Columns()[index].GetDiagnosticNameString(),
                keyType);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Validates that number of locks doesn't exceed #MaxColumnLockCount.
void ValidateLocks(const TTableSchema& schema)
{
    THashSet<TString> lockNames;
    YT_VERIFY(lockNames.insert(PrimaryLockName).second);
    for (const auto& column : schema.Columns()) {
        if (column.Lock()) {
            lockNames.insert(*column.Lock());
        }
    }

    if (lockNames.size() > MaxColumnLockCount) {
        THROW_ERROR_EXCEPTION("Too many column locks in table schema: actual %v, limit %v",
            lockNames.size(),
            MaxColumnLockCount);
    }
}

//! Validates that key columns form a prefix of a table schema.
void ValidateKeyColumnsFormPrefix(const TTableSchema& schema)
{
    for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
        if (!schema.Columns()[index].SortOrder()) {
            THROW_ERROR_EXCEPTION("Key columns must form a prefix of schema");
        }
    }
    // The fact that first GetKeyColumnCount() columns have SortOrder automatically
    // implies that the rest of columns don't have SortOrder, so we don't need to check it.
}

//! Validates |$timestamp| column, if any.
/*!
 *  Validate that:
 *  - |$timestamp| column cannot be a part of key.
 *  - |$timestamp| column can only be present in ordered tables.
 *  - |$timestamp| column has type |uint64|.
 */
void ValidateTimestampColumn(const TTableSchema& schema)
{
    auto* column = schema.FindColumn(TimestampColumnName);
    if (!column) {
        return;
    }

    if (column->SortOrder()) {
        THROW_ERROR_EXCEPTION("Column %Qv cannot be a part of key",
            TimestampColumnName);
    }

    if (!column->IsOfV1Type(ESimpleLogicalValueType::Uint64)) {
        THROW_ERROR_EXCEPTION("Column %Qv must have %Qlv type",
            TimestampColumnName,
            EValueType::Uint64);
    }

    if (schema.IsSorted()) {
        THROW_ERROR_EXCEPTION("Column %Qv cannot appear in a sorted table",
            TimestampColumnName);
    }
}

//! Validates |$ttl| column, if any.
/*!
 *  Validate that:
 *  - |$ttl| column cannot be a part of key.
 *  - |$ttl| column can only be present in sorted tables.
 *  - |$ttl| column has type |uint64|.
 */
void ValidateTtlColumn(const TTableSchema& schema)
{
    auto* column = schema.FindColumn(TtlColumnName);
    if (!column) {
        return;
    }

    if (column->SortOrder()) {
        THROW_ERROR_EXCEPTION("Column %Qv cannot be a part of key",
            TtlColumnName);
    }

    if (!column->IsOfV1Type(ESimpleLogicalValueType::Uint64)) {
        THROW_ERROR_EXCEPTION("Column %Qv must have %Qlv type",
            TtlColumnName,
            EValueType::Uint64);
    }

    if (!schema.IsSorted()) {
        THROW_ERROR_EXCEPTION("Column %Qv cannot appear in an ordered table",
            TtlColumnName);
    }
}

//! Validates |$cumulative_data_weight| column, if any.
/*!
 *  Validate that:
 *  - |$cumulative_data_weight| column cannot be a part of key.
 *  - |$cumulative_data_weight| column can only be present in ordered tables.
 *  - |$cumulative_data_weight| column has type |int64|.
 */
void ValidateCumulativeDataWeightColumn(const TTableSchema& schema)
{
    auto* column = schema.FindColumn(CumulativeDataWeightColumnName);
    if (!column) {
        return;
    }

    if (column->SortOrder()) {
        THROW_ERROR_EXCEPTION(
            "Column %Qv cannot be a part of key",
            CumulativeDataWeightColumnName);
    }

    if (!column->IsOfV1Type(ESimpleLogicalValueType::Int64)) {
        THROW_ERROR_EXCEPTION(
            "Column %Qv must have %Qlv type",
            CumulativeDataWeightColumnName,
            EValueType::Int64);
    }

    if (schema.IsSorted()) {
        THROW_ERROR_EXCEPTION(
            "Column %Qv cannot appear in a sorted table",
            CumulativeDataWeightColumnName);
    }
}

// Validate schema attributes.
void ValidateSchemaAttributes(const TTableSchema& schema)
{
    if (schema.GetUniqueKeys() && schema.GetKeyColumnCount() == 0) {
        THROW_ERROR_EXCEPTION("\"unique_keys\" can only be true if key columns are present");
    }
}

void ValidateTableSchema(
    const TTableSchema& schema,
    bool isTableDynamic,
    bool allowUnversionedUpdateColumns,
    bool allowTimestampColumns)
{
    int totalTypeComplexity = 0;
    for (const auto& column : schema.Columns()) {
        ValidateColumnSchema(
            column,
            schema.IsSorted(),
            isTableDynamic,
            allowUnversionedUpdateColumns,
            allowTimestampColumns);
        if (!schema.GetStrict() && column.IsRenamed()) {
            THROW_ERROR_EXCEPTION("Renamed column %v in non-strict schema",
                column.GetDiagnosticNameString());
        }
        totalTypeComplexity += column.LogicalType()->GetTypeComplexity();
    }
    if (totalTypeComplexity >= MaxSchemaTotalTypeComplexity) {
        THROW_ERROR_EXCEPTION("Table schema is too complex, reduce number of columns or simplify their types");
    }
    ValidateColumnUniqueness(schema);
    ValidateLocks(schema);
    ValidateKeyColumnsFormPrefix(schema);
    ValidateTimestampColumn(schema);
    ValidateTtlColumn(schema);
    ValidateCumulativeDataWeightColumn(schema);
    ValidateSchemaAttributes(schema);
    if (isTableDynamic) {
        ValidateDynamicTableConstraints(schema);
    }

    // Validate nested table names.
    std::vector<TStringBuf> definedNestedTableNames;
    std::vector<TStringBuf> usedNestedTableNames;

    for (const auto& columnSchema : schema.Columns()) {
        if (columnSchema.Aggregate()) {
            const auto& aggregateName = *columnSchema.Aggregate();

            if (auto nested = TryParseNestedAggregate(aggregateName)) {
                // Already validated.
                YT_VERIFY(!nested->NestedTableName.empty());

                if (nested->IsKey) {
                    definedNestedTableNames.push_back(nested->NestedTableName);
                } else {
                    usedNestedTableNames.push_back(nested->NestedTableName);
                }
            }
        }
    }

    std::sort(definedNestedTableNames.begin(), definedNestedTableNames.end());
    definedNestedTableNames.erase(
        std::unique(definedNestedTableNames.begin(), definedNestedTableNames.end()),
        definedNestedTableNames.end());

    std::sort(usedNestedTableNames.begin(), usedNestedTableNames.end());
    usedNestedTableNames.erase(
        std::unique(usedNestedTableNames.begin(), usedNestedTableNames.end()),
        usedNestedTableNames.end());

    for (auto nestedName : usedNestedTableNames) {
        if (!std::binary_search(definedNestedTableNames.begin(), definedNestedTableNames.end(), nestedName)) {
            THROW_ERROR_EXCEPTION("No key columns for nested table %Qv",
                nestedName);
        }
    }
}

void ValidateNoDescendingSortOrder(const TTableSchema& schema)
{
    for (const auto& column : schema.Columns()) {
        if (column.SortOrder() == ESortOrder::Descending) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::InvalidSchemaValue,
                "Descending sort order is not available in this context yet")
                << TErrorAttribute("column_name", column.Name());
        }
    }
}

void ValidateNoRenamedColumns(const TTableSchema& schema)
{
    for (const auto& column : schema.Columns()) {
        if (column.IsRenamed()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::InvalidSchemaValue,
                "Table column renaming is not available yet")
                << TErrorAttribute("renamed_column", column.GetDiagnosticNameString());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, int> GetLocksMapping(
    const NTableClient::TTableSchema& schema,
    bool fullAtomicity,
    std::vector<int>* columnIndexToLockIndex,
    std::vector<TString>* lockIndexToName)
{
    if (columnIndexToLockIndex) {
        // Assign dummy lock indexes to key components.
        columnIndexToLockIndex->assign(schema.Columns().size(), -1);
    }

    if (lockIndexToName) {
        lockIndexToName->push_back(PrimaryLockName);
    }

    THashMap<TString, int> groupToIndex;
    if (fullAtomicity) {
        // Assign lock indexes to data components.
        for (int index = schema.GetKeyColumnCount(); index < std::ssize(schema.Columns()); ++index) {
            const auto& columnSchema = schema.Columns()[index];
            int lockIndex = PrimaryLockIndex;

            if (columnSchema.Lock()) {
                auto emplaced = groupToIndex.emplace(*columnSchema.Lock(), groupToIndex.size() + 1);
                if (emplaced.second && lockIndexToName) {
                    lockIndexToName->push_back(*columnSchema.Lock());
                }
                lockIndex = emplaced.first->second;
            }

            if (columnIndexToLockIndex) {
                (*columnIndexToLockIndex)[index] = lockIndex;
            }
        }
    } else if (columnIndexToLockIndex) {
        // No locking supported for non-atomic tablets, however we still need the primary
        // lock descriptor to maintain last commit timestamps.
        for (int index = schema.GetKeyColumnCount(); index < std::ssize(schema.Columns()); ++index) {
            (*columnIndexToLockIndex)[index] = PrimaryLockIndex;
        }
    }
    return groupToIndex;
}

TLockMask GetLockMask(
    const NTableClient::TTableSchema& schema,
    bool fullAtomicity,
    const std::vector<TString>& locks,
    ELockType lockType)
{
    auto groupToIndex = GetLocksMapping(schema, fullAtomicity);

    TLockMask lockMask;
    for (const auto& lock : locks) {
        auto it = groupToIndex.find(lock);
        if (it != groupToIndex.end()) {
            lockMask.Set(it->second, lockType);
        } else {
            THROW_ERROR_EXCEPTION("Lock group %Qv not found in schema", lock);
        }
    }
    return lockMask;
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

using NYT::ToProto;
using NYT::FromProto;

void ToProto(TKeyColumnsExt* protoKeyColumns, const TKeyColumns& keyColumns)
{
    ToProto(protoKeyColumns->mutable_names(), keyColumns);
}

void FromProto(TKeyColumns* keyColumns, const TKeyColumnsExt& protoKeyColumns)
{
    *keyColumns = FromProto<TKeyColumns>(protoKeyColumns.names());
}

void ToProto(TColumnFilter* protoColumnFilter, const NTableClient::TColumnFilter& columnFilter)
{
    if (!columnFilter.IsUniversal()) {
        for (auto index : columnFilter.GetIndexes()) {
            protoColumnFilter->add_indexes(index);
        }
    }
}

void FromProto(NTableClient::TColumnFilter* columnFilter, const TColumnFilter& protoColumnFilter)
{
    *columnFilter = protoColumnFilter.indexes().empty()
        ? NTableClient::TColumnFilter()
        : NTableClient::TColumnFilter(FromProto<NTableClient::TColumnFilter::TIndexes>(protoColumnFilter.indexes()));
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

TCellTaggedTableSchema::TCellTaggedTableSchema(TTableSchema tableSchema, TCellTag cellTag)
    : TableSchema(std::move(tableSchema))
    , CellTag(cellTag)
{ }

////////////////////////////////////////////////////////////////////////////////

TCellTaggedTableSchemaPtr::TCellTaggedTableSchemaPtr(TTableSchemaPtr tableSchema, TCellTag cellTag)
    : TableSchema(std::move(tableSchema))
    , CellTag(cellTag)
{
    YT_VERIFY(TableSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

size_t THash<NYT::NTableClient::TColumnStableName>::operator()(const NYT::NTableClient::TColumnStableName& stableName) const
{
    return THash<TString>()(stableName.Underlying());
}

size_t THash<NYT::NTableClient::TColumnSchema>::operator()(const NYT::NTableClient::TColumnSchema& columnSchema) const
{
    return MultiHash(
        columnSchema.StableName(),
        columnSchema.Name(),
        *columnSchema.LogicalType(),
        columnSchema.SortOrder(),
        columnSchema.Lock(),
        columnSchema.Expression(),
        columnSchema.Aggregate(),
        columnSchema.Group(),
        columnSchema.MaxInlineHunkSize());
}

size_t THash<NYT::NTableClient::TDeletedColumn>::operator()(const NYT::NTableClient::TDeletedColumn& columnSchema) const
{
    return THash<NYT::NTableClient::TColumnStableName>()(columnSchema.StableName());
}

size_t THash<NYT::NTableClient::TTableSchema>::operator()(const NYT::NTableClient::TTableSchema& tableSchema) const
{
    size_t result = CombineHashes(THash<bool>()(tableSchema.GetUniqueKeys()), THash<bool>()(tableSchema.GetStrict()));
    if (tableSchema.HasNontrivialSchemaModification()) {
        result = CombineHashes(
            result,
            THash<NYT::NTableClient::ETableSchemaModification>()(tableSchema.GetSchemaModification()));
    }
    for (const auto& columnSchema : tableSchema.Columns()) {
        result = CombineHashes(result, THash<NYT::NTableClient::TColumnSchema>()(columnSchema));
    }
    for (const auto& deletedColumnSchema : tableSchema.DeletedColumns()) {
        result = CombineHashes(result, THash<NYT::NTableClient::TDeletedColumn>()(
            deletedColumnSchema));
    }
    return result;
}
