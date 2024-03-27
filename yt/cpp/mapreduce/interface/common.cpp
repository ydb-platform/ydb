#include "common.h"

#include "errors.h"
#include "format.h"
#include "serialize.h"
#include "fluent.h"

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/type_info/type.h>

#include <util/generic/xrange.h>

namespace NYT {

using ::google::protobuf::Descriptor;

////////////////////////////////////////////////////////////////////////////////

TSortColumn::TSortColumn(TStringBuf name, ESortOrder sortOrder)
    : Name_(name)
    , SortOrder_(sortOrder)
{ }

TSortColumn::TSortColumn(const TString& name, ESortOrder sortOrder)
    : TSortColumn(static_cast<TStringBuf>(name), sortOrder)
{ }

TSortColumn::TSortColumn(const char* name, ESortOrder sortOrder)
    : TSortColumn(static_cast<TStringBuf>(name), sortOrder)
{ }

const TSortColumn& TSortColumn::EnsureAscending() const
{
    Y_ENSURE(SortOrder() == ESortOrder::SO_ASCENDING);
    return *this;
}

TNode TSortColumn::ToNode() const
{
    return BuildYsonNodeFluently().Value(*this);
}

////////////////////////////////////////////////////////////////////////////////
// Below lie backward compatibility methods.
////////////////////////////////////////////////////////////////////////////////

TSortColumn& TSortColumn::operator = (TStringBuf name)
{
    EnsureAscending();
    Name_ = name;
    return *this;
}

TSortColumn& TSortColumn::operator = (const TString& name)
{
    return (*this = static_cast<TStringBuf>(name));
}

TSortColumn& TSortColumn::operator = (const char* name)
{
    return (*this = static_cast<TStringBuf>(name));
}

bool TSortColumn::operator == (TStringBuf rhsName) const
{
    EnsureAscending();
    return Name_ == rhsName;
}

bool TSortColumn::operator == (const TString& rhsName) const
{
    return *this == static_cast<TStringBuf>(rhsName);
}

bool TSortColumn::operator == (const char* rhsName) const
{
    return *this == static_cast<TStringBuf>(rhsName);
}

TSortColumn::operator TStringBuf() const
{
    EnsureAscending();
    return Name_;
}

TSortColumn::operator TString() const
{
    return TString(static_cast<TStringBuf>(*this));
}

TSortColumn::operator std::string() const
{
    EnsureAscending();
    return static_cast<std::string>(Name_);
}

////////////////////////////////////////////////////////////////////////////////

TSortColumns::TSortColumns()
{ }

TSortColumns::TSortColumns(const TVector<TString>& names)
{
    Parts_.assign(names.begin(), names.end());
}

TSortColumns::TSortColumns(const TColumnNames& names)
    : TSortColumns(names.Parts_)
{ }

TSortColumns::operator TColumnNames() const
{
    return TColumnNames(EnsureAscending().GetNames());
}

const TSortColumns& TSortColumns::EnsureAscending() const
{
    for (const auto& sortColumn : Parts_) {
        sortColumn.EnsureAscending();
    }
    return *this;
}

TVector<TString> TSortColumns::GetNames() const
{
    TVector<TString> names;
    names.reserve(Parts_.size());
    for (const auto& sortColumn : Parts_) {
        names.push_back(sortColumn.Name());
    }
    return names;
}

////////////////////////////////////////////////////////////////////////////////

static NTi::TTypePtr OldTypeToTypeV3(EValueType type)
{
    switch (type) {
        case VT_INT64:
            return NTi::Int64();
        case VT_UINT64:
            return NTi::Uint64();

        case VT_DOUBLE:
            return NTi::Double();

        case VT_BOOLEAN:
            return NTi::Bool();

        case VT_STRING:
            return NTi::String();

        case VT_ANY:
            return NTi::Yson();

        case VT_INT8:
            return NTi::Int8();
        case VT_INT16:
            return NTi::Int16();
        case VT_INT32:
            return NTi::Int32();

        case VT_UINT8:
            return NTi::Uint8();
        case VT_UINT16:
            return NTi::Uint16();
        case VT_UINT32:
            return NTi::Uint32();

        case VT_UTF8:
            return NTi::Utf8();

        case VT_NULL:
            return NTi::Null();

        case VT_VOID:
            return NTi::Void();

        case VT_DATE:
            return NTi::Date();
        case VT_DATETIME:
            return NTi::Datetime();
        case VT_TIMESTAMP:
            return NTi::Timestamp();
        case VT_INTERVAL:
            return NTi::Interval();

        case VT_FLOAT:
            return NTi::Float();
        case VT_JSON:
            return NTi::Json();

        case VT_DATE32:
            return NTi::Date32();
        case VT_DATETIME64:
            return NTi::Datetime64();
        case VT_TIMESTAMP64:
            return NTi::Timestamp64();
        case VT_INTERVAL64:
            return NTi::Interval64();
    }
}

static std::pair<EValueType, bool> Simplify(const NTi::TTypePtr& type)
{
    using namespace NTi;
    const auto typeName = type->GetTypeName();
    switch (typeName) {
        case ETypeName::Bool:
            return {VT_BOOLEAN, true};

        case ETypeName::Int8:
            return {VT_INT8, true};
        case ETypeName::Int16:
            return {VT_INT16, true};
        case ETypeName::Int32:
            return {VT_INT32, true};
        case ETypeName::Int64:
            return {VT_INT64, true};

        case ETypeName::Uint8:
            return {VT_UINT8, true};
        case ETypeName::Uint16:
            return {VT_UINT16, true};
        case ETypeName::Uint32:
            return {VT_UINT32, true};
        case ETypeName::Uint64:
            return {VT_UINT64, true};

        case ETypeName::Float:
            return {VT_FLOAT, true};
        case ETypeName::Double:
            return {VT_DOUBLE, true};

        case ETypeName::String:
            return {VT_STRING, true};
        case ETypeName::Utf8:
            return {VT_UTF8, true};

        case ETypeName::Date:
            return {VT_DATE, true};
        case ETypeName::Datetime:
            return {VT_DATETIME, true};
        case ETypeName::Timestamp:
            return {VT_TIMESTAMP, true};
        case ETypeName::Interval:
            return {VT_INTERVAL, true};

        case ETypeName::TzDate:
        case ETypeName::TzDatetime:
        case ETypeName::TzTimestamp:
            break;

        case ETypeName::Json:
            return {VT_JSON, true};
        case ETypeName::Decimal:
            return {VT_STRING, true};
        case ETypeName::Uuid:
            break;
        case ETypeName::Yson:
            return {VT_ANY, true};

        case ETypeName::Date32:
            return {VT_DATE32, true};
        case ETypeName::Datetime64:
            return {VT_DATETIME64, true};
        case ETypeName::Timestamp64:
            return {VT_TIMESTAMP64, true};
        case ETypeName::Interval64:
            return {VT_INTERVAL64, true};

        case ETypeName::Void:
            return {VT_VOID, false};
        case ETypeName::Null:
            return {VT_NULL, false};

        case ETypeName::Optional:
            {
                auto itemType = type->AsOptional()->GetItemType();
                if (itemType->IsPrimitive()) {
                    auto simplified = Simplify(itemType->AsPrimitive());
                    if (simplified.second) {
                        simplified.second = false;
                        return simplified;
                    }
                }
                return {VT_ANY, false};
            }
        case ETypeName::List:
            return {VT_ANY, true};
        case ETypeName::Dict:
            return {VT_ANY, true};
        case ETypeName::Struct:
            return {VT_ANY, true};
        case ETypeName::Tuple:
            return {VT_ANY, true};
        case ETypeName::Variant:
            return {VT_ANY, true};
        case ETypeName::Tagged:
            return Simplify(type->AsTagged()->GetItemType());
    }
    ythrow TApiUsageError() << "Unsupported type: " << typeName;
}

NTi::TTypePtr ToTypeV3(EValueType type, bool required)
{
    auto typeV3 = OldTypeToTypeV3(type);
    if (!Simplify(typeV3).second) {
        if (required) {
            ythrow TApiUsageError() << "type: " << type << " cannot be required";
        } else {
            return typeV3;
        }
    }
    if (required) {
        return typeV3;
    } else {
        return NTi::Optional(typeV3);
    }
}

TColumnSchema::TColumnSchema()
    : TypeV3_(NTi::Optional(NTi::Int64()))
{ }

EValueType TColumnSchema::Type() const
{
    return Simplify(TypeV3_).first;
}

TColumnSchema& TColumnSchema::Type(EValueType type) &
{
    return Type(ToTypeV3(type, false));
}

TColumnSchema TColumnSchema::Type(EValueType type) &&
{
    return Type(ToTypeV3(type, false));
}

TColumnSchema& TColumnSchema::Type(const NTi::TTypePtr& type) &
{
    Y_ABORT_UNLESS(type.Get(), "Cannot create column schema with nullptr type");
    TypeV3_ = type;
    return *this;
}

TColumnSchema TColumnSchema::Type(const NTi::TTypePtr& type) &&
{
    Y_ABORT_UNLESS(type.Get(), "Cannot create column schema with nullptr type");
    TypeV3_ = type;
    return *this;
}

TColumnSchema& TColumnSchema::TypeV3(const NTi::TTypePtr& type) &
{
    return Type(type);
}

TColumnSchema TColumnSchema::TypeV3(const NTi::TTypePtr& type) &&
{
    return Type(type);
}

NTi::TTypePtr TColumnSchema::TypeV3() const
{
    return TypeV3_;
}

bool TColumnSchema::Required() const
{
    return Simplify(TypeV3_).second;
}

TColumnSchema& TColumnSchema::Type(EValueType type, bool required) &
{
    return Type(ToTypeV3(type, required));
}

TColumnSchema TColumnSchema::Type(EValueType type, bool required) &&
{
    return Type(ToTypeV3(type, required));
}

bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return
        lhs.Name() == rhs.Name() &&
        NTi::NEq::TStrictlyEqual()(lhs.TypeV3(), rhs.TypeV3()) &&
        lhs.SortOrder() == rhs.SortOrder() &&
        lhs.Lock() == rhs.Lock() &&
        lhs.Expression() == rhs.Expression() &&
        lhs.Aggregate() == rhs.Aggregate() &&
        lhs.Group() == rhs.Group();
}

////////////////////////////////////////////////////////////////////////////////

bool TTableSchema::Empty() const
{
    return Columns_.empty();
}

TTableSchema& TTableSchema::AddColumn(const TString& name, EValueType type) &
{
    Columns_.push_back(TColumnSchema().Name(name).Type(type));
    return *this;
}

TTableSchema TTableSchema::AddColumn(const TString& name, EValueType type) &&
{
    return std::move(AddColumn(name, type));
}

TTableSchema& TTableSchema::AddColumn(const TString& name, EValueType type, ESortOrder sortOrder) &
{
    Columns_.push_back(TColumnSchema().Name(name).Type(type).SortOrder(sortOrder));
    return *this;
}

TTableSchema TTableSchema::AddColumn(const TString& name, EValueType type, ESortOrder sortOrder) &&
{
    return std::move(AddColumn(name, type, sortOrder));
}

TTableSchema& TTableSchema::AddColumn(const TString& name, const NTi::TTypePtr& type) &
{
    Columns_.push_back(TColumnSchema().Name(name).Type(type));
    return *this;
}

TTableSchema TTableSchema::AddColumn(const TString& name, const NTi::TTypePtr& type) &&
{
    return std::move(AddColumn(name, type));
}

TTableSchema& TTableSchema::AddColumn(const TString& name, const NTi::TTypePtr& type, ESortOrder sortOrder) &
{
    Columns_.push_back(TColumnSchema().Name(name).Type(type).SortOrder(sortOrder));
    return *this;
}

TTableSchema TTableSchema::AddColumn(const TString& name, const NTi::TTypePtr& type, ESortOrder sortOrder) &&
{
    return std::move(AddColumn(name, type, sortOrder));
}

TTableSchema& TTableSchema::SortBy(const TSortColumns& sortColumns) &
{
    Y_ENSURE(sortColumns.Parts_.size() <= Columns_.size());

    THashMap<TString, ui64> sortColumnIndex;
    for (auto i: xrange(sortColumns.Parts_.size())) {
        Y_ENSURE(sortColumnIndex.emplace(sortColumns.Parts_[i].Name(), i).second,
            "Key column name '" << sortColumns.Parts_[i].Name() << "' repeats in columns list");
    }

    TVector<TColumnSchema> newColumnsSorted(sortColumns.Parts_.size());
    TVector<TColumnSchema> newColumnsUnsorted;
    for (auto& column : Columns_) {
        auto it = sortColumnIndex.find(column.Name());
        if (it == sortColumnIndex.end()) {
            column.ResetSortOrder();
            newColumnsUnsorted.push_back(std::move(column));
        } else {
            auto index = it->second;
            const auto& sortColumn = sortColumns.Parts_[index];
            column.SortOrder(sortColumn.SortOrder());
            newColumnsSorted[index] = std::move(column);
            sortColumnIndex.erase(it);
        }
    }

    Y_ENSURE(sortColumnIndex.empty(), "Column name '" << sortColumnIndex.begin()->first
            << "' not found in table schema");

    newColumnsSorted.insert(newColumnsSorted.end(), newColumnsUnsorted.begin(), newColumnsUnsorted.end());
    Columns_ = std::move(newColumnsSorted);

    return *this;
}

TTableSchema TTableSchema::SortBy(const TSortColumns& sortColumns) &&
{
    return std::move(SortBy(sortColumns));
}

TVector<TColumnSchema>& TTableSchema::MutableColumns()
{
    return Columns_;
}

TNode TTableSchema::ToNode() const
{
    TNode result;
    TNodeBuilder builder(&result);
    Serialize(*this, &builder);
    return result;
}

TTableSchema TTableSchema::FromNode(const TNode& node)
{
    TTableSchema schema;
    Deserialize(schema, node);
    return schema;
}

bool operator==(const TTableSchema& lhs, const TTableSchema& rhs)
{
    return
        lhs.Columns() == rhs.Columns() &&
        lhs.Strict() == rhs.Strict() &&
        lhs.UniqueKeys() == rhs.UniqueKeys();
}

void PrintTo(const TTableSchema& schema, std::ostream* out)
{
    (*out) << NodeToYsonString(schema.ToNode(), NYson::EYsonFormat::Pretty);
}

////////////////////////////////////////////////////////////////////////////////

TKeyBound::TKeyBound(ERelation relation, TKey key)
    : Relation_(relation)
    , Key_(std::move(key))
{ }

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateTableSchema(
    const Descriptor& messageDescriptor,
    const TSortColumns& sortColumns,
    bool keepFieldsWithoutExtension)
{
    auto result = CreateTableSchema(messageDescriptor, keepFieldsWithoutExtension);
    if (!sortColumns.Parts_.empty()) {
        result.SortBy(sortColumns.Parts_);
    }
    return result;
}

TTableSchema CreateTableSchema(NTi::TTypePtr type)
{
    Y_ABORT_UNLESS(type);
    TTableSchema schema;
    Deserialize(schema, NodeFromYsonString(NTi::NIo::AsYtSchema(type.Get())));
    return schema;
}

////////////////////////////////////////////////////////////////////////////////

bool IsTrivial(const TReadLimit& readLimit)
{
    return !readLimit.Key_ && !readLimit.RowIndex_ && !readLimit.Offset_ && !readLimit.TabletIndex_ && !readLimit.KeyBound_;
}

EValueType NodeTypeToValueType(TNode::EType nodeType)
{
    switch (nodeType) {
        case TNode::EType::Int64: return VT_INT64;
        case TNode::EType::Uint64: return VT_UINT64;
        case TNode::EType::String: return VT_STRING;
        case TNode::EType::Double: return VT_DOUBLE;
        case TNode::EType::Bool: return VT_BOOLEAN;
        default:
            ythrow yexception() << "Cannot convert TNode type " << nodeType << " to EValueType";
    }
}

////////////////////////////////////////////////////////////////////////////////

const TVector<TReadRange>& GetRangesCompat(const TRichYPath& path)
{
    static const TVector<TReadRange> empty;

    const auto& maybeRanges = path.GetRanges();
    if (maybeRanges.Empty()) {
        return empty;
    } else if (maybeRanges->size() > 0) {
        return *maybeRanges;
    } else {
        // If you see this exception, that means that caller of this function doesn't known what to do
        // with RichYPath that has set range list, but the range list is empty.
        //
        // To avoid this exception caller must explicitly handle such case.
        // NB. YT-17683
        ythrow TApiUsageError() << "Unsupported RichYPath: explicitly empty range list";
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TString ToString(EValueType type)
{
    switch (type) {
        case VT_INT8:
            return "int8";
        case VT_INT16:
            return "int16";
        case VT_INT32:
            return "int32";
        case VT_INT64:
            return "int64";

        case VT_UINT8:
            return "uint8";
        case VT_UINT16:
            return "uint16";
        case VT_UINT32:
            return "uint32";
        case VT_UINT64:
            return "uint64";

        case VT_DOUBLE:
            return "double";

        case VT_BOOLEAN:
            return "boolean";

        case VT_STRING:
            return "string";
        case VT_UTF8:
            return "utf8";

        case VT_ANY:
            return "any";

        case VT_NULL:
            return "null";
        case VT_VOID:
            return "void";

        case VT_DATE:
            return "date";
        case VT_DATETIME:
            return "datetime";
        case VT_TIMESTAMP:
            return "timestamp";
        case VT_INTERVAL:
            return "interval";

        case VT_FLOAT:
            return "float";

        case VT_JSON:
            return "json";

        case VT_DATE32:
            return "date32";
        case VT_DATETIME64:
            return "datetime64";
        case VT_TIMESTAMP64:
            return "timestamp64";
        case VT_INTERVAL64:
            return "interval64";
    }
    ythrow yexception() << "Invalid value type " << static_cast<int>(type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT

template <>
void Out<NYT::TSortColumn>(IOutputStream& os, const NYT::TSortColumn& sortColumn)
{
    if (sortColumn.SortOrder() == NYT::ESortOrder::SO_ASCENDING) {
        os << sortColumn.Name();
    } else {
        os << NYT::BuildYsonStringFluently(NYson::EYsonFormat::Text).Value(sortColumn);
    }
}
