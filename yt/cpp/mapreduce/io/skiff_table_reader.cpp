#include "skiff_table_reader.h"

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/skiff/wire_type.h>
#include <yt/cpp/mapreduce/skiff/skiff_schema.h>

#include <util/string/cast.h>

namespace NYT {
namespace NDetail {
namespace {

////////////////////////////////////////////////////////////////////////////////

enum EColumnType : i8
{
    Dense,
    KeySwitch,
    RangeIndex,
    RowIndex
};

struct TSkiffColumnSchema
{
    EColumnType Type;
    bool Required;
    NSkiff::EWireType WireType;
    TString Name;

    TSkiffColumnSchema(EColumnType type, bool required, NSkiff::EWireType wireType, const TString& name)
        : Type(type)
        , Required(required)
        , WireType(wireType)
        , Name(name)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

struct TSkiffTableReader::TSkiffTableSchema
{
    TVector<TSkiffColumnSchema> Columns;
};

TSkiffTableReader::TSkiffTableReader(
    ::TIntrusivePtr<TRawTableReader> input,
    const NSkiff::TSkiffSchemaPtr& schema)
    : Input_(std::move(input))
    , BufferedInput_(&Input_)
    , Parser_(&BufferedInput_)
    , Schemas_(CreateSkiffTableSchemas(schema))
{
    Next();
}

TSkiffTableReader::~TSkiffTableReader() = default;

const TNode& TSkiffTableReader::GetRow() const
{
    EnsureValidity();
    Y_ENSURE(!Row_.IsUndefined(), "Row is moved");
    return Row_;
}

void TSkiffTableReader::MoveRow(TNode* result)
{
    EnsureValidity();
    Y_ENSURE(!Row_.IsUndefined(), "Row is moved");
    *result = std::move(Row_);
    Row_ = TNode();
}

bool TSkiffTableReader::IsValid() const
{
    return Valid_;
}

void TSkiffTableReader::Next()
{
    EnsureValidity();
    if (Y_UNLIKELY(Finished_ || !Parser_->HasMoreData())) {
        Finished_ = true;
        Valid_ = false;
        return;
    }

    if (AfterKeySwitch_) {
        AfterKeySwitch_ = false;
        return;
    }

    while (true) {
        try {
            ReadRow();
            break;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR("Read error: %v", ex.what());
            if (!Input_.Retry(RangeIndex_, RowIndex_, std::make_exception_ptr(ex))) {
                throw;
            }
            BufferedInput_ = TBufferedInput(&Input_);
            Parser_.emplace(NSkiff::TUncheckedSkiffParser(&BufferedInput_));
            if (RangeIndex_) {
                RangeIndexShift_ += *RangeIndex_;
            }
            RangeIndex_.Clear();
            RowIndex_.Clear();
        }
    }
}

ui32 TSkiffTableReader::GetTableIndex() const
{
    EnsureValidity();
    return TableIndex_;
}

ui32 TSkiffTableReader::GetRangeIndex() const
{
    EnsureValidity();
    return RangeIndex_.GetOrElse(0) + RangeIndexShift_;
}

ui64 TSkiffTableReader::GetRowIndex() const
{
    EnsureValidity();
    return RowIndex_.GetOrElse(0ULL);
}

void TSkiffTableReader::NextKey()
{
    while (Valid_) {
        Next();
    }

    if (Finished_) {
        return;
    }

    Valid_ = true;
}

TMaybe<size_t> TSkiffTableReader::GetReadByteCount() const
{
    return Input_.GetReadByteCount();
}

bool TSkiffTableReader::IsRawReaderExhausted() const
{
    return Finished_;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TSkiffTableReader::TSkiffTableSchema> TSkiffTableReader::CreateSkiffTableSchemas(
    const NSkiff::TSkiffSchemaPtr& schema)
{
    using NSkiff::EWireType;

    constexpr auto keySwitchColumnName = "$key_switch";
    constexpr auto rangeIndexColumnName = "$range_index";
    constexpr auto rowIndexColumnName = "$row_index";

    static const THashMap<TString, TSkiffColumnSchema> specialColumns = {
        {keySwitchColumnName,  {EColumnType::KeySwitch,  true,  EWireType::Boolean, keySwitchColumnName}},
        {rangeIndexColumnName, {EColumnType::RangeIndex, false, EWireType::Int64,   rangeIndexColumnName}},
        {rowIndexColumnName,   {EColumnType::RowIndex,   false, EWireType::Int64,   rowIndexColumnName}},
    };

    Y_ENSURE(schema->GetWireType() == EWireType::Variant16,
        "Expected 'variant16' wire type for schema, got '" << schema->GetWireType() << "'");
    TVector<TSkiffTableSchema> result;
    for (const auto& tableSchema : schema->GetChildren()) {
        Y_ENSURE(tableSchema->GetWireType() == EWireType::Tuple,
            "Expected 'tuple' wire type for table schema, got '" << tableSchema->GetWireType() << "'");
        TVector<TSkiffColumnSchema> columns;
        for (const auto& columnSchema : tableSchema->GetChildren()) {
            auto specialColumnIter = specialColumns.find(columnSchema->GetName());
            if (specialColumnIter != specialColumns.end()) {
                columns.push_back(specialColumnIter->second);
            } else {
                auto wireType = columnSchema->GetWireType();
                bool required = true;
                if (wireType == EWireType::Variant8) {
                    const auto& children = columnSchema->GetChildren();
                    Y_ENSURE(
                        children.size() == 2 && children[0]->GetWireType() == EWireType::Nothing &&
                        NSkiff::IsSimpleType(children[1]->GetWireType()),
                        "Expected schema of form 'variant8<nothing, simple-type>', got "
                            << NSkiff::GetShortDebugString(columnSchema));
                    wireType = children[1]->GetWireType();
                    required = false;
                }
                Y_ENSURE(NSkiff::IsSimpleType(wireType),
                    "Expected column schema to be of simple type, got " << NSkiff::GetShortDebugString(columnSchema));
                columns.emplace_back(
                    EColumnType::Dense,
                    required,
                    wireType,
                    columnSchema->GetName());
            }
        }
        result.push_back({std::move(columns)});
    }
    return result;
}

void TSkiffTableReader::ReadRow()
{
    if (Row_.IsUndefined()) {
        Row_ = TNode::CreateMap();
    } else {
        Row_.AsMap().clear();
    }

    if (RowIndex_) {
        ++*RowIndex_;
    }

    TableIndex_ = Parser_->ParseVariant16Tag();
    Y_ENSURE(TableIndex_ < Schemas_.size(), "Table index out of range: " << TableIndex_ << " >= " << Schemas_.size());
    const auto& tableSchema = Schemas_[TableIndex_];

    auto parse = [&](NSkiff::EWireType wireType) -> TNode {
        switch (wireType) {
            case NSkiff::EWireType::Int64:
                return Parser_->ParseInt64();
            case NSkiff::EWireType::Uint64:
                return Parser_->ParseUint64();
            case NSkiff::EWireType::Boolean:
                return Parser_->ParseBoolean();
            case NSkiff::EWireType::Double:
                return Parser_->ParseDouble();
            case NSkiff::EWireType::String32:
                return Parser_->ParseString32();
            case NSkiff::EWireType::Yson32:
                return NodeFromYsonString(Parser_->ParseYson32());
            case NSkiff::EWireType::Nothing:
                return TNode::CreateEntity();
            default:
                Y_ABORT("Bad column wire type: '%s'", ::ToString(wireType).data());
        }
    };

    for (const auto& columnSchema : tableSchema.Columns) {
        if (!columnSchema.Required) {
            auto tag = Parser_->ParseVariant8Tag();
            if (tag == 0) {
                if (columnSchema.Type == EColumnType::Dense) {
                    Row_[columnSchema.Name] = TNode::CreateEntity();
                }
                continue;
            }
            Y_ENSURE(tag == 1, "Tag for 'variant8<nothing," << columnSchema.WireType
                << ">' expected to be 0 or 1, got " << tag);
        }
        auto value = parse(columnSchema.WireType);
        switch (columnSchema.Type) {
            case EColumnType::Dense:
                Row_[columnSchema.Name] = std::move(value);
                break;
            case EColumnType::KeySwitch:
                if (value.AsBool()) {
                    AfterKeySwitch_ = true;
                    Valid_ = false;
                }
                break;
            case EColumnType::RangeIndex:
                RangeIndex_ = value.AsInt64();
                break;
            case EColumnType::RowIndex:
                RowIndex_ = value.AsInt64();
                break;
            default:
                Y_ABORT("Bad column type: %d", static_cast<int>(columnSchema.Type));
        }
    }

    // We successfully parsed one more row from the stream,
    // so reset retry count to their initial value.
    Input_.ResetRetries();
}

void TSkiffTableReader::EnsureValidity() const
{
    Y_ENSURE(Valid_, "Iterator is not valid");
}

} // namespace NDetail
} // namespace NYT
