#include "row_base.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(EValueType type, std::ostream* os)
{
    *os << ToString(type);
}

void PrintTo(ESimpleLogicalValueType type, std::ostream* os)
{
    *os << ToString(type);
}

////////////////////////////////////////////////////////////////////////////////

TColumnFilter::TColumnFilter()
    : Universal_(true)
{ }

TColumnFilter::TColumnFilter(const std::initializer_list<int>& indexes)
    : Universal_(false)
    , Indexes_(indexes.begin(), indexes.end())
{ }

TColumnFilter::TColumnFilter(TIndexes&& indexes)
    : Universal_(false)
    , Indexes_(std::move(indexes))
{ }

TColumnFilter::TColumnFilter(const std::vector<int>& indexes)
    : Universal_(false)
    , Indexes_(indexes.begin(), indexes.end())
{ }

TColumnFilter::TColumnFilter(int schemaColumnCount)
    : Universal_(false)
{
    for (int i = 0; i < schemaColumnCount; ++i) {
        Indexes_.push_back(i);
    }
}

std::optional<int> TColumnFilter::FindPosition(int columnIndex) const
{
    if (Universal_) {
        THROW_ERROR_EXCEPTION("Unable to search index in column filter with IsUniversal flag");
    }
    auto it = std::find(Indexes_.begin(), Indexes_.end(), columnIndex);
    if (it == Indexes_.end()) {
        return std::nullopt;
    }
    return std::distance(Indexes_.begin(), it);
}

int TColumnFilter::GetPosition(int columnIndex) const
{
    if (auto indexOrNull = FindPosition(columnIndex)) {
        return *indexOrNull;
    }

    THROW_ERROR_EXCEPTION("Column filter does not contain column index %Qv", columnIndex);
}

bool TColumnFilter::ContainsIndex(int columnIndex) const
{
    if (Universal_) {
        return true;
    }

    return std::find(Indexes_.begin(), Indexes_.end(), columnIndex) != Indexes_.end();
}

const TColumnFilter::TIndexes& TColumnFilter::GetIndexes() const
{
    YT_VERIFY(!Universal_);
    return Indexes_;
}

bool TColumnFilter::IsUniversal() const
{
    return Universal_;
}

const TColumnFilter& TColumnFilter::MakeUniversal()
{
    static const TColumnFilter Result;
    return Result;
}

////////////////////////////////////////////////////////////////////////////////

void ValidateDataValueType(EValueType type)
{
    if (type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String &&
        type != EValueType::Any &&
        type != EValueType::Composite &&
        type != EValueType::Null)
    {
        THROW_ERROR_EXCEPTION("Invalid data value type %Qlv", type);
    }
}

void ValidateKeyValueType(EValueType type)
{
    if (type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String &&
        type != EValueType::Any &&
        type != EValueType::Composite &&
        type != EValueType::Null &&
        type != EValueType::Min &&
        type != EValueType::Max)
    {
        THROW_ERROR_EXCEPTION("Invalid key value type %Qlv", type);
    }
}

void ValidateSchemaValueType(EValueType type)
{
    if (type != EValueType::Null &&
        type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String &&
        type != EValueType::Any &&
        type != EValueType::Composite)
    {
        THROW_ERROR_EXCEPTION("Invalid value type %Qlv", type);
    }
}

void ValidateColumnFilter(const TColumnFilter& columnFilter, int schemaColumnCount)
{
    if (columnFilter.IsUniversal()) {
        return;
    }

    TCompactVector<bool, TypicalColumnCount> flags;
    flags.resize(schemaColumnCount);

    for (int index : columnFilter.GetIndexes()) {
        if (index < 0 || index >= schemaColumnCount) {
            THROW_ERROR_EXCEPTION("Column filter contains invalid index: actual %v, expected in range [0, %v]",
                index,
                schemaColumnCount - 1);
        }
        if (flags[index]) {
            THROW_ERROR_EXCEPTION("Column filter contains duplicate index %v",
                index);
        }
        flags[index] = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TColumnFilter& columnFilter, TStringBuf spec)
{
    if (columnFilter.IsUniversal()) {
        FormatValue(builder, TStringBuf{"{All}"}, spec);
    } else {
        FormatValue(builder, Format("{%v}", columnFilter.GetIndexes()), spec);
    }
}

////////////////////////////////////////////////////////////////////////////////

[[noreturn]] void ThrowUnexpectedValueType(EValueType valueType)
{
    THROW_ERROR_EXCEPTION("Unexpected value type %Qlv",
        valueType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
