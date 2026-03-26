#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <stdexcept>

#include <util/system/types.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/util/bitmap.h>

#include <common/StringRef.h>
#include <common/extended_types.h>
#include <common/defines.h>

#include <Common/PODArray_fwd.h>

namespace CH
{

/// What to do if the limit is exceeded.
enum class OverflowMode
{
    THROW     = 0,    /// Throw exception.
    BREAK     = 1,    /// Abort query execution, return what is.

    /** Only for GROUP BY: do not add new rows to the set,
      * but continue to aggregate for keys that are already in the set.
      */
    ANY       = 2,
};

using Exception = std::runtime_error;
using ColumnNumbers = std::vector<uint32_t>; // it's vector<size_t> in CH
using Names = std::vector<std::string>;

using Block = std::shared_ptr<arrow20::RecordBatch>;
using BlocksList = std::list<Block>;
using Array = arrow20::ScalarVector;
using ColumnWithTypeAndName = arrow20::Field;
using ColumnsWithTypeAndName = arrow20::FieldVector;
using Header = std::shared_ptr<arrow20::Schema>;
using Sizes = std::vector<size_t>;

// TODO: replace with arrow20::memory_pool
class Arena;
using ArenaPtr = std::shared_ptr<Arena>;
using ConstArenaPtr = std::shared_ptr<const Arena>;
using ConstArenas = std::vector<ConstArenaPtr>;

using IColumn = arrow20::Array;
using ColumnPtr = std::shared_ptr<IColumn>;
using Columns = std::vector<ColumnPtr>;
using ColumnRawPtrs = std::vector<const IColumn *>;

using MutableColumn = arrow20::ArrayBuilder;
using MutableColumnPtr = std::shared_ptr<arrow20::ArrayBuilder>;
using MutableColumns = std::vector<MutableColumnPtr>;

struct XColumn {
    using Offset = UInt64;
    using Offsets = PaddedPODArray<Offset>;

    using ColumnIndex = UInt64;
    using Selector = PaddedPODArray<ColumnIndex>;

    using Filter = PaddedPODArray<UInt8>;
};

using ColumnInt8 = arrow20::NumericArray<arrow20::Int8Type>;
using ColumnInt16 = arrow20::NumericArray<arrow20::Int16Type>;
using ColumnInt32 = arrow20::NumericArray<arrow20::Int32Type>;
using ColumnInt64 = arrow20::NumericArray<arrow20::Int64Type>;

using ColumnUInt8 = arrow20::NumericArray<arrow20::UInt8Type>;
using ColumnUInt16 = arrow20::NumericArray<arrow20::UInt16Type>;
using ColumnUInt32 = arrow20::NumericArray<arrow20::UInt32Type>;
using ColumnUInt64 = arrow20::NumericArray<arrow20::UInt64Type>;

using ColumnFloat32 = arrow20::NumericArray<arrow20::FloatType>;
using ColumnFloat64 = arrow20::NumericArray<arrow20::DoubleType>;

using ColumnBinary = arrow20::BinaryArray;
using ColumnString = arrow20::StringArray;
using ColumnFixedString = arrow20::FixedSizeBinaryArray;

using ColumnTimestamp = arrow20::TimestampArray;
using ColumnDuration = arrow20::DurationArray;
using ColumnDecimal = arrow20::DecimalArray;

using MutableColumnInt8 = arrow20::Int8Builder;
using MutableColumnInt16 = arrow20::Int16Builder;
using MutableColumnInt32 = arrow20::Int32Builder;
using MutableColumnInt64 = arrow20::Int64Builder;

using MutableColumnUInt8 = arrow20::UInt8Builder;
using MutableColumnUInt16 = arrow20::UInt16Builder;
using MutableColumnUInt32 = arrow20::UInt32Builder;
using MutableColumnUInt64 = arrow20::UInt64Builder;

using MutableColumnFloat32 = arrow20::FloatBuilder;
using MutableColumnFloat64 = arrow20::DoubleBuilder;

using MutableColumnBinary = arrow20::BinaryBuilder;
using MutableColumnString = arrow20::StringBuilder;
using MutableColumnFixedString = arrow20::FixedSizeBinaryBuilder;

using MutableColumnTimestamp = arrow20::TimestampBuilder;
using MutableColumnDuration = arrow20::DurationBuilder;
using MutableColumnDecimal = arrow20::DecimalBuilder;

using IDataType = arrow20::DataType;
using DataTypePtr = std::shared_ptr<IDataType>;
using DataTypes = arrow20::DataTypeVector;

using DataTypeInt8 = arrow20::Int8Type;
using DataTypeInt16 = arrow20::Int16Type;
using DataTypeInt32 = arrow20::Int32Type;
using DataTypeInt64 = arrow20::Int64Type;

using DataTypeUInt8 = arrow20::UInt8Type;
using DataTypeUInt16 = arrow20::UInt16Type;
using DataTypeUInt32 = arrow20::UInt32Type;
using DataTypeUInt64 = arrow20::UInt64Type;

using DataTypeFloat32 = arrow20::FloatType;
using DataTypeFloat64 = arrow20::DoubleType;

using DataTypeBinary = arrow20::BinaryType;
using DataTypeString = arrow20::StringType;
using DataTypeFixedString = arrow20::FixedSizeBinaryType;

using DataTypeTimestamp = arrow20::TimestampType;
using DataTypeDuration = arrow20::DurationType;
using DataTypeDecimal = arrow20::DecimalType;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    ColumnNumbers arguments;
    Names argument_names;    /// used if no `arguments` are specified.
    String column_name;      /// What name to use for a column with aggregate function values
};

using AggregateDescriptions = std::vector<AggregateDescription>;

using AggregateColumnsData = std::vector<arrow20::UInt64Builder *>;
using AggregateColumnsConstData = std::vector<const arrow20::UInt64Array *>;


inline Columns columnsFromHeader(const Header& schema, size_t num_rows = 0)
{
    std::vector<std::shared_ptr<arrow20::Array>> columns;
    columns.reserve(schema->num_fields());

    for (auto& field : schema->fields()) {
        columns.emplace_back(*arrow20::MakeArrayOfNull(field->type(), num_rows));
    }
    return columns;
}

inline Block blockFromHeader(const Header& schema, size_t num_rows = 0)
{
    return arrow20::RecordBatch::Make(schema, num_rows, columnsFromHeader(schema, num_rows));
}

template <typename To, typename From>
inline To assert_cast(From && from)
{
#ifndef NDEBUG
    if constexpr (std::is_pointer_v<To>) {
        if (!dynamic_cast<To>(from)) {
            throw std::bad_cast();
        }
    }
    return dynamic_cast<To>(from);
#else
    return static_cast<To>(from);
#endif
}

template <typename To>
inline To assert_same_size_cast(const IColumn * from)
{
#ifndef NDEBUG
    using ArrayType = typename std::remove_pointer<To>::type;
    using CTo = typename ArrayType::value_type;

    auto type_id = from->type_id();
    if (arrow20::is_primitive(type_id) && sizeof(CTo) == (bit_width(type_id) / 8))
        return static_cast<To>(from);
    return assert_cast<To>(from);
#else
    return static_cast<To>(from);
#endif
}

template <typename To>
inline To assert_same_size_cast(MutableColumn & from)
{
#ifndef NDEBUG
    using ArrayType = typename std::remove_reference<To>::type;
    using CTo = typename ArrayType::value_type;

    auto type_id = from.type()->id();
    if (arrow20::is_primitive(type_id) && sizeof(CTo) == (bit_width(type_id) / 8))
        return static_cast<To>(from);
    return assert_cast<To>(from);
#else
    return static_cast<To>(from);
#endif
}

}
