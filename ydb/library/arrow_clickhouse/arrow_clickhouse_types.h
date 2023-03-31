#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <stdexcept>

#include <util/system/types.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/bitmap.h>

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

using Block = std::shared_ptr<arrow::RecordBatch>;
using BlocksList = std::list<Block>;
using Array = arrow::ScalarVector;
using ColumnWithTypeAndName = arrow::Field;
using ColumnsWithTypeAndName = arrow::FieldVector;
using Header = std::shared_ptr<arrow::Schema>;
using Sizes = std::vector<size_t>;

// TODO: replace with arrow::memory_pool
class Arena;
using ArenaPtr = std::shared_ptr<Arena>;
using ConstArenaPtr = std::shared_ptr<const Arena>;
using ConstArenas = std::vector<ConstArenaPtr>;

using IColumn = arrow::Array;
using ColumnPtr = std::shared_ptr<IColumn>;
using Columns = std::vector<ColumnPtr>;
using ColumnRawPtrs = std::vector<const IColumn *>;

using MutableColumn = arrow::ArrayBuilder;
using MutableColumnPtr = std::shared_ptr<arrow::ArrayBuilder>;
using MutableColumns = std::vector<MutableColumnPtr>;

struct XColumn {
    using Offset = UInt64;
    using Offsets = PaddedPODArray<Offset>;

    using ColumnIndex = UInt64;
    using Selector = PaddedPODArray<ColumnIndex>;

    using Filter = PaddedPODArray<UInt8>;
};

using ColumnInt8 = arrow::NumericArray<arrow::Int8Type>;
using ColumnInt16 = arrow::NumericArray<arrow::Int16Type>;
using ColumnInt32 = arrow::NumericArray<arrow::Int32Type>;
using ColumnInt64 = arrow::NumericArray<arrow::Int64Type>;

using ColumnUInt8 = arrow::NumericArray<arrow::UInt8Type>;
using ColumnUInt16 = arrow::NumericArray<arrow::UInt16Type>;
using ColumnUInt32 = arrow::NumericArray<arrow::UInt32Type>;
using ColumnUInt64 = arrow::NumericArray<arrow::UInt64Type>;

using ColumnFloat32 = arrow::NumericArray<arrow::FloatType>;
using ColumnFloat64 = arrow::NumericArray<arrow::DoubleType>;

using ColumnBinary = arrow::BinaryArray;
using ColumnString = arrow::StringArray;
using ColumnFixedString = arrow::FixedSizeBinaryArray;

using ColumnTimestamp = arrow::TimestampArray;
using ColumnDuration = arrow::DurationArray;
using ColumnDecimal = arrow::DecimalArray;

using MutableColumnInt8 = arrow::Int8Builder;
using MutableColumnInt16 = arrow::Int16Builder;
using MutableColumnInt32 = arrow::Int32Builder;
using MutableColumnInt64 = arrow::Int64Builder;

using MutableColumnUInt8 = arrow::UInt8Builder;
using MutableColumnUInt16 = arrow::UInt16Builder;
using MutableColumnUInt32 = arrow::UInt32Builder;
using MutableColumnUInt64 = arrow::UInt64Builder;

using MutableColumnFloat32 = arrow::FloatBuilder;
using MutableColumnFloat64 = arrow::DoubleBuilder;

using MutableColumnBinary = arrow::BinaryBuilder;
using MutableColumnString = arrow::StringBuilder;
using MutableColumnFixedString = arrow::FixedSizeBinaryBuilder;

using MutableColumnTimestamp = arrow::TimestampBuilder;
using MutableColumnDuration = arrow::DurationBuilder;
using MutableColumnDecimal = arrow::DecimalBuilder;

using IDataType = arrow::DataType;
using DataTypePtr = std::shared_ptr<IDataType>;
using DataTypes = arrow::DataTypeVector;

using DataTypeInt8 = arrow::Int8Type;
using DataTypeInt16 = arrow::Int16Type;
using DataTypeInt32 = arrow::Int32Type;
using DataTypeInt64 = arrow::Int64Type;

using DataTypeUInt8 = arrow::UInt8Type;
using DataTypeUInt16 = arrow::UInt16Type;
using DataTypeUInt32 = arrow::UInt32Type;
using DataTypeUInt64 = arrow::UInt64Type;

using DataTypeFloat32 = arrow::FloatType;
using DataTypeFloat64 = arrow::DoubleType;

using DataTypeBinary = arrow::BinaryType;
using DataTypeString = arrow::StringType;
using DataTypeFixedString = arrow::FixedSizeBinaryType;

using DataTypeTimestamp = arrow::TimestampType;
using DataTypeDuration = arrow::DurationType;
using DataTypeDecimal = arrow::DecimalType;

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

using AggregateColumnsData = std::vector<arrow::UInt64Builder *>;
using AggregateColumnsConstData = std::vector<const arrow::UInt64Array *>;


inline Columns columnsFromHeader(const Header& schema, size_t num_rows = 0)
{
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(schema->num_fields());

    for (auto& field : schema->fields()) {
        columns.emplace_back(*arrow::MakeArrayOfNull(field->type(), num_rows));
    }
    return columns;
}

inline Block blockFromHeader(const Header& schema, size_t num_rows = 0)
{
    return arrow::RecordBatch::Make(schema, num_rows, columnsFromHeader(schema, num_rows));
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
    if (arrow::is_primitive(type_id) && sizeof(CTo) == (bit_width(type_id) / 8))
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
    if (arrow::is_primitive(type_id) && sizeof(CTo) == (bit_width(type_id) / 8))
        return static_cast<To>(from);
    return assert_cast<To>(from);
#else
    return static_cast<To>(from);
#endif
}

}
