// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Common/BitHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionWrapper.h>


namespace CH
{

/// For numeric values.
template <typename ArrowType>
struct SingleValueDataFixed
{
private:
    using Self = SingleValueDataFixed;
    using ColumnType = arrow::NumericArray<ArrowType>;
    using MutableColumnType = arrow::NumericBuilder<ArrowType>;

    bool has_value = false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    typename arrow::TypeTraits<ArrowType>::CType value;

public:
    static constexpr bool is_any = false;

    bool has() const
    {
        return has_value;
    }

    void insertResultInto(MutableColumn & to) const
    {
        if (has())
            assert_cast<MutableColumnType &>(to).Append(value).ok();
        else
            assert_cast<MutableColumnType &>(to).AppendNull().ok();
    }

    void change(const IColumn & column, size_t row_num, Arena *)
    {
        has_value = true;
        value = assert_cast<const ColumnType &>(column).Value(row_num);
    }

    /// Assuming to.has()
    void change(const Self & to, Arena *)
    {
        has_value = true;
        value = to.value;
    }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has() || assert_cast<const ColumnType &>(column).Value(row_num) < value)
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.value < value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has() || assert_cast<const ColumnType &>(column).Value(row_num) > value)
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.value > value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const Self & to) const
    {
        return has() && to.value == value;
    }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && assert_cast<const ColumnType &>(column).Value(row_num) == value;
    }

    static bool allocatesMemoryInArena()
    {
        return false;
    }
};


/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
  */
template <bool is_utf8_string>
struct SingleValueDataString
{
private:
    using Self = SingleValueDataString<is_utf8_string>;
    using ColumnType = std::conditional_t<is_utf8_string, ColumnString, ColumnBinary>;
    using MutableColumnType = std::conditional_t<is_utf8_string, MutableColumnString, MutableColumnBinary>;

    Int32 size = -1;    /// -1 indicates that there is no value.
    Int32 capacity = 0;    /// power of two or zero
    char * large_data;

public:
    static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
    static constexpr Int32 MAX_SMALL_STRING_SIZE = AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data);

private:
    char small_data[MAX_SMALL_STRING_SIZE]; /// Including the terminating zero.

public:
    static constexpr bool is_any = false;

    bool has() const
    {
        return size >= 0;
    }

    const char * getData() const
    {
        return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data;
    }

    arrow::util::string_view getStringView() const
    {
        if (!has())
            return {};
        return arrow::util::string_view(getData(), size);
    }

    void insertResultInto(MutableColumn & to) const
    {
        if (has())
            assert_cast<MutableColumnType &>(to).Append(getData(), size).ok();
        else
            assert_cast<MutableColumnType &>(to).AppendNull().ok();
    }

    /// Assuming to.has()
    void changeImpl(arrow::util::string_view value, Arena * arena)
    {
        Int32 value_size = value.size();

        if (value_size <= MAX_SMALL_STRING_SIZE)
        {
            /// Don't free large_data here.
            size = value_size;

            if (size > 0)
                memcpy(small_data, value.data(), size);
        }
        else
        {
            if (capacity < value_size)
            {
                /// Don't free large_data here.
                capacity = roundUpToPowerOfTwoOrZero(value_size);
                large_data = arena->alloc(capacity);
            }

            size = value_size;
            memcpy(large_data, value.data(), size);
        }
    }

    void change(const IColumn & column, size_t row_num, Arena * arena)
    {
        changeImpl(assert_cast<const ColumnType &>(column).Value(row_num), arena);
    }

    void change(const Self & to, Arena * arena)
    {
        changeImpl(to.getStringView(), arena);
    }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has() || assert_cast<const ColumnType &>(column).Value(row_num) < getStringView())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.getStringView() < getStringView()))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has() || assert_cast<const ColumnType &>(column).Value(row_num) > getStringView())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.getStringView() > getStringView()))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const Self & to) const
    {
        return has() && to.getStringView() == getStringView();
    }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && assert_cast<const ColumnType &>(column).Value(row_num) == getStringView();
    }

    static bool allocatesMemoryInArena()
    {
        return true;
    }
};

static_assert(sizeof(SingleValueDataString<false>) == SingleValueDataString<false>::AUTOMATIC_STORAGE_SIZE,
              "Incorrect size of SingleValueDataString struct");
static_assert(sizeof(SingleValueDataString<true>) == SingleValueDataString<true>::AUTOMATIC_STORAGE_SIZE,
              "Incorrect size of SingleValueDataString struct");


#if 0
/// For any other value types.
struct SingleValueDataGeneric
{
private:
    using Self = SingleValueDataGeneric;
    static constexpr bool is_any = false;

    Field value;

public:
    bool has() const
    {
        return !value.isNull();
    }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            to.insert(value);
        else
            to.insertDefault();
    }

    void change(const IColumn & column, size_t row_num, Arena *)
    {
        column.get(row_num, value);
    }

    void change(const Self & to, Arena *)
    {
        value = to.value;
    }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
        {
            Field new_value;
            column.get(row_num, new_value);
            if (new_value < value)
            {
                value = new_value;
                return true;
            }
            else
                return false;
        }
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.value < value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
        {
            Field new_value;
            column.get(row_num, new_value);
            if (new_value > value)
            {
                value = new_value;
                return true;
            }
            else
                return false;
        }
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.value > value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && value == column[row_num];
    }

    bool isEqualTo(const Self & to) const
    {
        return has() && to.value == value;
    }

    static bool allocatesMemoryInArena()
    {
        return false;
    }
};
#endif

/** What is the difference between the aggregate functions min, max, any, anyLast
  *  (the condition that the stored value is replaced by a new one,
  *   as well as, of course, the name).
  */

template <typename Data>
struct AggregateFunctionMinData : Data
{
    using Self = AggregateFunctionMinData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeIfLess(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena)                        { return this->changeIfLess(to, arena); }
};

template <typename Data>
struct AggregateFunctionMaxData : Data
{
    using Self = AggregateFunctionMaxData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeIfGreater(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena)                        { return this->changeIfGreater(to, arena); }
};

template <typename Data>
struct AggregateFunctionAnyData : Data
{
    using Self = AggregateFunctionAnyData;
    static constexpr bool is_any = true;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeFirstTime(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena)                        { return this->changeFirstTime(to, arena); }
};

template <typename Data>
class AggregateFunctionsSingleValue final : public IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>
{
    static constexpr bool is_any = Data::is_any;

private:
    DataTypePtr & type;

public:
    AggregateFunctionsSingleValue(const DataTypePtr & type_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>({type_}, {})
        , type(this->argument_types[0])
    {
#if 0
        if (StringRef(Data::name()) == StringRef("min")
            || StringRef(Data::name()) == StringRef("max"))
        {
            if (!type->isComparable())
                throw Exception("Illegal type " + type->getName() + " of argument of aggregate function " + getName()
                    + " because the values of that data type are not comparable");
        }
#endif
    }

    DataTypePtr getReturnType() const override
    {
        return type;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).changeIfBetter(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena * arena) const override
    {
        if constexpr (is_any)
            if (this->data(place).has())
                return;

        const auto & column = *columns[0];
        if (column.null_bitmap_data())
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (column.IsValid(i))
                {
                    this->data(place).changeIfBetter(column, i, arena);
                    if constexpr (is_any)
                        break;
                }
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                this->data(place).changeIfBetter(column, i, arena);
                if constexpr (is_any)
                    break;
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).changeIfBetter(this->data(rhs), arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return Data::allocatesMemoryInArena();
    }

    void insertResultInto(AggregateDataPtr __restrict place, MutableColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

template <template <typename> typename AggFunc, template <typename> typename AggData>
inline std::shared_ptr<IAggregateFunction> createAggregateFunctionSingleValue(const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        return {};

    const DataTypePtr & argument_type = argument_types[0];

    switch (argument_type->id()) {
        case arrow::Type::INT8:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::Int8Type>>>>(argument_type);
        case arrow::Type::INT16:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::Int16Type>>>>(argument_type);
        case arrow::Type::INT32:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::Int32Type>>>>(argument_type);
        case arrow::Type::INT64:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::Int64Type>>>>(argument_type);
        case arrow::Type::UINT8:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::UInt8Type>>>>(argument_type);
        case arrow::Type::UINT16:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::UInt16Type>>>>(argument_type);
        case arrow::Type::UINT32:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::UInt32Type>>>>(argument_type);
        case arrow::Type::UINT64:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::UInt64Type>>>>(argument_type);
        case arrow::Type::FLOAT:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::FloatType>>>>(argument_type);
        case arrow::Type::DOUBLE:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::DoubleType>>>>(argument_type);
        case arrow::Type::TIMESTAMP:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::TimestampType>>>>(argument_type);
        case arrow::Type::DURATION:
            return std::make_shared<AggFunc<AggData<SingleValueDataFixed<arrow::DurationType>>>>(argument_type);
        case arrow::Type::BINARY:
            return std::make_shared<AggFunc<AggData<SingleValueDataString<false>>>>(argument_type);
        case arrow::Type::STRING:
            return std::make_shared<AggFunc<AggData<SingleValueDataString<true>>>>(argument_type);
        default:
            break;
    }

    //return std::make_shared<AggFunc<AggData<SingleValueDataGeneric>>>(argument_type); // TODO
    return {};
}

template <template <typename> typename AggFunc, template <typename> typename AggData>
class WrappedMinMaxAny final : public ArrowAggregateFunctionWrapper
{
public:
    WrappedMinMaxAny(std::string name)
        : ArrowAggregateFunctionWrapper(std::move(name))
    {}

    AggregateFunctionPtr getHouseFunction(const DataTypes & argument_types) const override
    {
        return createAggregateFunctionSingleValue<AggFunc, AggData>(argument_types);
    }
};

using WrappedMin = WrappedMinMaxAny<AggregateFunctionsSingleValue, AggregateFunctionMinData>;
using WrappedMax = WrappedMinMaxAny<AggregateFunctionsSingleValue, AggregateFunctionMaxData>;
using WrappedAny = WrappedMinMaxAny<AggregateFunctionsSingleValue, AggregateFunctionAnyData>;

}
