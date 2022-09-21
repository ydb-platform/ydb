// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <AggregateFunctions/IAggregateFunction.h>

#include <common/StringRef.h>
#include <ranges>

namespace CH
{

class DataTypeAggregateFunction final : public arrow::ExtensionType
{
public:
    static constexpr const char * FAMILY_NAME = "aggregate_function";

    DataTypeAggregateFunction(const AggregateFunctionPtr & function_,
                              const DataTypes & argument_types_,
                              const Array & parameters_)
        : arrow::ExtensionType(arrow::uint64())
        , function(function_)
        , argument_types(argument_types_)
        , parameters(parameters_)
    {}

    std::string extension_name() const override { return FAMILY_NAME; }

    bool ExtensionEquals(const arrow::ExtensionType& other) const override
    {
        return extension_name() != other.extension_name(); // TODO
    }

    std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

    virtual arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(std::shared_ptr<arrow::DataType> /*storage_type*/,
                                                                        const std::string& /*serialized_data*/) const override
    {
        return std::make_shared<DataTypeAggregateFunction>(AggregateFunctionPtr{}, DataTypes{}, Array{}); // TODO
    }

    std::string Serialize() const override { return {}; } // TODO

    AggregateFunctionPtr getFunction() const { return function; }

private:
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array parameters;
};

/** Column of states of aggregate functions.
  * Presented as an array of pointers to the states of aggregate functions (data).
  * The states themselves are stored in one of the pools (arenas).
  *
  * It can be in two variants:
  *
  * 1. Own its values - that is, be responsible for destroying them.
  * The column consists of the values "assigned to it" after the aggregation is performed (see Aggregator, convertToBlocks function),
  *  or from values created by itself (see `insert` method).
  * In this case, `src` will be `nullptr`, and the column itself will be destroyed (call `IAggregateFunction::destroy`)
  *  states of aggregate functions in the destructor.
  *
  * 2. Do not own its values, but use values taken from another ColumnAggregateFunction column.
  * For example, this is a column obtained by permutation/filtering or other transformations from another column.
  * In this case, `src` will be `shared ptr` to the source column. Destruction of values will be handled by this source column.
  *
  * This solution is somewhat limited:
  * - the variant in which the column contains a part of "it's own" and a part of "another's" values is not supported;
  * - the option of having multiple source columns is not supported, which may be necessary for a more optimal merge of the two columns.
  *
  * These restrictions can be removed if you add an array of flags or even refcount,
  *  specifying which individual values should be destroyed and which ones should not.
  * Clearly, this method would have a substantially non-zero price.
  */
class ColumnAggregateFunction final : public arrow::ExtensionArray
{
private:
#if 0
    /// Arenas used by function states that are created elsewhere. We own these
    /// arenas in the sense of extending their lifetime, but do not modify them.
    /// Even reading these arenas is unsafe, because they may be shared with
    /// other data blocks and modified by other threads concurrently.
    ConstArenas foreign_arenas;
#endif
    /// Used for destroying states and for finalization of values.
    AggregateFunctionPtr func;

    /// Source column. Used (holds source from destruction),
    ///  if this column has been constructed from another and uses all or part of its values.
    ColumnPtr src;

public:
    ColumnAggregateFunction(const std::shared_ptr<DataTypeAggregateFunction> & data_type)
        : arrow::ExtensionArray(data_type, *arrow::MakeArrayOfNull(arrow::uint64(), 0))
        , func(data_type->getFunction())
    {}

    explicit ColumnAggregateFunction(const std::shared_ptr<arrow::ArrayData>& data)
        : arrow::ExtensionArray(data)
        , func(std::static_pointer_cast<DataTypeAggregateFunction>(data->type)->getFunction())
    {}

    ~ColumnAggregateFunction() override;

    const arrow::UInt64Array & getData() const { return static_cast<arrow::UInt64Array &>(*storage()); }
    const AggregateDataPtr * rawData() const { return reinterpret_cast<const AggregateDataPtr *>(getData().raw_values()); }
};


class MutableColumnAggregateFunction final : public arrow::ArrayBuilder
{
public:
    MutableColumnAggregateFunction(const std::shared_ptr<DataTypeAggregateFunction> & data_type_,
                                   arrow::MemoryPool* pool = arrow::default_memory_pool())
        : arrow::ArrayBuilder(pool)
        , data_type(data_type_)
        , builder(std::make_shared<arrow::UInt64Builder>(pool))
    {}

    std::shared_ptr<arrow::DataType> type() const override { return data_type; }

    arrow::Status AppendNull() override { return arrow::Status(arrow::StatusCode::NotImplemented, __FUNCTION__); }
    arrow::Status AppendNulls(int64_t) override { return arrow::Status(arrow::StatusCode::NotImplemented, __FUNCTION__); }
    arrow::Status AppendEmptyValue() override { return arrow::Status(arrow::StatusCode::NotImplemented, __FUNCTION__); }
    arrow::Status AppendEmptyValues(int64_t) override { return arrow::Status(arrow::StatusCode::NotImplemented, __FUNCTION__); }

    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override
    {
        auto array = *builder->Finish();
        *out = array->data()->Copy();
        (*out)->type = data_type;
        // TODO: add arenas
        return arrow::Status::OK();
    }

    arrow::UInt64Builder & getData() { return *builder; }

private:
    std::shared_ptr<DataTypeAggregateFunction> data_type;
    std::shared_ptr<arrow::UInt64Builder> builder;
};

}
