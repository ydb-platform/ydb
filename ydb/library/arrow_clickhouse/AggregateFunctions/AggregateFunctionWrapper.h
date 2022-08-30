#pragma once
#include "arrow_clickhouse_types.h"
#include "Aggregator.h"
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/AggregatingBlockInputStream.h>

namespace CH
{

class ArrowAggregateFunctionWrapper : public arrow::compute::ScalarAggregateFunction
{
public:
    ArrowAggregateFunctionWrapper(std::string name)
        : arrow::compute::ScalarAggregateFunction(std::move(name), arrow::compute::Arity::Unary(), nullptr)
    {}

    virtual AggregateFunctionPtr getHouseFunction(const DataTypes & argument_types) const = 0;

    arrow::Result<arrow::Datum> Execute(
        const std::vector<arrow::Datum>& args,
        const arrow::compute::FunctionOptions* /*options*/,
        arrow::compute::ExecContext* /*ctx*/) const override
    {
        static const std::string result_name = "res";
        static const std::vector<std::string> arg_names = {"0", "1", "2", "3", "4", "5"};
        if (args.size() > arg_names.size())
            return arrow::Status::Invalid("unexpected arguments count");

        std::vector<uint32_t> arg_positions;
        arg_positions.reserve(args.size());

        DataTypes types;
        arrow::FieldVector fields;
        std::vector<std::shared_ptr<arrow::Array>> columns;

        types.reserve(args.size());
        fields.reserve(args.size());
        columns.reserve(args.size());

        int num_rows = 0;
        uint32_t arg_num = 0;
        for (auto& arg : args)
        {
            if (!arg.is_array())
                return arrow::Status::Invalid("argument is not an array");

            columns.push_back(arg.make_array());
            types.push_back(columns.back()->type());
            fields.push_back(std::make_shared<arrow::Field>(arg_names[arg_num], types.back()));

            if (!arg_num)
                num_rows = columns.back()->length();
            else if (num_rows != columns.back()->length())
                return arrow::Status::Invalid("different argiments length");

            arg_positions.push_back(arg_num);
            ++arg_num;
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), num_rows, columns);

        AggregateDescription description {
            .function = getHouseFunction(types),
            .arguments = arg_positions,
            .column_name = result_name
        };

        auto input_stream = std::make_shared<OneBlockInputStream>(batch);

        // no agg keys, final aggregate states
        Aggregator::Params agg_params(false, input_stream->getHeader(), {}, {description}, false);
        AggregatingBlockInputStream agg_stream(input_stream, agg_params, true);

        auto result_batch = agg_stream.read();
        if (!result_batch || result_batch->num_rows() != 1)
            return arrow::Status::Invalid("unexpected arrgerate result");
        if (agg_stream.read())
            return arrow::Status::Invalid("unexpected second batch in aggregate result");

        auto res_column = result_batch->GetColumnByName(result_name);
        if (!res_column || res_column->length() != 1)
            return arrow::Status::Invalid("no result value");

        return res_column->GetScalar(0);
    }
};

}
