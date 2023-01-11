#pragma once
#include "arrow_clickhouse_types.h"
#include "Aggregator.h"
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/AggregatingBlockInputStream.h>

#include <unordered_set>

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
        const arrow::compute::FunctionOptions* options,
        arrow::compute::ExecContext* /*ctx*/) const override
    {
        static const std::string result_name = "res";
        static const std::vector<std::string> arg_names = {"0", "1", "2", "3", "4", "5"};
        if (args.size() > arg_names.size())
            return arrow::Status::Invalid("unexpected arguments count");

        bool has_nullable_key = true;
        if (options)
        {
            if (auto* opts = dynamic_cast<const GroupByOptions*>(options))
                has_nullable_key = opts->has_nullable_key;
        }

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
        if (!batch)
            return arrow::Status::Invalid("Wrong aggregation arguments: cannot make batch");

        AggregateDescription description {
            .function = getHouseFunction(types),
            .arguments = arg_positions,
            .column_name = result_name
        };

        auto input_stream = std::make_shared<OneBlockInputStream>(batch);

        // no agg keys, final aggregate states
        Aggregator::Params agg_params(false, input_stream->getHeader(), {}, {description}, false);
        agg_params.has_nullable_key = has_nullable_key;
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

class ArrowGroupBy : public arrow::compute::ScalarAggregateFunction
{
public:
    ArrowGroupBy(std::string name)
        : arrow::compute::ScalarAggregateFunction(std::move(name), arrow::compute::Arity::VarArgs(1), nullptr)
    {}

    arrow::Result<arrow::Datum> Execute(
        const std::vector<arrow::Datum>& args,
        const arrow::compute::FunctionOptions* options,
        arrow::compute::ExecContext* /*ctx*/) const override
    {
        if (args.empty())
            return arrow::Status::Invalid("GROUP BY without arguments");
        if (!options)
            return arrow::Status::Invalid("GROUP BY without options");

        auto* opts = dynamic_cast<const GroupByOptions*>(options);
        if (!opts || !opts->schema)
            return arrow::Status::Invalid("Wrong GROUP BY options");
        if ((int)args.size() != opts->schema->num_fields())
            return arrow::Status::Invalid("Wrong GROUP BY arguments count");

        // Find needed columns
        std::unordered_set<std::string> needed_columns;
        needed_columns.reserve(opts->assigns.size());
        for (auto& assign : opts->assigns)
        {
            if (assign.function != AggFunctionId::AGG_UNSPECIFIED)
            {
                for (auto& agg_arg : assign.arguments)
                    needed_columns.insert(agg_arg);
            }
            else
                needed_columns.insert(assign.result_column);
        }

        // Make batch with needed columns
        std::shared_ptr<arrow::RecordBatch> batch;
        {
            std::vector<std::shared_ptr<arrow::Array>> columns;
            std::vector<std::shared_ptr<arrow::Field>> fields;
            columns.reserve(needed_columns.size());
            fields.reserve(needed_columns.size());

            std::optional<int64_t> num_rows;
            for (int i = 0; i < opts->schema->num_fields(); ++i)
            {
                auto& datum = args[i];
                auto& field = opts->schema->field(i);

                if (!needed_columns.count(field->name()))
                    continue;

                if (datum.is_array())
                {
                    if (num_rows && *num_rows != datum.mutable_array()->length)
                        return arrow::Status::Invalid("Arrays have different length");
                    num_rows = datum.mutable_array()->length;
                }
                else if (!datum.is_scalar())
                    return arrow::Status::Invalid("Bad scalar: '" + field->name() + "'");
            }
            if (!num_rows) // All datums are scalars
                num_rows = 1;

            for (int i = 0; i < opts->schema->num_fields(); ++i)
            {
                auto& datum = args[i];
                auto& field = opts->schema->field(i);

                if (!needed_columns.count(field->name()))
                    continue;

                if (datum.is_scalar())
                {
                    // TODO: better GROUP BY over scalars
                    auto res = arrow::MakeArrayFromScalar(*datum.scalar(), *num_rows);
                    if (!res.ok())
                        return arrow::Status::Invalid("Bad scalar for '" + field->name() + "', " + res.status().ToString());
                    columns.push_back(*res);
                }
                else
                    columns.push_back(datum.make_array());

                fields.push_back(field);
            }

            batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), *num_rows, columns);
            if (!batch)
                return arrow::Status::Invalid("Wrong GROUP BY arguments: cannot make batch");
        }

        // Make aggregats descriptions
        std::vector<AggregateDescription> descriptions;
        ColumnNumbers keys;
        {
            descriptions.reserve(opts->assigns.size());
            keys.reserve(opts->assigns.size());

            auto& schema = batch->schema();

            for (auto& assign : opts->assigns)
            {
                if (assign.function != AggFunctionId::AGG_UNSPECIFIED)
                {
                    ColumnNumbers arg_positions;
                    arg_positions.reserve(assign.arguments.size());
                    DataTypes types;
                    types.reserve(assign.arguments.size());

                    for (auto& agg_arg : assign.arguments) {
                        int pos = schema->GetFieldIndex(agg_arg);
                        if (pos < 0)
                            return arrow::Status::Invalid("Unexpected aggregate function argument in GROUP BY");
                        arg_positions.push_back(pos);
                        types.push_back(schema->field(pos)->type());
                    }

                    AggregateFunctionPtr func = GetAggregateFunction(assign.function, types);
                    if (!func)
                        return arrow::Status::Invalid("Unexpected agregate function in GROUP BY");

                    descriptions.emplace_back(AggregateDescription{
                        .function = func,
                        .arguments = arg_positions,
                        .column_name = assign.result_column
                    });
                } else {
                    int pos = schema->GetFieldIndex(assign.result_column);
                    if (pos < 0)
                        return arrow::Status::Invalid("Unexpected key in GROUP BY: '" + assign.result_column + "'");
                    keys.push_back(pos);
                }
            }
        }

        // GROUP BY

        auto input_stream = std::make_shared<OneBlockInputStream>(batch);

        Aggregator::Params agg_params(false, input_stream->getHeader(), keys, descriptions, false);
        agg_params.has_nullable_key = opts->has_nullable_key;
        AggregatingBlockInputStream agg_stream(input_stream, agg_params, true);

        auto result_batch = agg_stream.read();
        if (!result_batch || (batch->num_rows() && !result_batch->num_rows()))
            return arrow::Status::Invalid("unexpected arrgerate result");
        if (agg_stream.read())
            return arrow::Status::Invalid("unexpected second batch in aggregate result");

        return arrow::Result<arrow::Datum>(result_batch);
    }
};

}
