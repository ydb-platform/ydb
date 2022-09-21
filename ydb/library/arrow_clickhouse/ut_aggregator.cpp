#include <array>
#include <memory>
#include <vector>
#include <iostream>
#include <iomanip>

#include <library/cpp/testing/unittest/registar.h>

#include "Aggregator.h"
#include "DataStreams/OneBlockInputStream.h"
#include "DataStreams/AggregatingBlockInputStream.h"
#include "DataStreams/MergingAggregatedBlockInputStream.h"
#include "AggregateFunctions/AggregateFunctionCount.h"
#include "AggregateFunctions/AggregateFunctionMinMaxAny.h"
#include "AggregateFunctions/AggregateFunctionSum.h"
#include "AggregateFunctions/AggregateFunctionAvg.h"

namespace CH {

void RegisterAggregates(arrow::compute::FunctionRegistry * registry = nullptr) {
    if (!registry)
        registry = arrow::compute::GetFunctionRegistry();

    registry->AddFunction(std::make_shared<CH::WrappedCount>("ch.count")).ok();
    registry->AddFunction(std::make_shared<CH::WrappedMin>("ch.min")).ok();
    registry->AddFunction(std::make_shared<CH::WrappedMax>("ch.max")).ok();
    registry->AddFunction(std::make_shared<CH::WrappedAny>("ch.any")).ok();
    registry->AddFunction(std::make_shared<CH::WrappedSum>("ch.sum")).ok();
    registry->AddFunction(std::make_shared<CH::WrappedAvg>("ch.avg")).ok();
}

// {i16, ui32, s1, s2}
Block makeTestBlock(size_t num_rows) {
    std::vector<std::string> strings = {"abc", "def", "abcd", "defg", "ac"};

    arrow::FieldVector fields;
    arrow::ArrayVector columns;

    {
        auto field = std::make_shared<arrow::Field>("i16", arrow::int16());
        arrow::Int16Builder col;
        col.Reserve(num_rows).ok();

        for (size_t i = 0; i < num_rows; ++i)
            col.Append(i % 9).ok();

        fields.emplace_back(std::move(field));
        columns.emplace_back(std::move(*col.Finish()));
    }

    {
        auto field = std::make_shared<arrow::Field>("ui32", arrow::uint32());
        arrow::UInt32Builder col;
        col.Reserve(num_rows).ok();

        for (size_t i = 0; i < num_rows; ++i)
            col.Append(i % 7).ok();

        fields.emplace_back(std::move(field));
        columns.emplace_back(std::move(*col.Finish()));
    }

    {
        auto field = std::make_shared<arrow::Field>("s1", arrow::binary());
        arrow::BinaryBuilder col;
        col.Reserve(num_rows).ok();

        for (size_t i = 0; i < num_rows; ++i)
            col.Append(strings[i % strings.size()]).ok();

        fields.emplace_back(std::move(field));
        columns.emplace_back(std::move(*col.Finish()));
    }

    {
        auto field = std::make_shared<arrow::Field>("s2", arrow::binary());
        arrow::BinaryBuilder col;
        col.Reserve(num_rows).ok();

        for (size_t i = 0; i < num_rows; ++i)
            col.Append(strings[i % 3]).ok();

        fields.emplace_back(std::move(field));
        columns.emplace_back(std::move(*col.Finish()));
    }

    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), num_rows, columns);
}

AggregateDescription MakeCountDescription(const std::string & column_name = "cnt")
{
    auto * registry = arrow::compute::GetFunctionRegistry();
    auto func = registry->GetFunction("ch.count");
    auto wrapped = std::static_pointer_cast<ArrowAggregateFunctionWrapper>(*func);

    DataTypes empty_list_of_types;
    return AggregateDescription {
        .function = wrapped->getHouseFunction(empty_list_of_types),
        .column_name = column_name
    };
}

AggregateDescription MakeMinMaxAnyDescription(const std::string & agg_name, DataTypePtr data_type,
                                                 uint32_t column_id)
{
    auto * registry = arrow::compute::GetFunctionRegistry();
    auto func = registry->GetFunction(agg_name);
    auto wrapped = std::static_pointer_cast<ArrowAggregateFunctionWrapper>(*func);

    DataTypes list_of_types = {data_type};
    return AggregateDescription {
        .function = wrapped->getHouseFunction(list_of_types),
        .arguments = {column_id},
        .column_name = "res_" + agg_name
    };
}

AggregateDescription MakeSumDescription(DataTypePtr data_type, uint32_t column_id,
                                        const std::string & column_name = "res_sum")
{
    auto * registry = arrow::compute::GetFunctionRegistry();
    auto func = registry->GetFunction("ch.sum");
    auto wrapped = std::static_pointer_cast<ArrowAggregateFunctionWrapper>(*func);

    DataTypes list_of_types = {data_type};
    return AggregateDescription {
        .function = wrapped->getHouseFunction(list_of_types),
        .arguments = {column_id},
        .column_name = column_name
    };
}

AggregateDescription MakeAvgDescription(DataTypePtr data_type, uint32_t column_id,
                                        const std::string & column_name = "res_avg")
{
    auto * registry = arrow::compute::GetFunctionRegistry();
    auto func = registry->GetFunction("ch.avg");
    auto wrapped = std::static_pointer_cast<ArrowAggregateFunctionWrapper>(*func);

    DataTypes list_of_types = {data_type};
    return AggregateDescription {
        .function = wrapped->getHouseFunction(list_of_types),
        .arguments = {column_id},
        .column_name = column_name
    };
}

BlockInputStreamPtr MakeAggregatingStream(const BlockInputStreamPtr & stream,
                                          const ColumnNumbers & agg_keys,
                                          const AggregateDescriptions & aggregate_descriptions)
{
    Header src_header = stream->getHeader();
    Aggregator::Params agg_params(false, src_header, agg_keys, aggregate_descriptions, false);
    BlockInputStreamPtr agg_stream = std::make_shared<AggregatingBlockInputStream>(stream, agg_params, false);

    ColumnNumbers merge_keys;
    {
        Header agg_header = agg_stream->getHeader();
        for (const auto & key : agg_keys)
            merge_keys.push_back(agg_header->GetFieldIndex(src_header->field(key)->name()));
    }

    Aggregator::Params merge_params(true, agg_stream->getHeader(), merge_keys, aggregate_descriptions, false);
    return std::make_shared<MergingAggregatedBlockInputStream>(agg_stream, merge_params, true);
}

bool TestExecute(const Block & block, const ColumnNumbers & agg_keys)
{
    try
    {
        BlockInputStreamPtr stream = std::make_shared<OneBlockInputStream>(block);

        AggregateDescription aggregate_description = MakeCountDescription();
        Aggregator::Params params(false, stream->getHeader(), agg_keys, {aggregate_description}, false);
        Aggregator aggregator(params);

        AggregatedDataVariants aggregated_data_variants;

        {
            //Stopwatch stopwatch;
            //stopwatch.start();

            aggregator.execute(stream, aggregated_data_variants);

            //stopwatch.stop();
            //std::cout << std::fixed << std::setprecision(2)
            //    << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
            //    << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
            //    << std::endl;
        }
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}

size_t TestAggregate(const Block & block, const ColumnNumbers & agg_keys, const AggregateDescription & description)
{
    size_t rows = 0;

    try
    {
        std::cerr << "aggregate by keys: ";
        for (auto& key : agg_keys) {
            std::cerr << key << " ";
        }
        std::cerr << std::endl;

        auto stream = MakeAggregatingStream(std::make_shared<OneBlockInputStream>(block), agg_keys, {description});

        while (auto block = stream->read()) {
            std::cerr << "result rows: " << block->num_rows() << std::endl;
            rows += block->num_rows();
        }
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << std::endl;
        return 0;
    }

    return rows;
}

}


Y_UNIT_TEST_SUITE(CH_Aggregator) {
    Y_UNIT_TEST(ExecuteCount) {
        CH::RegisterAggregates();

        auto block = CH::makeTestBlock(1000);

        UNIT_ASSERT(CH::TestExecute(block, {0, 1}));
        UNIT_ASSERT(CH::TestExecute(block, {1, 0}));
        UNIT_ASSERT(CH::TestExecute(block, {0, 2}));
        UNIT_ASSERT(CH::TestExecute(block, {2, 0}));
        UNIT_ASSERT(CH::TestExecute(block, {2, 3}));
        UNIT_ASSERT(CH::TestExecute(block, {0, 1, 2, 3}));
    }

    Y_UNIT_TEST(AggregateCount) {
        CH::RegisterAggregates();

        auto block = CH::makeTestBlock(1000);

        auto agg_count = CH::MakeCountDescription();

        UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1}, agg_count), 9*7);
        UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {1, 0}, agg_count), 7*9);
        UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 2}, agg_count), 9*5);
        UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 0}, agg_count), 5*9);
        UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 3}, agg_count), 5*3);
        UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1, 2, 3}, agg_count), 9*7*5);
    }

    Y_UNIT_TEST(AggregateMin) {
        CH::RegisterAggregates();

        auto block = CH::makeTestBlock(1000);

        for (int i = 0; i < block->num_columns(); ++i) {
            auto type = block->column(i)->type();
            auto agg_descr = CH::MakeMinMaxAnyDescription("ch.min", type, i);

            UNIT_ASSERT(agg_descr.function);
            UNIT_ASSERT_VALUES_EQUAL(agg_descr.arguments.size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1}, agg_descr), 9*7);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {1, 0}, agg_descr), 7*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 2}, agg_descr), 9*5);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 0}, agg_descr), 5*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 3}, agg_descr), 5*3);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9*7*5);
        }
    }

    Y_UNIT_TEST(AggregateMax) {
        CH::RegisterAggregates();

        auto block = CH::makeTestBlock(1000);

        for (int i = 0; i < block->num_columns(); ++i) {
            auto type = block->column(i)->type();
            auto agg_descr = CH::MakeMinMaxAnyDescription("ch.max", type, i);

            UNIT_ASSERT(agg_descr.function);
            UNIT_ASSERT_VALUES_EQUAL(agg_descr.arguments.size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1}, agg_descr), 9*7);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {1, 0}, agg_descr), 7*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 2}, agg_descr), 9*5);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 0}, agg_descr), 5*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 3}, agg_descr), 5*3);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9*7*5);
        }
    }

    Y_UNIT_TEST(AggregateAny) {
        CH::RegisterAggregates();

        auto block = CH::makeTestBlock(1000);

        for (int i = 0; i < block->num_columns(); ++i) {
            auto type = block->column(i)->type();
            auto agg_descr = CH::MakeMinMaxAnyDescription("ch.any", type, i);

            UNIT_ASSERT(agg_descr.function);
            UNIT_ASSERT_VALUES_EQUAL(agg_descr.arguments.size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1}, agg_descr), 9*7);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {1, 0}, agg_descr), 7*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 2}, agg_descr), 9*5);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 0}, agg_descr), 5*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 3}, agg_descr), 5*3);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9*7*5);
        }
    }

    Y_UNIT_TEST(AggregateSum) {
        CH::RegisterAggregates();

        auto block = CH::makeTestBlock(1000);

        for (int i = 0; i < 2; ++i) {
            auto type = block->column(i)->type();
            auto agg_descr = CH::MakeSumDescription(type, i);

            UNIT_ASSERT(agg_descr.function);
            UNIT_ASSERT_VALUES_EQUAL(agg_descr.arguments.size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1}, agg_descr), 9*7);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {1, 0}, agg_descr), 7*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 2}, agg_descr), 9*5);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 0}, agg_descr), 5*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 3}, agg_descr), 5*3);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9*7*5);
        }
    }

    Y_UNIT_TEST(AggregateAvg) {
        CH::RegisterAggregates();

        auto block = CH::makeTestBlock(1000);

        for (int i = 0; i < 2; ++i) {
            auto type = block->column(i)->type();
            auto agg_descr = CH::MakeAvgDescription(type, i);

            UNIT_ASSERT(agg_descr.function);
            UNIT_ASSERT_VALUES_EQUAL(agg_descr.arguments.size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1}, agg_descr), 9*7);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {1, 0}, agg_descr), 7*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 2}, agg_descr), 9*5);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 0}, agg_descr), 5*9);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {2, 3}, agg_descr), 5*3);
            UNIT_ASSERT_VALUES_EQUAL(CH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9*7*5);
        }
    }
}
