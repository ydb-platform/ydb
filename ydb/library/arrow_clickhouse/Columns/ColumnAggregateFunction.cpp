// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include <sstream>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsCommon.h>
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>


namespace CH
{

std::shared_ptr<arrow::Array> DataTypeAggregateFunction::MakeArray(std::shared_ptr<arrow::ArrayData> data) const
{
    return std::make_shared<ColumnAggregateFunction>(data);
}

ColumnAggregateFunction::~ColumnAggregateFunction()
{
    if (!func->hasTrivialDestructor() && !src)
    {
        auto & arr = getData();
        for (int64_t i = 0; i < arr.length(); ++i)
            func->destroy(reinterpret_cast<AggregateDataPtr>(arr.Value(i)));
    }
}

}
