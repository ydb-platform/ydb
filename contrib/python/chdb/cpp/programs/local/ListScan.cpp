#include "ListScan.h"
#include "PythonConversion.h"

#include <Columns/ColumnObject.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationJSON.h>
#include <IO/WriteHelpers.h>

namespace DB_CHDB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PY_EXCEPTION_OCCURED;
}

}

using namespace DB_CHDB;

namespace CHDB {

ColumnPtr ListScan::scanObject(
    const ColumnWrapper & col_wrap,
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings)
{
    innerCheck(col_wrap);

    auto & data_type = col_wrap.dest_type;
    auto column = data_type->createColumn();
    auto serialization = data_type->getDefaultSerialization();

    innerScanObject(cursor, count, format_settings, serialization, col_wrap.data, column);

    return column;
}

void ListScan::scanObject(
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings,
    const py::handle & obj,
    MutableColumnPtr & column)
{
    auto data_type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
    SerializationPtr serialization = data_type->getDefaultSerialization();

    innerScanObject(cursor, count, format_settings, serialization, obj, column);
}

void ListScan::innerScanObject(
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings,
    SerializationPtr & serialization,
    const py::handle & obj,
    MutableColumnPtr & column)
{
    py::gil_scoped_acquire acquire;

    auto list = obj.cast<py::list>();

    for (size_t i = cursor; i < cursor + count; ++i)
    {
        auto item = list.attr("__getitem__")(i);
        if (!tryInsertJsonResult(item, format_settings, column, serialization))
            column->insertDefault();
	}
}

void ListScan::innerCheck(const ColumnWrapper & col_wrap)
{
    if (col_wrap.data.is_none())
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Column data is None");

    if (!col_wrap.buf)
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Column buffer is null");
}

} // namespace CHDB
