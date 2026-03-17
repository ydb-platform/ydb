#include "PandasScan.h"
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

ColumnPtr PandasScan::scanObject(
    const ColumnWrapper & col_wrap,
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings)
{
    innerCheck(col_wrap);

    auto & data_type = col_wrap.dest_type;
    auto column = data_type->createColumn();
    auto ** object_array = static_cast<PyObject **>(col_wrap.buf);
    auto serialization = data_type->getDefaultSerialization();

    innerScanObject(cursor, count, format_settings, serialization, object_array, column);

    return column;
}

void PandasScan::scanObject(
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings,
    const void * buf,
    MutableColumnPtr & column)
{
    auto object_array = static_cast<PyObject **>(const_cast<void *>(buf));
    auto data_type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
    SerializationPtr serialization = data_type->getDefaultSerialization();

    innerScanObject(cursor, count, format_settings, serialization, object_array, column);
}

void PandasScan::innerScanObject(
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings,
    SerializationPtr & serialization,
    PyObject ** objects,
    MutableColumnPtr & column)
{
    py::gil_scoped_acquire acquire;

    for (size_t i = cursor; i < cursor + count; ++i)
    {
        auto * obj = objects[i];
        auto handle = py::handle(obj);

        if (!tryInsertJsonResult(handle, format_settings, column, serialization))
            column->insertDefault();
    }
}

void PandasScan::innerCheck(const ColumnWrapper & col_wrap)
{
    if (col_wrap.data.is_none())
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Column data is None");

    if (!col_wrap.buf)
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Column buffer is null");
}

} // namespace CHDB
