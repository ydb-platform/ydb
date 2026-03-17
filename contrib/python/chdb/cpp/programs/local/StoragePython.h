#pragma once

#include "clickhouse_config.h"

#include <memory>
#include <string>
#include <vector>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <pybind11/cast.h>
#include <pybind11/functional.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <Common/Exception.h>
#include "PythonUtils.h"


namespace DB_CHDB
{

namespace py = pybind11;

namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT;
extern const int NOT_IMPLEMENTED;
extern const int PY_EXCEPTION_OCCURED;
}

void registerStoragePython(StorageFactory & factory);

class PyReader
{
public:
    explicit PyReader(const py::object & data) : data(data) { }
    ~PyReader()
    {
        py::gil_scoped_acquire acquire;
        if (data.is_none())
            return;
        data.release();
    }

    // Read `count` rows from the data, and return a list of columns
    // chdb todo: maybe return py::list is better, but this is just a shallow copy
    std::vector<py::object> read(const std::vector<std::string> & /*col_names*/, int /*count*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "read() method is not implemented");
    }

    // // readDataPtr is similar to readData but return pointer to py::object
    // static std::shared_ptr<std::vector<std::vector<PyObject *>>>
    // readDataPtr(const py::object & data, const std::vector<std::string> & col_names, size_t & cursor, size_t count)
    // {
    //     py::gil_scoped_acquire acquire;
    //     auto block = std::make_shared<std::vector<std::vector<PyObject *>>>();
    //     // Access columns directly by name and slice
    //     for (const auto & col : col_names)
    //     {
    //         py::object col_data = data[py::str(col)]; // Use dictionary-style access
    //         auto col_block = std::make_shared<std::vector<PyObject *>>();
    //         for (size_t i = cursor; i < cursor + count; i++)
    //             col_block->push_back(col_data.attr("__getitem__")(i).ptr());
    //         block->push_back(col_block);
    //     }

    //     if (!block->empty())
    //         cursor += py::len((*block)[0]); // Update cursor based on the length of the first column slice

    //     return block;
    // }

    // Return a vector of column names and their types, as a list of pairs.
    // The order is important, and should match the order of the data.
    // This is the default implementation, which trys to infer the schema from the every first row
    // of this.data column.
    // The logic is:
    //  1. If the data is a map with column names as keys and column data as values, then we use
    //    the key and type of every first element in the value list.
    //      eg:
    //          d = {'a': [1, 2, 3], 'b': ['x', 'y', 'z'], 'c': [1.0, 1e10, 1.2e100]}
    //          schema = {name: repr(type(value[0])) for name, value in d.items()}
    //      out:
    //          schema = {'a': "<class 'int'>", 'b': "<class 'str'>", 'c': "<class 'float'>"}
    //  2. If the data is a Pandas DataFrame, then we use the column names and dtypes.
    //    We use the repr of the dtype, which is a string representation of the dtype.
    //      eg:
    //          df = pd.DataFrame(d)
    //          schema = {name: repr(dtype) for name, dtype in zip(df.columns, df.dtypes)}
    //      out:
    //          schema = {'a': "dtype('int64')", 'b': "dtype('O')", 'c': "dtype('float64')"}
    //      Note:
    //          1. dtype('O') means object type, which is a catch-all for any types. we just treat it as string.
    //          2. the dtype of a Pandas DataFrame is a numpy.dtype object, which is not a Python type object.
    //
    //      When using Pandas >= 2.0, we can use the pyarrow as dtype_backend:
    //      eg:
    //          df_arr = pd.read_json('{"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [1.0, 1.111, 2.222]}', dtype_backend="pyarrow")
    //          schema = {name: repr(dtype) for name, dtype in zip(df_arr.columns, df_arr.dtypes)}
    //      out:
    //          schema = {'a': 'int64[pyarrow]', 'b': 'string[pyarrow]', 'c': 'double[pyarrow]'}
    //  3. if the data is a Pyarrow Table, then we use the column names and types.
    //      eg:
    //          tbl = pa.Table.from_pandas(df)
    //          schema = {field.name: repr(field.type) for field in tbl.schema}
    //      out:
    //          schema = {'a': 'DataType(int64)', 'b': 'DataType(string)', 'c': 'DataType(double)'}
    //  4. User can override this function to provide a more accurate schema.
    //      eg: "DataTypeUInt8", "DataTypeUInt16", "DataTypeUInt32", "DataTypeUInt64", "DataTypeUInt128", "DataTypeUInt256",
    //      "DataTypeInt8", "DataTypeInt16", "DataTypeInt32", "DataTypeInt64", "DataTypeInt128", "DataTypeInt256",
    //      "DataTypeFloat32", "DataTypeFloat64", "DataTypeString",

    static std::vector<std::pair<std::string, std::string>> getSchemaFromPyObj(py::object data);

    std::vector<std::pair<std::string, std::string>> getSchema() { return getSchemaFromPyObj(data); }

protected:
    py::object data;
};

// // Trampoline class
// // see: https://pybind11.readthedocs.io/en/stable/advanced/classes.html#trampolines
// class PyReaderTrampoline : public PyReader
// {
// public:
//     using PyReader::PyReader; // Inherit constructors

//     // Just forward the virtual function call to Python
//     std::vector<py::object> read(const std::vector<std::string> & col_names, int count) override
//     {
//         PYBIND11_OVERRIDE_PURE(
//             std::vector<py::object>, // Return type List[object]
//             PyReader, // Parent class
//             read, // Name of the function in C++ (must match Python name)
//             col_names, // Argument(s)
//             count);
//     }
// };

class StoragePython : public IStorage, public WithContext
{
public:
    StoragePython(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        py::object reader_,
        ContextPtr context_);

    ~StoragePython() override
    {
        // Destroy the reader with the GIL
        py::gil_scoped_acquire acquire;
        data_source.dec_ref();
        data_source.release();
    }

    std::string getName() const override { return "Python"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    Block prepareSampleBlock(const Names & column_names, const StorageSnapshotPtr & storage_snapshot);

    static ColumnsDescription getTableStructureFromData(std::vector<std::pair<std::string, std::string>> & schema);

private:
    void prepareColumnCache(const Names & names, const Columns & columns, const Block & sample_block);
    py::object data_source;
    PyColumnVecPtr column_cache;
    size_t data_source_row_count;
    CHDBPoco::Logger * logger = &CHDBPoco::Logger::get("StoragePython");
};

}
