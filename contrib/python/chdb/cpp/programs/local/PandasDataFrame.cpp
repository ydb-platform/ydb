#include "PandasDataFrame.h"
#include "FormatHelper.h"
#include "NumpyType.h"
#include "PandasAnalyzer.h"
#include "PandasCacheItem.h"
#include "PythonImporter.h"

#include <Common/Exception.h>
#include <Interpreters/Context.h>

namespace DB_CHDB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

using namespace DB_CHDB;

namespace CHDB {

template <typename T>
static bool ModuleIsLoaded()
{
    auto dict = pybind11::module_::import("sys").attr("modules");
    return dict.contains(py::str(T::Name));
}

struct PandasBindColumn {
public:
    PandasBindColumn(py::handle name, py::handle type, py::object column)
        : name(name), type(type), handle(std::move(column))
    {}

public:
    py::handle name;
    py::handle type;
    py::object handle;
};

struct PandasDataFrameBind {
public:
    explicit PandasDataFrameBind(const py::handle & df)
    {
        names = py::list(df.attr("columns"));
        types = py::list(df.attr("dtypes"));
        getter = df.attr("__getitem__");
    }

    PandasBindColumn operator[](size_t index) const {
        auto column = py::reinterpret_borrow<py::object>(getter(names[index]));
        auto type = types[index];
        auto name = names[index];
        return PandasBindColumn(name, type, column);
     }

public:
     py::list names;
     py::list types;

private:
    py::object getter;
};

static DataTypePtr inferDataTypeFromPandasColumn(PandasBindColumn & column, ContextPtr & context)
{
    auto numpy_type = ConvertNumpyType(column.type);

    /// TODO: support masked object, timezone and category.

    if (numpy_type.type == NumpyNullableType::OBJECT)
    {
        if (!isJSONSupported())
        {
            numpy_type.type = NumpyNullableType::STRING;
            return NumpyToDataType(numpy_type);
        }

		PandasAnalyzer analyzer(context->getSettingsRef());
		if (analyzer.Analyze(column.handle)) {
			return analyzer.analyzedType();
		}

        numpy_type.type = NumpyNullableType::STRING;
	}

    return NumpyToDataType(numpy_type);
}

ColumnsDescription PandasDataFrame::getActualTableStructure(const py::object & object, ContextPtr & context)
{
    NamesAndTypesList names_and_types;

    PandasDataFrameBind df(object);
	size_t column_count = py::len(df.names);
	if (column_count == 0 || py::len(df.types) == 0)
		throw DB_CHDB::Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected empty DataFrame");

    if (column_count != py::len(df.types))
        throw DB_CHDB::Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Unexpected DataFrame with column count: {} and type count: {}", column_count, py::len(df.types));

	for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
		auto col_name = py::str(df.names[col_idx]);
		auto column = df[col_idx];
		auto data_type = inferDataTypeFromPandasColumn(column, context);

        names_and_types.push_back({col_name, data_type});
	}

    return ColumnsDescription(names_and_types);
}

bool PandasDataFrame::isPandasDataframe(const py::object & object)
{
    if (!ModuleIsLoaded<PandasCacheItem>())
		return false;

	auto & importer_cache = PythonImporter::ImportCache();
	bool is_df = py::isinstance(object, importer_cache.pandas.DataFrame());

    if (!is_df)
        return false;

	auto arrow_dtype = importer_cache.pandas.ArrowDtype();
	py::list dtypes = object.attr("dtypes");
	for (auto & dtype : dtypes) {
		if (py::isinstance(dtype, arrow_dtype))
			return false;
	}

	return true;
}

bool PandasDataFrame::IsPyArrowBacked(const py::handle & object)
{
    /// TODO: check if object is pyarrow backed
    return false;
}

} // namespace CHDB
