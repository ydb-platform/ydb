#include "PythonTableCache.h"
#include "PybindWrapper.h"
#include "PythonUtils.h"

#include <Common/re2.h>

namespace CHDB {

std::unordered_map<String, py::handle> PythonTableCache::py_table_cache;

/// Function to find instance of PyReader, pandas DataFrame, or PyArrow Table, filtered by variable name
static py::object findQueryableObj(const String & var_name)
{
    py::module inspect = py::module_::import("inspect");
    py::object current_frame = inspect.attr("currentframe")();

    while (!current_frame.is_none())
    {
        // Get f_locals and f_globals
        py::object locals_obj = current_frame.attr("f_locals");
        py::object globals_obj = current_frame.attr("f_globals");

        // For each namespace (locals and globals)
        for (const auto & namespace_obj : {locals_obj, globals_obj})
        {
            // Use Python's __contains__ method to check if the key exists
            // This works with both regular dicts and FrameLocalsProxy (Python 3.13+)
            if (py::bool_(namespace_obj.attr("__contains__")(var_name)))
            {
                py::object obj;
                try
                {
                    // Get the object using Python's indexing syntax
                    obj = namespace_obj[py::cast(var_name)];
                    if (DB_CHDB::isInheritsFromPyReader(obj) || DB_CHDB::isPandasDf(obj) || DB_CHDB::isPyarrowTable(obj) || DB_CHDB::hasGetItem(obj))
                    {
                        return obj;
                    }
                }
                catch (const py::error_already_set &)
                {
                    continue; // If getting the value fails, continue to the next namespace
                }
            }
        }

        // Move to the parent frame
        current_frame = current_frame.attr("f_back");
    }

    // Object not found
    return py::none();
}

void PythonTableCache::findQueryableObjFromQuery(const String & query_str)
{
    // Find the queriable object in the Python environment
    // return nullptr if no Python obj is referenced in query string
    // return py::none if the obj referenced not found
    // return the Python object if found
    // The object name is extracted from the query string, must referenced by
    // Python(var_name) or Python('var_name') or python("var_name") or python('var_name')
    // such as:
    //  - `SELECT * FROM Python('PyReader')`
    //  - `SELECT * FROM Python(PyReader_instance)`
    //  - `SELECT * FROM Python(some_var_with_type_pandas_DataFrame_or_pyarrow_Table)`
    // The object can be any thing that Python Table supported, like PyReader, pandas DataFrame, or PyArrow Table
    // The object should be in the global or local scope

    py::gil_assert();

    // RE2 pattern to match Python()/python() patterns with single/double quotes or no quotes
    static const RE2 pattern(R"([Pp]ython\s*\(\s*(?:['"]([^'"]+)['"]|([a-zA-Z_][a-zA-Z0-9_]*))\s*\))");

    re2::StringPiece input(query_str);
    std::string quoted_match, unquoted_match;

    // Try to match and extract the groups
    while (RE2::FindAndConsume(&input, pattern, &quoted_match, &unquoted_match))
    {
        // If quoted string was matched
        if (!quoted_match.empty())
        {
            auto handle = findQueryableObj(quoted_match);
            if (!handle.is_none())
                py_table_cache.emplace(quoted_match, handle);
        }
        // If unquoted identifier was matched
        else if (!unquoted_match.empty())
        {
            auto handle = findQueryableObj(unquoted_match);
            if (!handle.is_none())
                py_table_cache.emplace(unquoted_match, handle);
        }
    }
}

py::handle PythonTableCache::getQueryableObj(const String & table_name)
{
    auto iter = py_table_cache.find(table_name);

    if (iter != py_table_cache.end())
        return iter->second;

    return py::none();
}

void PythonTableCache::clear()
{
    try
	{
        py::gil_scoped_acquire acquire;
        py_table_cache.clear();
	}
	catch (...)
	{
	}
}

} // namespace CHDB
