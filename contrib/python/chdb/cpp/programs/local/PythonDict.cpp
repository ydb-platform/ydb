#include "PythonDict.h"
#include "FormatHelper.h"
#include "StoragePython.h"

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

ColumnsDescription PythonDict::getActualTableStructure(const py::object & object, ContextPtr & context)
{
    std::vector<std::pair<std::string, std::string>> schema;

    for (auto item : object.cast<py::dict>())
    {
        auto key = py::str(item.first).cast<std::string>();
        if (!py::isinstance<py::list>(item.second))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Dictionary value must be a list type");
        }

        auto values = py::cast<py::list>(item.second);

        if (!values.empty())
        {
            py::handle element = values[0];

            if (isJSONSupported() && py::isinstance<py::dict>(element))
            {
                schema.emplace_back(key, "json");
            }
            else
            {
                auto dtype = py::str(element.attr("__class__").attr("__name__")).cast<std::string>();
                schema.emplace_back(key, dtype);
            }
        }
    }

    return StoragePython::getTableStructureFromData(schema);
}

bool PythonDict::isPythonDict(const py::object & object)
{
    return py::isinstance<py::dict>(object);
}

} // namespace CHDB
