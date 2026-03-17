#include "PythonReader.h"
#include "StoragePython.h"

namespace DB_CHDB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

using namespace DB_CHDB;

namespace CHDB {

ColumnsDescription PythonReader::getActualTableStructure(const py::object & object, ContextPtr & context)
{
    std::vector<std::pair<std::string, std::string>> schema;

    schema = object.attr("get_schema")().cast<std::vector<std::pair<std::string, std::string>>>();

    return StoragePython::getTableStructureFromData(schema);
}

bool PythonReader::isPythonReader(const py::object & object)
{
    return isInheritsFromPyReader(object);
}

} // namespace CHDB
