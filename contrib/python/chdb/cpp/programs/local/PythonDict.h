#pragma once

#include "PybindWrapper.h"

#include <Storages/ColumnsDescription.h>

namespace CHDB {

class PythonDict {
public:
    static DB_CHDB::ColumnsDescription getActualTableStructure(const py::object & object, DB_CHDB::ContextPtr & context);

    static bool isPythonDict(const py::object & object);
};

} // namespace CHDB
