#pragma once

#include "PybindWrapper.h"

#include <Storages/ColumnsDescription.h>

namespace CHDB {

class PythonReader {
public:
    static DB_CHDB::ColumnsDescription getActualTableStructure(const py::object & object, DB_CHDB::ContextPtr & context);

    static bool isPythonReader(const py::object & object);
};

} // namespace CHDB
