#pragma once

#include "PybindWrapper.h"

#include <unordered_map>
#include <base/types.h>

namespace CHDB {

class PythonTableCache {
public:
    static void findQueryableObjFromQuery(const String & query_str);

    static py::handle getQueryableObj(const String & table_name);

    static void clear();

private:
    static std::unordered_map<String, py::handle> py_table_cache;
};

} // namespace CHDB
