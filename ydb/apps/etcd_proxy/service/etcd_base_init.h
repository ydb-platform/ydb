#pragma once

#include <string>

namespace NEtcd {

std::string GetCreateTablesSQL(const std::string& prefix);

std::string GetInitializeTablesSQL(const std::string& prefix);

}

