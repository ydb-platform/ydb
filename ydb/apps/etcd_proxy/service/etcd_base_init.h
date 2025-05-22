#pragma once

#include <string>

namespace NEtcd {

std::string GetCreateTablesSQL(const std::string& prefix);

std::string GetLastRevisionSQL(const std::string& prefix);

}

