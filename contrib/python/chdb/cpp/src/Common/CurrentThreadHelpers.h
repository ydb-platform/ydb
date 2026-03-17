#pragma once

#include <Core/LogsLevel.h>

namespace DB_CHDB
{
bool currentThreadHasGroup();
LogsLevel currentThreadLogsLevel();
}
