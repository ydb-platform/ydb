#include <Common/CurrentThread.h>
#include <Common/CurrentThreadHelpers.h>

namespace DB_CHDB
{

bool currentThreadHasGroup()
{
    return DB_CHDB::CurrentThread::getGroup() != nullptr;
}

LogsLevel currentThreadLogsLevel()
{
    return DB_CHDB::CurrentThread::get().getClientLogsLevel();
}
}
