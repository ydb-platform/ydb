#pragma once

namespace DB_CHDB
{
enum VisitorStatus
{
    Ok,
    Exhausted,
    Error,
    Ignore
};

}
