#pragma once

#include <memory>

namespace DB_CHDB
{

class Throttler;
using ThrottlerPtr = std::shared_ptr<Throttler>;

}
