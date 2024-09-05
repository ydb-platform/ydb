#pragma once

#include <util/memory/pool.h>
#include <memory>

namespace NYql {
std::unique_ptr<IAllocator> MakeLimitingAllocator(size_t limit, IAllocator* underlying);
}
