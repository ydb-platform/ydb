#pragma once

#include <util/memory/pool.h>
#include <memory>

namespace NYql {

class ILimitingAllocator: public IAllocator {
public:
    virtual size_t GetAllocatedSize() const = 0;
    virtual size_t GetLimitSize() const = 0;
};

std::unique_ptr<ILimitingAllocator> MakeLimitingAllocator(size_t limit, IAllocator* underlying);
} // namespace NYql
