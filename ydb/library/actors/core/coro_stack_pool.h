#pragma once

#include <util/generic/noncopyable.h>
#include <util/system/types.h>

#include <cstddef>
#include <memory>

namespace NActors {

class IStackMem {
public:
    virtual ~IStackMem() = default;

    virtual char* Begin() noexcept = 0;
    virtual char* End() noexcept = 0;
};


class TStackMemPool : public NNonCopyable::TNonCopyable {
public:
    struct TPageBucket {
        static TPageBucket Bytes(ui32 sz);
        size_t Size() const noexcept;
        ui32 Index;
    };

    static TStackMemPool* GetMemPool(TPageBucket pageBucket) noexcept;
    std::unique_ptr<IStackMem> Allocate();
private:
    explicit TStackMemPool(TPageBucket pageBucket) noexcept;

private:
    const TPageBucket PageBucket;
};


}
