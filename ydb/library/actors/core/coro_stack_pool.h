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
    struct TPageNumber {
        static TPageNumber Bytes(ui32 sz);
        size_t Size() const noexcept;
        ui32 NumPages;
    };

    static TStackMemPool* GetMemPool(TPageNumber pn) noexcept;
    std::unique_ptr<IStackMem> Allocate();
private:
    explicit TStackMemPool(TPageNumber pageNumber) noexcept;

private:
    const TPageNumber PageNumber;
};


}
