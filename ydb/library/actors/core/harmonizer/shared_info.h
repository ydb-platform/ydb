#pragma once

#include "defs.h"

namespace NActors {

class ISharedExecutorPool;

struct TSharedInfo {
    std::vector<bool> HasSharedThread;
    std::vector<bool> HasSharedThreadWhichWasNotBorrowed;
    std::vector<bool> HasBorrowedSharedThread;

    void Init(i16 poolCount);
    void Pull(const ISharedExecutorPool& shared);

    TString ToString() const;
}; // struct TSharedInfo

} // namespace NActors
