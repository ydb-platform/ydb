#include "objects_holder.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void* TObjectsHolder::Register(void* pointer, void(*deleter)(void*))
{
    try {
        auto holder = std::unique_ptr<void, void(*)(void*)>(pointer, deleter);
        Objects_.push_back(std::move(holder));
        return pointer;
    } catch (...) {
        return nullptr;
    }
}

void TObjectsHolder::Clear()
{
    Objects_.clear();
}

void TObjectsHolder::Merge(TObjectsHolder&& other)
{
    std::move(other.Objects_.begin(), other.Objects_.end(), std::back_inserter(Objects_));
    other.Objects_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
