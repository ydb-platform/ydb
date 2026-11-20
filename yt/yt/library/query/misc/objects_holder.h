#pragma once

#include <memory>
#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TObjectsHolder
{
public:
    void* Register(void* pointer, void(*deleter)(void*));

    template <class T>
    T* Register(T* pointer)
    {
        auto deleter = [] (void* ptr) {
            static_assert(sizeof(T) > 0, "Cannot delete incomplete type.");
            delete static_cast<T*>(ptr);
        };

        return static_cast<T*>(Register(pointer, deleter));
    }

    template <class T, class... TArgs>
    T* New(TArgs&&... args)
    {
        return TObjectsHolder::Register(new T(std::forward<TArgs>(args)...));
    }

    void Clear();

    void Merge(TObjectsHolder&& other);

    TObjectsHolder() = default;
    TObjectsHolder(TObjectsHolder&&) = default;
    TObjectsHolder(const TObjectsHolder&) = delete;

private:
    std::vector<std::unique_ptr<void, void(*)(void*)>> Objects_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
