#pragma once

#include "access.h"

namespace NYT::NGlobal {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Defined in impl.cpp.
void RegisterVariable(const TVariableTag& tag, TAccessor accessor);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <CTriviallyErasable<GlobalVariableMaxByteSize> T>
class TVariable
{
public:
    TVariable(
        const TVariableTag& tag,
        TAccessor accessor,
        T initValue = {}) noexcept;

    TVariable(const TVariable& other) = delete;
    TVariable& operator=(const TVariable& other) = delete;

    T Get() const noexcept;
    void Set(T value) noexcept;

private:
    // NB(arkady-e1ppa): Ban TVariable<TVariable<T>>.
    static_assert(!requires (T t) {
        [] <class U> (TVariable<U>&) { } (t);
    });

    T Value_;
};

////////////////////////////////////////////////////////////////////////////////

#define YT_DEFINE_TRACKED_GLOBAL(Type, Name, Tag, InitExpr)
#define YT_DEFINE_TRACKED_THREAD_LOCAL(Type, Name, Tag, ...)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGlobal

#define GLOBAL_VARIABLE_INL_H_
#include "variable-inl.h"
#undef GLOBAL_VARIABLE_INL_H_
