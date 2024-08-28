#ifndef GLOBAL_VARIABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include variable.h"
// For the sake of sane code completion.
#include "variable.h"
#endif

namespace NYT::NGlobal {

////////////////////////////////////////////////////////////////////////////////

template <CTriviallyErasable<GlobalVariableMaxByteSize> T>
TVariable<T>::TVariable(
    const TVariableTag& tag,
    TAccessor accessor,
    T initValue) noexcept
    : Value_(initValue)
{
    NDetail::RegisterVariable(tag, accessor);
}

template <CTriviallyErasable<GlobalVariableMaxByteSize> T>
T TVariable<T>::Get() const noexcept
{
    return Value_;
}

template <CTriviallyErasable<GlobalVariableMaxByteSize> T>
void TVariable<T>::Set(T value) noexcept
{
    Value_ = value;
}

////////////////////////////////////////////////////////////////////////////////

#undef YT_DEFINE_TRACKED_GLOBAL
#undef YT_DEFINE_TRACKED_THREAD_LOCAL

////////////////////////////////////////////////////////////////////////////////

#define YT_DEFINE_TRACKED_GLOBAL(Type, Name, Tag, InitExpr) \
    namespace NGlobalTracking##Name##Tag { \
    \
    ::NYT::NGlobal::TErasedStorage GetErased##Name() noexcept; \
    \
    static ::NYT::NGlobal::TVariable<Type> Name{Tag, GetErased##Name, (InitExpr)}; \
    \
    ::NYT::NGlobal::TErasedStorage GetErased##Name() noexcept \
    { \
        return ::NYT::NGlobal::TErasedStorage{Name.Get()}; \
    } \
    \
    } /*namespace NGlobalTracking##Name##Tag*/ \
    using NGlobalTracking##Name##Tag::Name; \
    static_assert(true)

// NB(arkady-e1ppa): We must ensure that tracker is constructed thus
// we have to call ref tracker inside tls accessor.
// NB(arkady-e1ppa): Unlike normal static variable, we cannot just pull
// varibale name out as we might want to forward-declare thread local variable
// now that it is modelled as function. Pulling alias from ns unfortunately
// doesn't work as function definition :(.
#define YT_DEFINE_TRACKED_THREAD_LOCAL(Type, Name, Tag, ...) \
    Y_NO_INLINE Type& Name(); \
    namespace NGlobalTracking##Name##Tag { \
    \
    void EnsureTracked() noexcept; \
    ::NYT::NGlobal::TErasedStorage GetErased##Name() noexcept; \
    \
    static ::NYT::NGlobal::TVariable<std::byte> TlsTrackerFor##Name{Tag, GetErased##Name}; \
    \
    void EnsureTracked() noexcept \
    { \
        auto val = TlsTrackerFor##Name.Get(); \
        Y_UNUSED(val); \
    } \
    \
    ::NYT::NGlobal::TErasedStorage GetErased##Name() noexcept \
    { \
        return ::NYT::NGlobal::TErasedStorage{Name()}; \
    } \
    \
    } /*namespace NGlobalTracking##Name##Tag*/ \
    Y_NO_INLINE Type& Name() \
    { \
        thread_local Type tlsData { __VA_ARGS__ }; \
        asm volatile(""); \
        NGlobalTracking##Name##Tag::EnsureTracked(); \
        return tlsData; \
    } \
    \
    static_assert(true)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGlobal
