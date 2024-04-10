#ifndef BIND_INL_H_
#error "Direct inclusion of this file is not allowed, include bind.h"
// For the sake of sane code completion.
#include "bind.h"
#endif
#undef BIND_INL_H_

#include <yt/yt/core/concurrency/propagating_storage.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
class TUnretainedWrapper
{
public:
    explicit TUnretainedWrapper(T* x)
        : T_(x)
    { }

    T* Unwrap() const
    {
        return T_;
    }

private:
    T* T_;
};

template <class T>
class TOwnedWrapper
{
public:
    explicit TOwnedWrapper(T* x)
        : T_(x)
    { }

    TOwnedWrapper(const TOwnedWrapper& other)
        : T_(other.T_)
    {
        other.T_ = nullptr;
    }

    ~TOwnedWrapper()
    {
        delete T_;
    }

    T* Unwrap() const
    {
        return T_;
    }

private:
    mutable T* T_;
};

template <class T>
class TPassedWrapper
{
public:
    explicit TPassedWrapper(T&& x)
        : IsValid_(true)
        , T_(std::move(x))
    { }

    TPassedWrapper(const TPassedWrapper& other)
        : IsValid_(other.IsValid_)
        , T_(std::move(other.T_))
    {
        other.IsValid_ = false;
    }

    TPassedWrapper(TPassedWrapper&& other)
        : IsValid_(other.IsValid_)
        , T_(std::move(other.T_))
    {
        other.IsValid_ = false;
    }

    T&& Unwrap() const
    {
        YT_ASSERT(IsValid_);
        IsValid_ = false;
        return std::move(T_);
    }

private:
    mutable bool IsValid_;
    mutable T T_;
};

template <class T>
class TConstRefWrapper
{
public:
    explicit TConstRefWrapper(const T& x)
        : T_(&x)
    { }

    const T& Unwrap() const
    {
        return *T_;
    }

private:
    const T* T_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
const T& Unwrap(T& value)
{
    return value;
}

template <class T>
T* Unwrap(const TUnretainedWrapper<T>& wrapper)
{
    return wrapper.Unwrap();
}

template <class T>
T* Unwrap(const TOwnedWrapper<T>& wrapper)
{
    return wrapper.Unwrap();
}

template <class T>
T&& Unwrap(const TPassedWrapper<T>& wrapper)
{
    return wrapper.Unwrap();
}

template <class T>
const T& Unwrap(const TConstRefWrapper<T>& wrapper)
{
    return wrapper.Unwrap();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TIgnoreResultWrapper
{
public:
    explicit TIgnoreResultWrapper(const T& functor)
        : Functor_(functor)
    { }

    T& Unwrap()
    {
        return Functor_;
    }

private:
    T Functor_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TThrowOnDestroyedWrapper
{
public:
    explicit TThrowOnDestroyedWrapper(const T& functor)
        : Functor_(functor)
    { }

    T& Unwrap()
    {
        return Functor_;
    }

private:
    T Functor_;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
auto IgnoreResult(const T& x)
{
    return NYT::NDetail::TIgnoreResultWrapper<T>(x);
}

template <class T>
auto ThrowOnDestroyed(const T& x)
{
    return NYT::NDetail::TThrowOnDestroyedWrapper<T>(x);
}

template <class T>
auto Unretained(T* x)
{
    return NYT::NDetail::TUnretainedWrapper<T>(x);
}

template <class T>
auto Owned(T* x)
{
    return NYT::NDetail::TOwnedWrapper<T>(x);
}

template <class T>
auto Passed(T&& x)
{
    return NYT::NDetail::TPassedWrapper<T>(std::forward<T>(x));
}

template <class T>
auto ConstRef(const T& x)
{
    return NYT::NDetail::TConstRefWrapper<T>(x);
}

template <class U>
U& WrapToPassed(U& arg)
{
    return arg;
}

template <class U>
auto WrapToPassed(U&& arg)
{
    return Passed(std::move(arg));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class F>
struct TFunctorTraits;

template <class TMethod>
class TMethodInvoker
{
public:
    explicit TMethodInvoker(TMethod method)
        : Method_(method)
    { }

    template <class D, class... XAs>
    auto operator()(D* this_, XAs&&... args) const
    {
        static_assert(
            !std::is_array_v<D>,
            "First bound argument to a method cannot be an array");

        return (this_->*Method_)(std::forward<XAs>(args)...);
    }

    template <class D, class... XAs>
    void operator()(const TWeakPtr<D>& weakThis, XAs&&... args) const
    {
        using TResult = typename TFunctorTraits<TMethod>::TResult;
        static_assert(
            std::is_void_v<TResult>,
            "Weak calls are only supported for methods with a void return type");

        auto strongThis = weakThis.Lock();
        if (!strongThis) {
            return;
        }

        (strongThis.Get()->*Method_)(std::forward<XAs>(args)...);
    }

    template <class D, class... XAs>
    auto operator()(const TIntrusivePtr<D>& this_, XAs&&... args) const
    {
        return (this_.Get()->*Method_)(std::forward<XAs>(args)...);
    }

private:
    const TMethod Method_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TToVoidSignature;

template <class TR, class... TAs>
struct TToVoidSignature<TR(TAs...)>
{
    using TType = void (TAs...);
};

////////////////////////////////////////////////////////////////////////////////

template <class>
struct TCallableSignature;

template <class TR, class TC, class... TAs>
struct TCallableSignature<TR (TC::*)(TAs...) const>
{
    using TSignature = TR (TAs...);
};

template <class TR, class TC, class... TAs>
struct TCallableSignature<TR (TC::*)(TAs...)>
{
    using TSignature = TR (TAs...);
};

////////////////////////////////////////////////////////////////////////////////

// Matches functor and TCallback
template <class F>
struct TFunctorTraits
    : public TCallableSignature<decltype(&F::operator())>
{
    using TInvoker = F;
};

template <class TR, class... TAs>
struct TFunctorTraits<TR (TAs...)>
{
    using TInvoker = TR (*)(TAs...);
    using TSignature = TR (TAs...);
};

// Matches function.
template <class TR, class... TAs>
struct TFunctorTraits<TR (*)(TAs...)>
{
    using TInvoker = TR (*)(TAs...);
    using TSignature = TR (TAs...);
};

// Matches method.
template <class TR, class C, class... TAs>
struct TFunctorTraits<TR (C::*)(TAs...)>
{
    using TInvoker = TMethodInvoker<TR (C::*)(TAs...)>;
    using TSignature = TR (C*, TAs...);
    using TResult = TR;
};

template <class TR, class C, class... TAs>
struct TFunctorTraits<TR (C::*)(TAs...) const>
{
    using TInvoker = TMethodInvoker<TR (C::*)(TAs...) const>;
    using TSignature = TR (const C*, TAs...);
    using TResult = TR;
};

template <class TR, class C, class... TAs>
struct TFunctorTraits<TR (C::*)(TAs...) noexcept>
{
    using TInvoker = TMethodInvoker<TR (C::*)(TAs...) noexcept>;
    using TSignature = TR (C*, TAs...);
    using TResult = TR;
};

template <class TR, class C, class... TAs>
struct TFunctorTraits<TR (C::*)(TAs...) const noexcept>
{
    using TInvoker = TMethodInvoker<TR (C::*)(TAs...) const noexcept>;
    using TSignature = TR (const C*, TAs...);
    using TResult = TR;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TIgnoreResultInvoker
{
public:
    using TInvoker = typename TFunctorTraits<T>::TInvoker;

    explicit TIgnoreResultInvoker(NDetail::TIgnoreResultWrapper<T>&& wrapper)
        : Invoker_(std::move(wrapper.Unwrap()))
    { }

    template <class... XAs>
    void operator()(XAs&&... args) const
    {
        Invoker_(std::forward<XAs>(args)...);
    }

private:
    TInvoker Invoker_;
};

template <class T>
struct TFunctorTraits<NDetail::TIgnoreResultWrapper<T>>
{
    using TInvoker = TIgnoreResultInvoker<T>;
    using TSignature = typename TToVoidSignature<typename TFunctorTraits<T>::TSignature>::TType;
};

////////////////////////////////////////////////////////////////////////////////

template <class TMethod>
class TThrowOnDestroyedInvoker
{
public:
    explicit TThrowOnDestroyedInvoker(NDetail::TThrowOnDestroyedWrapper<TMethod>&& wrapper)
        : Method_(wrapper.Unwrap())
    { }

    template <class D, class... XAs>
    auto operator()(const TWeakPtr<D>& weakThis, XAs&&... args) const
    {
        auto strongThis = weakThis.Lock();
        if (!strongThis) {
            THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled, "Object destroyed");
        }

        return (strongThis.Get()->*Method_)(std::forward<XAs>(args)...);
    }

private:
    const TMethod Method_;
};

template <class TR, class C, class... TAs>
struct TFunctorTraits<NDetail::TThrowOnDestroyedWrapper<TR (C::*)(TAs...)>>
{
    using TInvoker = TThrowOnDestroyedInvoker<TR (C::*)(TAs...)>;
    using TSignature = TR (C*, TAs...);
    using TResult = TR;
};

template <class TR, class C, class... TAs>
struct TFunctorTraits<NDetail::TThrowOnDestroyedWrapper<TR (C::*)(TAs...) const>>
{
    using TInvoker = TThrowOnDestroyedInvoker<TR (C::*)(TAs...) const>;
    using TSignature = TR (C*, TAs...);
    using TResult = TR;
};

template <class TR, class C, class... TAs>
struct TFunctorTraits<NDetail::TThrowOnDestroyedWrapper<TR (C::*)(TAs...) noexcept>>
{
    using TInvoker = TThrowOnDestroyedInvoker<TR (C::*)(TAs...) noexcept>;
    using TSignature = TR (C*, TAs...);
    using TResult = TR;
};

template <class TR, class C, class... TAs>
struct TFunctorTraits<NDetail::TThrowOnDestroyedWrapper<TR (C::*)(TAs...) const noexcept>>
{
    using TInvoker = TThrowOnDestroyedInvoker<TR (C::*)(TAs...) const noexcept>;
    using TSignature = TR (C*, TAs...);
    using TResult = TR;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TIsNonConstReference
    : public std::false_type
{ };

template <class T>
struct TIsNonConstReference<T&>
    : public std::true_type
{ };

template <class T>
struct TIsNonConstReference<const T&>
    : public std::false_type
{ };

template <class T>
struct TCheckNoRawPtrToRefCountedType
{
    static_assert(
        !(std::is_pointer_v<T> && (
            std::is_convertible_v<T, const TRefCounted*> ||
            std::is_convertible_v<T, TRefCounted*>
        )),
        "T has reference-counted type and should not be bound by the raw pointer");
};

template <class... TArgs>
struct TCheckParamsNoRawPtrToRefCountedType
    : public std::tuple<TCheckNoRawPtrToRefCountedType<TArgs>...>
{ };

////////////////////////////////////////////////////////////////////////////////

template <unsigned N, class TSignature>
struct TSplitHelper;

template <unsigned N, class TSignature>
struct TSplit
    : public TSplitHelper<N, TSignature>
{ };

template <class TSignature>
struct TSplit<0, TSignature>
{
    using TResult = TSignature;
};

template <unsigned N, class TR, class TA0, class... TAs>
struct TSplitHelper<N, TR (TA0, TAs...)>
    : public TSplit<N - 1, TR (TAs...)>
{
    static_assert(
        !TIsNonConstReference<TA0>::value,
        "T is a non-const reference and should not be bound.");
};

////////////////////////////////////////////////////////////////////////////////

template <bool Propagate>
class TPropagateMixin;

template <>
class TPropagateMixin<true>
{
public:
    TPropagateMixin()
        : Storage_(NConcurrency::GetCurrentPropagatingStorage())
    { }

    NConcurrency::TPropagatingStorageGuard MakePropagatingStorageGuard()
    {
        return NConcurrency::TPropagatingStorageGuard(Storage_);
    }

private:
    const NConcurrency::TPropagatingStorage Storage_;
};

template <>
class TPropagateMixin<false>
{
public:
    std::monostate MakePropagatingStorageGuard()
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool Propagate, class TFunctor, class TSequence, class... TBs>
class TBindState;

template <bool Propagate, class TFunctor, class... TBs, size_t... BoundIndexes>
class TBindState<Propagate, TFunctor, std::index_sequence<BoundIndexes...>, TBs...>
    : public NDetail::TBindStateBase
    , public TPropagateMixin<Propagate>
{
public:
    template <class XFunctor, class... XBs>
    TBindState(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        const NYT::TSourceLocation& location,
#endif
        XFunctor&& functor,
        XBs&&... boundArgs)
        : TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            location
#endif
        )
        , Functor_(std::forward<XFunctor>(functor))
        , BoundArgs_(std::forward<XBs>(boundArgs)...)
    { }

    // Keep minimum frame count.
    template <class... TAs>
    static auto Run(TCallArg<TAs>... args, NDetail::TBindStateBase* base)
    {
        auto* volatile state = static_cast<TBindState*>(base);

        // Prevent optimizing |state| away for GDB printer.
        // See devtools/gdb/yt_fibers_printer.py.
        auto* volatile unoptimizedState = state;
        Y_UNUSED(unoptimizedState);

        auto propagatingStorageGuard = state->MakePropagatingStorageGuard();
        Y_UNUSED(propagatingStorageGuard);

        return state->Functor_(
            NDetail::Unwrap(std::get<BoundIndexes>(state->BoundArgs_))...,
            std::forward<TAs>(args)...);
    }

private:
    TFunctor Functor_;
    const std::tuple<TBs...> BoundArgs_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
struct TBindHelper;

template <class TR, class... TAs>
struct TBindHelper<TR(TAs...)>
{
    template <class TState>
    static constexpr auto GetInvokeFunction()
    {
        return &TState::template Run<TAs...>;
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <
    bool Propagate,
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    class TTag,
    int Counter,
#endif
    class TFunctor,
    class... TBs>
auto Bind(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    TFunctor&& functor,
    TBs&&... bound)
{
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    Y_UNUSED(location);
#endif

    using TTraits = NDetail::TFunctorTraits<typename std::decay_t<TFunctor>>;
    using TRunSignature = typename NDetail::TSplit<sizeof...(TBs), typename TTraits::TSignature>::TResult;

    NYT::NDetail::TCheckParamsNoRawPtrToRefCountedType<typename std::decay_t<TBs>...> checkParamsIsRawPtrToRefCountedType;
    Y_UNUSED(checkParamsIsRawPtrToRefCountedType);

    using TState = NYT::NDetail::TBindState<
        Propagate,
        typename TTraits::TInvoker,
        typename std::make_index_sequence<sizeof...(TBs)>,
        typename std::decay_t<TBs>...>;

    using THelper = NYT::NDetail::TBindHelper<TRunSignature>;

    return TExtendedCallback<TRunSignature>{
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        NewWithLocation<TState, TTag, Counter>(location, location, std::forward<TFunctor>(functor), std::forward<TBs>(bound)...),
#else
        New<TState>(std::forward<TFunctor>(functor), std::forward<TBs>(bound)...),
#endif
        THelper::template GetInvokeFunction<TState>()};
}

template <
    bool Propagate,
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    class TTag,
    int Counter,
#endif
    class T
>
auto Bind(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    const TCallback<T>& callback)
{
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    Y_UNUSED(location);
#endif
    return TExtendedCallback<T>(callback);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
