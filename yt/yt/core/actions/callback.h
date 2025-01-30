#pragma once

// NOTE: Header files that do not require the full definition of #TCallback<> or
// #TClosure should include "public.h" instead of this file.

////////////////////////////////////////////////////////////////////////////////
//
// WHAT IS THIS
//
// The templated #TCallback<> class is a generalized function object.
// Together with the #Bind() function in "bind.h" they provide a type-safe
// method for performing currying of arguments and creating a "closure".
//
// In programming languages, a closure is a first-class function where all its
// parameters have been bound (usually via currying). Closures are well-suited
// for representing and passing around a unit of delayed execution.
//
//
// MEMORY MANAGEMENT AND PASSING
//
// The #TCallback<> objects themselves should be passed by const reference, and
// stored by copy. They internally store their state in a reference-counted
// class and thus do not need to be deleted.
//
// The reason to pass via a const reference is to avoid unnecessary
// Ref/Unref pairs to the internal state.
//
// However, the #TCallback<> have Ref/Unref-efficient move constructors and
// assignment operators so they also may be efficiently moved.
//
//
// EXAMPLE USAGE
//
// (see "bind_ut.cpp")
//
//
// HOW THE IMPLEMENTATION WORKS:
//
// There are three main components to the system:
//   1) The #TCallback<> classes.
//   2) The #Bind() functions.
//   3) The arguments wrappers (e.g., #Unretained() and #ConstRef()).
//
// The #TCallback<> classes represent a generic function pointer. Internally,
// it stores a reference-counted piece of state that represents the target
// function and all its bound parameters. Each #TCallback<> specialization has
// a templated constructor that takes an #TBindState<>*. In the context of
// the constructor, the static type of this #TBindState<> pointer uniquely
// identifies the function it is representing, all its bound parameters,
// and operator() that is capable of invoking the target.
//
// #TCallback<>'s constructor takes the #TBindState<>* that has the full static
// type and erases the target function type as well as the types of the bound
// parameters. It does this by storing a pointer to the specific operator()
// and upcasting the state of #TBindState<>* to a #TBindStateBase*.
// This is safe as long as this #TBindStateBase pointer is only used with
// the stored operator() pointer.
//
// To #TBindState<> objects are created inside the #Bind() functions.
// These functions, along with a set of internal templates, are responsible for:
//
//   - Unwrapping the function signature into return type, and parameters,
//   - Determining the number of parameters that are bound,
//   - Creating the #TBindState<> storing the bound parameters,
//   - Performing compile-time asserts to avoid error-prone behavior,
//   - Returning a #TCallback<> with an arity matching the number of unbound
//     parameters and that knows the correct reference counting semantics for
//     the target object if we are binding a method.
//
// The #Bind() functions do the above using type-inference, and template
// specializations.
//
// By default #Bind() will store copies of all bound parameters, and attempt
// to reference count a target object if the function being bound is
// a class method.
//
// To change this behavior, we introduce a set of argument wrappers
// (e.g., #Unretained(), and #ConstRef()). These are simple container templates
// that are passed by value, and wrap a pointer to an argument.
// See the file-level comment in "bind.h" for more information.
//
// These types are passed to #Unwrap() functions, and #TMaybeRefCountHelper()
// functions respectively to modify the behavior of #Bind(). #Unwrap()
// and #TMaybeRefCountHelper() functions change behavior by doing partial
// specialization based on whether or not a parameter is a wrapper type.
//
// #ConstRef() is similar to #tr1::cref().
// #Unretained() is specific.
//
////////////////////////////////////////////////////////////////////////////////

#include "public.h"
#include "callback_internal.h"

#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
#include <library/cpp/yt/misc/source_location.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// We pass trivially copyable arguments by value. This helps to avoid
// putting them on the stack sometimes.
//
// Kudos to folly::Function authors.
template <typename T>
using TCallArg = std::conditional_t<std::is_trivially_copyable_v<T>, T, T&&>;

////////////////////////////////////////////////////////////////////////////////

template <class S1, class S2>
struct TCallableBindState;

template <class S1, class R2, class... TArgs2>
struct TCallableBindState<S1, R2(TArgs2...)>
    : public NYT::NDetail::TBindStateBase
{
    TCallback<S1> Callback;

    explicit TCallableBindState(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        const TSourceLocation& location,
#endif
        TCallback<S1> callback)
        : NYT::NDetail::TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            location
#endif
        )
        , Callback(std::move(callback))
    { }

    static R2 Run(TCallArg<TArgs2>... args, NYT::NDetail::TBindStateBase* base)
    {
        auto* state = static_cast<TCallableBindState*>(base);
        return state->Callback(std::forward<TArgs2>(args)...);
    }
};

template <class S1, class... TArgs2>
struct TCallableBindState<S1, void(TArgs2...)>
    : public NYT::NDetail::TBindStateBase
{
    TCallback<S1> Callback;

    explicit TCallableBindState(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        const TSourceLocation& location,
#endif
        TCallback<S1> callback)
        : NYT::NDetail::TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            location
#endif
        )
        , Callback(std::move(callback))
    { }

    static void Run(TCallArg<TArgs2>... args, NYT::NDetail::TBindStateBase* base)
    {
        auto* state = static_cast<TCallableBindState*>(base);
        state->Callback(std::forward<TArgs2>(args)...);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class R, class... TArgs>
class TCallback<R(TArgs...)>
    : public NYT::NDetail::TCallbackBase
{
private:
    // We pass TBindStateBase as a last argument.
    // Thus, all the arguments for calling the user's function are already
    // at the right registers.
    //
    // Kudos to folly::Function authors.
    using TTypedInvokeFunction = R(*)(TCallArg<TArgs>..., NYT::NDetail::TBindStateBase*);

public:
    using TSignature = R(TArgs...);

    TCallback()
        : TCallbackBase(TIntrusivePtr<NYT::NDetail::TBindStateBase>())
    { }

    TCallback(const TCallback& other)
        : TCallbackBase(other)
    { }

    TCallback(TCallback&& other) noexcept
        : TCallbackBase(std::move(other))
    { }

    TCallback(TIntrusivePtr<NYT::NDetail::TBindStateBase>&& bindState, TTypedInvokeFunction invokeFunction)
        : TCallbackBase(std::move(bindState))
    {
        UntypedInvoke = reinterpret_cast<TUntypedInvokeFunction>(invokeFunction);
    }

    template <class R2, class... TArgs2>
    explicit operator TCallback<R2(TArgs2...)>() const
    {
        return CastImpl<R2, TArgs2...>();
    }

    template <class... TArgs2>
    operator TCallback<R(TArgs2...)>() const
    {
        return CastImpl<R, TArgs2...>();
    }

#ifndef __cpp_impl_three_way_comparison
    using TCallbackBase::operator ==;
    using TCallbackBase::operator !=;
#endif

    TCallback& operator=(const TCallback& other)
    {
        TCallback(other).Swap(*this);
        return *this;
    }

    TCallback& operator=(TCallback&& other)
    {
        TCallback(std::move(other)).Swap(*this);
        return *this;
    }

    R operator()(TArgs... args) const
    {
        auto invokeFunction = reinterpret_cast<TTypedInvokeFunction>(UntypedInvoke);
        return invokeFunction(std::forward<TArgs>(args)..., BindState.Get());
    }

    R Run(TArgs... args) const
    {
        return operator()(std::forward<TArgs>(args)...);
    }

private:
    template <class R2, class... TArgs2>
    TCallback<R2(TArgs2...)> CastImpl() const
    {
        using TBindState = TCallableBindState<R(TArgs...), R2(TArgs2...)>;

        return TCallback<R2(TArgs2...)>(
            New<TBindState>(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
                BindState->Location,
#endif
                *this),
            &TBindState::Run);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

