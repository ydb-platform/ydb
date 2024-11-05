#pragma once

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
//
// This file defines a set of argument wrappers that can be used specify
// the reference counting and reference semantics of arguments that are bound
// by the #Bind() function in "bind.h".
//
////////////////////////////////////////////////////////////////////////////////
//
// ARGUMENT BINDING WRAPPERS
//
// The wrapper functions are #Unretained(), #Owned(), #Passed() and #ConstRef().
//
// #Unretained() allows #Bind() to bind a non-reference counted class,
// and to disable reference counting on arguments that are reference counted
// objects.
//
// #Owned() transfers ownership of an object to the #TCallback<> resulting from
// binding; the object will be deleted when the #TCallback<> is deleted.
//
// #Passed() is for transferring movable-but-not-copyable types through
// a #TCallback<>. Logically, this signifies a destructive transfer of the state
// of the argument into the target function. Invoking #TCallback<>::operator() twice
// on a TCallback that was created with a #Passed() argument will YT_ASSERT()
// because the first invocation would have already transferred ownership
// to the target function.
//
// #ConstRef() allows binding a const reference to an argument rather than
// a copy.
//
//
// EXAMPLE OF Unretained()
//
//   class TFoo {
//     public:
//       void Bar() { Cout << "Hello!" << Endl; }
//   };
//
//   // Somewhere else.
//   TFoo foo;
//   TClosure cb = Bind(&TFoo::Bar, Unretained(&foo));
//   cb(); // Prints "Hello!".
//
// Without the #Unretained() wrapper on |&foo|, the above call would fail
// to compile because |TFoo| does not support the Ref() and Unref() methods.
//
//
// EXAMPLE OF Owned()
//
//   void Foo(int* arg) { Cout << *arg << Endl; }
//
//   int* px = new int(17);
//   TClosure cb = Bind(&Foo, Owned(px));
//
//   cb(); // Prints "17"
//   cb(); // Prints "17"
//   *px = 42;
//   cb(); // Prints "42"
//
//   cb.Reset(); // |px| is deleted.
//   // Also will happen when |cb| goes out of scope.
//
// Without #Owned(), someone would have to know to delete |px| when the last
// reference to the #TCallback<> is deleted.
//
//
// EXAMPLE OF Passed()
//
//   void TakesOwnership(TIntrusivePtr<TFoo> arg) { ... }
//   TIntrusivePtr<TFoo> CreateFoo() { return New<TFoo>(); }
//   TIntrusivePtr<TFoo> foo = New<TFoo>();
//
//   // |cb| is given ownership of the |TFoo| instance. |foo| is now NULL.
//   // You may also use std::move(foo), but its more verbose.
//   TClosure cb = Bind(&TakesOwnership, Passed(&foo));
//
//   // Operator() was never called so |cb| still owns the instance and deletes
//   // it on #Reset().
//   cb.Reset();
//
//   // |cb| is given a new |TFoo| created by |CreateFoo()|.
//   TClosure cb = Bind(&TakesOwnership, Passed(CreateFoo()));
//
//   // |arg| in TakesOwnership() is given ownership of |TFoo|.
//   // |cb| no longer owns |TFoo| and, if reset, would not delete anything.
//   cb(); // |TFoo| is now transferred to |arg| and deleted.
//   cb(); // This YT_ASSERT()s since |TFoo| already been used once.
//
//
// EXAMPLE OF ConstRef()
//
//   void Foo(int arg) { Cout << arg << Endl; }
//
//   int n = 1;
//   TClosure noRef = Bind(&Foo, n);
//   TClosure hasRef = Bind(&Foo, ConstRef(n));
//
//   noRef();  // Prints "1"
//   hasRef(); // Prints "1"
//   n = 2;
//   noRef();  // Prints "1"
//   hasRef(); // Prints "2"
//
// Note that because #ConstRef() takes a reference on |n|,
// |n| must outlive all its bound callbacks.
//
////////////////////////////////////////////////////////////////////////////////

template <class T>
static auto Unretained(T* x);

template <class T>
static auto Owned(T* x);

template <class T>
static auto Passed(T&& x);

template <class T>
static auto ConstRef(const T& x);

template <class T>
static auto IgnoreResult(const T& x);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFuture;

// NB: Needed here due to TExtendedCallback::Via and TExtendedCallback::AsyncVia.
template <class T>
struct TFutureTraits
{
    using TUnderlying = T;
    using TWrapped = TFuture<T>;
};

template <class T>
struct TFutureTraits<TFuture<T>>
{
    using TUnderlying = T;
    using TWrapped = TFuture<T>;
};


template <class TSignature>
class TExtendedCallback;

template <class TR, class... TAs>
class TExtendedCallback<TR(TAs...)>
    : public TCallback<TR(TAs...)>
{
public:
    using TCallback<TR(TAs...)>::TCallback;

    TExtendedCallback(const TCallback<TR(TAs...)>& callback)
        : TCallback<TR(TAs...)>(callback)
    { }
#ifndef __cpp_impl_three_way_comparison
    using TCallback<TR(TAs...)>::operator ==;
    using TCallback<TR(TAs...)>::operator !=;
#endif

    TExtendedCallback Via(TIntrusivePtr<IInvoker> invoker) const &;
    TExtendedCallback Via(TIntrusivePtr<IInvoker> invoker) &&;

    TExtendedCallback<typename TFutureTraits<TR>::TWrapped(TAs...)>
    AsyncVia(TIntrusivePtr<IInvoker> invoker) const &;

    TExtendedCallback<typename TFutureTraits<TR>::TWrapped(TAs...)>
    AsyncVia(TIntrusivePtr<IInvoker> invoker) &&;

    //! This version of AsyncVia is designed for cases when invoker may discard submitted callbacks
    //! (for example, if it is cancellable invoker). Regular AsyncVia results in "Promise abandoned"
    //! error, which is almost non-informative and quite frustrating, while this overload
    //! allows you to specify the cancellation error, which costs a bit more allocations
    //! but much more convenient.
    TExtendedCallback<typename TFutureTraits<TR>::TWrapped(TAs...)>
    AsyncViaGuarded(TIntrusivePtr<IInvoker> invoker, TError cancellationError) const &;

    TExtendedCallback<typename TFutureTraits<TR>::TWrapped(TAs...)>
    AsyncViaGuarded(TIntrusivePtr<IInvoker> invoker, TError cancellationError) &&;

private:
    static TExtendedCallback ViaImpl(TExtendedCallback callback, TIntrusivePtr<IInvoker> invoker);
};

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
    TBs&&... bound);

////////////////////////////////////////////////////////////////////////////////

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
    const TCallback<T>& callback);

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    #define BIND_IMPL(propagate, ...) ::NYT::Bind<propagate, ::NYT::TCurrentTranslationUnitTag, __COUNTER__>(YT_CURRENT_SOURCE_LOCATION, __VA_ARGS__)
#else
    #define BIND_IMPL(propagate, ...) ::NYT::Bind<propagate>(__VA_ARGS__)
#endif

#define BIND(...) BIND_IMPL(true, __VA_ARGS__)
#define BIND_NO_PROPAGATE(...) BIND_IMPL(false, __VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define BIND_INL_H_
#include "bind-inl.h"
#undef BIND_INL_H_
