/*******************************************************************************
 * tlx/delegate.hpp
 *
 * Replacement for std::function with ideas and base code borrowed from
 * http://codereview.stackexchange.com/questions/14730/impossibly-fast-delegate
 * Massively rewritten, commented, simplified, and improved.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_DELEGATE_HEADER
#define TLX_DELEGATE_HEADER

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

namespace tlx {

template <typename T, typename Allocator = std::allocator<void> >
class Delegate;

/*!
 * This is a faster replacement than std::function. Besides being faster and
 * doing less allocations when used correctly, we use it in places where
 * move-only lambda captures are necessary. std::function is required by the
 * standard to be copy-constructible, and hence does not allow move-only
 * lambda captures.
 *
 * A Delegate contains a reference to any of the following callable objects:
 * - an immediate function (called via one indirection)
 * - a mutable function pointer (copied into the Delegate)
 * - an immediate class::method call (called via one indirection)
 * - a functor object (the whole object is copied into the Delegate)
 *
 * All callable objects must have the signature ReturnType(Arguments ...). If a
 * callable has this signature, it can be bound to the Delegate.
 *
 * To implement all this the Delegate contains one pointer to a "caller stub"
 * function, which depends on the contained object and can be an immediate
 * function call, a pointer to the object associated with the callable, and a
 * memory pointer (managed by shared_ptr) for holding larger callables that need
 * to be copied.
 *
 * A functor object can be a lambda function with its capture, an internally
 * wrapped mutable class::method class stored as pair<object, method_ptr>, or
 * any other old-school functor object.
 *
 * Delegates can be constructed similar to std::function.
\code
// in defining the Delegate we decide the ReturnType(Arguments ...) signature
using MyDelegate = Delegate<int(double)>;

// this is a plain function bound to the Delegate as a function pointer
int func(double a) { return a + 10; }
MyDelegate d1 = MyDelegate(func);

class AClass {
public:
    int method(double d) { return d * d; }
};

AClass a;

// this is class::method bound to the Delegate via indirection, warning: this
// creates a needless allocation, because it is stored as pair<Class,Method>
MyDelegate d2 = MyDelegate(a, &AClass::method);
// same as above
MyDelegate d3 = MyDelegate::make(a, &AClass::method);

// class::method bound to the Delegate via instantiation of an immediate caller
// to the method AClass::method. this is preferred and does not require any
// memory allocation!
MyDelegate d4 = MyDelegate::make<AClass, &AClass::method>(a);

// a lambda with capture bound to the Delegate, this always performs a memory
// allocation to copy the capture closure.
double offset = 42.0;
MyDelegate d5 = [&](double a) { return a + offset; };
\endcode
 *
 */
template <typename R, typename... A, typename Allocator>
class Delegate<R(A...), Allocator>
{
public:
    //! default constructor
    Delegate() = default;

    //! copy constructor
    Delegate(const Delegate&) = default;

    //! move constructor
    Delegate(Delegate&&) = default;

    //! copy assignment operator
    Delegate& operator = (const Delegate&) = default;

    //! move assignment operator
    Delegate& operator = (Delegate&&) = default;

    //! \name Immediate Function Calls
    //! \{

    //! construction from an immediate function with no object or pointer.
    template <R(*const Function)(A...)>
    static Delegate make() noexcept {
        return Delegate(function_caller<Function>, nullptr);
    }

    //! \}

    //! \name Function Pointer Calls
    //! \{

    //! constructor from a plain function pointer with no object.
    explicit Delegate(R(*const function_ptr)(A...)) noexcept
        : Delegate(function_ptr_caller,
                   * reinterpret_cast<void* const*>(&function_ptr)) { }

    static_assert(sizeof(void*) == sizeof(void (*)(void)),
                  "object pointer and function pointer sizes must equal");

    //! construction from a plain function pointer with no object.
    static Delegate make(R(*const function_ptr)(A...)) noexcept {
        return Delegate(function_ptr);
    }

    //! \}

    //! \name Immediate Class::Method Calls with Objects
    //! \{

    //! construction for an immediate class::method with class object
    template <class C, R(C::* const Method)(A...)>
    static Delegate make(C* const object_ptr) noexcept {
        return Delegate(method_caller<C, Method>, object_ptr);
    }

    //! construction for an immediate class::method with class object
    template <class C, R(C::* const Method)(A...) const>
    static Delegate make(C const* const object_ptr) noexcept {
        return Delegate(const_method_caller<C, Method>,
                        const_cast<C*>(object_ptr));
    }

    //! construction for an immediate class::method with class object by
    //! reference
    template <class C, R(C::* const Method)(A...)>
    static Delegate make(C& object) noexcept {
        return Delegate(method_caller<C, Method>, &object);
    }

    //! construction for an immediate class::method with class object by
    //! reference
    template <class C, R(C::* const Method)(A...) const>
    static Delegate make(C const& object) noexcept {
        return Delegate(const_method_caller<C, Method>,
                        const_cast<C*>(&object));
    }

    //! \}

    //! \name Lambdas with Captures and Wrapped Class::Method Calls with Objects
    //! \{

    //! constructor from any functor object T, which may be a lambda with
    //! capture or a MemberPair or ConstMemberPair wrapper.
    template <
        typename T,
        typename = typename std::enable_if<
            !std::is_same<Delegate, typename std::decay<T>::type>::value
            >::type
        >
    Delegate(T&& f)
        : store_(
              // allocate memory for T in shared_ptr with appropriate deleter
              typename std::allocator_traits<Allocator>::template rebind_alloc<
                  typename std::decay<T>::type>{ }.allocate(1),
              store_deleter<typename std::decay<T>::type>, Allocator()) {

        using Functor = typename std::decay<T>::type;
        using Rebind = typename std::allocator_traits<Allocator>::template rebind_alloc<Functor>;

        // copy-construct T into shared_ptr memory.
        Rebind rebind{ };
        std::allocator_traits<Rebind>::construct(
            rebind, static_cast<Functor*>(store_.get()), Functor(std::forward<T>(f)));

        object_ptr_ = store_.get();

        caller_ = functor_caller<Functor>;
    }

    //! constructor from any functor object T, which may be a lambda with
    //! capture or a MemberPair or ConstMemberPair wrapper.
    template <typename T>
    static Delegate make(T&& f) {
        return std::forward<T>(f);
    }

    //! constructor for wrapping a class::method with object pointer.
    template <class C>
    Delegate(C* const object_ptr, R(C::* const method_ptr)(A...))
        : Delegate(MemberPair<C>(object_ptr, method_ptr)) { }

    //! constructor for wrapping a const class::method with object pointer.
    template <class C>
    Delegate(C* const object_ptr, R(C::* const method_ptr)(A...) const)
        : Delegate(ConstMemberPair<C>(object_ptr, method_ptr)) { }

    //! constructor for wrapping a class::method with object reference.
    template <class C>
    Delegate(C& object, R(C::* const method_ptr)(A...))
        : Delegate(MemberPair<C>(&object, method_ptr)) { }

    //! constructor for wrapping a const class::method with object reference.
    template <class C>
    Delegate(C const& object, R(C::* const method_ptr)(A...) const)
        : Delegate(ConstMemberPair<C>(&object, method_ptr)) { }

    //! constructor for wrapping a class::method with object pointer.
    template <class C>
    static Delegate make(C* const object_ptr,
                         R(C::* const method_ptr)(A...)) {
        return MemberPair<C>(object_ptr, method_ptr);
    }

    //! constructor for wrapping a const class::method with object pointer.
    template <class C>
    static Delegate make(C const* const object_ptr,
                         R(C::* const method_ptr)(A...) const) {
        return ConstMemberPair<C>(object_ptr, method_ptr);
    }

    //! constructor for wrapping a class::method with object reference.
    template <class C>
    static Delegate make(C& object, R(C::* const method_ptr)(A...)) {
        return MemberPair<C>(&object, method_ptr);
    }

    //! constructor for wrapping a const class::method with object reference.
    template <class C>
    static Delegate make(C const& object,
                         R(C::* const method_ptr)(A...) const) {
        return ConstMemberPair<C>(&object, method_ptr);
    }

    //! \}

    //! \name Miscellaneous
    //! \{

    //! reset delegate to invalid.
    void reset() { caller_ = nullptr; store_.reset(); }

    void reset_caller() noexcept { caller_ = nullptr; }

    //! swap delegates
    void swap(Delegate& other) noexcept { std::swap(*this, other); }

    //! compare delegate with another
    bool operator == (Delegate const& rhs) const noexcept {
        return (object_ptr_ == rhs.object_ptr_) && (caller_ == rhs.caller_);
    }

    //! compare delegate with another
    bool operator != (Delegate const& rhs) const noexcept {
        return !operator == (rhs);
    }

    //! compare delegate with another
    bool operator < (Delegate const& rhs) const noexcept {
        return (object_ptr_ < rhs.object_ptr_) ||
               ((object_ptr_ == rhs.object_ptr_) && (reinterpret_cast<const void*>(caller_) < reinterpret_cast<const void*>(rhs.caller_)));
    }

    //! compare delegate with another
    bool operator == (std::nullptr_t const) const noexcept {
        return caller_ == nullptr;
    }

    //! compare delegate with another
    bool operator != (std::nullptr_t const) const noexcept {
        return caller_ != nullptr;
    }

    //! explicit conversion to bool -> valid or invalid.
    explicit operator bool () const noexcept { return caller_ != nullptr; }

    //! most important method: call. The call is forwarded to the selected
    //! function caller.
    R operator () (A... args) const {
        assert(caller_);
        return caller_(object_ptr_, std::forward<A>(args) ...);
    }

    //! \}

private:
    //! type of the function caller pointer.
    using Caller = R (*)(void*, A&& ...);

    using Deleter = void (*)(void*);

    //! pointer to function caller which depends on the type in object_ptr_. The
    //! caller_ contains a plain pointer to either function_caller, a
    //! function_ptr_caller, a method_caller, a const_method_caller, or a
    //! functor_caller.
    Caller caller_ = nullptr;

    //! pointer to object held by the delegate: for plain function pointers it
    //! is the function pointer, for class::methods it is a pointer to the class
    //! instance, for functors it is a pointer to the shared_ptr store_
    //! contents.
    void* object_ptr_ = nullptr;

    //! shared_ptr used to contain a memory object containing the callable, like
    //! lambdas with closures, or our own wrappers.
    std::shared_ptr<void> store_;

    //! private constructor for plain
    Delegate(const Caller& m, void* const obj) noexcept
        : caller_(m), object_ptr_(obj) { }

    //! deleter for stored functor closures
    template <typename T>
    static void store_deleter(void* const ptr) {
        using Rebind = typename std::allocator_traits<Allocator>::template rebind_alloc<T>;
        Rebind rebind{ };

        std::allocator_traits<Rebind>::destroy(rebind, static_cast<T*>(ptr));
        std::allocator_traits<Rebind>::deallocate(rebind, static_cast<T*>(ptr), 1);
    }

    //! \name Callers for simple function and immediate class::method calls.
    //! \{

    //! caller for an immediate function with no object or pointer.
    template <R(*Function)(A...)>
    static R function_caller(void* const, A&& ... args) {
        return Function(std::forward<A>(args) ...);
    }

    //! caller for a plain function pointer.
    static R function_ptr_caller(void* const object_ptr, A&& ... args) {
        return (*reinterpret_cast<R(*const*)(A...)>(&object_ptr))(args...);
    }

    //! function caller for immediate class::method function calls
    template <class C, R(C::* method_ptr)(A...)>
    static R method_caller(void* const object_ptr, A&& ... args) {
        return (static_cast<C*>(object_ptr)->*method_ptr)(
            std::forward<A>(args) ...);
    }

    //! function caller for immediate const class::method functions calls.
    template <class C, R(C::* method_ptr)(A...) const>
    static R const_method_caller(void* const object_ptr, A&& ... args) {
        return (static_cast<C const*>(object_ptr)->*method_ptr)(
            std::forward<A>(args) ...);
    }

    //! \}

    //! \name Wrappers for indirect class::method calls.
    //! \{

    //! wrappers for indirect class::method calls containing (object,
    //! method_ptr)
    template <class C>
    using MemberPair =
        std::pair<C* const, R(C::* const)(A...)>;

    //! wrappers for indirect const class::method calls containing (object,
    //! const method_ptr)
    template <class C>
    using ConstMemberPair =
        std::pair<C const* const, R(C::* const)(A...) const>;

    //! template for class::function selector
    template <typename>
    struct IsMemberPair : std::false_type { };

    //! specialization for class::function selector
    template <class C>
    struct IsMemberPair<MemberPair<C> >: std::true_type { };

    //! template for const class::function selector
    template <typename>
    struct IsConstMemberPair : std::false_type { };

    //! specialization for const class::function selector
    template <class C>
    struct IsConstMemberPair<ConstMemberPair<C> >: std::true_type { };

    //! function caller for functor class.
    template <typename T>
    static typename std::enable_if<
        !(IsMemberPair<T>::value || IsConstMemberPair<T>::value), R
        >::type
    functor_caller(void* const object_ptr, A&& ... args) {
        return (*static_cast<T*>(object_ptr))(std::forward<A>(args) ...);
    }

    //! function caller for const functor class.
    template <typename T>
    static typename std::enable_if<
        (IsMemberPair<T>::value || IsConstMemberPair<T>::value), R
        >::type
    functor_caller(void* const object_ptr, A&& ... args) {
        return (static_cast<T*>(object_ptr)->first->*
                static_cast<T*>(object_ptr)->second)(std::forward<A>(args) ...);
    }

    //! \}
};

//! make template alias due to similarity with std::function
template <typename T, typename Allocator = std::allocator<void> >
using delegate = Delegate<T, Allocator>;

//! constructor for wrapping a class::method with object pointer.
template <class C, typename R, typename... A>
inline Delegate<R(A...)>
make_delegate(
    C* const object_ptr, R(C::* const method_ptr)(A...)) noexcept {
    return Delegate<R(A...)>::template make<C>(object_ptr, method_ptr);
}

//! constructor for wrapping a const class::method with object pointer.
template <class C, typename R, typename... A>
inline Delegate<R(A...)>
make_delegate(
    C* const object_ptr, R(C::* const method_ptr)(A...) const) noexcept {
    return Delegate<R(A...)>::template make<C>(object_ptr, method_ptr);
}

//! constructor for wrapping a class::method with object reference.
template <class C, typename R, typename... A>
inline Delegate<R(A...)>
make_delegate(
    C& object_ptr, R(C::* const method_ptr)(A...)) noexcept {   // NOLINT
    return Delegate<R(A...)>::template make<C>(object_ptr, method_ptr);
}

//! constructor for wrapping a const class::method with object reference.
template <class C, typename R, typename... A>
inline Delegate<R(A...)>
make_delegate(
    C const& object_ptr, R(C::* const method_ptr)(A...) const) noexcept {
    return Delegate<R(A...)>::template make<C>(object_ptr, method_ptr);
}

} // namespace tlx

#endif // !TLX_DELEGATE_HEADER

/******************************************************************************/
