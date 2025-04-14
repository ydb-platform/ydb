/*******************************************************************************
 * tlx/counting_ptr.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2013-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_COUNTING_PTR_HEADER
#define TLX_COUNTING_PTR_HEADER

#include <algorithm>
#include <atomic>
#include <cassert>
#include <iosfwd>
#include <type_traits>
#include <utility>

namespace tlx {

//! default deleter for CountingPtr
class CountingPtrDefaultDeleter
{
public:
    template <typename Type>
    void operator () (Type* ptr) const noexcept {
        delete ptr;
    }
};

//! dummy deleter for CountingPtr
class CountingPtrNoOperationDeleter
{
public:
    template <typename Type>
    void operator () (Type*) const noexcept { }
};

/*!
 * High-performance smart pointer used as a wrapping reference counting pointer.
 *
 * This smart pointer class requires two functions in the template type: void
 * inc_reference() and void dec_reference(). These must increment and decrement
 * a reference count inside the templated object. When initialized, the type
 * must have reference count zero. Each new object referencing the data calls
 * inc_reference() and each destroying holder calls del_reference(). When the
 * data object determines that it's internal count is zero, then it must destroy
 * itself.
 *
 * Accompanying the CountingPtr is a class ReferenceCounter, from which
 * reference counted classes may be derive from. The class ReferenceCounter
 * implement all methods required for reference counting.
 *
 * The whole method is more similar to boost's instrusive_ptr, but also yields
 * something resembling std::shared_ptr. However, compared to std::shared_ptr,
 * this class only contains a single pointer, while shared_ptr contains two
 * which are only related if constructed with std::make_shared.
 *
 * Another advantage with this method is that no kludges like
 * std::enable_shared_from_this are needed.
 */
template <typename Type, typename Deleter = CountingPtrDefaultDeleter>
class CountingPtr
{
public:
    //! contained type.
    using element_type = Type;

private:
    //! the pointer to the currently referenced object.
    Type* ptr_;

    //! increment reference count of object.
    void inc_reference(Type* o) noexcept
    { if (o) o->inc_reference(); }

    //! decrement reference count of current object and maybe delete it.
    void dec_reference() noexcept {
        if (ptr_ && ptr_->dec_reference())
            Deleter()(ptr_);
    }

public:
    //! all CountingPtr are friends such that they may steal pointers.
    template <typename Other, typename OtherDeleter>
    friend class CountingPtr;

    //! \name Construction, Assignment and Destruction
    //! \{

    //! default constructor: contains a nullptr pointer.
    CountingPtr() noexcept
        : ptr_(nullptr) { }

    //! implicit conversion from nullptr_t: contains a nullptr pointer.
    CountingPtr(std::nullptr_t) noexcept // NOLINT
        : ptr_(nullptr) { }

    //! constructor from pointer: initializes new reference to ptr.
    explicit CountingPtr(Type* ptr) noexcept
        : ptr_(ptr)
    { inc_reference(ptr_); }

    //! copy-constructor: also initializes new reference to ptr.
    CountingPtr(const CountingPtr& other) noexcept
        : ptr_(other.ptr_)
    { inc_reference(ptr_); }

    //! copy-constructor: also initializes new reference to ptr.
    template <typename Subclass,
              typename = typename std::enable_if<
                  std::is_convertible<Subclass*, Type*>::value, void>::type>
    CountingPtr(const CountingPtr<Subclass, Deleter>& other) noexcept
        : ptr_(other.ptr_)
    { inc_reference(ptr_); }

    //! move-constructor: just moves pointer, does not change reference counts.
    CountingPtr(CountingPtr&& other) noexcept
        : ptr_(other.ptr_)
    { other.ptr_ = nullptr; }

    //! move-constructor: just moves pointer, does not change reference counts.
    template <typename Subclass,
              typename = typename std::enable_if<
                  std::is_convertible<Subclass*, Type*>::value, void>::type>
    CountingPtr(CountingPtr<Subclass, Deleter>&& other) noexcept
        : ptr_(other.ptr_)
    { other.ptr_ = nullptr; }

    //! copy-assignment operator: acquire reference on new one and dereference
    //! current object.
    CountingPtr& operator = (const CountingPtr& other) noexcept {
        if (ptr_ == other.ptr_)
            return *this;
        inc_reference(other.ptr_);
        dec_reference();
        ptr_ = other.ptr_;
        return *this;
    }

    //! copy-assignment operator: acquire reference on new one and dereference
    //! current object.
    template <typename Subclass,
              typename = typename std::enable_if<
                  std::is_convertible<Subclass*, Type*>::value, void>::type>
    CountingPtr&
    operator = (const CountingPtr<Subclass, Deleter>& other) noexcept {
        if (ptr_ == other.ptr_)
            return *this;
        inc_reference(other.ptr_);
        dec_reference();
        ptr_ = other.ptr_;
        return *this;
    }

    //! move-assignment operator: move reference of other to current object.
    CountingPtr& operator = (CountingPtr&& other) noexcept {
        if (ptr_ == other.ptr_)
            return *this;
        dec_reference();
        ptr_ = other.ptr_;
        other.ptr_ = nullptr;
        return *this;
    }

    //! move-assignment operator: move reference of other to current object.
    template <typename Subclass,
              typename = typename std::enable_if<
                  std::is_convertible<Subclass*, Type*>::value, void>::type>
    CountingPtr& operator = (CountingPtr<Subclass, Deleter>&& other) noexcept {
        if (ptr_ == other.ptr_)
            return *this;
        dec_reference();
        ptr_ = other.ptr_;
        other.ptr_ = nullptr;
        return *this;
    }

    //! destructor: decrements reference count in ptr.
    ~CountingPtr() { dec_reference(); }

    //! \}

    //! \name Observers
    //! \{

    //! return the enclosed object as reference.
    Type& operator * () const noexcept {
        assert(ptr_);
        return *ptr_;
    }

    //! return the enclosed pointer.
    Type* operator -> () const noexcept {
        assert(ptr_);
        return ptr_;
    }

    //! return the enclosed pointer.
    Type * get() const noexcept { return ptr_; }

    //! test for a non-nullptr pointer
    bool valid() const noexcept
    { return (ptr_ != nullptr); }

    //! cast to bool checks for a nullptr pointer
    operator bool () const noexcept
    { return valid(); }

    //! test for a nullptr pointer
    bool empty() const noexcept
    { return (ptr_ == nullptr); }

    //! if the object is referred by this CountingPtr only
    bool unique() const noexcept
    { return ptr_ && ptr_->unique(); }

    //! Returns the number of different shared_ptr instances managing the
    //! current object.
    size_t use_count() const noexcept
    { return ptr_->reference_count(); }

    //! \}

    //! \name Modifiers
    //! \{

    //! release contained pointer, frees object if this is the last reference.
    void reset() {
        dec_reference();
        ptr_ = nullptr;
    }

    //! swap enclosed object with another counting pointer (no reference counts
    //! need change)
    void swap(CountingPtr& b) noexcept
    { std::swap(ptr_, b.ptr_); }

    //! make and refer a copy if the original object was shared.
    void unify() {
        if (ptr_ && !ptr_->unique())
            operator = (CountingPtr(new Type(*ptr_)));
    }

    //! \}

    //! \name Comparison Operators
    //! \{

    //! test equality of only the pointer values.
    bool operator == (const CountingPtr& other) const noexcept
    { return ptr_ == other.ptr_; }

    //! test inequality of only the pointer values.
    bool operator != (const CountingPtr& other) const noexcept
    { return ptr_ != other.ptr_; }

    //! test equality of only the address pointed to
    bool operator == (Type* other) const noexcept
    { return ptr_ == other; }

    //! test inequality of only the address pointed to
    bool operator != (Type* other) const noexcept
    { return ptr_ != other; }

    //! compare the pointer values.
    bool operator < (const CountingPtr& other) const noexcept
    { return ptr_ < other.ptr_; }

    //! compare the pointer values.
    bool operator <= (const CountingPtr& other) const noexcept
    { return ptr_ <= other.ptr_; }

    //! compare the pointer values.
    bool operator > (const CountingPtr& other) const noexcept
    { return ptr_ > other.ptr_; }

    //! compare the pointer values.
    bool operator >= (const CountingPtr& other) const noexcept
    { return ptr_ >= other.ptr_; }

    //! compare the pointer values.
    bool operator < (Type* other) const noexcept
    { return ptr_ < other; }

    //! compare the pointer values.
    bool operator <= (Type* other) const noexcept
    { return ptr_ <= other; }

    //! compare the pointer values.
    bool operator > (Type* other) const noexcept
    { return ptr_ > other; }

    //! compare the pointer values.
    bool operator >= (Type* other) const noexcept
    { return ptr_ >= other; }

    //! \}
};

//! make alias due to similarity with std::shared_ptr<T>
template <typename Type>
using counting_ptr = CountingPtr<Type>;

//! make alias for dummy deleter
template <typename Type>
using CountingPtrNoDelete = CountingPtr<Type, CountingPtrNoOperationDeleter>;

//! method analogous to std::make_shared and std::make_unique.
template <typename Type, typename... Args>
CountingPtr<Type> make_counting(Args&& ... args) {
    return CountingPtr<Type>(new Type(std::forward<Args>(args) ...));
}

//! swap enclosed object with another counting pointer (no reference counts need
//! change)
template <typename A, typename D>
void swap(CountingPtr<A, D>& a1, CountingPtr<A, D>& a2) noexcept {
    a1.swap(a2);
}

//! print pointer
template <typename A, typename D>
std::ostream& operator << (std::ostream& os, const CountingPtr<A, D>& c) {
    return os << c.get();
}

/*!
 * Provides reference counting abilities for use with CountingPtr.
 *
 * Use as superclass of the actual object, this adds a reference_count_
 * value. Then either use CountingPtr as pointer to manage references and
 * deletion, or just do normal new and delete.
 */
class ReferenceCounter
{
private:
    //! the reference count is kept mutable for CountingPtr<const Type> to
    //! change the reference count.
    mutable std::atomic<size_t> reference_count_;

public:
    //! new objects have zero reference count
    ReferenceCounter() noexcept
        : reference_count_(0) { }

    //! coping still creates a new object with zero reference count
    ReferenceCounter(const ReferenceCounter&) noexcept
        : reference_count_(0) { }

    //! assignment operator, leaves pointers unchanged
    ReferenceCounter& operator = (const ReferenceCounter&) noexcept {
        // changing the contents leaves pointers unchanged
        return *this;
    }

    ~ReferenceCounter()
    { assert(reference_count_ == 0); }

public:
    //! Call whenever setting a pointer to the object.
    void inc_reference() const noexcept
    { ++reference_count_; }

    /*!
     * Call whenever resetting (i.e. overwriting) a pointer to the object.
     * IMPORTANT: In case of self-assignment, call AFTER inc_reference().
     *
     * \return if the object has to be deleted (i.e. if it's reference count
     * dropped to zero)
     */
    bool dec_reference() const noexcept {
        assert(reference_count_ > 0);
        return (--reference_count_ == 0);
    }

    //! Test if the ReferenceCounter is referenced by only one CountingPtr.
    bool unique() const noexcept
    { return (reference_count_ == 1); }

    //! Return the number of references to this object (for debugging)
    size_t reference_count() const noexcept
    { return reference_count_; }
};

//! make alias due to CountingPtr's similarity with std::shared_ptr<T>
using reference_counter = ReferenceCounter;

/** \page tlx_counting_ptr CountingPtr – an intrusive reference counting pointer

\brief \ref CountingPtr is an implementation of <b>intrusive reference counting</b>.
This is similar, but not identical to boost or C++ TR1's \c
shared_ptr. Intrusive reference counting requires the counted class to contain
the counter, which is not required by <tt>std::shared_ptr</tt>.

Intrusive counting is often faster due to fewer cache faults. Furthermore, \ref
CountingPtr is a <b>single pointer</b>, whereas <tt>std::shared_ptr</tt>
actually contains (at least) two pointers. \ref CountingPtr also creates a lot
less debug info due to reduced complexity.

\ref CountingPtr is accompanied by \ref ReferenceCounter, which contains the
actual reference counter (a single integer). A reference counted object must
derive from \ref ReferenceCounter :

\code
struct Something : public tlx::ReferenceCounter
{
};
\endcode

Code that now wishes to use pointers referencing this object, will typedef an
\ref CountingPtr, which is used to increment and decrement the included
reference counter automatically.

\code
using SomethingPtr = tlx::CountingPtr<Something>;
{
    // create new instance of something
    SomethingPtr p1 = new something;
    {
        // create a new reference to the same instance (no deep copy!)
        SomethingPtr p2 = p1;
        // this block end will decrement the reference count, but not delete the object
    }
    // this block end will delete the object
}
\endcode

The \ref CountingPtr can generally be used like a usual pointer or \c
std::shared_ptr (see the docs for more).

*/

} // namespace tlx

#endif // !TLX_COUNTING_PTR_HEADER

/******************************************************************************/
