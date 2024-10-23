/*******************************************************************************
 * tlx/container/simple_vector.hpp
 *
 * Copied and modified from STXXL, see http://stxxl.org
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2011 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_SIMPLE_VECTOR_HEADER
#define TLX_CONTAINER_SIMPLE_VECTOR_HEADER

#include <algorithm>
#include <cstdlib>
#include <utility>

namespace tlx {

//! \addtogroup tlx_container
//! \{

//! enum class to select SimpleVector object initialization
enum class SimpleVectorMode {
    //! Initialize objects at allocation and destroy on deallocation
    Normal,
    //! Do not initialize objects at allocation, but destroy on
    //! deallocation. Thus, all objects must be constructed from outside.
    NoInitButDestroy,
    //! Do not initialize objects at allocation and do not destroy them.
    NoInitNoDestroy,
};

/*!
 * Simpler non-growing vector without initialization.
 *
 * SimpleVector can be used a replacement for std::vector when only a
 * non-growing array of simple types is needed. The advantages of SimpleVector
 * are that it does not initilize memory for POD types (-> faster), while normal
 * structs are supported as well if default-contractible. The simple pointer
 * types allow faster compilation and is less error prone to copying and other
 * problems.
 */
template <typename ValueType,
          SimpleVectorMode Mode = SimpleVectorMode::Normal>
class SimpleVector
{
public:
    using value_type = ValueType;
    using size_type = size_t;

protected:
    //! size of allocated memory
    size_type size_;

    //! pointer to allocated memory area
    value_type* array_;

public:
    // *** simple pointer iterators

    using iterator = value_type*;
    using const_iterator = const value_type*;
    using reference = value_type&;
    using const_reference = const value_type&;

public:
    //! allocate empty simple vector
    SimpleVector()
        : size_(0), array_(nullptr)
    { }

    //! allocate vector's memory
    explicit SimpleVector(const size_type& sz)
        : size_(sz), array_(nullptr) {
        if (size_ > 0)
            array_ = create_array(size_);
    }

    //! non-copyable: delete copy-constructor
    SimpleVector(const SimpleVector&) = delete;
    //! non-copyable: delete assignment operator
    SimpleVector& operator = (const SimpleVector&) = delete;

    //! move-constructor
    SimpleVector(SimpleVector&& v) noexcept
        : size_(v.size_), array_(v.array_)
    { v.size_ = 0, v.array_ = nullptr; }

    //! move-assignment
    SimpleVector& operator = (SimpleVector&& v) noexcept {
        if (&v == this) return *this;
        destroy_array(array_, size_);
        size_ = v.size_, array_ = v.array_;
        v.size_ = 0, v.array_ = nullptr;
        return *this;
    }

    //! swap vector with another one
    void swap(SimpleVector& obj) noexcept {
        std::swap(size_, obj.size_);
        std::swap(array_, obj.array_);
    }

    //! delete vector
    ~SimpleVector() {
        destroy_array(array_, size_);
    }

    //! return iterator to beginning of vector
    iterator data() noexcept {
        return array_;
    }
    //! return iterator to beginning of vector
    const_iterator data() const noexcept {
        return array_;
    }
    //! return number of items in vector
    size_type size() const noexcept {
        return size_;
    }

    //! return mutable iterator to first element
    iterator begin() noexcept {
        return array_;
    }
    //! return constant iterator to first element
    const_iterator begin() const noexcept {
        return array_;
    }
    //! return constant iterator to first element
    const_iterator cbegin() const noexcept {
        return begin();
    }

    //! return mutable iterator beyond last element
    iterator end() noexcept {
        return array_ + size_;
    }
    //! return constant iterator beyond last element
    const_iterator end() const noexcept {
        return array_ + size_;
    }
    //! return constant iterator beyond last element
    const_iterator cend() const noexcept {
        return end();
    }

    //! returns reference to the last element in the container
    reference front() noexcept {
        return array_[0];
    }
    //! returns reference to the first element in the container
    const_reference front() const noexcept {
        return array_[0];
    }
    //! returns reference to the first element in the container
    reference back() noexcept {
        return array_[size_ - 1];
    }
    //! returns reference to the last element in the container
    const_reference back() const noexcept {
        return array_[size_ - 1];
    }

    //! return the i-th position of the vector
    reference operator [] (size_type i) noexcept {
        return *(begin() + i);
    }
    //! return constant reference to the i-th position of the vector
    const_reference operator [] (size_type i) const noexcept {
        return *(begin() + i);
    }

    //! return the i-th position of the vector
    reference at(size_type i) noexcept {
        return *(begin() + i);
    }
    //! return constant reference to the i-th position of the vector
    const_reference at(size_type i) const noexcept {
        return *(begin() + i);
    }

    //! resize the array to contain exactly new_size items
    void resize(size_type new_size) {
        if (array_) {
            value_type* tmp = array_;
            array_ = create_array(new_size);
            std::move(tmp, tmp + std::min(size_, new_size), array_);
            destroy_array(tmp, size_);
            size_ = new_size;
        }
        else {
            array_ = create_array(new_size);
            size_ = new_size;
        }
    }

    //! deallocate contained array
    void destroy() {
        destroy_array(array_, size_);
        array_ = nullptr;
        size_ = 0;
    }

    //! Zero the whole array content.
    void fill(const value_type& v = value_type()) noexcept {
        std::fill(array_, array_ + size_, v);
    }

private:
    static ValueType * create_array(size_t size) {
        switch (Mode)
        {
        case SimpleVectorMode::Normal:
            // with normal object construction
            return new value_type[size];
        case SimpleVectorMode::NoInitButDestroy:
        case SimpleVectorMode::NoInitNoDestroy:
            // operator new allocates bytes
            return static_cast<ValueType*>(
                operator new (size * sizeof(ValueType)));
        }
        abort();
    }

    static void destroy_array(ValueType* array, size_t size) {
        switch (Mode)
        {
        case SimpleVectorMode::Normal:
            // with normal object destruction
            delete[] array;
            return;
        case SimpleVectorMode::NoInitButDestroy:
            // destroy objects and deallocate memory
            for (size_t i = 0; i < size; ++i)
                array[i].~ValueType();
            operator delete (array);
            return;
        case SimpleVectorMode::NoInitNoDestroy:
            // only deallocate memory
            operator delete (array);
            return;
        }
        abort();
    }
};

//! make template alias due to similarity with std::vector
template <typename T>
using simple_vector = SimpleVector<T>;

//! \}

} // namespace tlx

#endif // !TLX_CONTAINER_SIMPLE_VECTOR_HEADER

/******************************************************************************/
