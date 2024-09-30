/*******************************************************************************
 * tlx/container/ring_buffer.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_RING_BUFFER_HEADER
#define TLX_CONTAINER_RING_BUFFER_HEADER

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <vector>

#include <tlx/math/round_to_power_of_two.hpp>

namespace tlx {

//! \addtogroup tlx_container
//! \{

/*!
 * A ring (circular) buffer of static (non-growing) size.
 *
 * Due to many modulo operations with capacity_, the capacity is rounded up to
 * the next power of two, even for powers of two! This is because otherwise
 * size() == end - begin == 0 after filling the ring buffer, and adding another
 * size_ member requires more book-keeping.
 */
template <typename Type, class Allocator = std::allocator<Type> >
class RingBuffer
{
public:
    using value_type = Type;
    using allocator_type = Allocator;

    using alloc_traits = std::allocator_traits<allocator_type>;

    typedef Type& reference;
    typedef const Type& const_reference;
    typedef Type* pointer;
    typedef const Type* const_pointer;

    using size_type = typename allocator_type::size_type;
    using difference_type = typename allocator_type::difference_type;

    // using iterator;
    // using const_iterator;
    // using reverse_iterator = std::reverse_iterator<iterator>;
    // using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    explicit RingBuffer(const Allocator& alloc = allocator_type()) noexcept
        : max_size_(0), alloc_(alloc),
          capacity_(0), mask_(0), data_(nullptr) { }

    explicit RingBuffer(size_t max_size,
                        const Allocator& alloc = allocator_type())
        : max_size_(max_size), alloc_(alloc),
          capacity_(round_up_to_power_of_two(max_size + 1)),
          mask_(capacity_ - 1),
          data_(alloc_.allocate(capacity_)) { }

    //! copy-constructor: create new ring buffer
    RingBuffer(const RingBuffer& rb)
        : max_size_(rb.max_size_),
          alloc_(rb.alloc_),
          capacity_(rb.capacity_),
          mask_(rb.mask_),
          data_(alloc_.allocate(capacity_)) {
        // copy items using existing methods (we cannot just flat copy the array
        // due to item construction).
        for (size_t i = 0; i < rb.size(); ++i) {
            push_back(rb[i]);
        }
    }

    //! copyable: create new ring buffer
    RingBuffer& operator = (const RingBuffer& rb) {
        if (this == &rb) return *this;
        // empty this buffer
        clear();
        // reallocate buffer if the size changes
        if (capacity_ != rb.capacity_ || alloc_ != rb.alloc_)
        {
            alloc_.deallocate(data_, capacity_);
            alloc_ = rb.alloc_;
            capacity_ = rb.capacity_;
            data_ = alloc_.allocate(capacity_);
        }
        // copy over fields
        max_size_ = rb.max_size_;
        mask_ = rb.mask_;
        begin_ = end_ = 0;
        // copy over items (we cannot just flat copy the array due to item
        // construction).
        for (size_t i = 0; i < rb.size(); ++i)
            push_back(rb[i]);
        return *this;
    }

    //! move-constructor: move buffer
    RingBuffer(RingBuffer&& rb) noexcept
        : max_size_(rb.max_size_),
          alloc_(std::move(rb.alloc_)),
          capacity_(rb.capacity_), mask_(rb.mask_),
          data_(rb.data_),
          begin_(rb.begin_), end_(rb.end_) {
        // clear other buffer
        rb.data_ = nullptr;
        rb.begin_ = rb.end_ = 0;
        rb.capacity_ = 0;
    }

    //! move-assignment operator: default
    RingBuffer& operator = (RingBuffer&& rb) noexcept {
        if (this == &rb) return *this;
        // empty this buffer
        clear();
        alloc_.deallocate(data_, capacity_);
        // move over variables
        max_size_ = rb.max_size_;
        alloc_ = std::move(rb.alloc_);
        capacity_ = rb.capacity_, mask_ = rb.mask_;
        data_ = rb.data_;
        begin_ = rb.begin_, end_ = rb.end_;
        // clear other buffer
        rb.data_ = nullptr;
        rb.begin_ = rb.end_ = 0;
        rb.capacity_ = 0;
        return *this;
    }

    ~RingBuffer() {
        clear();
        alloc_.deallocate(data_, capacity_);
    }

    //! allocate buffer
    void allocate(size_t max_size) {
        assert(!data_);
        max_size_ = max_size;
        capacity_ = round_up_to_power_of_two(max_size + 1);
        mask_ = capacity_ - 1;
        data_ = alloc_.allocate(capacity_);
    }

    //! deallocate buffer
    void deallocate() {
        if (data_) {
            clear();
            alloc_.deallocate(data_, capacity_);
            data_ = nullptr;
        }
    }

    //! \name Modifiers
    //! \{

    //! add element at the end
    void push_back(const value_type& t) {
        assert(size() + 1 <= max_size_);
        alloc_traits::construct(alloc_, std::addressof(data_[end_]), t);
        ++end_ &= mask_;
    }

    //! add element at the end
    void push_back(value_type&& t) {
        assert(size() + 1 <= max_size_);
        alloc_traits::construct(
            alloc_, std::addressof(data_[end_]), std::move(t));
        ++end_ &= mask_;
    }

    //! emplace element at the end
    template <typename... Args>
    void emplace_back(Args&& ... args) {
        assert(size() + 1 <= max_size_);
        alloc_traits::construct(alloc_, std::addressof(data_[end_]),
                                std::forward<Args>(args) ...);
        ++end_ &= mask_;
    }

    //! add element at the beginning
    void push_front(const value_type& t) {
        assert(size() + 1 <= max_size_);
        --begin_ &= mask_;
        alloc_traits::construct(alloc_, std::addressof(data_[begin_]), t);
    }

    //! add element at the beginning
    void push_front(value_type&& t) {
        assert(size() + 1 <= max_size_);
        --begin_ &= mask_;
        alloc_traits::construct(
            alloc_, std::addressof(data_[begin_]), std::move(t));
    }

    //! emplace element at the beginning
    template <typename... Args>
    void emplace_front(Args&& ... args) {
        assert(size() + 1 <= max_size_);
        --begin_ &= mask_;
        alloc_traits::construct(alloc_, std::addressof(data_[begin_]),
                                std::forward<Args>(args) ...);
    }

    //! remove element at the beginning
    void pop_front() {
        assert(!empty());
        alloc_traits::destroy(alloc_, std::addressof(data_[begin_]));
        ++begin_ &= mask_;
    }

    //! remove element at the end
    void pop_back() {
        assert(!empty());
        alloc_traits::destroy(alloc_, std::addressof(data_[begin_]));
        --end_ &= mask_;
    }

    //! reset buffer contents
    void clear() {
        while (begin_ != end_)
            pop_front();
    }

    //! copy all element into the vector
    void copy_to(std::vector<value_type>* out) const {
        for (size_t i = 0; i < size(); ++i)
            out->emplace_back(operator [] (i));
    }

    //! move all element from the RingBuffer into the vector
    void move_to(std::vector<value_type>* out) {
        while (!empty()) {
            out->emplace_back(std::move(front()));
            pop_front();
        }
    }

    //! \}

    //! \name Element access
    //! \{

    //! Returns a reference to the i-th element.
    reference operator [] (size_type i) noexcept {
        assert(i < size());
        return data_[(begin_ + i) & mask_];
    }
    //! Returns a reference to the i-th element.
    const_reference operator [] (size_type i) const noexcept {
        assert(i < size());
        return data_[(begin_ + i) & mask_];
    }

    //! Returns a reference to the first element.
    reference front() noexcept {
        assert(!empty());
        return data_[begin_];
    }
    //! Returns a reference to the first element.
    const_reference front() const noexcept {
        assert(!empty());
        return data_[begin_];
    }

    //! Returns a reference to the last element.
    reference back() noexcept {
        assert(!empty());
        return data_[(end_ - 1) & mask_];
    }
    //! Returns a reference to the last element.
    const_reference back() const noexcept {
        assert(!empty());
        return data_[(end_ - 1) & mask_];
    }

    //! \}

    //! \name Capacity
    //! \{

    //! return the number of items in the buffer
    size_type size() const noexcept {
        return (end_ - begin_) & mask_;
    }

    //! return the maximum number of items in the buffer.
    size_t max_size() const noexcept {
        return max_size_;
    }

    //! return actual capacity of the ring buffer.
    size_t capacity() const noexcept {
        return capacity_;
    }

    //! returns true if no items are in the buffer
    bool empty() const noexcept {
        return size() == 0;
    }

    //! \}

    //! \name Serialization Methods for cereal
    //! \{

    template <class Archive>
    void save(Archive& ar) const { // NOLINT
        std::uint32_t ar_size = size();
        ar(max_size_, ar_size);
        for (size_t i = 0; i < ar_size; ++i) ar(operator [] (i));
    }

    template <class Archive>
    void load(Archive& ar) { // NOLINT
        ar(max_size_);

        if (data_) {
            clear();
            alloc_.deallocate(data_, capacity_);
        }

        // setup buffer
        capacity_ = round_up_to_power_of_two(max_size_ + 1);
        mask_ = capacity_ - 1;
        data_ = alloc_.allocate(capacity_);
        begin_ = end_ = 0;

        // load items
        std::uint32_t ar_size;
        ar(ar_size);

        for (size_t i = 0; i < ar_size; ++i) {
            push_back(Type());
            ar(back());
        }
    }

    //! \}

protected:
    //! target max_size of circular buffer prescribed by the user. Never equal
    //! to the data_.size(), which is rounded up to a power of two.
    size_t max_size_;

    //! used allocator
    allocator_type alloc_;

    //! capacity of data buffer. rounded up from max_size_ to next unequal power
    //! of two.
    size_t capacity_;

    //! one-bits mask for calculating modulo of capacity using AND-mask.
    size_t mask_;

    //! the circular buffer of static size.
    Type* data_;

    //! iterator at current begin of ring buffer
    size_type begin_ = 0;

    //! iterator at current begin of ring buffer
    size_type end_ = 0;
};

//! \}

} // namespace tlx

#endif // !TLX_CONTAINER_RING_BUFFER_HEADER

/******************************************************************************/
