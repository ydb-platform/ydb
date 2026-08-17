//////////////////////////////////////////////////////////////////////////////
//
// (C) Copyright Joaquin M Lopez Munoz 2025-2026.
// (C) Copyright Ion Gaztanaga 2025-2026.
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//
// See http://www.boost.org/libs/container for documentation.
//
//////////////////////////////////////////////////////////////////////////////
#ifndef BOOST_CONTAINER_EXPERIMENTAL_NEST_HPP
#define BOOST_CONTAINER_EXPERIMENTAL_NEST_HPP

#ifndef BOOST_CONFIG_HPP
#  include <boost/config.hpp>
#endif

#if defined(BOOST_HAS_PRAGMA_ONCE)
#  pragma once
#endif

#include <boost/container/detail/config_begin.hpp>
#include <boost/container/detail/workaround.hpp>

// container
#include <boost/container/container_fwd.hpp>
#include <boost/container/options.hpp>
#include <boost/container/new_allocator.hpp>
#include <boost/container/allocator_traits.hpp>
// container/detail
#include <boost/container/detail/compare_functors.hpp>
#include <boost/container/detail/iterator.hpp>
#include <boost/container/detail/iterators.hpp>
#include <boost/container/detail/mpl.hpp>
#include <boost/container/detail/type_traits.hpp>
// move
#include <boost/move/utility_core.hpp>
#include <boost/move/core.hpp>
#include <boost/move/traits.hpp>
#include <boost/move/adl_move_swap.hpp>
#include <boost/move/iterator.hpp>
#if defined(BOOST_NO_CXX11_VARIADIC_TEMPLATES)
#  include <boost/move/detail/fwd_macros.hpp>
#endif
#include <boost/move/detail/move_helpers.hpp>
#include <boost/move/detail/to_raw_pointer.hpp>
// intrusive
#include <boost/intrusive/pointer_traits.hpp>
// core
#include <boost/assert.hpp>
#include <boost/core/addressof.hpp>
#include <boost/core/empty_value.hpp>
#include <boost/core/no_exceptions_support.hpp>
#include <boost/core/bit.hpp>
// std
#include <algorithm>
#include <cstddef>
#include <functional>
#include <new>
#include <boost/cstdint.hpp>
#include <climits>

#if !defined(BOOST_NO_CXX11_HDR_INITIALIZER_LIST)
#include <initializer_list>
#endif

#if !defined(BOOST_CONTAINER_NEST_DISABLE_SSE2)
#if defined(BOOST_CONTAINER_NEST_ENABLE_SSE2)|| \
    defined(__SSE2__) || \
    defined(_M_X64) || (defined(_M_IX86_FP)&&_M_IX86_FP>=2)
#define BOOST_CONTAINER_NEST_SSE2
#endif
#endif

#if defined(BOOST_CONTAINER_NEST_SSE2)
#include <emmintrin.h>
#endif

#ifdef __has_builtin
#define BOOST_CONTAINER_NEST_HAS_BUILTIN(x) __has_builtin(x)
#else
#define BOOST_CONTAINER_NEST_HAS_BUILTIN(x) 0
#endif

#if !defined(NDEBUG)
#define BOOST_CONTAINER_NEST_ASSUME(cond) BOOST_ASSERT(cond)
#elif BOOST_CONTAINER_NEST_HAS_BUILTIN(__builtin_assume)
#define BOOST_CONTAINER_NEST_ASSUME(cond) __builtin_assume(cond)
#elif defined(__GNUC__) || \
      BOOST_CONTAINER_NEST_HAS_BUILTIN(__builtin_unreachable)
#define BOOST_CONTAINER_NEST_ASSUME(cond)           \
  do{                                    \
    if(!(cond)) __builtin_unreachable(); \
  } while(0)
#elif defined(_MSC_VER)
#define BOOST_CONTAINER_NEST_ASSUME(cond) __assume(cond)
#else
#define BOOST_CONTAINER_NEST_ASSUME(cond)          \
  do{                                   \
    static_cast<void>(false && (cond)); \
  } while(0)
#endif

/* We use BOOST_CONTAINER_NEST_PREFETCH[_BLOCK] macros rather than proper
 * functions because of https://gcc.gnu.org/bugzilla/show_bug.cgi?id=109985
 */

#if defined(BOOST_GCC) || defined(BOOST_CLANG)
#define BOOST_CONTAINER_NEST_PREFETCH(p) \
__builtin_prefetch(static_cast<const char*>(static_cast<const void*>(boost::movelib::to_raw_pointer(p))))
#elif defined(BOOST_CONTAINER_NEST_SSE2)
#define BOOST_CONTAINER_NEST_PREFETCH(p) \
_mm_prefetch(static_cast<const char*>(static_cast<const void*>(boost::movelib::to_raw_pointer(p))), _MM_HINT_T0)
#else
#define BOOST_CONTAINER_NEST_PREFETCH(p) ((void)(p))
#endif

#define BOOST_CONTAINER_NEST_PREFETCH_BLOCK(pbb, Block) \
do{                                                    \
  Block &p0_ = static_cast<Block&>(*(pbb));           \
  BOOST_CONTAINER_NEST_PREFETCH(p0_.data());           \
} while(0)

#if defined(BOOST_MSVC)
#pragma warning(push)
#pragma warning(disable:4714) /* marked as __forceinline not inlined */
#endif

namespace boost {
namespace container {

////////////////////////////////////////////////////////////////
//
//
//          OPTIONS FOR NEST CONTAINER (EXPERIMENTAL)
//
//
////////////////////////////////////////////////////////////////

#if !defined(BOOST_CONTAINER_DOXYGEN_INVOKED)

template<bool StoreDataInBlock, bool Prefetch>
struct nest_opt
{
   BOOST_STATIC_CONSTEXPR bool store_data_in_block = StoreDataInBlock;
   BOOST_STATIC_CONSTEXPR bool prefetch = Prefetch;
};

typedef nest_opt<false, true> nest_null_opt;

#endif   //   !defined(BOOST_CONTAINER_DOXYGEN_INVOKED)

//! This option specifies whether block data (value storage) is inlined
//! in each block (true) or allocated separately via the allocator (false).
//! Inline data can improve locality but increases block size.
//!
//!\tparam Enabled A boolean value.
BOOST_INTRUSIVE_OPTION_CONSTANT(store_data_in_block, bool, Enabled, store_data_in_block)

//! This option specifies if hardware prefetch instructions
//! are emitted during iteration and other traversal operations.
//! When enabled, prefetching can improve performance by reducing
//! cache misses. The default value is true.
//!
//!\tparam Enabled A boolean value. True to enable prefetching.
BOOST_INTRUSIVE_OPTION_CONSTANT(prefetch, bool, Enabled, prefetch)

//! Helper metafunction to combine options into a single type to be used
//! by \c boost::container::nest.
#if defined(BOOST_CONTAINER_DOXYGEN_INVOKED) || defined(BOOST_CONTAINER_VARIADIC_TEMPLATES)
template<class ...Options>
#else
template<class O1 = void, class O2 = void, class O3 = void, class O4 = void>
#endif
struct nest_options
{
   /// @cond
   typedef typename ::boost::intrusive::pack_options
      < nest_null_opt,
      #if !defined(BOOST_CONTAINER_VARIADIC_TEMPLATES)
      O1, O2, O3, O4
      #else
      Options...
      #endif
      >::type packed_options;
   typedef nest_opt<packed_options::store_data_in_block, packed_options::prefetch> implementation_defined;
   /// @endcond
   typedef implementation_defined type;
};

#if !defined(BOOST_NO_CXX11_TEMPLATE_ALIASES)

template<class ...Options>
using nest_options_t = typename boost::container::nest_options<Options...>::type;

#endif

#endif   //   !defined(BOOST_CONTAINER_DOXYGEN_INVOKED)

#ifndef BOOST_CONTAINER_DOXYGEN_INVOKED

template <class T
         ,class Allocator = void
         ,class Options = void>
class nest;

namespace nest_detail {

//////////////////////////////////////////////
//
//      pointer_rebind helper
//
//////////////////////////////////////////////

template<class Pointer, class T>
struct pointer_rebind
{
   typedef typename boost::intrusive::pointer_traits<Pointer>::
      template rebind_pointer<T>::type type;
};

//////////////////////////////////////////////
//
//      bit manipulation helpers
//
//////////////////////////////////////////////

inline int unchecked_countr_zero(boost::uint64_t x)
{
#if defined(BOOST_MSVC) && (defined(_M_X64) || defined(_M_ARM64))
   unsigned long r;
   _BitScanForward64(&r, x);
   return (int)r;
#elif defined(BOOST_GCC) || defined(BOOST_CLANG)
   return (int)__builtin_ctzll(x);
#else
   BOOST_CONTAINER_NEST_ASSUME(x != 0);
   return (int)boost::core::countr_zero(x);
#endif
}

BOOST_CONTAINER_FORCEINLINE int unchecked_countr_one(boost::uint64_t x)
{
   return unchecked_countr_zero(~x);
}

BOOST_CONTAINER_FORCEINLINE int unchecked_countl_zero(boost::uint64_t x)
{
#if defined(BOOST_MSVC) && (defined(_M_X64) || defined(_M_ARM64))
   unsigned long r;
   _BitScanReverse64(&r, x);
   return (int)(63 - r);
#elif defined(BOOST_GCC) || defined(BOOST_CLANG)
   return (int)__builtin_clzll(x);
#else
   BOOST_CONTAINER_NEST_ASSUME(x != 0);
   return (int)boost::core::countl_zero(x);
#endif
}

//////////////////////////////////////////////
//
//      find_if_not (C++03-compatible)
//
//////////////////////////////////////////////

template<class It, class Pred>
It find_if_not(It first, It last, Pred pred)
{
   for (; first != last; ++first)
      if (!pred(*first))
         break;
   return first;
}

//////////////////////////////////////////////
//
//      block_base / block
//
//////////////////////////////////////////////

template<class VoidPointer>
struct block_base
{
   typedef typename pointer_rebind<VoidPointer, block_base>::type       pointer;
   typedef typename pointer_rebind<VoidPointer, const block_base>::type const_pointer;
   typedef boost::uint64_t mask_type;

   BOOST_STATIC_CONSTEXPR std::size_t N = sizeof(mask_type)*CHAR_BIT;
   BOOST_STATIC_CONSTEXPR mask_type full = (mask_type)(-1);

   BOOST_CONTAINER_FORCEINLINE static pointer pointer_to(block_base& x) BOOST_NOEXCEPT
   {
      return boost::intrusive::pointer_traits<pointer>::pointer_to(x);
   }

   BOOST_CONTAINER_FORCEINLINE static const_pointer pointer_to(const block_base& x) BOOST_NOEXCEPT
   {
      return boost::intrusive::pointer_traits<const_pointer>::pointer_to(x);
   }

   block_base()
   {
      this->reset();
      mask = 1; /* sentinel */
   }

   BOOST_CONTAINER_FORCEINLINE void link_available_before(pointer p) BOOST_NOEXCEPT
   {
      next_available = p;
      prev_available = p->prev_available;
      pointer const pthis = pointer_to(*this);
      next_available->prev_available = pthis;
      prev_available->next_available = pthis;
   }

   BOOST_CONTAINER_FORCEINLINE void link_available_after(pointer p) BOOST_NOEXCEPT
   {
      prev_available = p;
      next_available = p->next_available;
      pointer const pthis = pointer_to(*this);
      next_available->prev_available = pthis;
      prev_available->next_available = pthis;
   }

   BOOST_CONTAINER_FORCEINLINE void unlink_available() BOOST_NOEXCEPT
   {
      prev_available->next_available = next_available;
      next_available->prev_available = prev_available;
   }

   BOOST_CONTAINER_FORCEINLINE void link_before(pointer p) BOOST_NOEXCEPT
   {
      next = p;
      prev = p->prev;
      pointer const pthis = pointer_to(*this);
      next->prev = pthis;
      prev->next = pthis;
   }

   BOOST_CONTAINER_FORCEINLINE void link_after(pointer p) BOOST_NOEXCEPT
   {
      prev = p;
      next = p->next;
      pointer const pthis = pointer_to(*this);
      next->prev = pthis;
      prev->next = pthis;
   }

   BOOST_CONTAINER_FORCEINLINE void unlink() BOOST_NOEXCEPT
   {
      prev->next = next;
      next->prev = prev;
   }

   BOOST_CONTAINER_FORCEINLINE pointer header() BOOST_NOEXCEPT
   {
      return pointer_to(*this);
   }

   BOOST_CONTAINER_FORCEINLINE const_pointer header() const BOOST_NOEXCEPT
   {
      return pointer_to(*this);
   }

   BOOST_CONTAINER_FORCEINLINE void link_at_back(pointer pb) BOOST_NOEXCEPT
   {
      pb->link_before(header());
   }

   BOOST_CONTAINER_FORCEINLINE void link_before(
      pointer pbx, pointer pby) BOOST_NOEXCEPT
   {
      pbx->link_before(pby);
   }

   BOOST_CONTAINER_FORCEINLINE static void unlink(pointer pb) BOOST_NOEXCEPT
   {
      pb->unlink();
   }

   BOOST_CONTAINER_FORCEINLINE void link_available_at_back(pointer pb) BOOST_NOEXCEPT
   {
      pb->link_available_before(header());
   }

   BOOST_CONTAINER_FORCEINLINE void link_available_at_front(pointer pb) BOOST_NOEXCEPT
   {
      pb->link_available_after(header());
   }

   BOOST_CONTAINER_FORCEINLINE void unlink_available(pointer pb) BOOST_NOEXCEPT
   {
      pb->unlink_available();
   }

   void reset() BOOST_NOEXCEPT
   {
      pointer const h = header();
      prev_available = h;
      next_available = h;
      prev = h;
      next = h;
   }

   block_base(BOOST_RV_REF(block_base) x) BOOST_NOEXCEPT
   {
      mask = 1; /* sentinel */
      this->operator=(boost::move(x));
   }

   block_base& operator=(BOOST_RV_REF(block_base) x) BOOST_NOEXCEPT
   {
      pointer const x_header = x.header();
      pointer const t_header = this->header();

      if (x.next_available != x_header) {
         prev_available = x.prev_available;
         next_available = x.next_available;
         next_available->prev_available = t_header;
         prev_available->next_available = t_header;
      }
      else {
         prev_available = t_header;
         next_available = t_header;
      }

      if (x.prev != x_header) {
         prev = x.prev;
         next = x.next;
         next->prev = t_header;
         prev->next = t_header;
      }
      else {
         prev = t_header;
         next = t_header;
      }

      x.reset();
      return *this;
   }

   pointer   prev_available;
   pointer   next_available;
   pointer   prev;
   pointer   next;
   mask_type mask;
private:
   BOOST_MOVABLE_BUT_NOT_COPYABLE(block_base)
};

template<class ValuePointer, bool StoreDataInBlock>
struct block
   : block_base<typename pointer_rebind<ValuePointer, void>::type>
{
   typedef block_base<typename pointer_rebind<ValuePointer, void>::type> block_base_type;
   typedef typename boost::intrusive::pointer_traits<ValuePointer>::element_type value_type;

   BOOST_CONTAINER_FORCEINLINE ValuePointer data() BOOST_NOEXCEPT { return data_; }
   BOOST_CONTAINER_FORCEINLINE void set_data_null() BOOST_NOEXCEPT { data_ = ValuePointer(); }

   block()
      : block_base_type(), data_()
   {}

   block(BOOST_RV_REF(block) x) BOOST_NOEXCEPT
      : block_base_type(BOOST_MOVE_BASE(block_base_type, x)), data_()
   {}

   block& operator=(BOOST_RV_REF(block) x) BOOST_NOEXCEPT
   {
      this->block_base_type::operator=(boost::move(x));
      return *this;
   }

   ValuePointer data_;
private:
   BOOST_MOVABLE_BUT_NOT_COPYABLE(block)
};

template<class ValuePointer>
struct block<ValuePointer, true>
   : block_base<typename pointer_rebind<ValuePointer, void>::type>
{
   typedef block_base<typename pointer_rebind<ValuePointer, void>::type> block_base_type;
   typedef typename boost::intrusive::pointer_traits<ValuePointer>::element_type value_type;

   BOOST_CONTAINER_FORCEINLINE ValuePointer data() BOOST_NOEXCEPT { return static_cast<ValuePointer>(static_cast<void*>(&data_stor)); }
   BOOST_CONTAINER_FORCEINLINE void set_data_null() BOOST_NOEXCEPT {}

   block()
      : block_base_type()
   {}

   block(BOOST_RV_REF(block) x) BOOST_NOEXCEPT
      : block_base_type(BOOST_MOVE_BASE(block_base_type, x))
   {}

   block& operator=(BOOST_RV_REF(block) x) BOOST_NOEXCEPT
   {
      this->block_base_type::operator=(boost::move(x));
      return *this;
   }

   typename dtl::aligned_storage<sizeof(value_type)*64u, dtl::alignment_of<value_type>::value>::type data_stor;
private:
   BOOST_MOVABLE_BUT_NOT_COPYABLE(block)
};

template<class ValuePointer, bool StoreDataInBlock>
void swap_payload(block<ValuePointer, StoreDataInBlock>& x, block<ValuePointer, StoreDataInBlock>& y) BOOST_NOEXCEPT;

template<class ValuePointer>
BOOST_CONTAINER_FORCEINLINE void swap_payload(block<ValuePointer, true>& x, block<ValuePointer, true>& y) BOOST_NOEXCEPT
{
   boost::adl_move_swap(x.mask, y.mask);
}

template<class ValuePointer>
BOOST_CONTAINER_FORCEINLINE void swap_payload(block<ValuePointer, false>& x, block<ValuePointer, false>& y) BOOST_NOEXCEPT
{
   boost::adl_move_swap(x.mask, y.mask);
   boost::adl_move_swap(x.data_, y.data_);
}

//////////////////////////////////////////////
//
//      iterator
//
//////////////////////////////////////////////

template<class ValuePointer, bool StoreDataInBlock, bool Prefetch>
class iterator
{
   typedef typename boost::intrusive::pointer_traits<ValuePointer>::element_type element_type;


public:
   typedef typename dtl::remove_const<element_type>::type                     value_type;
   typedef typename boost::intrusive::pointer_traits<ValuePointer>::difference_type difference_type;
   typedef ValuePointer                                                       pointer;
   typedef element_type&                                                      reference;
   typedef std::bidirectional_iterator_tag                                    iterator_category;

   struct nat
   {
      nat() : pbb(), n() {}
      pointer pbb;
      int n;
   };

   typedef typename nest_detail::pointer_rebind<pointer, value_type>::type maybe_nonconst_pointer;

   typedef typename dtl::if_c< boost::move_detail::is_const<element_type>::value
                             , iterator< maybe_nonconst_pointer, StoreDataInBlock, Prefetch >
                             , nat>::type                            maybe_nonconst_iterator;

   iterator() BOOST_NOEXCEPT
      : pbb(), n(0)
   {}
   
   iterator(const iterator& x) BOOST_NOEXCEPT
      : pbb(x.pbb), n(x.n)
   {}
   
   iterator(const maybe_nonconst_iterator& x) BOOST_NOEXCEPT
      : pbb(x.pbb), n(x.n)
   {}

   iterator& operator=(const iterator& x) BOOST_NOEXCEPT
   {
      pbb = x.pbb;
      n = x.n;
      return *this;
   }

   iterator& operator=(const maybe_nonconst_iterator& x) BOOST_NOEXCEPT
   {
      pbb = x.pbb;
      n = x.n;
      return *this;
   }

   BOOST_CONTAINER_FORCEINLINE pointer operator->() const BOOST_NOEXCEPT
   {
      return static_cast<block_type&>(*pbb).data() + n;
   }

   BOOST_CONTAINER_FORCEINLINE reference operator*() const BOOST_NOEXCEPT
   {
      return *operator->();
   }

   BOOST_CONTAINER_FORCEINLINE iterator& operator++() BOOST_NOEXCEPT
   {
      mask_type m = pbb->mask & (full << 1 << std::size_t(n));
      if(BOOST_UNLIKELY(m == 0)) {
         pbb = pbb->next;
         BOOST_IF_CONSTEXPR(Prefetch) {
            BOOST_CONTAINER_NEST_PREFETCH_BLOCK(pbb->next, block_type);
         }
         m = pbb->mask;
      }
      n = nest_detail::unchecked_countr_zero(m);
      return *this;
   }

   BOOST_CONTAINER_FORCEINLINE iterator operator++(int) BOOST_NOEXCEPT
   {
      iterator tmp(*this);
      this->operator++();
      return tmp;
   }

   BOOST_CONTAINER_FORCEINLINE iterator& operator--() BOOST_NOEXCEPT
   {
      mask_type m = pbb->mask & (full >> 1 >> (N - 1 - std::size_t(n)));
      if(BOOST_UNLIKELY(m == 0)) {
         pbb = pbb->prev;
         BOOST_IF_CONSTEXPR(Prefetch) {
            BOOST_CONTAINER_NEST_PREFETCH_BLOCK(pbb->prev, block_type);
         }
         m = pbb->mask;
      }
      n = int(N - 1 - (std::size_t)nest_detail::unchecked_countl_zero(m));
      return *this;
   }

   BOOST_CONTAINER_FORCEINLINE iterator operator--(int) BOOST_NOEXCEPT
   {
      iterator tmp(*this);
      this->operator--();
      return tmp;
   }

   friend bool operator==(const iterator& x, const iterator& y) BOOST_NOEXCEPT
   {
      return x.pbb == y.pbb && x.n == y.n;
   }

   friend bool operator!=(const iterator& x, const iterator& y) BOOST_NOEXCEPT
   {
      return !(x == y);
   }

private:
   template<class, bool, bool> friend class iterator;
   template<class, class, class> friend class boost::container::nest;

   typedef typename pointer_rebind<ValuePointer, void>::type              void_pointer;
   typedef nest_detail::block_base<void_pointer>                           block_base_type;
   typedef typename pointer_rebind<ValuePointer, block_base_type>::type   block_base_pointer;
   typedef typename pointer_rebind<ValuePointer, const block_base_type>::type const_block_base_pointer;
   typedef typename pointer_rebind<ValuePointer, value_type>::type        nonconst_pointer;
   typedef nest_detail::block<nonconst_pointer, StoreDataInBlock>                block_type;
   typedef typename block_base_type::mask_type                            mask_type;

   BOOST_STATIC_CONSTEXPR std::size_t  N = block_base_type::N;
   BOOST_STATIC_CONSTEXPR mask_type full = block_base_type::full;

   iterator(const_block_base_pointer pbb_, int n_) BOOST_NOEXCEPT
      : pbb(const_cast_block_base_pointer(pbb_)), n(n_)
   {}

   explicit iterator(const_block_base_pointer pbb_) BOOST_NOEXCEPT
      : pbb(const_cast_block_base_pointer(pbb_))
      , n(nest_detail::unchecked_countr_zero(pbb->mask))
   {}

   static block_base_pointer
   const_cast_block_base_pointer(const_block_base_pointer pbb_) BOOST_NOEXCEPT
   {
      return block_base_type::pointer_to(const_cast<block_base_type&>(*pbb_));
   }

   block_base_pointer pbb;
   int                n;
};

//////////////////////////////////////////////
//
//      sort_iterator
//
//////////////////////////////////////////////

template<class T, std::size_t N>
struct sort_iterator
{
   typedef T                                 value_type;
   typedef std::ptrdiff_t                    difference_type;
   typedef T*                                pointer;
   typedef T&                                reference;
   typedef std::random_access_iterator_tag   iterator_category;

   sort_iterator(T** pp_, std::size_t index_)
      : pp(pp_), index(index_)
   {}

   pointer operator->() const BOOST_NOEXCEPT
   { return pp[index / N] + (index % N); }

   reference operator*() const BOOST_NOEXCEPT
   { return *operator->(); }

   sort_iterator& operator++() BOOST_NOEXCEPT       { ++index; return *this; }
   sort_iterator  operator++(int) BOOST_NOEXCEPT     { sort_iterator t(*this); ++index; return t; }
   sort_iterator& operator--() BOOST_NOEXCEPT        { --index; return *this; }
   sort_iterator  operator--(int) BOOST_NOEXCEPT     { sort_iterator t(*this); --index; return t; }

   friend difference_type
   operator-(const sort_iterator& x, const sort_iterator& y) BOOST_NOEXCEPT
   { return (difference_type)(x.index - y.index); }

   sort_iterator& operator+=(difference_type d) BOOST_NOEXCEPT
   { index += std::size_t(d); return *this; }

   friend sort_iterator
   operator+(const sort_iterator& x, difference_type d) BOOST_NOEXCEPT
   { return sort_iterator(x.pp, x.index + static_cast<std::size_t>(d)); }

   friend sort_iterator
   operator+(difference_type d, const sort_iterator& x) BOOST_NOEXCEPT
   { return sort_iterator(x.pp, d + x.index); }

   sort_iterator& operator-=(difference_type d) BOOST_NOEXCEPT
   { index -= std::size_t(d); return *this; }

   friend sort_iterator
   operator-(const sort_iterator& x, difference_type d) BOOST_NOEXCEPT
   { return sort_iterator(x.pp, x.index - static_cast<std::size_t>(d)); }

   reference operator[](difference_type d) const BOOST_NOEXCEPT
   { return *(*this + d); }

   friend bool operator==(const sort_iterator& x, const sort_iterator& y) BOOST_NOEXCEPT
   { return x.index == y.index; }
   friend bool operator!=(const sort_iterator& x, const sort_iterator& y) BOOST_NOEXCEPT
   { return x.index != y.index; }
   friend bool operator< (const sort_iterator& x, const sort_iterator& y) BOOST_NOEXCEPT
   { return x.index <  y.index; }
   friend bool operator> (const sort_iterator& x, const sort_iterator& y) BOOST_NOEXCEPT
   { return x.index >  y.index; }
   friend bool operator<=(const sort_iterator& x, const sort_iterator& y) BOOST_NOEXCEPT
   { return x.index <= y.index; }
   friend bool operator>=(const sort_iterator& x, const sort_iterator& y) BOOST_NOEXCEPT
   { return x.index >= y.index; }

   T**         pp;
   std::size_t index;
};

//////////////////////////////////////////////
//
//      RAII helpers (replacing unique_ptr)
//
//////////////////////////////////////////////

template<class T, class Allocator>
struct buffer
{
   typedef boost::container::allocator_traits<Allocator> alloc_traits;

   buffer(std::size_t n_, Allocator al_)
      : al(al_), begin_idx(0), end_idx(0), cap(0), data(0)
   {
      data = static_cast<T*>(::operator new(n_ * sizeof(T), std::nothrow));
      if(data) cap = n_;
   }

   ~buffer()
   {
      if(data) {
         for(; begin_idx != end_idx; ++begin_idx) {
            alloc_traits::destroy(al, data + begin_idx);
         }
         ::operator delete(static_cast<void*>(data));
      }
   }

   T* begin() const BOOST_NOEXCEPT { return data + begin_idx; }
   T* end()   const BOOST_NOEXCEPT { return data + end_idx; }

   void push_back_move(T& v)
   {
      BOOST_ASSERT(data && end_idx != cap);
      alloc_traits::construct(al, data + end_idx, boost::move(v));
      ++end_idx;
   }

   void erase_front() BOOST_NOEXCEPT
   {
      BOOST_ASSERT(data && begin_idx != end_idx);
      alloc_traits::destroy(al, data + begin_idx);
      ++begin_idx;
   }

   Allocator   al;
   std::size_t begin_idx;
   std::size_t end_idx;
   std::size_t cap;
   T*          data;

private:
   buffer(const buffer&);
   buffer& operator=(const buffer&);
};

// RAII wrapper for raw memory (replaces unique_ptr with nodtor_deleter)
struct raw_memory_holder
{
   explicit raw_memory_holder(void* p_) BOOST_NOEXCEPT : p(p_) {}
   ~raw_memory_holder() { if(p) ::operator delete(p); }
   void release() BOOST_NOEXCEPT { p = 0; }
   void* get() const BOOST_NOEXCEPT { return p; }
   void* p;
private:
   raw_memory_holder(const raw_memory_holder&);
   raw_memory_holder& operator=(const raw_memory_holder&);
};

//////////////////////////////////////////////
//
//    allocator propagation helpers
//
//////////////////////////////////////////////

template<class T>
void copy_assign_if(dtl::true_type, T& x, const T& y) { x = y; }

template<class T>
void copy_assign_if(dtl::false_type, T&, const T&) {}

template<class T>
void move_assign_if(dtl::true_type, T& x, T& y) { x = boost::move(y); }

template<class T>
void move_assign_if(dtl::false_type, T&, T&) {}

//////////////////////////////////////////////
//
//    block_typedefs
//
//////////////////////////////////////////////

template<class ValueAllocator, bool StoreDataInBlock>
struct block_typedefs
{
   typedef boost::container::allocator_traits<ValueAllocator>   val_alloc_traits;
   typedef typename val_alloc_traits::pointer                   value_pointer;
   typedef typename pointer_rebind<value_pointer, void>::type   void_pointer;

   typedef nest_detail::block_base<void_pointer>                 block_base_t;
   typedef typename pointer_rebind<
      value_pointer, block_base_t>::type                        block_base_pointer;
   typedef typename pointer_rebind<
      value_pointer, const block_base_t>::type                  const_block_base_pointer;

   typedef nest_detail::block<value_pointer, StoreDataInBlock>        block_t;
   typedef typename pointer_rebind<
      value_pointer, block_t>::type                             block_pointer;

   typedef typename val_alloc_traits::
      template portable_rebind_alloc<block_t>::type             block_allocator;
};

//////////////////////////////////////////////
//
//    predicate adaptor for unique()
//
//////////////////////////////////////////////

template<class T, class BinaryPredicate>
struct unique_pred_adaptor
{
   const T*         value_ptr;
   BinaryPredicate& pred;
   unique_pred_adaptor(const T* v, BinaryPredicate& p) : value_ptr(v), pred(p) {}
   bool operator()(const T& x) const { return pred(x, *value_ptr); }
};

//////////////////////////////////////////////
//
//    sort_proxy_comparator
//
//////////////////////////////////////////////

template<class T, class Compare>
struct sort_proxy_comparator
{
   Compare& comp;
   explicit sort_proxy_comparator(Compare& c) : comp(c) {}

   template<class SortProxy>
   bool operator()(const SortProxy& x, const SortProxy& y) const
   {
      return comp(
         const_cast<const T&>(*x.p),
         const_cast<const T&>(*y.p));
   }
};

//////////////////////////////////////////////
//
//    visit_to_visit_while adaptor
//
//////////////////////////////////////////////

template<class F, class T>
struct visit_adaptor
{
   F& f;
   explicit visit_adaptor(F& f_) : f(f_) {}
   bool operator()(T& x) const { f(x); return true; }
};

template<class F, class T>
struct const_conditional_visit_adaptor
{
   F& f;
   explicit const_conditional_visit_adaptor(F& f_) : f(f_) {}
   bool operator()(const T& x) const { return f(x); }
};

} // namespace nest_detail

#ifndef BOOST_CONTAINER_DOXYGEN_INVOKED

template<class Options>
struct get_nest_opt
{
   typedef nest_opt<Options::store_data_in_block, Options::prefetch> type;
};

template<>
struct get_nest_opt<void>
{
   typedef nest_null_opt type;
};

template<bool B, bool P>
struct get_nest_opt<nest_opt<B, P> >
{
   typedef nest_opt<B, P> type;
};

#endif // BOOST_CONTAINER_DOXYGEN_INVOKED

//////////////////////////////////////////////////////////////////////////////
//
//                                nest
//
//////////////////////////////////////////////////////////////////////////////

//! A nest is a node-like container with a fixed block size of 64 elements
//! and bitmask-based slot management. It provides stable iterators and
//! efficient insertion and erasure operations in constant time.
//!
//! \tparam T The type of object stored in the nest
//! \tparam Allocator The allocator used for all internal memory management, use void
//!   for the default allocator
//! \tparam Options A type produced from \c boost::container::nest_options (e.g. \c store_data_in_block).
#ifdef BOOST_CONTAINER_DOXYGEN_INVOKED
template <class T, class Allocator = void, class Options = void>
#else
template <class T, class Allocator, class Options>
#endif
class nest
   : private boost::empty_value<
        typename nest_detail::block_typedefs<
           typename real_allocator<T, Allocator>::type
         , get_nest_opt<Options>::type::store_data_in_block
        >::block_allocator, 0>
{
   #ifndef BOOST_CONTAINER_DOXYGEN_INVOKED
   typedef typename real_allocator<T, Allocator>::type             ValueAllocator;
   typedef typename get_nest_opt<Options>::type                    options_type;
   BOOST_STATIC_CONSTEXPR bool store_data_in_block = options_type::store_data_in_block;
   BOOST_STATIC_CONSTEXPR bool prefetch_enabled    = options_type::prefetch;
   typedef boost::container::allocator_traits<ValueAllocator>      allocator_traits_type;
   typedef nest_detail::block_typedefs<ValueAllocator, store_data_in_block> btd;
   typedef typename btd::block_base_t                              block_base;
   typedef typename btd::block_base_pointer                        block_base_pointer;
   typedef typename btd::const_block_base_pointer                  const_block_base_pointer;
   typedef typename btd::block_t                                   block;
   typedef typename btd::block_pointer                             block_pointer;
   typedef typename btd::block_allocator                           block_allocator;
   typedef boost::empty_value<block_allocator, 0>                  allocator_base;
   typedef typename block_base::mask_type                          mask_type;
   typedef boost::container::allocator_traits<block_allocator>     block_alloc_traits;

   BOOST_STATIC_CONSTEXPR std::size_t     N    = block_base::N;
   BOOST_STATIC_CONSTEXPR mask_type full = block_base::full;

   BOOST_COPYABLE_AND_MOVABLE(nest)
   #endif // BOOST_CONTAINER_DOXYGEN_INVOKED

   public:
   //////////////////////////////////////////////
   //
   //                    types
   //
   //////////////////////////////////////////////

   typedef T                                                                value_type;
   typedef ValueAllocator                                                   allocator_type;
   typedef typename allocator_traits_type::pointer                          pointer;
   typedef typename allocator_traits_type::const_pointer                    const_pointer;
   typedef T&                                                               reference;
   typedef const T&                                                         const_reference;
   typedef typename allocator_traits_type::size_type                        size_type;
   typedef typename allocator_traits_type::difference_type                  difference_type;
   typedef BOOST_CONTAINER_IMPDEF(nest_detail::iterator<pointer BOOST_MOVE_I store_data_in_block BOOST_MOVE_I prefetch_enabled>)            iterator;
   typedef BOOST_CONTAINER_IMPDEF(nest_detail::iterator<const_pointer BOOST_MOVE_I store_data_in_block BOOST_MOVE_I prefetch_enabled>)      const_iterator;
   typedef BOOST_CONTAINER_IMPDEF(boost::container::reverse_iterator<iterator>)       reverse_iterator;
   typedef BOOST_CONTAINER_IMPDEF(boost::container::reverse_iterator<const_iterator>) const_reverse_iterator;

   //////////////////////////////////////////////
   //
   //          construct/copy/destroy
   //
   //////////////////////////////////////////////

   //! <b>Effects</b>: Default constructs a nest.
   //!
   //! <b>Throws</b>: If allocator_type's default constructor throws.
   //!
   //! <b>Complexity</b>: Constant.
   nest() BOOST_NOEXCEPT_IF(dtl::is_nothrow_default_constructible<ValueAllocator>::value)
      : allocator_base(boost::empty_init_t())
      , blist()
      , num_blocks(0)
      , size_(0)
   {}

   //! <b>Effects</b>: Constructs a nest taking the allocator as parameter.
   //!
   //! <b>Throws</b>: Nothing.
   //!
   //! <b>Complexity</b>: Constant.
   explicit nest(const allocator_type& a) BOOST_NOEXCEPT_OR_NOTHROW
      : allocator_base(boost::empty_init_t(), block_allocator(a))
      , blist()
      , num_blocks(0)
      , size_(0)
   {}

   //! <b>Effects</b>: Constructs a nest and inserts n value-initialized elements.
   //!
   //! <b>Throws</b>: If allocator_type's default constructor
   //!   throws or T's default constructor throws.
   //!
   //! <b>Complexity</b>: Linear to n.
   explicit nest(size_type n, const allocator_type& a = allocator_type())
      : allocator_base(boost::empty_init_t(), block_allocator(a))
      , blist()
      , num_blocks(0)
      , size_(0)
   {
      priv_insert_n_default(n);
   }

   //! <b>Effects</b>: Constructs a nest and inserts n copies of x.
   //!
   //! <b>Throws</b>: If allocator_type's default constructor
   //!   throws or T's copy constructor throws.
   //!
   //! <b>Complexity</b>: Linear to n.
   nest(size_type n, const T& x, const allocator_type& a = allocator_type())
      : allocator_base(boost::empty_init_t(), block_allocator(a))
      , blist()
      , num_blocks(0)
      , size_(0)
   {
      insert(n, x);
   }

   //! <b>Effects</b>: Constructs a nest and inserts a copy of [first, last).
   //!
   //! <b>Throws</b>: If allocator_type's default constructor
   //!   throws or T's constructor taking a dereferenced InpIt throws.
   //!
   //! <b>Complexity</b>: Linear to the range [first, last).
   template<class InpIt>
   nest(InpIt first, InpIt last
      ,const allocator_type& a = allocator_type()
      #if !defined(BOOST_CONTAINER_DOXYGEN_INVOKED)
      , typename dtl::disable_if_convertible<InpIt, size_type>::type* = 0
      #endif
      )
      : allocator_base(boost::empty_init_t(), block_allocator(a))
      , blist()
      , num_blocks(0)
      , size_(0)
   {
      insert(first, last);
   }

   //! <b>Effects</b>: Copy constructs a nest.
   //!
   //! <b>Postcondition</b>: x == *this.
   //!
   //! <b>Throws</b>: If allocator_type's copy constructor throws.
   //!
   //! <b>Complexity</b>: Linear to the elements x contains.
   nest(const nest& x)
      : allocator_base(boost::empty_init_t(), block_allocator(
           allocator_traits_type::select_on_container_copy_construction(x.priv_alloc())))
      , blist()
      , num_blocks(0)
      , size_(0)
   {
      insert(x.begin(), x.end());
   }

   //! <b>Effects</b>: Move constructor. Moves x's resources to *this.
   //!
   //! <b>Throws</b>: Nothing.
   //!
   //! <b>Complexity</b>: Constant.
   nest(BOOST_RV_REF(nest) x) BOOST_NOEXCEPT_OR_NOTHROW
      : allocator_base(boost::empty_init_t(), boost::move(x.al()))
      , blist(boost::move(x.blist))
      , num_blocks(x.num_blocks)
      , size_(x.size_)
   {
      x.num_blocks = 0;
      x.size_ = 0;
   }

   //! <b>Effects</b>: Copy constructs a nest using the specified allocator.
   //!
   //! <b>Postcondition</b>: x == *this.
   //!
   //! <b>Throws</b>: If allocator_type's copy constructor throws.
   //!
   //! <b>Complexity</b>: Linear to the elements x contains.
   nest(const nest& x, const allocator_type& a)
      : allocator_base(boost::empty_init_t(), block_allocator(a))
      , blist()
      , num_blocks(0)
      , size_(0)
   {
      insert(x.begin(), x.end());
   }

   //! <b>Effects</b>: Move constructor using the specified allocator.
   //!                 Moves x's resources to *this.
   //!
   //! <b>Throws</b>: If allocation or value_type's copy constructor throws.
   //!
   //! <b>Complexity</b>: Constant if a == x.get_allocator(), linear otherwise.
   nest(BOOST_RV_REF(nest) x, const allocator_type& a)
      : allocator_base(boost::empty_init_t(), block_allocator(a))
      , blist()
      , num_blocks(0)
      , size_(0)
   {
      if(al() == x.al()){
         blist = boost::move(x.blist);
         num_blocks = x.num_blocks;
         size_ = x.size_;
         x.num_blocks = 0;
         x.size_ = 0;
      }
      else{
         priv_insert_range_move(x.begin(), x.end());
      }
   }

   #if !defined(BOOST_NO_CXX11_HDR_INITIALIZER_LIST)
   //! <b>Effects</b>: Constructs a nest and inserts elements from il.
   //!
   //! <b>Complexity</b>: Linear to the range [il.begin(), il.end()).
   nest(std::initializer_list<value_type> il, const allocator_type& a = allocator_type())
      : allocator_base(boost::empty_init_t(), block_allocator(a))
      , blist()
      , num_blocks(0)
      , size_(0)
   {
      insert(il.begin(), il.end());
   }
   #endif

   //! <b>Effects</b>: Destroys the nest. All stored values are destroyed
   //!   and used memory is deallocated.
   //!
   //! <b>Throws</b>: Nothing.
   //!
   //! <b>Complexity</b>: Linear to the number of elements.
   ~nest() BOOST_NOEXCEPT_OR_NOTHROW
   {
      priv_reset();
   }

   //! <b>Effects</b>: Makes *this contain the same elements as x.
   //!
   //! <b>Throws</b>: If memory allocation throws or T's copy constructor throws.
   //!
   //! <b>Complexity</b>: Linear to the number of elements in x.
   nest& operator=(BOOST_COPY_ASSIGN_REF(nest) x)
   {
      if (BOOST_LIKELY(this != &x)) {
         dtl::bool_<allocator_traits_type::
            propagate_on_container_copy_assignment::value> pocca;
         if(priv_alloc() != x.priv_alloc() && pocca.value) {
            priv_reset();
            nest_detail::copy_assign_if(pocca, al(), x.al());
            insert(x.begin(), x.end());
         }
         else{
            nest_detail::copy_assign_if(pocca, al(), x.al());
            assign(x.begin(), x.end());
         }
      }
      return *this;
   }

   //! <b>Effects</b>: Move assignment. All x's values are transferred to *this.
   //!
   //! <b>Throws</b>: If allocator_traits_type::propagate_on_container_move_assignment
   //!   is false and (allocation throws or value_type's move constructor throws).
   //!
   //! <b>Complexity</b>: Constant if allocator_traits_type::
   //!   propagate_on_container_move_assignment is true or
   //!   this->get_allocator() == x.get_allocator(). Linear otherwise.
   nest& operator=(BOOST_RV_REF(nest) x)
      BOOST_NOEXCEPT_IF(allocator_traits_type::propagate_on_container_move_assignment::value
                                  || allocator_traits_type::is_always_equal::value)
   {
      if (BOOST_LIKELY(this != &x)) {
         BOOST_STATIC_CONSTEXPR bool can_steal =
            allocator_traits_type::propagate_on_container_move_assignment::value ||
            allocator_traits_type::is_always_equal::value;
         priv_move_assign(x, dtl::bool_<can_steal>());
      }
      return *this;
   }

   #if !defined(BOOST_NO_CXX11_HDR_INITIALIZER_LIST)
   //! <b>Effects</b>: Assigns the elements of il to *this.
   //!
   //! <b>Complexity</b>: Linear.
   nest& operator=(std::initializer_list<value_type> il)
   {
      assign(il.begin(), il.end());
      return *this;
   }
   #endif

   //! <b>Effects</b>: Assigns [first, last) to *this.
   //!
   //! <b>Complexity</b>: Linear.
   template<class InpIt>
   void assign(InpIt first, InpIt last
      #if !defined(BOOST_CONTAINER_DOXYGEN_INVOKED)
      , typename dtl::disable_if_convertible<InpIt, size_type>::type* = 0
      #endif
      )
   {
      priv_range_assign(first, last);
   }

   //! <b>Effects</b>: Assigns n copies of val to *this.
   //!
   //! <b>Complexity</b>: Linear.
   void assign(size_type n, const T& val)
   {
      priv_assign_n(n, val);
   }

   #if !defined(BOOST_NO_CXX11_HDR_INITIALIZER_LIST)
   //! <b>Effects</b>: Assigns the elements of il to *this.
   //!
   //! <b>Complexity</b>: Linear.
   void assign(std::initializer_list<value_type> il)
   { assign(il.begin(), il.end()); }
   #endif

   //! <b>Effects</b>: Returns a copy of the allocator.
   //!
   //! <b>Throws</b>: Nothing.
   //!
   //! <b>Complexity</b>: Constant.
   allocator_type get_allocator() const BOOST_NOEXCEPT_OR_NOTHROW
   { return allocator_type(al()); }

   //////////////////////////////////////////////
   //
   //                iterators
   //
   //////////////////////////////////////////////

   //! <b>Effects</b>: Returns an iterator to the first element.
   //!
   //! <b>Complexity</b>: Constant.
   iterator               begin()        BOOST_NOEXCEPT { return ++end(); }
   //! <b>Effects</b>: Returns a const_iterator to the first element.
   const_iterator         begin()  const BOOST_NOEXCEPT { return ++end(); }
   //! <b>Effects</b>: Returns an iterator to the end.
   iterator               end()          BOOST_NOEXCEPT { return iterator(blist.header(), 0); }
   //! <b>Effects</b>: Returns a const_iterator to the end.
   const_iterator         end()    const BOOST_NOEXCEPT { return const_iterator(blist.header(), 0); }
   //! <b>Effects</b>: Returns a reverse_iterator to the rbegin.
   reverse_iterator       rbegin()       BOOST_NOEXCEPT { return reverse_iterator(end()); }
   //! <b>Effects</b>: Returns a const_reverse_iterator to the rbegin.
   const_reverse_iterator rbegin() const BOOST_NOEXCEPT { return const_reverse_iterator(end()); }
   //! <b>Effects</b>: Returns a reverse_iterator to the rend.
   reverse_iterator       rend()         BOOST_NOEXCEPT { return reverse_iterator(begin()); }
   //! <b>Effects</b>: Returns a const_reverse_iterator to the rend.
   const_reverse_iterator rend()   const BOOST_NOEXCEPT { return const_reverse_iterator(begin()); }
   //! <b>Effects</b>: Returns a const_iterator to the first element.
   const_iterator         cbegin() const BOOST_NOEXCEPT { return begin(); }
   //! <b>Effects</b>: Returns a const_iterator to the end.
   const_iterator         cend()   const BOOST_NOEXCEPT { return end(); }
   //! <b>Effects</b>: Returns a const_reverse_iterator to the rbegin.
   const_reverse_iterator crbegin()const BOOST_NOEXCEPT { return rbegin(); }
   //! <b>Effects</b>: Returns a const_reverse_iterator to the rend.
   const_reverse_iterator crend()  const BOOST_NOEXCEPT { return rend(); }

   //////////////////////////////////////////////
   //
   //                capacity
   //
   //////////////////////////////////////////////

   //! <b>Effects</b>: Returns true if the nest contains no elements.
   //!
   //! <b>Complexity</b>: Constant.
   bool      empty()    const BOOST_NOEXCEPT { return size_ == 0; }

   //! <b>Effects</b>: Returns the number of elements.
   //!
   //! <b>Complexity</b>: Constant.
   size_type size()     const BOOST_NOEXCEPT { return size_; }

   //! <b>Effects</b>: Returns the maximum possible number of elements.
   //!
   //! <b>Complexity</b>: Constant.
   size_type max_size() const BOOST_NOEXCEPT
   {
      std::size_t bs = (std::size_t)block_alloc_traits::max_size(al()) * sizeof(block);
      allocator_type val_al(al());
      std::size_t vs = (std::size_t)allocator_traits_type::max_size(val_al) * sizeof(T);
      return (size_type)((std::min)(bs, vs) / (sizeof(block) + sizeof(T) * N) * N);
   }

   //! <b>Effects</b>: Returns the total number of slots (used and unused).
   //!
   //! <b>Complexity</b>: Constant.
   size_type capacity() const BOOST_NOEXCEPT { return num_blocks * N; }

   //! <b>Effects</b>: Reserves space for at least n elements.
   //!
   //! <b>Complexity</b>: Linear.
   void reserve(size_type n)
   {
      while(capacity() < n) (void)priv_create_new_available_block();
   }

   //! <b>Effects</b>: Compacts elements and removes unused blocks.
   //!
   //! <b>Complexity</b>: Linear.
   void shrink_to_fit()
   {
      priv_compact_noop_track();
      trim_capacity();
   }

   //! <b>Effects</b>: Releases all reserved (empty) blocks.
   //!
   //! <b>Complexity</b>: Linear on the number of available blocks.
   void trim_capacity() BOOST_NOEXCEPT { trim_capacity(0); }

   //! <b>Effects</b>: Releases reserved blocks until capacity() <= n.
   //!
   //! <b>Complexity</b>: Linear on the number of available blocks.
   void trim_capacity(size_type n) BOOST_NOEXCEPT
   {
      block_base_pointer pbb = blist.header()->next_available;
      while(capacity() > n && pbb != blist.header()) {
         block_pointer pb = static_cast_block_pointer(pbb);
         pbb = pbb->next_available;
         if(pb->mask == 0) {
            blist.unlink_available(pb);
            priv_delete_block(pb);
            --num_blocks;
         }
      }
   }

   //////////////////////////////////////////////
   //
   //                modifiers
   //
   //////////////////////////////////////////////

   #if !defined(BOOST_NO_CXX11_VARIADIC_TEMPLATES)

   //! <b>Effects</b>: Inserts an element constructed in-place with args.
   //!
   //! <b>Returns</b>: An iterator to the inserted element.
   //!
   //! <b>Complexity</b>: Constant (amortized).
   template<class ...Args>
   inline iterator emplace(BOOST_FWD_REF(Args)... args)
   {
      int n;
      block_pointer const pb = priv_retrieve_available_block(n);
      block_alloc_traits::construct(
         al(), boost::movelib::to_raw_pointer(pb->data() + n),
         boost::forward<Args>(args)...);
      pb->mask |= pb->mask + 1;
      const mask_type m = pb->mask;
      if(BOOST_UNLIKELY(m + 1 <= 2)) {
         if(m == 1) blist.link_at_back(pb);
         else       blist.unlink_available(pb);
      }
      ++size_;
      return iterator(pb, n);
   }

   //! <b>Effects</b>: Inserts an element constructed in-place with args (hint ignored).
   //!
   //! <b>Returns</b>: An iterator to the inserted element.
   //!
   //! <b>Complexity</b>: Constant (amortized).
   template<class ...Args>
   BOOST_CONTAINER_FORCEINLINE iterator emplace_hint(const_iterator, BOOST_FWD_REF(Args)... args)
   { return emplace(boost::forward<Args>(args)...); }

   #else // BOOST_NO_CXX11_VARIADIC_TEMPLATES

   #define BOOST_CONTAINER_NEST_EMPLACE_CODE(N) \
   BOOST_MOVE_TMPL_LT##N BOOST_MOVE_CLASS##N BOOST_MOVE_GT##N \
   BOOST_CONTAINER_FORCEINLINE iterator emplace(BOOST_MOVE_UREF##N)         \
   {                                                                         \
      int n_;                                                                \
      block_pointer pb = priv_retrieve_available_block(n_);                  \
      block_alloc_traits::construct(                                         \
         al(), boost::movelib::to_raw_pointer(pb->data() + n_)              \
         BOOST_MOVE_I##N BOOST_MOVE_FWD##N);                                \
      pb->mask |= pb->mask + 1;                                             \
      if(BOOST_UNLIKELY(pb->mask + 1 <= 2)) {                               \
         if(pb->mask == 1) blist.link_at_back(pb);                          \
         else              blist.unlink_available(pb);                       \
      }                                                                      \
      ++size_;                                                               \
      return iterator(pb, n_);                                               \
   }                                                                         \
   \
   BOOST_MOVE_TMPL_LT##N BOOST_MOVE_CLASS##N BOOST_MOVE_GT##N \
   BOOST_CONTAINER_FORCEINLINE iterator emplace_hint(const_iterator BOOST_MOVE_I##N BOOST_MOVE_UREF##N) \
   {  return emplace(BOOST_MOVE_FWD##N);  }                                 \
   //
   BOOST_MOVE_ITERATE_0TO9(BOOST_CONTAINER_NEST_EMPLACE_CODE)
   #undef BOOST_CONTAINER_NEST_EMPLACE_CODE

   #endif // BOOST_NO_CXX11_VARIADIC_TEMPLATES

   //! <b>Effects</b>: Inserts a copy of x.
   //!
   //! <b>Returns</b>: An iterator to the inserted element.
   //!
   //! <b>Complexity</b>: Constant (amortized).
   BOOST_CONTAINER_FORCEINLINE iterator insert(const T& x)
   { return emplace(x); }

   //! <b>Effects</b>: Inserts a copy of x (hint ignored).
   BOOST_CONTAINER_FORCEINLINE iterator insert(const_iterator, const T& x)
   { return emplace(x); }

   //! <b>Effects</b>: Inserts x by moving.
   //!
   //! <b>Returns</b>: An iterator to the inserted element.
   BOOST_CONTAINER_FORCEINLINE iterator insert(BOOST_RV_REF(value_type) x)
   { return emplace(boost::move(x)); }

   //! <b>Effects</b>: Inserts x by moving (hint ignored).
   BOOST_CONTAINER_FORCEINLINE iterator insert(const_iterator, BOOST_RV_REF(value_type) x)
   { return emplace(boost::move(x)); }

   #if !defined(BOOST_NO_CXX11_HDR_INITIALIZER_LIST)
   //! <b>Effects</b>: Inserts elements from il.
   //!
   //! <b>Complexity</b>: Linear.
   void insert(std::initializer_list<value_type> il)
   { insert(il.begin(), il.end()); }
   #endif

   //! <b>Effects</b>: Inserts copies of elements in [first, last).
   //!
   //! <b>Complexity</b>: Linear.
   template<class InpIt>
   void insert(InpIt first, InpIt last
      #if !defined(BOOST_CONTAINER_DOXYGEN_INVOKED)
      , typename dtl::disable_if_convertible<InpIt, size_type>::type* = 0
      #endif
      )
   {
      priv_insert_range_copy(first, last);
   }

   //! <b>Effects</b>: Inserts n copies of x.
   //!
   //! <b>Complexity</b>: Linear.
   void insert(size_type n, const T& x)
   {
      priv_insert_n_copies(n, x);
   }

   //! <b>Effects</b>: Erases the element at position pos.
   //!
   //! <b>Returns</b>: An iterator to the element after the erased one.
   //!
   //! <b>Complexity</b>: Constant.
   BOOST_CONTAINER_FORCEINLINE iterator erase(const_iterator pos)
   {
      block_base_pointer pbb = pos.pbb;
      int n = pos.n;
      ++pos;
      priv_erase_impl(pbb, n);
      return iterator(pos.pbb, pos.n);
   }

   //! <b>Effects</b>: Erases the element at pos without returning iterator.
   //!   Potentially faster than erase().
   //!
   //! <b>Complexity</b>: Constant.
   BOOST_CONTAINER_FORCEINLINE void erase_void(const_iterator pos)
   {
      priv_erase_impl(pos.pbb, pos.n);
   }

   //! <b>Effects</b>: Erases elements in [first, last).
   //!
   //! <b>Returns</b>: An iterator to the element after the erased range.
   //!
   //! <b>Complexity</b>: Linear to the range size.
   iterator erase(const_iterator first, const_iterator last)
   {
      {
         block_base_pointer pbb_first = first.pbb;
         while(first != last) {
            first = erase(first);
            if(first.pbb != pbb_first) break;
         }
      }
      block_base_pointer pbb = first.pbb;
      if(pbb != last.pbb){
         do {
            block_pointer pb = static_cast_block_pointer(pbb);
            pbb = pb->next;
            BOOST_IF_CONSTEXPR(prefetch_enabled) {
               BOOST_CONTAINER_NEST_PREFETCH_BLOCK(pbb, block);
            }
            size_ -= priv_destroy_all_in_nonempty_block(pb);
            blist.unlink(pb);
            if(BOOST_UNLIKELY(pb->mask == full)) blist.link_available_at_front(pb);
            pb->mask = 0;
         } while(pbb != last.pbb);
         first = const_iterator(pbb);
      }
      while(first != last) first = erase(first);
      return iterator(last.pbb, last.n);
   }

   //! <b>Effects</b>: Swaps the contents of *this and x.
   //!
   //! <b>Throws</b>: Nothing.
   //!
   //! <b>Complexity</b>: Constant.
   void swap(nest& x)
      BOOST_NOEXCEPT_IF(allocator_traits_type::propagate_on_container_swap::value
                                  || allocator_traits_type::is_always_equal::value)
   {
      BOOST_STATIC_CONSTEXPR bool pocs_val =
         allocator_traits_type::propagate_on_container_swap::value;
      priv_swap_impl(x, dtl::bool_<pocs_val>());
   }

   //! <b>Effects</b>: Erases all elements.
   //!
   //! <b>Complexity</b>: Linear.
   void clear() BOOST_NOEXCEPT
   { erase(begin(), end()); }

   //////////////////////////////////////////////
   //
   //            hive operations
   //
   //////////////////////////////////////////////

   //! <b>Effects</b>: Transfers all elements from x into *this.
   //!
   //! <b>Requires</b>: get_allocator() == x.get_allocator().
   //!
   //! <b>Complexity</b>: Linear in x.size().
   void splice(nest& x)
   {
      BOOST_ASSERT(this != &x);
      BOOST_ASSERT(priv_alloc() == x.priv_alloc());
      block_base_pointer pbb = x.blist.header()->next;
      while(pbb != x.blist.header()) {
         block_pointer pb = static_cast_block_pointer(pbb);
         pbb = pbb->next;
         if(pb->mask != full) {
            x.blist.unlink_available(pb);
            blist.link_available_at_front(pb);
         }
         x.blist.unlink(pb);
         blist.link_at_back(pb);
         --x.num_blocks;
         ++num_blocks;
         size_type s = static_cast<size_type>(boost::core::popcount(pb->mask));
         x.size_ -= s;
         size_ += s;
      }
   }

   //! <b>Effects</b>: Transfers all elements from x into *this.
   void splice(BOOST_RV_REF(nest) x)
   { this->splice(static_cast<nest&>(x)); }

   //! <b>Effects</b>: Removes consecutive duplicate elements.
   //!
   //! <b>Returns</b>: The number of elements removed.
   //!
   //! <b>Complexity</b>: Linear.
   template<class BinaryPredicate>
   size_type unique(BinaryPredicate pred)
   {
      size_type s = size_;
      const_iterator first = cbegin();
      const_iterator last  = cend();
      while(first != last) {
         const_iterator next_it = first;
         ++next_it;
         nest_detail::unique_pred_adaptor<T, BinaryPredicate> adaptor(
            boost::addressof(*first), pred);
         first = erase(next_it,
            nest_detail::find_if_not(next_it, last, adaptor));
      }
      return (size_type)(s - size_);
   }

   //! <b>Effects</b>: Removes consecutive equal elements.
   //!
   //! <b>Returns</b>: The number of elements removed.
   size_type unique()
   { return unique(std::equal_to<T>()); }

   //! <b>Effects</b>: Sorts elements according to comp.
   //!
   //! <b>Complexity</b>: O(n log n).
   template<class Compare>
   void sort(Compare comp)
   {
      priv_sort_impl(comp);
   }

   //! <b>Effects</b>: Sorts elements in ascending order.
   //!
   //! <b>Complexity</b>: O(n log n).
   void sort()
   { sort(std::less<T>()); }

   //! <b>Effects</b>: Returns an iterator to the element pointed to by p.
   //!
   //! <b>Complexity</b>: Linear.
   iterator get_iterator(const_pointer p) BOOST_NOEXCEPT
   {
      std::less<const T*> less_cmp;
      block_base_pointer pbb = blist.next;
      while(pbb != blist.header()) {
         block_pointer pb = static_cast_block_pointer(pbb);
         const T* raw_data = boost::movelib::to_raw_pointer(pb->data());
         const T* raw_p = boost::movelib::to_raw_pointer(p);
         if(!less_cmp(raw_p, raw_data) &&
             less_cmp(raw_p, raw_data + N)) {
            return iterator(pb, (int)(p - pb->data()));
         }
         pbb = pbb->next;
      }
      return end();
   }

   //! <b>Effects</b>: Returns a const_iterator to the element pointed to by p.
   //!
   //! <b>Complexity</b>: Linear.
   const_iterator get_iterator(const_pointer p) const BOOST_NOEXCEPT
   {
      return const_cast<nest*>(this)->get_iterator(p);
   }

   //////////////////////////////////////////////
   //
   //         internal visitation
   //
   //////////////////////////////////////////////

   //! <b>Effects</b>: Calls f(x) for each element x in [first, last).
   //!
   //! <b>Complexity</b>: Linear.
   template<class F>
   void visit(iterator first, iterator last, F f)
   {
      nest_detail::visit_adaptor<F, value_type> adaptor(f);
      visit_while(first, last, adaptor);
   }

   //! <b>Effects</b>: Calls f(x) for each const element x in [first, last).
   template<class F>
   void visit(const_iterator first, const_iterator last, F f) const
   {
      nest_detail::visit_adaptor<F, const value_type> adaptor(f);
      const_cast<nest*>(this)->visit_while(
         iterator(first.pbb, first.n),
         iterator(last.pbb, last.n),
         adaptor);
   }

   //! <b>Effects</b>: Calls f(x) for each element x in [first, last)
   //!   until f returns false.
   //!
   //! <b>Returns</b>: Iterator to the element where visitation stopped.
   template<class F>
   iterator visit_while(iterator first, iterator last, F f)
   {
      {
         block_base_pointer pbb = first.pbb;
         while(first != last) {
            if(!f(*first)) return first;
            ++first;
            if(first.pbb != pbb) break;
         }
      }
      if(first.pbb != last.pbb) {
         first = priv_visit_while_impl(first.pbb, last.pbb, f);
         if(first.pbb != last.pbb) return first;
      }
      for(; first != last; ++first) if(!f(*first)) return first;
      return first;
   }

   //! <b>Effects</b>: Calls f(x) for each const element until f returns false.
   //!
   //! <b>Returns</b>: const_iterator to the element where visitation stopped.
   template<class F>
   const_iterator visit_while(const_iterator first, const_iterator last, F f) const
   {
      nest_detail::const_conditional_visit_adaptor<F, value_type> adaptor(f);
      iterator it = const_cast<nest*>(this)->visit_while(
         iterator(first.pbb, first.n),
         iterator(last.pbb, last.n),
         adaptor);
      return const_iterator(it.pbb, it.n);
   }

   //! <b>Effects</b>: Calls f(x) for all elements.
   template<class F>
   void visit_all(F f)
   { visit(begin(), end(), f); }

   //! <b>Effects</b>: Calls f(x) for all const elements.
   template<class F>
   void visit_all(F f) const
   { visit(begin(), end(), f); }

   //! <b>Effects</b>: Calls f(x) for all elements until f returns false.
   //!
   //! <b>Returns</b>: Iterator to the element where visitation stopped.
   template<class F>
   iterator visit_all_while(F f)
   { return visit_while(begin(), end(), f); }

   //! <b>Effects</b>: Calls f(x) for all const elements until f returns false.
   template<class F>
   const_iterator visit_all_while(F f) const
   { return visit_while(begin(), end(), f); }

   #ifndef BOOST_CONTAINER_DOXYGEN_INVOKED
   private:

   template <class U, class A, class O, class P>
   friend typename nest<U, A, O>::size_type erase_if(nest<U, A, O>&, P);

   private:
   //////////////////////////////////////////////
   //
   //         private: allocator access
   //
   //////////////////////////////////////////////

   block_allocator&       al() BOOST_NOEXCEPT       { return allocator_base::get(); }
   const block_allocator& al() const BOOST_NOEXCEPT { return allocator_base::get(); }

   allocator_type priv_alloc() const BOOST_NOEXCEPT
   { return allocator_type(al()); }

   //////////////////////////////////////////////
   //
   //         private: block management
   //
   //////////////////////////////////////////////

   BOOST_CONTAINER_FORCEINLINE  static block_pointer
   static_cast_block_pointer(block_base_pointer pbb) BOOST_NOEXCEPT
   {
      return boost::intrusive::pointer_traits<block_pointer>::pointer_to(
         static_cast<block&>(*pbb));
   }

   void priv_allocate_block_data(block_pointer pb, dtl::bool_<false>)
   {
      BOOST_TRY {
         allocator_type val_al(al());
         pb->data_ = allocator_traits_type::allocate(val_al, N);
      }
      BOOST_CATCH(...) {
         block_alloc_traits::deallocate(al(), pb, 1);
         BOOST_RETHROW;
      }
      BOOST_CATCH_END
   }

   BOOST_CONTAINER_FORCEINLINE void priv_allocate_block_data(block_pointer, dtl::bool_<true>) BOOST_NOEXCEPT {}

   void priv_deallocate_block_data(block_pointer pb, dtl::bool_<false>) BOOST_NOEXCEPT
   {
      allocator_type val_al(al());
      allocator_traits_type::deallocate(val_al, pb->data(), N);
   }

   BOOST_CONTAINER_FORCEINLINE void priv_deallocate_block_data(block_pointer, dtl::bool_<true>) BOOST_NOEXCEPT {}

   block_pointer priv_create_new_available_block()
   {
      block_pointer pb = block_alloc_traits::allocate(al(), 1);
      pb->mask = 0;
      priv_allocate_block_data(pb, dtl::bool_<store_data_in_block>());
      blist.link_available_at_back(pb);
      ++num_blocks;
      return pb;
   }

   void priv_delete_block(block_pointer pb) BOOST_NOEXCEPT
   {
      priv_deallocate_block_data(pb, dtl::bool_<store_data_in_block>());
      block_alloc_traits::deallocate(al(), pb, 1);
   }

   BOOST_CONTAINER_FORCEINLINE block_pointer priv_retrieve_available_block(int& n)
   {
      if(BOOST_LIKELY(blist.next_available != blist.header())){
         block_pointer pb = static_cast_block_pointer(blist.next_available);
         n = nest_detail::unchecked_countr_one(pb->mask);
         return pb;
      }
      else {
         n = 0;
         return priv_create_new_available_block();
      }
   }

   //////////////////////////////////////////////
   //
   //   private: destruction helpers
   //
   //////////////////////////////////////////////

   size_type priv_destroy_all_in_nonempty_block(block_pointer pb) BOOST_NOEXCEPT
   {
      BOOST_ASSERT(pb->mask != 0);
      return priv_destroy_all_dispatch(pb,
         dtl::bool_<dtl::is_trivially_destructible<T>::value>());
   }

   size_type priv_destroy_all_dispatch(
      block_pointer pb, dtl::true_type /* trivially destructible */) BOOST_NOEXCEPT
   {
      return (size_type)boost::core::popcount(pb->mask);
   }

   size_type priv_destroy_all_dispatch(
      block_pointer pb, dtl::false_type /* use destroy */) BOOST_NOEXCEPT
   {
      size_type s = 0;
      mask_type m = pb->mask;
      do {
         int n = nest_detail::unchecked_countr_zero(m);
         block_alloc_traits::destroy(al(), boost::movelib::to_raw_pointer(pb->data() + n));
         ++s;
         m &= m - 1;
      } while(m);
      return s;
   }

   size_type priv_destroy_all_in_full_block(block_pointer pb) BOOST_NOEXCEPT
   {
      BOOST_ASSERT(pb->mask == full);
      for(std::size_t n = 0; n < N; ++n) {
         block_alloc_traits::destroy(al(), boost::movelib::to_raw_pointer(pb->data() + n));
      }
      return (size_type)N;
   }

   //////////////////////////////////////////////
   //
   //   private: reset (destroy all + free)
   //
   //////////////////////////////////////////////

   void priv_reset() BOOST_NOEXCEPT
   {
      // available blocks (with at least one empty slot)
      block_base_pointer pbb = blist.header()->next_available;
      while(pbb != blist.header()) {
         block_pointer pb = static_cast_block_pointer(pbb);
         pbb = pb->next_available;
         if(pb->mask != 0) {
            priv_destroy_all_in_nonempty_block(pb);
            blist.unlink(pb);
         }
         priv_delete_block(pb);
      }
      // full blocks remaining
      pbb = blist.next;
      while(pbb != blist.header()) {
         BOOST_ASSERT(pbb->mask == full);
         block_pointer pb = static_cast_block_pointer(pbb);
         pbb = pb->next;
         priv_destroy_all_in_full_block(pb);
         priv_delete_block(pb);
      }
      blist.reset();
      num_blocks = 0;
      size_ = 0;
   }

   //////////////////////////////////////////////
   //
   //   private: erase implementation
   //
   //////////////////////////////////////////////

   BOOST_CONTAINER_FORCEINLINE void priv_erase_impl(block_base_pointer pbb, int n) BOOST_NOEXCEPT
   {
      block_pointer pb = static_cast_block_pointer(pbb);
      block_alloc_traits::destroy(al(), boost::movelib::to_raw_pointer(pb->data() + n));
      if(BOOST_UNLIKELY(pb->mask == full)) blist.link_available_at_front(pb);
      pb->mask &= ~((mask_type)(1) << n);
      if(BOOST_UNLIKELY(pb->mask == 0)) blist.unlink(pb);
      --size_;
   }

   //////////////////////////////////////////////
   //
   //   private: range insert / assign
   //
   //////////////////////////////////////////////

   template<class InpIt>
   void priv_insert_range_copy(InpIt first, InpIt last)
   {
      while(first != last) {
         int  n;
         block_pointer pb = priv_retrieve_available_block(n);
         for(; ; ) {
            block_alloc_traits::construct(
               al(), boost::movelib::to_raw_pointer(pb->data() + n), *first);
            ++first;
            ++size_;
            if(BOOST_UNLIKELY(pb->mask == 0)) blist.link_at_back(pb);
            pb->mask |= pb->mask + 1;
            if(pb->mask == full){
               blist.unlink_available(pb);
               break;
            }
            else if(first == last) return;
            n = nest_detail::unchecked_countr_one(pb->mask);
         }
      }
   }

   template<class InpIt>
   void priv_insert_range_move(InpIt first, InpIt last)
   {
      this->priv_insert_range_copy( boost::make_move_iterator(first)
                                  , boost::make_move_iterator(last));
   }

   void priv_insert_n_copies(size_type count, const T& x)
   {
      typedef constant_iterator<T> const_it;
      this->priv_insert_range_copy(const_it(x, count), const_it());
   }

   void priv_insert_n_default(size_type count)
   {
      typedef value_init_construct_iterator<value_type> value_init_iterator;
      this->priv_insert_range_copy(value_init_iterator(count), value_init_iterator());
   }

   template<class InpIt>
   void priv_range_assign(InpIt first, InpIt last)
   {
      block_base_pointer pbb = blist.next;
      int n = 0;
      if(first != last) {
         for(; pbb != blist.header(); pbb = pbb->next, n = 0) {
            block_pointer pb = static_cast_block_pointer(pbb);
            for(mask_type bit = 1; bit; bit <<= 1, ++n) {
               if(pb->mask & bit) {
                  pb->data()[n] = *first;
                  ++first;
               }
               else {
                  block_alloc_traits::construct(
                     al(), boost::movelib::to_raw_pointer(pb->data() + n), *first);
                  ++first;
                  ++size_;
                  pb->mask |= bit;
                  if(pb->mask == full) blist.unlink_available(pb);
               }
               if(first == last) goto exit;
            }
         }
      exit: ;
      }
      if(first != last) {
         priv_insert_range_copy(first, last);
      }
      else {
         const_iterator it = (n == 0)
            ? const_iterator(pbb)
            : (const_iterator(pbb, n), ++const_iterator(pbb, n));
         //Advance from pbb,n to the next valid position
         if(n != 0) {
            it = const_iterator(pbb, n);
            ++it;
         } else {
            it = const_iterator(pbb);
         }
         erase(it, cend());
      }
   }

   void priv_assign_n(size_type count, const T& val)
   {
      typedef constant_iterator<T> const_it;
      this->priv_range_assign(const_it(val, count), const_it());
   }

   //////////////////////////////////////////////
   //
   //   private: move assign
   //
   //////////////////////////////////////////////

   void priv_move_assign(nest& x, dtl::true_type /* can transfer */)
   {
      dtl::bool_<allocator_traits_type::
         propagate_on_container_move_assignment::value> pocma;
      priv_reset();
      nest_detail::move_assign_if(pocma, al(), x.al());
      blist = boost::move(x.blist);
      num_blocks = x.num_blocks;
      size_ = x.size_;
      x.num_blocks = 0;
      x.size_ = 0;
   }

   void priv_move_assign(nest& x, dtl::false_type /* maybe move data */)
   {
      if(al() == x.al()) {
         priv_move_assign(x, dtl::true_type());
      }
      else {
         // Move-assign element by element
         priv_move_assign_elements(x);
      }
   }

   void priv_move_assign_elements(nest& x)
   {
      this->priv_range_assign( boost::make_move_iterator(x.begin())
                             , boost::make_move_iterator(x.end()) );
      x.clear();
   }

   //////////////////////////////////////////////
   //
   //   private: swap
   //
   //////////////////////////////////////////////

   void priv_swap_impl(nest& x, dtl::true_type /* propagate alloc */)
   {
      boost::adl_move_swap(al(), x.al());
      boost::adl_move_swap(blist, x.blist);
      boost::adl_move_swap(num_blocks, x.num_blocks);
      boost::adl_move_swap(size_, x.size_);
   }

   void priv_swap_impl(nest& x, dtl::false_type /* don't propagate */)
   {
      BOOST_ASSERT(al() == x.al());
      boost::adl_move_swap(blist, x.blist);
      boost::adl_move_swap(num_blocks, x.num_blocks);
      boost::adl_move_swap(size_, x.size_);
   }

   //////////////////////////////////////////////
   //
   //   private: sort
   //
   //////////////////////////////////////////////

   struct sort_proxy
   {
      T*        p;
      size_type n;
   };

   template<class Compare>
   void priv_sort_impl(Compare comp)
   {
      if(size_ <= 1) return;

      // Try transfer_sort for small element types
      BOOST_IF_CONSTEXPR(sizeof(T) <= sizeof(sort_proxy)) {
         if(priv_transfer_sort(comp)) return;
      }
      else {
         BOOST_STATIC_CONSTEXPR std::size_t memory_threshold = 2 * 1024 * 1024;
         if((std::size_t)size_ * sizeof(sort_proxy) <= memory_threshold) {
            if(priv_proxy_sort(comp)) return;
         }
      }
      priv_compact_sort(comp);
   }

   template<class Compare>
   bool priv_transfer_sort(Compare comp)
   {
      nest_detail::buffer<T, block_allocator> buf(size_, al());
      if(!buf.data) return false;

      // Move all elements to buffer
      priv_visit_all_move_to_buffer(buf);
      std::sort(buf.begin(), buf.end(), comp);
      // Move sorted elements back
      priv_visit_all_move_from_buffer(buf);
      return true;
   }

   void priv_visit_all_move_to_buffer(nest_detail::buffer<T, block_allocator>& buf)
   {
      block_base_pointer pbb = blist.next;
      while(pbb != blist.header()) {
         block_pointer pb = static_cast_block_pointer(pbb);
         pbb = pbb->next;
         mask_type m = pb->mask;
         while(m) {
            int n = nest_detail::unchecked_countr_zero(m);
            buf.push_back_move(pb->data()[n]);
            m &= m - 1;
         }
      }
   }

   void priv_visit_all_move_from_buffer(nest_detail::buffer<T, block_allocator>& buf)
   {
      block_base_pointer pbb = blist.next;
      while(pbb != blist.header()) {
         block_pointer pb = static_cast_block_pointer(pbb);
         pbb = pbb->next;
         mask_type m = pb->mask;
         while(m) {
            int n = nest_detail::unchecked_countr_zero(m);
            pb->data()[n] = boost::move(*buf.begin());
            buf.erase_front();
            m &= m - 1;
         }
      }
   }

   template<class Compare>
   bool priv_proxy_sort(Compare comp)
   {
      void* raw = ::operator new(size_ * sizeof(sort_proxy), std::nothrow);
      if(!raw) return false;
      nest_detail::raw_memory_holder holder(raw);
      sort_proxy* proxies = static_cast<sort_proxy*>(raw);

      size_type i = 0;
      block_base_pointer pbb = blist.next;
      while(pbb != blist.header()) {
         block_pointer pb = static_cast_block_pointer(pbb);
         pbb = pbb->next;
         mask_type m = pb->mask;
         while(m) {
            int n = nest_detail::unchecked_countr_zero(m);
            proxies[i].p = boost::movelib::to_raw_pointer(pb->data() + n);
            proxies[i].n = i;
            ++i;
            m &= m - 1;
         }
      }

      nest_detail::sort_proxy_comparator<T, Compare> proxy_comp(comp);
      std::sort(proxies, proxies + size_, proxy_comp);

      // Rearrange elements according to sorted proxy order
      for(i = 0; i < size_; ++i) {
         if(proxies[i].n != i) {
            T x = boost::move(*(proxies[i].p));
            size_type j = i;
            do {
               size_type k = proxies[j].n;
               *(proxies[j].p) = boost::move(*proxies[k].p);
               proxies[j].n = j;
               j = k;
            } while(proxies[j].n != i);
            *(proxies[j].p) = boost::move(x);
            proxies[j].n = j;
         }
      }
      return true;
   }

   template<class Compare>
   void priv_compact_sort(Compare comp)
   {
      typedef nest_detail::sort_iterator<T, N> sort_iter;

      std::size_t nblocks = (std::size_t)((size_ + N - 1) / N);
      void* raw = ::operator new(nblocks * sizeof(T*));
      nest_detail::raw_memory_holder holder(raw);
      T** ptrs = static_cast<T**>(raw);

      std::size_t idx = 0;
      priv_compact_with_tracking(ptrs, idx);
      BOOST_ASSERT(idx == nblocks);

      std::sort(sort_iter(ptrs, 0), sort_iter(ptrs, size_), comp);
   }

   //////////////////////////////////////////////
   //
   //   private: compact
   //
   //////////////////////////////////////////////

   void priv_compact_noop_track()
   {
      block_base_pointer pbbx = blist.next;
      while(pbbx != blist.header()) {
         block_pointer pbx = static_cast_block_pointer(pbbx);
         block_base_pointer pbby = pbbx->next;
         if(pbx->mask != full) {
            do {
               if(pbby->mask == full) {
                  do {
                     // skip full blocks (noop track)
                     pbby = pbby->next;
                  } while(pbby->mask == full);
                  blist.unlink(pbx);
                  blist.link_before(pbx, static_cast_block_pointer(pbby));
               }
               if(pbby == blist.header()) {
                  priv_compact_single(pbx);
                  return;
               }
               else {
                  block_pointer pby = static_cast_block_pointer(pbby);
                  priv_compact_pair(pbx, pby);
                  if(pby->mask == 0) {
                     pbby = pby->next;
                     blist.unlink(pby);
                  }
               }
            } while(pbx->mask != full);
            blist.unlink_available(pbx);
         }
         pbbx = pbby;
      }
   }

   void priv_compact_with_tracking(T** ptrs, std::size_t& idx)
   {
      block_base_pointer pbbx = blist.next;
      while(pbbx != blist.header()) {
         block_pointer pbx = static_cast_block_pointer(pbbx);
         block_base_pointer pbby = pbbx->next;
         if(pbx->mask != full) {
            do {
               if(pbby->mask == full) {
                  do {
                     ptrs[idx++] = boost::movelib::to_raw_pointer(
                        static_cast_block_pointer(pbby)->data());
                     pbby = pbby->next;
                  } while(pbby->mask == full);
                  blist.unlink(pbx);
                  blist.link_before(pbx, static_cast_block_pointer(pbby));
               }
               if(pbby == blist.header()) {
                  priv_compact_single(pbx);
                  ptrs[idx++] = boost::movelib::to_raw_pointer(pbx->data());
                  return;
               }
               else {
                  block_pointer pby = static_cast_block_pointer(pbby);
                  priv_compact_pair(pbx, pby);
                  if(pby->mask == 0) {
                     pbby = pby->next;
                     blist.unlink(pby);
                  }
               }
            } while(pbx->mask != full);
            blist.unlink_available(pbx);
         }
         ptrs[idx++] = boost::movelib::to_raw_pointer(pbx->data());
         pbbx = pbby;
      }
   }

   void priv_compact_pair(block_pointer& pbx, block_pointer& pby)
   {
      std::size_t cx = static_cast<std::size_t>(boost::core::popcount(pbx->mask));
      std::size_t cy = static_cast<std::size_t>(boost::core::popcount(pby->mask));
      if(cx < cy) {
         boost::adl_move_swap(cx, cy);
         nest_detail::swap_payload(*pbx, *pby);
      }
      std::size_t c = (std::min)(N - cx, cy);
      while(c--) {
         std::size_t n = static_cast<std::size_t>(nest_detail::unchecked_countr_one(pbx->mask));
         std::size_t m = N - 1u - static_cast<std::size_t>(nest_detail::unchecked_countl_zero(pby->mask));
         block_alloc_traits::construct(
            al(), boost::movelib::to_raw_pointer(pbx->data() + n),
            boost::move(pby->data()[m]));
         block_alloc_traits::destroy(
            al(), boost::movelib::to_raw_pointer(pby->data() + m));
         pbx->mask |= pbx->mask + 1;
         pby->mask &= ~((mask_type)(1) << m);
      }
   }

   void priv_compact_single(block_pointer pb)
   {
      for(; ;) {
         std::size_t n = (std::size_t)nest_detail::unchecked_countr_one(pb->mask);
         std::size_t m = N - 1 - (std::size_t)nest_detail::unchecked_countl_zero(pb->mask);
         if(n > m) return;
         block_alloc_traits::construct(
            al(), boost::movelib::to_raw_pointer(pb->data() + n),
            boost::move(pb->data()[m]));
         block_alloc_traits::destroy(
            al(), boost::movelib::to_raw_pointer(pb->data() + m));
         pb->mask |= pb->mask + 1;
         pb->mask &= ~((mask_type)(1) << m);
      }
   }

   //////////////////////////////////////////////
   //
   //   private: visit_while implementation
   //
   //////////////////////////////////////////////

   template<class F>
   iterator priv_visit_while_impl(
      block_base_pointer pbb, block_base_pointer last_pbb, F& f)
   {
      BOOST_ASSERT(pbb != last_pbb);
      block_pointer pb = static_cast_block_pointer(pbb);
      mask_type     m  = pb->mask;
      int           n  = nest_detail::unchecked_countr_zero(m);
      pointer       pd = pb->data();
      do {
         pbb = pb->next;
         mask_type next_mask = pbb->mask;
         int next_n = nest_detail::unchecked_countr_zero(next_mask);
         pointer next_pd = static_cast_block_pointer(pbb)->data();
         BOOST_IF_CONSTEXPR(prefetch_enabled) {
            BOOST_CONTAINER_NEST_PREFETCH(next_pd + next_n);
            BOOST_CONTAINER_NEST_PREFETCH(pbb->next);
         }
         for(; ; ) {
            if(!f(pd[n])) return iterator(pb, n);
            m &= m - 1;
            if(!m) break;
            n = nest_detail::unchecked_countr_zero(m);
         }
         pb = static_cast_block_pointer(pbb);
         m = next_mask;
         n = next_n;
         pd = next_pd;
      } while(pb != last_pbb);
      return iterator(last_pbb);
   }

   //////////////////////////////////////////////
   //
   //         private: erase_if impl
   //
   //////////////////////////////////////////////

   template<class Predicate>
   size_type priv_erase_if(Predicate pred)
   {
      size_type s = size_;
      block_base_pointer pbb = blist.next;
      while(pbb != blist.header()) {
         block_pointer pb = static_cast_block_pointer(pbb);
         pbb = pb->next;
         BOOST_IF_CONSTEXPR(prefetch_enabled) {
            BOOST_CONTAINER_NEST_PREFETCH_BLOCK(pbb, block);
         }
         mask_type m = pb->mask;
         do {
            int n = nest_detail::unchecked_countr_zero(m);
            if(pred(pb->data()[n])) priv_erase_impl(pb, n);
            m &= m - 1;
         } while(m);
      }
      return (size_type)(s - size_);
   }

   //////////////////////////////////////////////
   //
   //         private: data members
   //
   //////////////////////////////////////////////

   block_base blist;
   size_type  num_blocks;
   size_type  size_;

   #endif // BOOST_CONTAINER_DOXYGEN_INVOKED
};

//////////////////////////////////////////////
//
//       free functions
//
//////////////////////////////////////////////

//! <b>Effects</b>: Swaps x and y.
template<class T, class Allocator, class Options>
inline void swap(nest<T, Allocator, Options>& x, nest<T, Allocator, Options>& y)
   BOOST_NOEXCEPT_IF(BOOST_NOEXCEPT(x.swap(y)))
{
   x.swap(y);
}

//! <b>Effects</b>: Erases all elements for which pred returns true.
//!
//! <b>Returns</b>: The number of erased elements.
template<class T, class Allocator, class Options, class Predicate>
typename nest<T, Allocator, Options>::size_type
erase_if(nest<T, Allocator, Options>& x, Predicate pred)
{
   return x.priv_erase_if(pred);
}

//! <b>Effects</b>: Erases all elements equal to value.
//!
//! <b>Returns</b>: The number of erased elements.
template<class T, class Allocator, class Options>
typename nest<T, Allocator, Options>::size_type
erase(nest<T, Allocator, Options>& x, const T& value)
{
   return erase_if(x, equal_to_value<T>(value));
}

#ifndef BOOST_CONTAINER_NO_CXX17_CTAD

template<
   class InpIt,
   class Allocator = void
>
nest(InpIt, InpIt, Allocator = Allocator())
   -> nest<
      typename iterator_traits<InpIt>::value_type,
      Allocator,
      void>;

#endif

}} // namespace boost::container

#if defined(BOOST_MSVC)
#pragma warning(pop) /* C4714 */
#endif

#include <boost/container/detail/config_end.hpp>

#endif // BOOST_CONTAINER_EXPERIMENTAL_NEST_HPP
