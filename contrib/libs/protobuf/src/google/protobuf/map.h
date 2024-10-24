// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// This file defines the map container and its helpers to support protobuf maps.
//
// The Map and MapIterator types are provided by this header file.
// Please avoid using other types defined here, unless they are public
// types within Map or MapIterator, such as Map::value_type.

#ifndef GOOGLE_PROTOBUF_MAP_H__
#define GOOGLE_PROTOBUF_MAP_H__

#include <algorithm>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <limits>  // To support Visual Studio 2008
#include <string>
#include <type_traits>
#include <utility>

#if !defined(GOOGLE_PROTOBUF_NO_RDTSC) && defined(__APPLE__)
#define GOOGLE_PROTOBUF_NO_RDTSC 1
//#include <mach/mach_time.h>
#endif

#include "google/protobuf/stubs/common.h"
#include "y_absl/container/btree_map.h"
#include "y_absl/hash/hash.h"
#include "y_absl/meta/type_traits.h"
#include "y_absl/strings/string_view.h"
#include "google/protobuf/arena.h"
#include "google/protobuf/generated_enum_util.h"
#include "google/protobuf/map_type_handler.h"
#include "google/protobuf/port.h"


#ifdef SWIG
#error "You cannot SWIG proto headers"
#endif

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {

template <typename Key, typename T>
class Map;

class MapIterator;

template <typename Enum>
struct is_proto_enum;

namespace internal {
template <typename Derived, typename Key, typename T,
          WireFormatLite::FieldType key_wire_type,
          WireFormatLite::FieldType value_wire_type>
class MapFieldLite;

template <typename Derived, typename Key, typename T,
          WireFormatLite::FieldType key_wire_type,
          WireFormatLite::FieldType value_wire_type>
class MapField;

template <typename Key, typename T>
class TypeDefinedMapFieldBase;

class DynamicMapField;

class GeneratedMessageReflection;

// Internal type traits that can be used to define custom key/value types. These
// are only be specialized by protobuf internals, and never by users.
template <typename T, typename VoidT = void>
struct is_internal_map_key_type : std::false_type {};

template <typename T, typename VoidT = void>
struct is_internal_map_value_type : std::false_type {};

// re-implement std::allocator to use arena allocator for memory allocation.
// Used for Map implementation. Users should not use this class
// directly.
template <typename U>
class MapAllocator {
 public:
  using value_type = U;
  using pointer = value_type*;
  using const_pointer = const value_type*;
  using reference = value_type&;
  using const_reference = const value_type&;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  constexpr MapAllocator() : arena_(nullptr) {}
  explicit constexpr MapAllocator(Arena* arena) : arena_(arena) {}
  template <typename X>
  MapAllocator(const MapAllocator<X>& allocator)  // NOLINT(runtime/explicit)
      : arena_(allocator.arena()) {}

  // MapAllocator does not support alignments beyond 8. Technically we should
  // support up to std::max_align_t, but this fails with ubsan and tcmalloc
  // debug allocation logic which assume 8 as default alignment.
  static_assert(alignof(value_type) <= 8, "");

  pointer allocate(size_type n, const void* /* hint */ = nullptr) {
    // If arena is not given, malloc needs to be called which doesn't
    // construct element object.
    if (arena_ == nullptr) {
      return static_cast<pointer>(::operator new(n * sizeof(value_type)));
    } else {
      return reinterpret_cast<pointer>(
          Arena::CreateArray<uint8_t>(arena_, n * sizeof(value_type)));
    }
  }

  void deallocate(pointer p, size_type n) {
    if (arena_ == nullptr) {
      internal::SizedDelete(p, n * sizeof(value_type));
    }
  }

#if !defined(GOOGLE_PROTOBUF_OS_APPLE) && !defined(GOOGLE_PROTOBUF_OS_NACL) && \
    !defined(GOOGLE_PROTOBUF_OS_EMSCRIPTEN)
  template <class NodeType, class... Args>
  void construct(NodeType* p, Args&&... args) {
    // Clang 3.6 doesn't compile static casting to void* directly. (Issue
    // #1266) According C++ standard 5.2.9/1: "The static_cast operator shall
    // not cast away constness". So first the maybe const pointer is casted to
    // const void* and after the const void* is const casted.
    new (const_cast<void*>(static_cast<const void*>(p)))
        NodeType(std::forward<Args>(args)...);
  }

  template <class NodeType>
  void destroy(NodeType* p) {
    p->~NodeType();
  }
#else
  void construct(pointer p, const_reference t) { new (p) value_type(t); }

  void destroy(pointer p) { p->~value_type(); }
#endif

  template <typename X>
  struct rebind {
    using other = MapAllocator<X>;
  };

  template <typename X>
  bool operator==(const MapAllocator<X>& other) const {
    return arena_ == other.arena_;
  }

  template <typename X>
  bool operator!=(const MapAllocator<X>& other) const {
    return arena_ != other.arena_;
  }

  // To support Visual Studio 2008
  size_type max_size() const {
    // parentheses around (std::...:max) prevents macro warning of max()
    return (std::numeric_limits<size_type>::max)();
  }

  // To support gcc-4.4, which does not properly
  // support templated friend classes
  Arena* arena() const { return arena_; }

 private:
  using DestructorSkippable_ = void;
  Arena* arena_;
};

// To save on binary size and simplify generic uses of the map types we collapse
// signed/unsigned versions of the same sized integer to the unsigned version.
template <typename T, typename = void>
struct KeyForBaseImpl {
  using type = T;
};
template <typename T>
struct KeyForBaseImpl<T, std::enable_if_t<std::is_integral<T>::value &&
                                          std::is_signed<T>::value>> {
  using type = std::make_unsigned_t<T>;
};
template <typename T>
using KeyForBase = typename KeyForBaseImpl<T>::type;

// Default case: Not transparent.
// We use std::hash<key_type>/std::less<key_type> and all the lookup functions
// only accept `key_type`.
template <typename key_type>
struct TransparentSupport {
  using hash = std::hash<key_type>;
  using less = std::less<key_type>;

  static bool Equals(const key_type& a, const key_type& b) { return a == b; }

  template <typename K>
  using key_arg = key_type;
};

// We add transparent support for TProtoStringType keys. We use
// std::hash<y_absl::string_view> as it supports the input types we care about.
// The lookup functions accept arbitrary `K`. This will include any key type
// that is convertible to y_absl::string_view.
template <>
struct TransparentSupport<TProtoStringType> {
  // If the element is not convertible to y_absl::string_view, try to convert to
  // TProtoStringType first, and then fallback to support for converting from
  // std::string_view. The ranked overload pattern is used to specify our
  // order of preference.
  struct Rank0 {};
  struct Rank1 : Rank0 {};
  struct Rank2 : Rank1 {};
  template <typename T, typename = std::enable_if_t<
                            std::is_convertible<T, y_absl::string_view>::value>>
  static y_absl::string_view ImplicitConvertImpl(T&& str, Rank2) {
    y_absl::string_view ref = str;
    return ref;
  }
  template <typename T, typename = std::enable_if_t<
                            std::is_convertible<T, const TProtoStringType&>::value>>
  static y_absl::string_view ImplicitConvertImpl(T&& str, Rank1) {
    const TProtoStringType& ref = str;
    return ref;
  }
  template <typename T>
  static y_absl::string_view ImplicitConvertImpl(T&& str, Rank0) {
    return {str.data(), str.size()};
  }

  template <typename T>
  static y_absl::string_view ImplicitConvert(T&& str) {
    return ImplicitConvertImpl(std::forward<T>(str), Rank2{});
  }

  struct hash : public y_absl::Hash<y_absl::string_view> {
    using is_transparent = void;

    template <typename T>
    size_t operator()(T&& str) const {
      return y_absl::Hash<y_absl::string_view>::operator()(
          ImplicitConvert(std::forward<T>(str)));
    }
  };
  struct less {
    using is_transparent = void;

    template <typename T, typename U>
    bool operator()(T&& t, U&& u) const {
      return ImplicitConvert(std::forward<T>(t)) <
             ImplicitConvert(std::forward<U>(u));
    }
  };

  template <typename T, typename U>
  static bool Equals(T&& t, U&& u) {
    return ImplicitConvert(std::forward<T>(t)) ==
           ImplicitConvert(std::forward<U>(u));
  }

  template <typename K>
  using key_arg = K;
};

struct NodeBase {
  // Align the node to allow KeyNode to predict the location of the key.
  // This way sizeof(NodeBase) contains any possible padding it was going to
  // have between NodeBase and the key.
  alignas(kMaxMessageAlignment) NodeBase* next;
};

inline NodeBase* EraseFromLinkedList(NodeBase* item, NodeBase* head) {
  if (head == item) {
    return head->next;
  } else {
    head->next = EraseFromLinkedList(item, head->next);
    return head;
  }
}

inline bool TableEntryIsTooLong(NodeBase* node) {
  const size_t kMaxLength = 8;
  size_t count = 0;
  do {
    ++count;
    node = node->next;
  } while (node != nullptr);
  // Invariant: no linked list ever is more than kMaxLength in length.
  Y_ABSL_DCHECK_LE(count, kMaxLength);
  return count >= kMaxLength;
}

template <typename T>
using KeyForTree = std::conditional_t<std::is_integral<T>::value, arc_ui64,
                                      std::reference_wrapper<const T>>;

template <typename T>
using LessForTree = typename TransparentSupport<
    std::conditional_t<std::is_integral<T>::value, arc_ui64, T>>::less;

template <typename Key>
using TreeForMap =
    y_absl::btree_map<KeyForTree<Key>, NodeBase*, LessForTree<Key>,
                    MapAllocator<std::pair<const KeyForTree<Key>, NodeBase*>>>;

// Type safe tagged pointer.
// We convert to/from nodes and trees using the operations below.
// They ensure that the tags are used correctly.
// There are three states:
//  - x == 0: the entry is empty
//  - x != 0 && (x&1) == 0: the entry is a node list
//  - x != 0 && (x&1) == 1: the entry is a tree
enum class TableEntryPtr : uintptr_t;

inline bool TableEntryIsEmpty(TableEntryPtr entry) {
  return entry == TableEntryPtr{};
}
inline bool TableEntryIsTree(TableEntryPtr entry) {
  return (static_cast<uintptr_t>(entry) & 1) == 1;
}
inline bool TableEntryIsList(TableEntryPtr entry) {
  return !TableEntryIsTree(entry);
}
inline bool TableEntryIsNonEmptyList(TableEntryPtr entry) {
  return !TableEntryIsEmpty(entry) && TableEntryIsList(entry);
}
inline NodeBase* TableEntryToNode(TableEntryPtr entry) {
  Y_ABSL_DCHECK(TableEntryIsList(entry));
  return reinterpret_cast<NodeBase*>(static_cast<uintptr_t>(entry));
}
inline TableEntryPtr NodeToTableEntry(NodeBase* node) {
  Y_ABSL_DCHECK((reinterpret_cast<uintptr_t>(node) & 1) == 0);
  return static_cast<TableEntryPtr>(reinterpret_cast<uintptr_t>(node));
}
template <typename Tree>
Tree* TableEntryToTree(TableEntryPtr entry) {
  Y_ABSL_DCHECK(TableEntryIsTree(entry));
  return reinterpret_cast<Tree*>(static_cast<uintptr_t>(entry) - 1);
}
template <typename Tree>
TableEntryPtr TreeToTableEntry(Tree* node) {
  Y_ABSL_DCHECK((reinterpret_cast<uintptr_t>(node) & 1) == 0);
  return static_cast<TableEntryPtr>(reinterpret_cast<uintptr_t>(node) | 1);
}

// This captures all numeric types.
inline size_t MapValueSpaceUsedExcludingSelfLong(bool) { return 0; }
inline size_t MapValueSpaceUsedExcludingSelfLong(const TProtoStringType& str) {
  return StringSpaceUsedExcludingSelfLong(str);
}
template <typename T,
          typename = decltype(std::declval<const T&>().SpaceUsedLong())>
size_t MapValueSpaceUsedExcludingSelfLong(const T& message) {
  return message.SpaceUsedLong() - sizeof(T);
}

constexpr size_t kGlobalEmptyTableSize = 1;
PROTOBUF_EXPORT extern const TableEntryPtr
    kGlobalEmptyTable[kGlobalEmptyTableSize];

// Space used for the table, trees, and nodes.
// Does not include the indirect space used. Eg the data of a TProtoStringType.
template <typename Key>
PROTOBUF_NOINLINE size_t SpaceUsedInTable(TableEntryPtr* table,
                                          size_t num_buckets,
                                          size_t num_elements,
                                          size_t sizeof_node) {
  size_t size = 0;
  // The size of the table.
  size += sizeof(void*) * num_buckets;
  // All the nodes.
  size += sizeof_node * num_elements;
  // For each tree, count the overhead of the those nodes.
  // Two buckets at a time because we only care about trees.
  for (size_t b = 0; b < num_buckets; ++b) {
    if (internal::TableEntryIsTree(table[b])) {
      using Tree = TreeForMap<Key>;
      Tree* tree = TableEntryToTree<Tree>(table[b]);
      // Estimated cost of the red-black tree nodes, 3 pointers plus a
      // bool (plus alignment, so 4 pointers).
      size += tree->size() *
              (sizeof(typename Tree::value_type) + sizeof(void*) * 4);
    }
  }
  return size;
}

template <typename Map,
          typename = typename std::enable_if<
              !std::is_scalar<typename Map::key_type>::value ||
              !std::is_scalar<typename Map::mapped_type>::value>::type>
size_t SpaceUsedInValues(const Map* map) {
  size_t size = 0;
  for (const auto& v : *map) {
    size += internal::MapValueSpaceUsedExcludingSelfLong(v.first) +
            internal::MapValueSpaceUsedExcludingSelfLong(v.second);
  }
  return size;
}

inline size_t SpaceUsedInValues(const void*) { return 0; }

// Base class for all Map instantiations.
// This class holds all the data and provides the basic functionality shared
// among all instantiations.
// Having an untyped base class helps generic consumers (like the table-driven
// parser) by having non-template code that can handle all instantiations.
class PROTOBUF_EXPORT UntypedMapBase {
  using Allocator = internal::MapAllocator<void*>;

 public:
  using size_type = size_t;

  explicit constexpr UntypedMapBase(Arena* arena)
      : num_elements_(0),
        num_buckets_(internal::kGlobalEmptyTableSize),
        seed_(0),
        index_of_first_non_null_(internal::kGlobalEmptyTableSize),
        table_(const_cast<TableEntryPtr*>(internal::kGlobalEmptyTable)),
        alloc_(arena) {}

  UntypedMapBase(const UntypedMapBase&) = delete;
  UntypedMapBase& operator=(const UntypedMapBase&) = delete;

 protected:
  enum { kMinTableSize = 8 };

 public:
  Arena* arena() const { return this->alloc_.arena(); }

  void Swap(UntypedMapBase* other) {
    std::swap(num_elements_, other->num_elements_);
    std::swap(num_buckets_, other->num_buckets_);
    std::swap(seed_, other->seed_);
    std::swap(index_of_first_non_null_, other->index_of_first_non_null_);
    std::swap(table_, other->table_);
    std::swap(alloc_, other->alloc_);
  }

  static size_type max_size() {
    return static_cast<size_type>(1) << (sizeof(void**) >= 8 ? 60 : 28);
  }
  size_type size() const { return num_elements_; }
  bool empty() const { return size() == 0; }

 protected:
  friend class TcParser;

  struct NodeAndBucket {
    NodeBase* node;
    size_type bucket;
  };

  // Returns whether we should insert after the head of the list. For
  // non-optimized builds, we randomly decide whether to insert right at the
  // head of the list or just after the head. This helps add a little bit of
  // non-determinism to the map ordering.
  bool ShouldInsertAfterHead(void* node) {
#ifdef NDEBUG
    (void)node;
    return false;
#else
    // Doing modulo with a prime mixes the bits more.
    return (reinterpret_cast<uintptr_t>(node) ^ seed_) % 13 > 6;
#endif
  }

  // Helper for InsertUnique.  Handles the case where bucket b is a
  // not-too-long linked list.
  void InsertUniqueInList(size_type b, NodeBase* node) {
    if (!TableEntryIsEmpty(b) && ShouldInsertAfterHead(node)) {
      auto* first = TableEntryToNode(table_[b]);
      node->next = first->next;
      first->next = node;
    } else {
      node->next = TableEntryToNode(table_[b]);
      table_[b] = NodeToTableEntry(node);
    }
  }

  bool TableEntryIsEmpty(size_type b) const {
    return internal::TableEntryIsEmpty(table_[b]);
  }
  bool TableEntryIsNonEmptyList(size_type b) const {
    return internal::TableEntryIsNonEmptyList(table_[b]);
  }
  bool TableEntryIsTree(size_type b) const {
    return internal::TableEntryIsTree(table_[b]);
  }
  bool TableEntryIsList(size_type b) const {
    return internal::TableEntryIsList(table_[b]);
  }

  // Return whether table_[b] is a linked list that seems awfully long.
  // Requires table_[b] to point to a non-empty linked list.
  bool TableEntryIsTooLong(size_type b) {
    return internal::TableEntryIsTooLong(TableEntryToNode(table_[b]));
  }

  // Return a power of two no less than max(kMinTableSize, n).
  // Assumes either n < kMinTableSize or n is a power of two.
  size_type TableSize(size_type n) {
    return n < static_cast<size_type>(kMinTableSize)
               ? static_cast<size_type>(kMinTableSize)
               : n;
  }

  template <typename T>
  using AllocFor = y_absl::allocator_traits<Allocator>::template rebind_alloc<T>;

  // Alignment of the nodes is the same as alignment of NodeBase.
  NodeBase* AllocNode(size_t node_size) {
    PROTOBUF_ASSUME(node_size % sizeof(NodeBase) == 0);
    return AllocFor<NodeBase>(alloc_).allocate(node_size / sizeof(NodeBase));
  }

  void DeallocNode(NodeBase* node, size_t node_size) {
    PROTOBUF_ASSUME(node_size % sizeof(NodeBase) == 0);
    AllocFor<NodeBase>(alloc_).deallocate(node, node_size / sizeof(NodeBase));
  }

  void DeleteTable(TableEntryPtr* table, size_type n) {
    AllocFor<TableEntryPtr>(alloc_).deallocate(table, n);
  }

  TableEntryPtr* CreateEmptyTable(size_type n) {
    Y_ABSL_DCHECK_GE(n, size_type{kMinTableSize});
    Y_ABSL_DCHECK_EQ(n & (n - 1), 0u);
    TableEntryPtr* result = AllocFor<TableEntryPtr>(alloc_).allocate(n);
    memset(result, 0, n * sizeof(result[0]));
    return result;
  }

  // Return a randomish value.
  size_type Seed() const {
    // We get a little bit of randomness from the address of the map. The
    // lower bits are not very random, due to alignment, so we discard them
    // and shift the higher bits into their place.
    size_type s = reinterpret_cast<uintptr_t>(this) >> 4;
#if !defined(GOOGLE_PROTOBUF_NO_RDTSC)
#if defined(__APPLE__)
    // Use a commpage-based fast time function on Apple environments (MacOS,
    // iOS, tvOS, watchOS, etc).
    s += mach_absolute_time();
#elif defined(__x86_64__) && defined(__GNUC__)
    arc_ui32 hi, lo;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    s += ((static_cast<arc_ui64>(hi) << 32) | lo);
#elif defined(__aarch64__) && defined(__GNUC__)
    // There is no rdtsc on ARMv8. CNTVCT_EL0 is the virtual counter of the
    // system timer. It runs at a different frequency than the CPU's, but is
    // the best source of time-based entropy we get.
    arc_ui64 virtual_timer_value;
    asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
    s += virtual_timer_value;
#endif
#endif  // !defined(GOOGLE_PROTOBUF_NO_RDTSC)
    return s;
  }

  size_type num_elements_;
  size_type num_buckets_;
  size_type seed_;
  size_type index_of_first_non_null_;
  TableEntryPtr* table_;  // an array with num_buckets_ entries
  Allocator alloc_;
};

// The value might be of different signedness, so use memcpy to extract it.
template <typename T, std::enable_if_t<std::is_integral<T>::value, int> = 0>
T ReadKey(const void* ptr) {
  T out;
  memcpy(&out, ptr, sizeof(T));
  return out;
}

template <typename T, std::enable_if_t<!std::is_integral<T>::value, int> = 0>
const T& ReadKey(const void* ptr) {
  return *reinterpret_cast<const T*>(ptr);
}

// KeyMapBase is a chaining hash map with the additional feature that some
// buckets can be converted to use an ordered container.  This ensures O(lg n)
// bounds on find, insert, and erase, while avoiding the overheads of ordered
// containers most of the time.
//
// The implementation doesn't need the full generality of unordered_map,
// and it doesn't have it.  More bells and whistles can be added as needed.
// Some implementation details:
// 1. The number of buckets is a power of two.
// 2. As is typical for hash_map and such, the Keys and Values are always
//    stored in linked list nodes.  Pointers to elements are never invalidated
//    until the element is deleted.
// 3. The trees' payload type is pointer to linked-list node.  Tree-converting
//    a bucket doesn't copy Key-Value pairs.
// 4. Once we've tree-converted a bucket, it is never converted back unless the
//    bucket is completely emptied out. Note that the items a tree contains may
//    wind up assigned to trees or lists upon a rehash.
// 5. Mutations to a map do not invalidate the map's iterators, pointers to
//    elements, or references to elements.
// 6. Except for erase(iterator), any non-const method can reorder iterators.
// 7. Uses KeyForTree<Key> when using the Tree representation, which
//    is either `arc_ui64` if `Key` is an integer, or
//    `reference_wrapper<const Key>` otherwise. This avoids unnecessary copies
//    of string keys, for example.

template <typename Key>
class KeyMapBase : public UntypedMapBase {
  static_assert(!std::is_signed<Key>::value || !std::is_integral<Key>::value,
                "");

 public:
  using hasher = typename TransparentSupport<Key>::hash;

  using UntypedMapBase::UntypedMapBase;

 protected:
  struct KeyNode : NodeBase {
    static constexpr size_t kOffset = sizeof(NodeBase);
    decltype(auto) key() const {
      return ReadKey<Key>(reinterpret_cast<const char*>(this) + kOffset);
    }
  };

  // Trees. The payload type is a copy of Key, so that we can query the tree
  // with Keys that are not in any particular data structure.
  // The value is a void* pointing to Node. We use void* instead of Node* to
  // avoid code bloat. That way there is only one instantiation of the tree
  // class per key type.
  using Tree = internal::TreeForMap<Key>;
  using TreeIterator = typename Tree::iterator;

  class KeyIteratorBase {
   public:
    // Invariants:
    // node_ is always correct. This is handy because the most common
    // operations are operator* and operator-> and they only use node_.
    // When node_ is set to a non-null value, all the other non-const fields
    // are updated to be correct also, but those fields can become stale
    // if the underlying map is modified.  When those fields are needed they
    // are rechecked, and updated if necessary.
    KeyIteratorBase() : node_(nullptr), m_(nullptr), bucket_index_(0) {}

    explicit KeyIteratorBase(const KeyMapBase* m) : m_(m) {
      SearchFrom(m->index_of_first_non_null_);
    }

    KeyIteratorBase(KeyNode* n, const KeyMapBase* m, size_type index)
        : node_(n), m_(m), bucket_index_(index) {}

    KeyIteratorBase(TreeIterator tree_it, const KeyMapBase* m, size_type index)
        : node_(NodeFromTreeIterator(tree_it)), m_(m), bucket_index_(index) {}

    // Advance through buckets, looking for the first that isn't empty.
    // If nothing non-empty is found then leave node_ == nullptr.
    void SearchFrom(size_type start_bucket) {
      Y_ABSL_DCHECK(m_->index_of_first_non_null_ == m_->num_buckets_ ||
                  !m_->TableEntryIsEmpty(m_->index_of_first_non_null_));
      for (size_type i = start_bucket; i < m_->num_buckets_; ++i) {
        TableEntryPtr entry = m_->table_[i];
        if (entry == TableEntryPtr{}) continue;
        bucket_index_ = i;
        if (PROTOBUF_PREDICT_TRUE(internal::TableEntryIsList(entry))) {
          node_ = static_cast<KeyNode*>(TableEntryToNode(entry));
        } else {
          Tree* tree = TableEntryToTree<Tree>(entry);
          Y_ABSL_DCHECK(!tree->empty());
          node_ = static_cast<KeyNode*>(tree->begin()->second);
        }
        return;
      }
      node_ = nullptr;
      bucket_index_ = 0;
    }

    // The definition of operator== is handled by the derived type. If we were
    // to do it in this class it would allow comparing iterators of different
    // map types.
    bool Equals(const KeyIteratorBase& other) const {
      return node_ == other.node_;
    }

    // The definition of operator++ is handled in the derived type. We would not
    // be able to return the right type from here.
    void PlusPlus() {
      if (node_->next == nullptr) {
        SearchFrom(bucket_index_ + 1);
      } else {
        node_ = static_cast<KeyNode*>(node_->next);
      }
    }

    KeyNode* node_;
    const KeyMapBase* m_;
    size_type bucket_index_;
  };

 public:
  hasher hash_function() const { return {}; }

 protected:
  PROTOBUF_NOINLINE void erase_no_destroy(size_type b, KeyNode* node) {
    TreeIterator tree_it;
    const bool is_list = revalidate_if_necessary(b, node, &tree_it);
    if (is_list) {
      Y_ABSL_DCHECK(TableEntryIsNonEmptyList(b));
      auto* head = TableEntryToNode(table_[b]);
      head = EraseFromLinkedList(node, head);
      table_[b] = NodeToTableEntry(head);
    } else {
      Y_ABSL_DCHECK(this->TableEntryIsTree(b));
      Tree* tree = internal::TableEntryToTree<Tree>(this->table_[b]);
      if (tree_it != tree->begin()) {
        auto* prev = std::prev(tree_it)->second;
        prev->next = prev->next->next;
      }
      tree->erase(tree_it);
      if (tree->empty()) {
        this->DestroyTree(tree);
        this->table_[b] = TableEntryPtr{};
      }
    }
    --num_elements_;
    if (PROTOBUF_PREDICT_FALSE(b == index_of_first_non_null_)) {
      while (index_of_first_non_null_ < num_buckets_ &&
             TableEntryIsEmpty(index_of_first_non_null_)) {
        ++index_of_first_non_null_;
      }
    }
  }

  // TODO(sbenza): We can reduce duplication by coercing `K` to a common type.
  // Eg, for string keys we can coerce to string_view. Otherwise, we instantiate
  // this with all the different `char[N]` of the caller.
  template <typename K>
  NodeAndBucket FindHelper(const K& k, TreeIterator* it = nullptr) const {
    size_type b = BucketNumber(k);
    if (TableEntryIsNonEmptyList(b)) {
      auto* node = internal::TableEntryToNode(table_[b]);
      do {
        if (internal::TransparentSupport<Key>::Equals(
                static_cast<KeyNode*>(node)->key(), k)) {
          return {node, b};
        } else {
          node = node->next;
        }
      } while (node != nullptr);
    } else if (TableEntryIsTree(b)) {
      Tree* tree = internal::TableEntryToTree<Tree>(table_[b]);
      auto tree_it = tree->find(k);
      if (it != nullptr) *it = tree_it;
      if (tree_it != tree->end()) {
        return {tree_it->second, b};
      }
    }
    return {nullptr, b};
  }

  // Insert the given Node in bucket b.  If that would make bucket b too big,
  // and bucket b is not a tree, create a tree for buckets b.
  // Requires count(*KeyPtrFromNodePtr(node)) == 0 and that b is the correct
  // bucket.  num_elements_ is not modified.
  void InsertUnique(size_type b, KeyNode* node) {
    Y_ABSL_DCHECK(index_of_first_non_null_ == num_buckets_ ||
                !TableEntryIsEmpty(index_of_first_non_null_));
    // In practice, the code that led to this point may have already
    // determined whether we are inserting into an empty list, a short list,
    // or whatever.  But it's probably cheap enough to recompute that here;
    // it's likely that we're inserting into an empty or short list.
    Y_ABSL_DCHECK(FindHelper(node->key()).node == nullptr);
    if (TableEntryIsEmpty(b)) {
      InsertUniqueInList(b, node);
      index_of_first_non_null_ = (std::min)(index_of_first_non_null_, b);
    } else if (TableEntryIsNonEmptyList(b) && !TableEntryIsTooLong(b)) {
      InsertUniqueInList(b, node);
    } else {
      if (TableEntryIsNonEmptyList(b)) {
        TreeConvert(b);
      }
      Y_ABSL_DCHECK(TableEntryIsTree(b))
          << (void*)table_[b] << " " << (uintptr_t)table_[b];
      InsertUniqueInTree(b, node);
      index_of_first_non_null_ = (std::min)(index_of_first_non_null_, b);
    }
  }

  // Helper for InsertUnique.  Handles the case where bucket b points to a
  // Tree.
  void InsertUniqueInTree(size_type b, KeyNode* node) {
    auto* tree = TableEntryToTree<Tree>(table_[b]);
    auto it = tree->insert({node->key(), node}).first;
    // Maintain the linked list of the nodes in the tree.
    // For simplicity, they are in the same order as the tree iteration.
    if (it != tree->begin()) {
      auto* prev = std::prev(it)->second;
      prev->next = node;
    }
    auto next = std::next(it);
    node->next = next != tree->end() ? next->second : nullptr;
  }

  // Returns whether it did resize.  Currently this is only used when
  // num_elements_ increases, though it could be used in other situations.
  // It checks for load too low as well as load too high: because any number
  // of erases can occur between inserts, the load could be as low as 0 here.
  // Resizing to a lower size is not always helpful, but failing to do so can
  // destroy the expected big-O bounds for some operations. By having the
  // policy that sometimes we resize down as well as up, clients can easily
  // keep O(size()) = O(number of buckets) if they want that.
  bool ResizeIfLoadIsOutOfRange(size_type new_size) {
    const size_type kMaxMapLoadTimes16 = 12;  // controls RAM vs CPU tradeoff
    const size_type hi_cutoff = num_buckets_ * kMaxMapLoadTimes16 / 16;
    const size_type lo_cutoff = hi_cutoff / 4;
    // We don't care how many elements are in trees.  If a lot are,
    // we may resize even though there are many empty buckets.  In
    // practice, this seems fine.
    if (PROTOBUF_PREDICT_FALSE(new_size >= hi_cutoff)) {
      if (num_buckets_ <= max_size() / 2) {
        Resize(num_buckets_ * 2);
        return true;
      }
    } else if (PROTOBUF_PREDICT_FALSE(new_size <= lo_cutoff &&
                                      num_buckets_ > kMinTableSize)) {
      size_type lg2_of_size_reduction_factor = 1;
      // It's possible we want to shrink a lot here... size() could even be 0.
      // So, estimate how much to shrink by making sure we don't shrink so
      // much that we would need to grow the table after a few inserts.
      const size_type hypothetical_size = new_size * 5 / 4 + 1;
      while ((hypothetical_size << lg2_of_size_reduction_factor) < hi_cutoff) {
        ++lg2_of_size_reduction_factor;
      }
      size_type new_num_buckets = std::max<size_type>(
          kMinTableSize, num_buckets_ >> lg2_of_size_reduction_factor);
      if (new_num_buckets != num_buckets_) {
        Resize(new_num_buckets);
        return true;
      }
    }
    return false;
  }

  // Resize to the given number of buckets.
  void Resize(size_t new_num_buckets) {
    if (num_buckets_ == kGlobalEmptyTableSize) {
      // This is the global empty array.
      // Just overwrite with a new one. No need to transfer or free anything.
      num_buckets_ = index_of_first_non_null_ = kMinTableSize;
      table_ = CreateEmptyTable(num_buckets_);
      seed_ = Seed();
      return;
    }

    Y_ABSL_DCHECK_GE(new_num_buckets, kMinTableSize);
    const auto old_table = table_;
    const size_type old_table_size = num_buckets_;
    num_buckets_ = new_num_buckets;
    table_ = CreateEmptyTable(num_buckets_);
    const size_type start = index_of_first_non_null_;
    index_of_first_non_null_ = num_buckets_;
    for (size_type i = start; i < old_table_size; ++i) {
      if (internal::TableEntryIsNonEmptyList(old_table[i])) {
        TransferList(static_cast<KeyNode*>(TableEntryToNode(old_table[i])));
      } else if (internal::TableEntryIsTree(old_table[i])) {
        TransferTree(TableEntryToTree<Tree>(old_table[i]));
      }
    }
    DeleteTable(old_table, old_table_size);
  }

  // Transfer all nodes in the list `node` into `this`.
  void TransferList(KeyNode* node) {
    do {
      auto* next = static_cast<KeyNode*>(node->next);
      InsertUnique(BucketNumber(node->key()), node);
      node = next;
    } while (node != nullptr);
  }

  // Transfer all nodes in the tree `tree` into `this` and destroy the tree.
  void TransferTree(Tree* tree) {
    auto* node = tree->begin()->second;
    DestroyTree(tree);
    TransferList(static_cast<KeyNode*>(node));
  }

  void TreeConvert(size_type b) {
    Y_ABSL_DCHECK(!TableEntryIsTree(b));
    Tree* tree =
        Arena::Create<Tree>(alloc_.arena(), typename Tree::key_compare(),
                            typename Tree::allocator_type(alloc_));
    size_type count = CopyListToTree(b, tree);
    Y_ABSL_DCHECK_EQ(count, tree->size());
    table_[b] = TreeToTableEntry(tree);
    // Relink the nodes.
    NodeBase* next = nullptr;
    auto it = tree->end();
    do {
      auto* node = (--it)->second;
      node->next = next;
      next = node;
    } while (it != tree->begin());
  }

  // Copy a linked list in the given bucket to a tree.
  // Returns the number of things it copied.
  size_type CopyListToTree(size_type b, Tree* tree) {
    size_type count = 0;
    auto* node = TableEntryToNode(table_[b]);
    while (node != nullptr) {
      tree->insert({static_cast<KeyNode*>(node)->key(), node});
      ++count;
      auto* next = node->next;
      node->next = nullptr;
      node = next;
    }
    return count;
  }

  template <typename K>
  size_type BucketNumber(const K& k) const {
    // We xor the hash value against the random seed so that we effectively
    // have a random hash function.
    arc_ui64 h = hash_function()(k) ^ seed_;

    // We use the multiplication method to determine the bucket number from
    // the hash value. The constant kPhi (suggested by Knuth) is roughly
    // (sqrt(5) - 1) / 2 * 2^64.
    constexpr arc_ui64 kPhi = arc_ui64{0x9e3779b97f4a7c15};
    return ((kPhi * h) >> 32) & (num_buckets_ - 1);
  }

  void DestroyTree(Tree* tree) {
    if (alloc_.arena() == nullptr) {
      delete tree;
    }
  }

  // Assumes node_ and m_ are correct and non-null, but other fields may be
  // stale.  Fix them as needed.  Then return true iff node_ points to a
  // Node in a list.  If false is returned then *it is modified to be
  // a valid iterator for node_.
  bool revalidate_if_necessary(size_t& bucket_index, KeyNode* node,
                               TreeIterator* it) const {
    // Force bucket_index to be in range.
    bucket_index &= (num_buckets_ - 1);
    // Common case: the bucket we think is relevant points to `node`.
    if (table_[bucket_index] == NodeToTableEntry(node)) return true;
    // Less common: the bucket is a linked list with node_ somewhere in it,
    // but not at the head.
    if (TableEntryIsNonEmptyList(bucket_index)) {
      auto* l = TableEntryToNode(table_[bucket_index]);
      while ((l = l->next) != nullptr) {
        if (l == node) {
          return true;
        }
      }
    }
    // Well, bucket_index_ still might be correct, but probably
    // not.  Revalidate just to be sure.  This case is rare enough that we
    // don't worry about potential optimizations, such as having a custom
    // find-like method that compares Node* instead of the key.
    auto res = FindHelper(node->key(), it);
    bucket_index = res.bucket;
    return TableEntryIsList(bucket_index);
  }
};

}  // namespace internal

// This is the class for Map's internal value_type.
template <typename Key, typename T>
using MapPair = std::pair<const Key, T>;

// Map is an associative container type used to store protobuf map
// fields.  Each Map instance may or may not use a different hash function, a
// different iteration order, and so on.  E.g., please don't examine
// implementation details to decide if the following would work:
//  Map<int, int> m0, m1;
//  m0[0] = m1[0] = m0[1] = m1[1] = 0;
//  assert(m0.begin()->first == m1.begin()->first);  // Bug!
//
// Map's interface is similar to std::unordered_map, except that Map is not
// designed to play well with exceptions.
template <typename Key, typename T>
class Map : private internal::KeyMapBase<internal::KeyForBase<Key>> {
  using Base = typename Map::KeyMapBase;

 public:
  using key_type = Key;
  using mapped_type = T;
  using init_type = std::pair<Key, T>;
  using value_type = MapPair<Key, T>;

  using pointer = value_type*;
  using const_pointer = const value_type*;
  using reference = value_type&;
  using const_reference = const value_type&;

  using size_type = size_t;
  using hasher = typename internal::TransparentSupport<Key>::hash;

  constexpr Map() : Base(nullptr) { StaticValidityCheck(); }
  explicit Map(Arena* arena) : Base(arena) { StaticValidityCheck(); }

  Map(const Map& other) : Map() { insert(other.begin(), other.end()); }

  Map(Map&& other) noexcept : Map() {
    if (other.arena() != nullptr) {
      *this = other;
    } else {
      swap(other);
    }
  }

  Map& operator=(Map&& other) noexcept {
    if (this != &other) {
      if (arena() != other.arena()) {
        *this = other;
      } else {
        swap(other);
      }
    }
    return *this;
  }

  template <class InputIt>
  Map(const InputIt& first, const InputIt& last) : Map() {
    insert(first, last);
  }

  ~Map() {
    // Fail-safe in case we miss calling this in a constructor.  Note: this one
    // won't trigger for leaked maps that never get destructed.
    StaticValidityCheck();

    if (this->alloc_.arena() == nullptr &&
        this->num_buckets_ != internal::kGlobalEmptyTableSize) {
      clear();
      this->DeleteTable(this->table_, this->num_buckets_);
    }
  }

 private:
  static_assert(!std::is_const<mapped_type>::value &&
                    !std::is_const<key_type>::value,
                "We do not support const types.");
  static_assert(!std::is_volatile<mapped_type>::value &&
                    !std::is_volatile<key_type>::value,
                "We do not support volatile types.");
  static_assert(!std::is_pointer<mapped_type>::value &&
                    !std::is_pointer<key_type>::value,
                "We do not support pointer types.");
  static_assert(!std::is_reference<mapped_type>::value &&
                    !std::is_reference<key_type>::value,
                "We do not support reference types.");
  static constexpr PROTOBUF_ALWAYS_INLINE void StaticValidityCheck() {
    static_assert(alignof(internal::NodeBase) >= alignof(mapped_type),
                  "Alignment of mapped type is too high.");
    static_assert(
        y_absl::disjunction<internal::is_supported_integral_type<key_type>,
                          internal::is_supported_string_type<key_type>,
                          internal::is_internal_map_key_type<key_type>>::value,
        "We only support integer, string, or designated internal key "
        "types.");
    static_assert(y_absl::disjunction<
                      internal::is_supported_scalar_type<mapped_type>,
                      is_proto_enum<mapped_type>,
                      internal::is_supported_message_type<mapped_type>,
                      internal::is_internal_map_value_type<mapped_type>>::value,
                  "We only support scalar, Message, and designated internal "
                  "mapped types.");
  }

  template <typename P>
  struct SameAsElementReference
      : std::is_same<typename std::remove_cv<
                         typename std::remove_reference<reference>::type>::type,
                     typename std::remove_cv<
                         typename std::remove_reference<P>::type>::type> {};

  template <class P>
  using RequiresInsertable =
      typename std::enable_if<std::is_convertible<P, init_type>::value ||
                                  SameAsElementReference<P>::value,
                              int>::type;
  template <class P>
  using RequiresNotInit =
      typename std::enable_if<!std::is_same<P, init_type>::value, int>::type;

  template <typename LookupKey>
  using key_arg = typename internal::TransparentSupport<
      key_type>::template key_arg<LookupKey>;

 public:
  // Iterators
  class const_iterator : private Base::KeyIteratorBase {
    using BaseIt = typename Base::KeyIteratorBase;

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename Map::value_type;
    using difference_type = ptrdiff_t;
    using pointer = const value_type*;
    using reference = const value_type&;

    const_iterator() {}
    const_iterator(const const_iterator&) = default;
    const_iterator& operator=(const const_iterator&) = default;

    reference operator*() const { return static_cast<Node*>(this->node_)->kv; }
    pointer operator->() const { return &(operator*()); }

    const_iterator& operator++() {
      this->PlusPlus();
      return *this;
    }
    const_iterator operator++(int) {
      auto copy = *this;
      this->PlusPlus();
      return copy;
    }

    friend bool operator==(const const_iterator& a, const const_iterator& b) {
      return a.Equals(b);
    }
    friend bool operator!=(const const_iterator& a, const const_iterator& b) {
      return !a.Equals(b);
    }

   private:
    using BaseIt::BaseIt;
    explicit const_iterator(const BaseIt& base) : BaseIt(base) {}
    friend class Map;
  };

  class iterator : private Base::KeyIteratorBase {
    using BaseIt = typename Base::KeyIteratorBase;

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename Map::value_type;
    using difference_type = ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;

    iterator() {}
    iterator(const iterator&) = default;
    iterator& operator=(const iterator&) = default;

    reference operator*() const { return static_cast<Node*>(this->node_)->kv; }
    pointer operator->() const { return &(operator*()); }

    iterator& operator++() {
      this->PlusPlus();
      return *this;
    }
    iterator operator++(int) {
      auto copy = *this;
      this->PlusPlus();
      return copy;
    }

    // Allow implicit conversion to const_iterator.
    operator const_iterator() const {  // NOLINT(runtime/explicit)
      return const_iterator(static_cast<const BaseIt&>(*this));
    }

    friend bool operator==(const iterator& a, const iterator& b) {
      return a.Equals(b);
    }
    friend bool operator!=(const iterator& a, const iterator& b) {
      return !a.Equals(b);
    }

   private:
    using BaseIt::BaseIt;
    friend class Map;
  };

  iterator begin() { return iterator(this); }
  iterator end() { return iterator(); }
  const_iterator begin() const { return const_iterator(this); }
  const_iterator end() const { return const_iterator(); }
  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

  using Base::empty;
  using Base::size;

  // Element access
  template <typename K = key_type>
  T& operator[](const key_arg<K>& key) {
    return try_emplace(key).first->second;
  }
  template <
      typename K = key_type,
      // Disable for integral types to reduce code bloat.
      typename = typename std::enable_if<!std::is_integral<K>::value>::type>
  T& operator[](key_arg<K>&& key) {
    return try_emplace(std::forward<K>(key)).first->second;
  }

  template <typename K = key_type>
  const T& at(const key_arg<K>& key) const {
    const_iterator it = find(key);
    Y_ABSL_CHECK(it != end()) << "key not found: " << static_cast<Key>(key);
    return it->second;
  }

  template <typename K = key_type>
  T& at(const key_arg<K>& key) {
    iterator it = find(key);
    Y_ABSL_CHECK(it != end()) << "key not found: " << static_cast<Key>(key);
    return it->second;
  }

  // Lookup
  template <typename K = key_type>
  size_type count(const key_arg<K>& key) const {
    return find(key) == end() ? 0 : 1;
  }

  template <typename K = key_type>
  const_iterator find(const key_arg<K>& key) const {
    return const_cast<Map*>(this)->find(key);
  }
  template <typename K = key_type>
  iterator find(const key_arg<K>& key) {
    auto res = this->FindHelper(key);
    return iterator(static_cast<Node*>(res.node), this, res.bucket);
  }

  template <typename K = key_type>
  bool contains(const key_arg<K>& key) const {
    return find(key) != end();
  }

  template <typename K = key_type>
  std::pair<const_iterator, const_iterator> equal_range(
      const key_arg<K>& key) const {
    const_iterator it = find(key);
    if (it == end()) {
      return std::pair<const_iterator, const_iterator>(it, it);
    } else {
      const_iterator begin = it++;
      return std::pair<const_iterator, const_iterator>(begin, it);
    }
  }

  template <typename K = key_type>
  std::pair<iterator, iterator> equal_range(const key_arg<K>& key) {
    iterator it = find(key);
    if (it == end()) {
      return std::pair<iterator, iterator>(it, it);
    } else {
      iterator begin = it++;
      return std::pair<iterator, iterator>(begin, it);
    }
  }

  // insert
  template <typename K, typename... Args>
  std::pair<iterator, bool> try_emplace(K&& k, Args&&... args) {
    // Inserts a new element into the container if there is no element with the
    // key in the container.
    // The new element is:
    //  (1) Constructed in-place with the given args, if mapped_type is not
    //      arena constructible.
    //  (2) Constructed in-place with the arena and then assigned with a
    //      mapped_type temporary constructed with the given args, otherwise.
    return ArenaAwareTryEmplace(Arena::is_arena_constructable<mapped_type>(),
                                std::forward<K>(k),
                                std::forward<Args>(args)...);
  }
  std::pair<iterator, bool> insert(init_type&& value) {
    return try_emplace(std::move(value.first), std::move(value.second));
  }
  template <typename P, RequiresInsertable<P> = 0>
  std::pair<iterator, bool> insert(P&& value) {
    return try_emplace(std::forward<P>(value).first,
                       std::forward<P>(value).second);
  }
  template <typename... Args>
  std::pair<iterator, bool> emplace(Args&&... args) {
    return EmplaceInternal(Rank0{}, std::forward<Args>(args)...);
  }
  template <class InputIt>
  void insert(InputIt first, InputIt last) {
    for (; first != last; ++first) {
      auto&& pair = *first;
      try_emplace(pair.first, pair.second);
    }
  }
  void insert(std::initializer_list<init_type> values) {
    insert(values.begin(), values.end());
  }
  template <typename P, RequiresNotInit<P> = 0,
            RequiresInsertable<const P&> = 0>
  void insert(std::initializer_list<P> values) {
    insert(values.begin(), values.end());
  }

  // Erase and clear
  template <typename K = key_type>
  size_type erase(const key_arg<K>& key) {
    iterator it = find(key);
    if (it == end()) {
      return 0;
    } else {
      erase(it);
      return 1;
    }
  }

  iterator erase(iterator pos) {
    auto next = std::next(pos);
    Y_ABSL_DCHECK_EQ(pos.m_, static_cast<Base*>(this));
    auto* node = static_cast<Node*>(pos.node_);
    this->erase_no_destroy(pos.bucket_index_, node);
    DestroyNode(node);
    return next;
  }

  void erase(iterator first, iterator last) {
    while (first != last) {
      first = erase(first);
    }
  }

  void clear() {
    for (size_type b = 0; b < this->num_buckets_; b++) {
      internal::NodeBase* node;
      if (this->TableEntryIsNonEmptyList(b)) {
        node = internal::TableEntryToNode(this->table_[b]);
        this->table_[b] = TableEntryPtr{};
      } else if (this->TableEntryIsTree(b)) {
        Tree* tree = internal::TableEntryToTree<Tree>(this->table_[b]);
        this->table_[b] = TableEntryPtr{};
        node = NodeFromTreeIterator(tree->begin());
        this->DestroyTree(tree);
      } else {
        continue;
      }
      do {
        auto* next = node->next;
        DestroyNode(static_cast<Node*>(node));
        node = next;
      } while (node != nullptr);
    }
    this->num_elements_ = 0;
    this->index_of_first_non_null_ = this->num_buckets_;
  }

  // Assign
  Map& operator=(const Map& other) {
    if (this != &other) {
      clear();
      insert(other.begin(), other.end());
    }
    return *this;
  }

  void swap(Map& other) {
    if (arena() == other.arena()) {
      InternalSwap(&other);
    } else {
      // TODO(zuguang): optimize this. The temporary copy can be allocated
      // in the same arena as the other message, and the "other = copy" can
      // be replaced with the fast-path swap above.
      Map copy = *this;
      *this = other;
      other = copy;
    }
  }

  void InternalSwap(Map* other) { this->Swap(other); }

  hasher hash_function() const { return {}; }

  size_t SpaceUsedExcludingSelfLong() const {
    if (empty()) return 0;
    return SpaceUsedInternal() + internal::SpaceUsedInValues(this);
  }

 private:
  struct Rank1 {};
  struct Rank0 : Rank1 {};

  // Linked-list nodes, as one would expect for a chaining hash table.
  struct Node : Base::KeyNode {
    value_type kv;
  };

  using Tree = internal::TreeForMap<Key>;
  using TreeIterator = typename Tree::iterator;
  using TableEntryPtr = internal::TableEntryPtr;

  static Node* NodeFromTreeIterator(TreeIterator it) {
    static_assert(
        PROTOBUF_FIELD_OFFSET(Node, kv.first) == Base::KeyNode::kOffset, "");
    static_assert(alignof(Node) == alignof(internal::NodeBase), "");
    return static_cast<Node*>(it->second);
  }

  void DestroyNode(Node* node) {
    if (this->alloc_.arena() == nullptr) {
      node->kv.first.~key_type();
      node->kv.second.~mapped_type();
      this->DeallocNode(node, sizeof(Node));
    }
  }

  size_t SpaceUsedInternal() const {
    return internal::SpaceUsedInTable<Key>(this->table_, this->num_buckets_,
                                           this->num_elements_, sizeof(Node));
  }

  // We try to construct `init_type` from `Args` with a fall back to
  // `value_type`. The latter is less desired as it unconditionally makes a copy
  // of `value_type::first`.
  template <typename... Args>
  auto EmplaceInternal(Rank0, Args&&... args) ->
      typename std::enable_if<std::is_constructible<init_type, Args...>::value,
                              std::pair<iterator, bool>>::type {
    return insert(init_type(std::forward<Args>(args)...));
  }
  template <typename... Args>
  std::pair<iterator, bool> EmplaceInternal(Rank1, Args&&... args) {
    return insert(value_type(std::forward<Args>(args)...));
  }

  template <typename K, typename... Args>
  std::pair<iterator, bool> TryEmplaceInternal(K&& k, Args&&... args) {
    auto p = this->FindHelper(k);
    // Case 1: key was already present.
    if (p.node != nullptr)
      return std::make_pair(
          iterator(static_cast<Node*>(p.node), this, p.bucket), false);
    // Case 2: insert.
    if (this->ResizeIfLoadIsOutOfRange(this->num_elements_ + 1)) {
      p = this->FindHelper(k);
    }
    const size_type b = p.bucket;  // bucket number
    // If K is not key_type, make the conversion to key_type explicit.
    using TypeToInit = typename std::conditional<
        std::is_same<typename std::decay<K>::type, key_type>::value, K&&,
        key_type>::type;
    Node* node = static_cast<Node*>(this->AllocNode(sizeof(Node)));
    // Even when arena is nullptr, CreateInArenaStorage is still used to
    // ensure the arena of submessage will be consistent. Otherwise,
    // submessage may have its own arena when message-owned arena is enabled.
    // Note: This only works if `Key` is not arena constructible.
    Arena::CreateInArenaStorage(const_cast<Key*>(&node->kv.first),
                                this->alloc_.arena(),
                                static_cast<TypeToInit>(std::forward<K>(k)));
    // Note: if `T` is arena constructible, `Args` needs to be empty.
    Arena::CreateInArenaStorage(&node->kv.second, this->alloc_.arena(),
                                std::forward<Args>(args)...);

    this->InsertUnique(b, node);
    ++this->num_elements_;
    return std::make_pair(iterator(node, this, b), true);
  }

  // A helper function to perform an assignment of `mapped_type`.
  // If the first argument is true, then it is a regular assignment.
  // Otherwise, we first create a temporary and then perform an assignment.
  template <typename V>
  static void AssignMapped(std::true_type, mapped_type& mapped, V&& v) {
    mapped = std::forward<V>(v);
  }
  template <typename... Args>
  static void AssignMapped(std::false_type, mapped_type& mapped,
                           Args&&... args) {
    mapped = mapped_type(std::forward<Args>(args)...);
  }

  // Case 1: `mapped_type` is arena constructible. A temporary object is
  // created and then (if `Args` are not empty) assigned to a mapped value
  // that was created with the arena.
  template <typename K>
  std::pair<iterator, bool> ArenaAwareTryEmplace(std::true_type, K&& k) {
    // case 1.1: "default" constructed (e.g. from arena only).
    return TryEmplaceInternal(std::forward<K>(k));
  }
  template <typename K, typename... Args>
  std::pair<iterator, bool> ArenaAwareTryEmplace(std::true_type, K&& k,
                                                 Args&&... args) {
    // case 1.2: "default" constructed + copy/move assignment
    auto p = TryEmplaceInternal(std::forward<K>(k));
    if (p.second) {
      AssignMapped(std::is_same<void(typename std::decay<Args>::type...),
                                void(mapped_type)>(),
                   p.first->second, std::forward<Args>(args)...);
    }
    return p;
  }
  // Case 2: `mapped_type` is not arena constructible. Using in-place
  // construction.
  template <typename... Args>
  std::pair<iterator, bool> ArenaAwareTryEmplace(std::false_type,
                                                 Args&&... args) {
    return TryEmplaceInternal(std::forward<Args>(args)...);
  }

  using Base::arena;

  friend class Arena;
  template <typename, typename>
  friend class internal::TypeDefinedMapFieldBase;
  using InternalArenaConstructable_ = void;
  using DestructorSkippable_ = void;
  template <typename Derived, typename K, typename V,
            internal::WireFormatLite::FieldType key_wire_type,
            internal::WireFormatLite::FieldType value_wire_type>
  friend class internal::MapFieldLite;
};

namespace internal {
template <typename... T>
PROTOBUF_NOINLINE void MapMergeFrom(Map<T...>& dest, const Map<T...>& src) {
  for (const auto& elem : src) {
    dest[elem.first] = elem.second;
  }
}
}  // namespace internal

}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"

#endif  // GOOGLE_PROTOBUF_MAP_H__
