#ifndef STAN_MATH_MEMORY_STACK_ALLOC_HPP
#define STAN_MATH_MEMORY_STACK_ALLOC_HPP

// TODO(Bob): <cstddef> replaces this ifdef in C++11, until then this
//            is best we can do to get safe pointer casts to uints.
#include <stdint.h>
#include <stan/math/prim/scal/meta/likely.hpp>
#include <cstdlib>
#include <cstddef>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace stan {
namespace math {

/**
 * Return <code>true</code> if the specified pointer is aligned
 * on the number of bytes.
 *
 * This doesn't really make sense other than for powers of 2.
 *
 * @param ptr Pointer to test.
 * @param bytes_aligned Number of bytes of alignment required.
 * @return <code>true</code> if pointer is aligned.
 * @tparam Type of object to which pointer points.
 */
template <typename T>
bool is_aligned(T* ptr, unsigned int bytes_aligned) {
  return (reinterpret_cast<uintptr_t>(ptr) % bytes_aligned) == 0U;
}

namespace internal {
const size_t DEFAULT_INITIAL_NBYTES = 1 << 16;  // 64KB

// FIXME: enforce alignment
// big fun to inline, but only called twice
inline char* eight_byte_aligned_malloc(size_t size) {
  char* ptr = static_cast<char*>(malloc(size));
  if (!ptr)
    return ptr;  // malloc failed to alloc
  if (!is_aligned(ptr, 8U)) {
    std::stringstream s;
    s << "invalid alignment to 8 bytes, ptr="
      << reinterpret_cast<uintptr_t>(ptr) << std::endl;
    throw std::runtime_error(s.str());
  }
  return ptr;
}
}  // namespace internal

/**
 * An instance of this class provides a memory pool through
 * which blocks of raw memory may be allocated and then collected
 * simultaneously.
 *
 * This class is useful in settings where large numbers of small
 * objects are allocated and then collected all at once.  This may
 * include objects whose destructors have no effect.
 *
 * Memory is allocated on a stack of blocks.  Each block allocated
 * is twice as large as the previous one.  The memory may be
 * recovered, with the blocks being reused, or all blocks may be
 * freed, resetting the stack of blocks to its original state.
 *
 * Alignment up to 8 byte boundaries guaranteed for the first malloc,
 * and after that it's up to the caller.  On 64-bit architectures,
 * all struct values should be padded to 8-byte boundaries if they
 * contain an 8-byte member or a virtual function.
 */
class stack_alloc {
 private:
  std::vector<char*> blocks_;  // storage for blocks,
                               // may be bigger than cur_block_
  std::vector<size_t> sizes_;  // could store initial & shift for others
  size_t cur_block_;           // index into blocks_ for next alloc
  char* cur_block_end_;        // ptr to cur_block_ptr_ + sizes_[cur_block_]
  char* next_loc_;             // ptr to next available spot in cur
                               // block
  // next three for keeping track of nested allocations on top of stack:
  std::vector<size_t> nested_cur_blocks_;
  std::vector<char*> nested_next_locs_;
  std::vector<char*> nested_cur_block_ends_;

  /**
   * Moves us to the next block of memory, allocating that block
   * if necessary, and allocates len bytes of memory within that
   * block.
   *
   * @param size_t $len Number of bytes to allocate.
   * @return A pointer to the allocated memory.
   */
  char* move_to_next_block(size_t len) {
    char* result;
    ++cur_block_;
    // Find the next block (if any) containing at least len bytes.
    while ((cur_block_ < blocks_.size()) && (sizes_[cur_block_] < len))
      ++cur_block_;
    // Allocate a new block if necessary.
    if (unlikely(cur_block_ >= blocks_.size())) {
      // New block should be max(2*size of last block, len) bytes.
      size_t newsize = sizes_.back() * 2;
      if (newsize < len)
        newsize = len;
      blocks_.push_back(internal::eight_byte_aligned_malloc(newsize));
      if (!blocks_.back())
        throw std::bad_alloc();
      sizes_.push_back(newsize);
    }
    result = blocks_[cur_block_];
    // Get the object's state back in order.
    next_loc_ = result + len;
    cur_block_end_ = result + sizes_[cur_block_];
    return result;
  }

 public:
  /**
   * Construct a resizable stack allocator initially holding the
   * specified number of bytes.
   *
   * @param initial_nbytes Initial number of bytes for the
   * allocator.  Defaults to <code>(1 << 16) = 64KB</code> initial bytes.
   * @throws std::runtime_error if the underlying malloc is not 8-byte
   * aligned.
   */
  explicit stack_alloc(size_t initial_nbytes = internal::DEFAULT_INITIAL_NBYTES)
      : blocks_(1, internal::eight_byte_aligned_malloc(initial_nbytes)),
        sizes_(1, initial_nbytes),
        cur_block_(0),
        cur_block_end_(blocks_[0] + initial_nbytes),
        next_loc_(blocks_[0]) {
    if (!blocks_[0])
      throw std::bad_alloc();  // no msg allowed in bad_alloc ctor
  }

  /**
   * Destroy this memory allocator.
   *
   * This is implemented as a no-op as there is no destruction
   * required.
   */
  ~stack_alloc() {
    // free ALL blocks
    for (auto& block : blocks_)
      if (block)
        free(block);
  }

  /**
   * Return a newly allocated block of memory of the appropriate
   * size managed by the stack allocator.
   *
   * The allocated pointer will be 8-byte aligned.
   *
   * This function may call C++'s <code>malloc()</code> function,
   * with any exceptions percolated throught this function.
   *
   * @param len Number of bytes to allocate.
   * @return A pointer to the allocated memory.
   */
  inline void* alloc(size_t len) {
    // Typically, just return and increment the next location.
    char* result = next_loc_;
    next_loc_ += len;
    // Occasionally, we have to switch blocks.
    if (unlikely(next_loc_ >= cur_block_end_))
      result = move_to_next_block(len);
    return reinterpret_cast<void*>(result);
  }

  /**
   * Allocate an array on the arena of the specified size to hold
   * values of the specified template parameter type.
   *
   * @tparam T type of entries in allocated array.
   * @param[in] n size of array to allocate.
   * @return new array allocated on the arena.
   */
  template <typename T>
  inline T* alloc_array(size_t n) {
    return static_cast<T*>(alloc(n * sizeof(T)));
  }

  /**
   * Recover all the memory used by the stack allocator.  The stack
   * of memory blocks allocated so far will be available for further
   * allocations.  To free memory back to the system, use the
   * function free_all().
   */
  inline void recover_all() {
    cur_block_ = 0;
    next_loc_ = blocks_[0];
    cur_block_end_ = next_loc_ + sizes_[0];
  }

  /**
   * Store current positions before doing nested operation so can
   * recover back to start.
   */
  inline void start_nested() {
    nested_cur_blocks_.push_back(cur_block_);
    nested_next_locs_.push_back(next_loc_);
    nested_cur_block_ends_.push_back(cur_block_end_);
  }

  /**
   * recover memory back to the last start_nested call.
   */
  inline void recover_nested() {
    if (unlikely(nested_cur_blocks_.empty()))
      recover_all();

    cur_block_ = nested_cur_blocks_.back();
    nested_cur_blocks_.pop_back();

    next_loc_ = nested_next_locs_.back();
    nested_next_locs_.pop_back();

    cur_block_end_ = nested_cur_block_ends_.back();
    nested_cur_block_ends_.pop_back();
  }

  /**
   * Free all memory used by the stack allocator other than the
   * initial block allocation back to the system.  Note:  the
   * destructor will free all memory.
   */
  inline void free_all() {
    // frees all BUT the first (index 0) block
    for (size_t i = 1; i < blocks_.size(); ++i)
      if (blocks_[i])
        free(blocks_[i]);
    sizes_.resize(1);
    blocks_.resize(1);
    recover_all();
  }

  /**
   * Return number of bytes allocated to this instance by the heap.
   * This is not the same as the number of bytes allocated through
   * calls to memalloc_.  The latter number is not calculatable
   * because space is wasted at the end of blocks if the next
   * alloc request doesn't fit.  (Perhaps we could trim down to
   * what is actually used?)
   *
   * @return number of bytes allocated to this instance
   */
  inline size_t bytes_allocated() const {
    size_t sum = 0;
    for (size_t i = 0; i <= cur_block_; ++i) {
      sum += sizes_[i];
    }
    return sum;
  }

  /**
   * Indicates whether the memory in the pointer
   * is in the stack.
   *
   * @param[in] ptr memory location
   * @return true if the pointer is in the stack,
   *    false otherwise.
   */
  inline bool in_stack(const void* ptr) const {
    for (size_t i = 0; i < cur_block_; ++i)
      if (ptr >= blocks_[i] && ptr < blocks_[i] + sizes_[i])
        return true;
    if (ptr >= blocks_[cur_block_] && ptr < next_loc_)
      return true;
    return false;
  }
};

}  // namespace math
}  // namespace stan
#endif
