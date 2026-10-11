#include <version>

#if !defined(__cpp_lib_atomic_shared_ptr) || __cpp_lib_atomic_shared_ptr != 201711L
#error atomic smart pointers must be advertised by <version>
#endif

#include <atomic>
#include <memory>

static_assert(!std::atomic<std::shared_ptr<int>>::is_always_lock_free);
static_assert(!std::atomic<std::weak_ptr<int>>::is_always_lock_free);
