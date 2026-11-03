#include <version>

#ifdef __cpp_lib_atomic_shared_ptr
#error atomic smart pointers must not be advertised by <version> yet
#endif

#include <atomic>
#include <cstdint>
#include <memory>

static_assert(!std::atomic<std::shared_ptr<std::int32_t>>::is_always_lock_free);
static_assert(!std::atomic<std::weak_ptr<std::int32_t>>::is_always_lock_free);
