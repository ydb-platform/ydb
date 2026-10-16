// <memory> must provide the specializations without relying on transitive includes from <atomic>.
#include <memory>

#ifdef __cpp_lib_atomic_shared_ptr
#error atomic smart pointers must not be advertised by <memory> yet
#endif

#include <cstdint>

namespace {
    struct TIncomplete;

    constinit std::atomic<std::shared_ptr<TIncomplete>> Shared;
    constinit std::atomic<std::shared_ptr<TIncomplete>> NullShared(nullptr);
    constinit std::atomic<std::weak_ptr<TIncomplete>> Weak;

    static_assert(noexcept(Shared.load()));
    static_assert(noexcept(Weak.load()));
    static_assert(sizeof(std::shared_ptr<std::int32_t>) == 2 * sizeof(void*));
    static_assert(sizeof(std::weak_ptr<std::int32_t>) == 2 * sizeof(void*));
}
