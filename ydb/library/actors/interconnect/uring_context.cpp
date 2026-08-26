#include "uring_context.h"

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include <ydb/library/uring/liburing_linux.h>

namespace NActors {

    bool TUringContext::IsAvailable() {
        static const bool available = [] {
            // Minimal capability: can we create a plain ring at all?
            struct io_uring ring;
            struct io_uring_params params = {};
            const int ret = io_uring_queue_init_params(8, &ring, &params);
            if (ret != 0) {
                return false;
            }
            io_uring_queue_exit(&ring);
            return true;
        }();
        return available;
    }

} // namespace NActors
