#include "moody_camel_blocking_concurrent_queue.h"
#include "moody_camel_nonblocking_concurrent_queue.h"

namespace moodycamel {

////////////////////////////////////////////////////////////////////////////////

#if defined(_tsan_enabled_)
template <typename T, typename Traits = ConcurrentQueueDefaultTraits>
using ConcurrentQueue = BlockingConcurrentQueue<T, Traits>;
#else
template <typename T, typename Traits = ConcurrentQueueDefaultTraits>
using ConcurrentQueue = NonBlockingConcurrentQueue<T, Traits>;
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace moodycamel
