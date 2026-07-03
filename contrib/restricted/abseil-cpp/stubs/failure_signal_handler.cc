#include <contrib/restricted/abseil-cpp/absl/debugging/failure_signal_handler.h>

namespace absl {
ABSL_NAMESPACE_BEGIN

    void InstallFailureSignalHandler(const FailureSignalHandlerOptions& /* options */) {
    }

    namespace debugging_internal {

        const char* FailureSignalToString(int /* signo */) {
            return nullptr;
        }

    }  // namespace debugging_internal

ABSL_NAMESPACE_END
}  // namespace absl
