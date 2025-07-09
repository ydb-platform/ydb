#include "procfs.h"

#include <util/stream/file.h>

namespace NYT::NProcFS {

////////////////////////////////////////////////////////////////////////////////

int GetThreadCount()
{
#ifdef _linux_
    TFileInput statusFile("/proc/self/status");
    TString line;
    std::optional<int> threads = 0;
    while (statusFile.ReadLine(line)) {
        constexpr std::string_view threadsHeader = "Threads:\t";
        if (auto pos = line.find(threadsHeader); pos != std::string::npos) {
            std::string_view threadsStr = std::string_view(line).substr(pos + size(threadsHeader));
            try {
                threads = std::stoi(threadsStr.data());
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION(EErrorCode::FailedToParseProcFS, "Failed to parse 'Threads' field in /proc/self/status")
                    << ex;
            }
        }
    }

    if (!threads.has_value()) {
        THROW_ERROR_EXCEPTION(EErrorCode::NoSuchInfoInProcFS, "No field 'Threads' in /proc/self/status");
    }

    return *threads;
#else // _linux_
    THROW_ERROR_EXCEPTION("There is no procfs on this platform");
#endif // _linux_
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProcFS
