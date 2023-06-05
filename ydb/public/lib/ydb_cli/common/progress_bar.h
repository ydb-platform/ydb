#pragma once

#include <cstddef>

namespace NYdb {
namespace NConsoleClient {

class TProgressBar {
public:
    explicit TProgressBar(size_t capacity);

    void AddProgress(size_t value);

private:
    size_t Capacity = 0;
    size_t CurProgress = 0;
};

} // namespace NConsoleClient
} // namespace NYdb
