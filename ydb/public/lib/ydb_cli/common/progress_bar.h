#pragma once

#include <cstddef>
#include <ydb/library/accessor/accessor.h>

namespace NYdb {
namespace NConsoleClient {

class TProgressBar {
public:
    explicit TProgressBar(size_t capacity);

    ~TProgressBar();

    void SetProcess(size_t progress);

    void AddProgress(size_t value);

    YDB_READONLY(size_t, Capacity, 0);
    YDB_READONLY(size_t, CurProgress, 0);
private:
    void Render();

    bool Finished = false;
};

} // namespace NConsoleClient
} // namespace NYdb
