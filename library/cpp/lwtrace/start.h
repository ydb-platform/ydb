#pragma once

#include <functional>

namespace NLWTrace {
    class TManager;

    void StartLwtraceFromEnv();
    void StartLwtraceFromEnv(std::function<void(TManager&)> prepare);

}
