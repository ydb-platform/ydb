#pragma once

#include <yql/essentials/utils/runnable.h>

namespace NYql::NFmr {

enum class EFmrWorkerRuntimeState {
    Stopped,
    Running,
};

class IFmrWorker: public IRunnable {
public:
    using TPtr = TIntrusivePtr<IFmrWorker>;

    virtual ~IFmrWorker() = default;

    virtual EFmrWorkerRuntimeState GetWorkerState() const = 0;
};

} // namespace NYql::NFmr
