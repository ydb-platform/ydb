#pragma once

#include <library/cpp/logger/backend.h>

#include <util/generic/ptr.h>

#include <utility>


namespace NYql {
namespace NLog {

/**
 * @brief Dispatches all invocations to default logger backend configured
 *        for current thread. Must be used in conjunction with
 *        SetLogBackendForCurrentThread() function or TScopedBackend class.
 */
class TTlsLogBackend: public TLogBackend {
public:
    TTlsLogBackend(TAutoPtr<TLogBackend> defaultBackend)
        : DefaultBackend_(defaultBackend)
    {
        Y_DEBUG_ABORT_UNLESS(DefaultBackend_, "default backend is not set");
    }

    void WriteData(const TLogRecord& rec) override;
    void ReopenLog() override;
    ELogPriority FiltrationLevel() const override;

private:
    TAutoPtr<TLogBackend> DefaultBackend_;
};

/**
 * @brief Sets given backend as default for current thread. Must be used in
 *        conjunction with TTlsLogBackend.
 *
 * @param backend - pointer to logger backend
 * @return previous default logger backend
 */
TLogBackend* SetLogBackendForCurrentThread(TLogBackend* backend);

/**
 * @brief Sets itself as default for current thread on instantiation
 *        and restores previous one on destruction. Must be used in
 *        conjunction with TTlsLogBackend.
 */
template <typename TBackend>
class TScopedBackend: public TBackend {
public:
    template <typename... TArgs>
    TScopedBackend(TArgs&&... args)
        : TBackend(std::forward<TArgs>(args)...)
        , PrevBacked_(SetLogBackendForCurrentThread(this))
    {
    }

    ~TScopedBackend() {
        SetLogBackendForCurrentThread(PrevBacked_);
    }

private:
    TLogBackend* PrevBacked_;
};

} // namspace NLog
} // namspace NYql
