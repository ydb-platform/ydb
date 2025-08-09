#pragma once

#include <library/cpp/logger/backend.h>

#include <util/generic/ptr.h>

namespace NYql::NLog {

    class TForwardingLogBackend: public TLogBackend {
    public:
        explicit TForwardingLogBackend(TAutoPtr<TLogBackend> child);

        void WriteData(const TLogRecord& rec) override;
        void ReopenLog() override;
        void ReopenLogNoFlush() override;
        ELogPriority FiltrationLevel() const override;
        size_t QueueSize() const override;

        void SetChild(TAutoPtr<TLogBackend> child);
        TAutoPtr<TLogBackend> GetChild() const;

    private:
        TAutoPtr<TLogBackend> Child_;
    };

} // namespace NYql::NLog
