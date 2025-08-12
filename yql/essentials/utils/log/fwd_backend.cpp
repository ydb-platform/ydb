#include "fwd_backend.h"

namespace NYql::NLog {

    TForwardingLogBackend::TForwardingLogBackend(TAutoPtr<TLogBackend> child)
        : Child_(std::move(child))
    {
    }

    void TForwardingLogBackend::WriteData(const TLogRecord& rec) {
        return Child_->WriteData(rec);
    }

    void TForwardingLogBackend::ReopenLog() {
        return Child_->ReopenLog();
    }

    void TForwardingLogBackend::ReopenLogNoFlush() {
        return Child_->ReopenLogNoFlush();
    }

    ELogPriority TForwardingLogBackend::FiltrationLevel() const {
        return Child_->FiltrationLevel();
    }

    size_t TForwardingLogBackend::QueueSize() const {
        return Child_->QueueSize();
    }

    void TForwardingLogBackend::SetChild(TAutoPtr<TLogBackend> child) {
        Child_ = std::move(child);
    }

    TAutoPtr<TLogBackend> TForwardingLogBackend::GetChild() const {
        return Child_;
    }

} // namespace NYql::NLog
