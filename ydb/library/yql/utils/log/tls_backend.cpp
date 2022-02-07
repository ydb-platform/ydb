#include "tls_backend.h"

#include <util/system/tls.h>


namespace NYql {
namespace NLog {
namespace {

Y_POD_STATIC_THREAD(TLogBackend*) CurrentBackend;

} // namspace

TLogBackend* SetLogBackendForCurrentThread(TLogBackend* backend) {
   TLogBackend* prev = *(&CurrentBackend);
   *(&CurrentBackend) = backend;
   return prev;
}

void TTlsLogBackend::WriteData(const TLogRecord& rec) {
    TLogBackend* backend = *(&CurrentBackend);
    if (backend) {
        backend->WriteData(rec);
    } else {
        DefaultBackend_->WriteData(rec);
    }
}

void TTlsLogBackend::ReopenLog() {
    TLogBackend* backend = *(&CurrentBackend);
    if (backend) {
        backend->ReopenLog();
    } else {
        DefaultBackend_->ReopenLog();
    }
}

ELogPriority TTlsLogBackend::FiltrationLevel() const {
    TLogBackend* backend = *(&CurrentBackend);
    if (backend) {
        return backend->FiltrationLevel();
    } else {
        return DefaultBackend_->FiltrationLevel();
    }
    return LOG_MAX_PRIORITY;
}

} // namspace NLog
} // namspace NYql
