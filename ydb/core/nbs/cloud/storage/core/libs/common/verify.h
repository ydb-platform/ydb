#pragma once

#include <library/cpp/logger/log.h>

#include <util/generic/singleton.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>
#include <util/system/yassert.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct TVerifyLog
{
    TLog Log;
    TAdaptiveLock Lock;
};

inline void SetVerifyLogBackend(std::unique_ptr<TLogBackend> backend)
{
    auto* vl = Singleton<TVerifyLog>();
    with_lock (vl->Lock) {
        vl->Log.ResetBackend(THolder<TLogBackend>(backend.release()));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TWellKnownEntityTypes
{
    static constexpr TStringBuf DISK = "Disk";
    static constexpr TStringBuf TABLET = "Tablet";
    static constexpr TStringBuf CLIENT = "Client";
    static constexpr TStringBuf ENDPOINT = "Endpoint";
    static constexpr TStringBuf DEVICE = "Device";
    static constexpr TStringBuf FILESYSTEM = "Filesystem";
    static constexpr TStringBuf AGENT = "Agent";
    static constexpr TStringBuf YDB_TABLE = "Ydb Table";
};

}   // namespace NYdb::NBS

#define STORAGE_VERIFY_C(expr, entityType, entityId, message)                  \
    do {                                                                       \
        if (Y_UNLIKELY(!(expr))) {                                             \
            TStringBuilder sb;                                                 \
            sb << "Problem with " << entityType << " " << entityId << ": "     \
               << #expr;                                                       \
            TStringBuilder msg;                                                \
            msg << message;                                                    \
            if (msg) {                                                         \
                sb << "\nMessage: " << msg;                                    \
            }                                                                  \
            auto* vl = Singleton<NYdb::NBS::TVerifyLog>();                     \
            with_lock (vl->Lock) {                                             \
                vl->Log.Write(                                                 \
                    ELogPriority::TLOG_EMERG,                                  \
                    TStringBuilder()                                           \
                        << entityType << "\t" << entityId << "\n");            \
            }                                                                  \
            Y_ABORT("%s", sb.c_str());                                         \
        }                                                                      \
    } while (false)   // STORAGE_VERIFY_C

#define STORAGE_VERIFY(expr, entityType, entityId)                             \
    STORAGE_VERIFY_C(expr, entityType, entityId, "");                          \
    // STORAGE_VERIFY

#ifndef NDEBUG
#define STORAGE_VERIFY_DEBUG STORAGE_VERIFY
#define STORAGE_VERIFY_DEBUG_C STORAGE_VERIFY_C
#else
#define STORAGE_VERIFY_DEBUG Y_DEBUG_ABORT_UNLESS
#define STORAGE_VERIFY_DEBUG_C Y_DEBUG_ABORT_UNLESS
#endif
