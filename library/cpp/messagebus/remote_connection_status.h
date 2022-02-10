#pragma once

#include "codegen.h"
#include "duration_histogram.h"
#include "message_counter.h"
#include "message_status_counter.h"
#include "queue_config.h"
#include "session_config.h"

#include <library/cpp/messagebus/actor/executor.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

namespace NBus {
    class TConnectionStatusMonRecord;
}

namespace NBus {
    namespace NPrivate {
#define WRITER_STATE_MAP(XX) \
    XX(WRITER_UNKNOWN)       \
    XX(WRITER_FILLING)       \
    XX(WRITER_FLUSHING)      \
    /**/

        // TODO: move elsewhere
        enum EWriterState {
            WRITER_STATE_MAP(ENUM_VALUE_GEN_NO_VALUE)
        };

        ENUM_TO_STRING(EWriterState, WRITER_STATE_MAP)

#define STRUCT_FIELD_ADD(name, type, func) func(name, that.name);

        template <typename T>
        void Reset(T& t) {
            t.~T();
            new (&t) T();
        }

#define DURATION_COUNTER_MAP(XX, comma)       \
    XX(Count, unsigned, Add)                  \
    comma                                     \
        XX(SumDuration, TDuration, Add) comma \
            XX(MaxDuration, TDuration, Max) /**/

        struct TDurationCounter {
            DURATION_COUNTER_MAP(STRUCT_FIELD_GEN, )

            TDuration AvgDuration() const;

            TDurationCounter();

            void AddDuration(TDuration d) {
                Count += 1;
                SumDuration += d;
                if (d > MaxDuration) {
                    MaxDuration = d;
                }
            }

            TDurationCounter& operator+=(const TDurationCounter&);

            TString ToString() const;
        };

#define REMOTE_CONNECTION_STATUS_BASE_MAP(XX, comma) \
    XX(ConnectionId, ui64, AssertZero)               \
    comma                                            \
        XX(Fd, SOCKET, AssertZero) comma             \
            XX(Acts, ui64, Add) comma                \
                XX(BufferSize, ui64, Add) /**/

        struct TRemoteConnectionStatusBase {
            REMOTE_CONNECTION_STATUS_BASE_MAP(STRUCT_FIELD_GEN, )

            TRemoteConnectionStatusBase& operator+=(const TRemoteConnectionStatusBase&);

            TRemoteConnectionStatusBase();
        };

#define REMOTE_CONNECTION_INCREMENTAL_STATUS_BASE_MAP(XX, comma) \
    XX(BufferDrops, unsigned, Add)                               \
    comma                                                        \
    XX(NetworkOps, unsigned, Add) /**/

        struct TRemoteConnectionIncrementalStatusBase {
            REMOTE_CONNECTION_INCREMENTAL_STATUS_BASE_MAP(STRUCT_FIELD_GEN, )

            TRemoteConnectionIncrementalStatusBase& operator+=(const TRemoteConnectionIncrementalStatusBase&);

            TRemoteConnectionIncrementalStatusBase();
        };

#define REMOTE_CONNECTION_READER_INCREMENTAL_STATUS_MAP(XX, comma) \
    XX(MessageCounter, TMessageCounter, Add)                       \
    comma                                                          \
        XX(StatusCounter, TMessageStatusCounter, Add) /**/

        struct TRemoteConnectionReaderIncrementalStatus: public TRemoteConnectionIncrementalStatusBase {
            REMOTE_CONNECTION_READER_INCREMENTAL_STATUS_MAP(STRUCT_FIELD_GEN, )

            TRemoteConnectionReaderIncrementalStatus& operator+=(const TRemoteConnectionReaderIncrementalStatus&);

            TRemoteConnectionReaderIncrementalStatus();
        };

#define REMOTE_CONNECTION_READER_STATUS_MAP(XX, comma) \
    XX(QuotaMsg, size_t, Add)                          \
    comma                                              \
        XX(QuotaBytes, size_t, Add) comma              \
            XX(QuotaExhausted, size_t, Add) comma      \
                XX(Incremental, TRemoteConnectionReaderIncrementalStatus, Add) /**/

        struct TRemoteConnectionReaderStatus: public TRemoteConnectionStatusBase {
            REMOTE_CONNECTION_READER_STATUS_MAP(STRUCT_FIELD_GEN, )

            TRemoteConnectionReaderStatus& operator+=(const TRemoteConnectionReaderStatus&);

            TRemoteConnectionReaderStatus();
        };

#define REMOTE_CONNECTION_WRITER_INCREMENTAL_STATUS(XX, comma) \
    XX(MessageCounter, TMessageCounter, Add)                   \
    comma                                                      \
        XX(StatusCounter, TMessageStatusCounter, Add) comma    \
            XX(ProcessDurationHistogram, TDurationHistogram, Add) /**/

        struct TRemoteConnectionWriterIncrementalStatus: public TRemoteConnectionIncrementalStatusBase {
            REMOTE_CONNECTION_WRITER_INCREMENTAL_STATUS(STRUCT_FIELD_GEN, )

            TRemoteConnectionWriterIncrementalStatus& operator+=(const TRemoteConnectionWriterIncrementalStatus&);

            TRemoteConnectionWriterIncrementalStatus();
        };

#define REMOTE_CONNECTION_WRITER_STATUS(XX, comma)                                                               \
    XX(Connected, bool, AssertZero)                                                                              \
    comma                                                                                                        \
        XX(ConnectTime, TInstant, AssertZero) comma /* either connect time on client or accept time on server */ \
        XX(ConnectError, int, AssertZero) comma                                                                  \
        XX(ConnectSyscalls, unsigned, Add) comma                                                                 \
            XX(PeerAddr, TNetAddr, AssertZero) comma                                                             \
                XX(MyAddr, TNetAddr, AssertZero) comma                                                           \
                    XX(State, EWriterState, AssertZero) comma                                                    \
                        XX(SendQueueSize, size_t, Add) comma                                                     \
                            XX(AckMessagesSize, size_t, Add) comma                       /* client only */       \
                                XX(DurationCounter, TDurationCounter, Add) comma         /* server only */       \
                                    XX(DurationCounterPrev, TDurationCounter, Add) comma /* server only */       \
                                        XX(Incremental, TRemoteConnectionWriterIncrementalStatus, Add) comma     \
                                            XX(ReaderWakeups, size_t, Add) /**/

        struct TRemoteConnectionWriterStatus: public TRemoteConnectionStatusBase {
            REMOTE_CONNECTION_WRITER_STATUS(STRUCT_FIELD_GEN, )

            TRemoteConnectionWriterStatus();

            TRemoteConnectionWriterStatus& operator+=(const TRemoteConnectionWriterStatus&);

            size_t GetInFlight() const;
        };

#define REMOTE_CONNECTION_STATUS_MAP(XX, comma) \
    XX(Summary, bool)                           \
    comma                                       \
    XX(Server, bool) /**/

        struct TRemoteConnectionStatus {
            REMOTE_CONNECTION_STATUS_MAP(STRUCT_FIELD_GEN, )

            TRemoteConnectionReaderStatus ReaderStatus;
            TRemoteConnectionWriterStatus WriterStatus;

            TRemoteConnectionStatus();

            TString PrintToString() const;
            TConnectionStatusMonRecord GetStatusProtobuf() const;
        };

        struct TBusSessionStatus {
            size_t InFlightCount;
            size_t InFlightSize;
            bool InputPaused;

            TBusSessionStatus();
        };

        struct TSessionDumpStatus {
            bool Shutdown;
            TString Head;
            TString Acceptors;
            TString ConnectionsSummary;
            TString Connections;
            TBusSessionStatus Status;
            TRemoteConnectionStatus ConnectionStatusSummary;
            TBusSessionConfig Config;

            TSessionDumpStatus()
                : Shutdown(false)
            {
            }

            TString PrintToString() const;
        };

        // without sessions
        struct TBusMessageQueueStatus {
            NActor::NPrivate::TExecutorStatus ExecutorStatus;
            TBusQueueConfig Config;

            TString PrintToString() const;
        };
    }
}
