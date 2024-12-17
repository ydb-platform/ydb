#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/include/counters.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <util/system/spinlock.h>

namespace NYdb::NTopic {

template <bool UseMigrationProtocol>
class TSingleClusterReadSessionImpl;

template <bool UseMigrationProtocol>
using TCallbackContextPtr = std::shared_ptr<TCallbackContext<TSingleClusterReadSessionImpl<UseMigrationProtocol>>>;

template<bool UseMigrationProtocol>
class TCountersLogger : public std::enable_shared_from_this<TCountersLogger<UseMigrationProtocol>> {
public:
    static constexpr auto UPDATE_PERIOD = TDuration::Seconds(1);

public:
    explicit TCountersLogger(std::shared_ptr<TGRpcConnectionsImpl> connections,
                             std::vector<TCallbackContextPtr<UseMigrationProtocol>> sessions,
                             TReaderCounters::TPtr counters, const TLog& log, TString prefix, TInstant startSessionTime)
        : Connections(std::move(connections))
        , SessionsContexts(std::move(sessions))
        , Counters(std::move(counters))
        , Log(log)
        , Prefix(std::move(prefix))
        , StartSessionTime(startSessionTime) {
    }

    std::shared_ptr<TCallbackContext<TCountersLogger<UseMigrationProtocol>>> MakeCallbackContext() {
        SelfContext = std::make_shared<TCallbackContext<TCountersLogger<UseMigrationProtocol>>>(this->shared_from_this());
        return SelfContext;
    }

    void Start() {
        Y_ABORT_UNLESS(SelfContext);
        ScheduleDumpCountersToLog();
    }

    void Stop() {
        Y_ABORT_UNLESS(SelfContext);

        with_lock(Lock) {
            Stopping = true;
        }

        // Log final counters.
        DumpCountersToLog();
    }

private:
    void ScheduleDumpCountersToLog(size_t timeNumber = 0) {
        with_lock(Lock) {
            if (Stopping) {
                return;
            }

            DumpCountersContext = Connections->CreateContext();
            if (DumpCountersContext) {
                auto callback = [ctx = SelfContext, timeNumber](bool ok) {
                    if (ok) {
                        if (auto borrowedSelf = ctx->LockShared()) {
                            borrowedSelf->DumpCountersToLog(timeNumber);
                        }
                    }
                };
                Connections->ScheduleCallback(UPDATE_PERIOD,
                                            std::move(callback),
                                            DumpCountersContext);
            }
        }
    }

    void DumpCountersToLog(size_t timeNumber = 0) {
        const bool logCounters = timeNumber % 60 == 0; // Every 1 minute.
        const bool dumpSessionsStatistics = timeNumber % 600 == 0; // Every 10 minutes.

        *Counters->CurrentSessionLifetimeMs = (TInstant::Now() - StartSessionTime).MilliSeconds();

        {
            TMaybe<TLogElement> log;
            bool dumpHeader = true;

            for (auto& sessionCtx : SessionsContexts) {
                if (auto borrowedSession = sessionCtx->LockShared()) {
                    borrowedSession->UpdateMemoryUsageStatistics();
                    if (dumpSessionsStatistics) {
                        if (dumpHeader) {
                            log.ConstructInPlace(&Log, TLOG_INFO);
                            (*log) << "Read/commit by partition streams (cluster:topic:partition:stream-id:read-offset:committed-offset):";
                            dumpHeader = false;
                        }
                        borrowedSession->DumpStatisticsToLog(*log);
                    }
                }
            }
        }

#define C(counter)                                                      \
        << " " Y_STRINGIZE(counter) ": "                                \
        << Counters->counter->Val()                                     \
            /**/

        if (logCounters) {
            LOG_LAZY(Log, TLOG_INFO,
                TStringBuilder() << Prefix << "Counters: {"
                C(Errors)
                C(CurrentSessionLifetimeMs)
                C(BytesRead)
                C(MessagesRead)
                C(BytesReadCompressed)
                C(BytesInflightUncompressed)
                C(BytesInflightCompressed)
                C(BytesInflightTotal)
                C(MessagesInflight)
                << " }"
            );
        }

#undef C

        ScheduleDumpCountersToLog(timeNumber + 1);
    }


private:
    std::shared_ptr<TCallbackContext<TCountersLogger<UseMigrationProtocol>>> SelfContext;

    TSpinLock Lock;

    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    std::vector<TCallbackContextPtr<UseMigrationProtocol>> SessionsContexts;

    IQueueClientContextPtr DumpCountersContext;

    TReaderCounters::TPtr Counters;

    TLog Log;
    const TString Prefix;
    const TInstant StartSessionTime;

    bool Stopping = false;
};

}  // namespace NYdb::NTopic
