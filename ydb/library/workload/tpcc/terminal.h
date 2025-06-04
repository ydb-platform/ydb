#pragma once

#include "task_queue.h"

#include "histogram.h"
#include "transactions.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <util/system/spinlock.h>

#include <atomic>
#include <stop_token>
#include <memory>
#include <array>

class TLog;

namespace NYdb::NTPCC {

//-----------------------------------------------------------------------------

class TTerminalStats {
public:
    // don't change the order
    enum ETransactionType {
        E_NEW_ORDER = 0,
        E_DELIVERY = 1,
        E_ORDER_STATUS = 2,
        E_PAYMENT = 3,
        E_STOCK_LEVEL = 4
    };

    struct TStats {
        // assumes that dst doesn't requre lock
        void Collect(TStats& dst) const {
            dst.OK.fetch_add(OK.load(std::memory_order_relaxed), std::memory_order_relaxed);
            dst.Failed.fetch_add(Failed.load(std::memory_order_relaxed), std::memory_order_relaxed);
            dst.UserAborted.fetch_add(UserAborted.load(std::memory_order_relaxed), std::memory_order_relaxed);

            TGuard guard(HistLock);
            dst.LatencyHistogramMs.Add(LatencyHistogramMs);
            dst.LatencyHistogramFullMs.Add(LatencyHistogramMs);
            dst.LatencyHistogramPure.Add(LatencyHistogramMs);
        }

        void Clear() {
            OK.store(0, std::memory_order_relaxed);
            Failed.store(0, std::memory_order_relaxed);
            UserAborted.store(0, std::memory_order_relaxed);
            TGuard guard(HistLock);
            LatencyHistogramMs.Reset();
            LatencyHistogramFullMs.Reset();
            LatencyHistogramPure.Reset();
        }

        std::atomic<size_t> OK = 0;
        std::atomic<size_t> Failed = 0;
        std::atomic<size_t> UserAborted = 0;

        // histograms protected by lock

        mutable TSpinLock HistLock;

        // Transaction latency observed by the terminal, i.e., includes session acquisition
        // and retries performed by the SDK
        THistogram LatencyHistogramMs{256, 32768};

        // As LatencyHistogramMs plus inflight wait time in the terminal
        THistogram LatencyHistogramFullMs{256, 32768};

        // Latency of a successful transaction measured directly in the transaction code,
        // when there is nothing to wait for except the queries
        THistogram LatencyHistogramPure{256, 32768};
    };

public:
    TTerminalStats() = default;

    const TStats& GetStats(ETransactionType type) const {
        return Stats[type];
    }

    void AddOK(
        ETransactionType type,
        std::chrono::milliseconds latency,
        std::chrono::milliseconds latencyFull,
        TDuration latencyPure)
    {
        auto& stats = Stats[type];
        stats.OK.fetch_add(1, std::memory_order_relaxed);
        {
            TGuard guard(stats.HistLock);
            stats.LatencyHistogramMs.RecordValue(latency.count());
            stats.LatencyHistogramFullMs.RecordValue(latencyFull.count());
            stats.LatencyHistogramPure.RecordValue(latencyPure.MilliSeconds());
        }
    }

    void IncFailed(ETransactionType type) {
        Stats[type].Failed.fetch_add(1, std::memory_order_relaxed);
    }

    void IncUserAborted(ETransactionType type) {
        Stats[type].UserAborted.fetch_add(1, std::memory_order_relaxed);
    }

    // assumes that dst doesn't requre lock
    void Collect(TTerminalStats& dst) const {
        for (size_t i = 0; i < Stats.size(); ++i) {
            Stats[i].Collect(dst.Stats[i]);
        }
    }

    void Clear() {
        for (auto& stats: Stats) {
            stats.Clear();
        }
    }

    // Thread-safe clear that happens only once, even if called multiple times
    void ClearOnce() {
        bool expected = false;
        if (WasCleared.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
            Clear();
        }
    }

private:
    std::array<TStats, 5> Stats;
    std::atomic<bool> WasCleared{false};
};

using TTerminalTask = TTask<void>;

//-----------------------------------------------------------------------------

class alignas(64) TTerminal {
public:
    TTerminal(
        size_t terminalID,
        size_t warehouseID,
        size_t warehouseCount,
        ITaskQueue& taskQueue,
        std::shared_ptr<NQuery::TQueryClient>& client,
        const TString& path,
        bool noSleep,
        int simulateTransactionMs,
        int simulateTransactionSelect1Count,
        std::stop_token stopToken,
        std::atomic<bool>& stopWarmup,
        std::shared_ptr<TTerminalStats>& stats,
        std::shared_ptr<TLog>& log);

    TTerminal(const TTerminal&) = delete;
    TTerminal& operator=(TTerminal&) = delete;

    TTerminal(TTerminal&&) = delete;
    TTerminal& operator=(TTerminal&&) = delete;

    size_t GetID() const {
        return Context.TerminalID;
    }

    void Start();

    bool IsDone() const;

private:
    TTerminalTask Run();

private:
    ITaskQueue& TaskQueue;
    TTransactionContext Context;
    bool NoSleep;
    std::stop_token StopToken;
    std::atomic<bool>& StopWarmup;
    std::shared_ptr<TTerminalStats> Stats;

    TTerminalTask Task;

    bool Started = false;
    bool WarmupWasStopped = false;
};

} // namespace NYdb::NTPCC
