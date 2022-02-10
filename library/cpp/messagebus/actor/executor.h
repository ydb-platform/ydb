#pragma once

#include "ring_buffer_with_spin_lock.h"

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>
#include <util/system/condvar.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

namespace NActor {
    namespace NPrivate {
        struct TExecutorHistory {
            struct THistoryRecord {
                ui32 MaxQueueSize;
            };
            TVector<THistoryRecord> HistoryRecords;
            ui64 LastHistoryRecordSecond;

            ui64 FirstHistoryRecordSecond() const {
                return LastHistoryRecordSecond - HistoryRecords.size() + 1;
            }
        };

        struct TExecutorStatus {
            size_t WorkQueueSize = 0;
            TExecutorHistory History;
            TString Status;
        };
    }

    class IWorkItem {
    public:
        virtual ~IWorkItem() {
        }
        virtual void DoWork(/* must release this */) = 0;
    };

    struct TExecutorWorker;

    class TExecutor: public TAtomicRefCount<TExecutor> {
        friend struct TExecutorWorker;

    public:
        struct TConfig {
            size_t WorkerCount;
            const char* Name;

            TConfig()
                : WorkerCount(1)
                , Name()
            {
            }
        };

    private:
        struct TImpl;
        THolder<TImpl> Impl;

        const TConfig Config;

        TAtomic ExitWorkers;

        TVector<TAutoPtr<TExecutorWorker>> WorkerThreads;

        TRingBufferWithSpinLock<IWorkItem*> WorkItems;

        TMutex WorkMutex;
        TCondVar WorkAvailable;

    public:
        explicit TExecutor(size_t workerCount);
        TExecutor(const TConfig& config);
        ~TExecutor();

        void Stop();

        void EnqueueWork(TArrayRef<IWorkItem* const> w);

        size_t GetWorkQueueSize() const;
        TString GetStatus() const;
        TString GetStatusSingleLine() const;
        NPrivate::TExecutorStatus GetStatusRecordInternal() const;

        bool IsInExecutorThread() const;

    private:
        void Init();

        TAutoPtr<IWorkItem> DequeueWork();

        void ProcessWorkQueueHere();

        inline void RunWorkItem(TAutoPtr<IWorkItem>);

        void RunWorker();

        ui32 GetMaxQueueSizeAndClear() const;
    };

    using TExecutorPtr = TIntrusivePtr<TExecutor>;

}
