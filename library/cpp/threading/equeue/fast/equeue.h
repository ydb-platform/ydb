#pragma once

#include <util/thread/pool.h>

#include <util/datetime/base.h>
#include <util/thread/lfqueue.h>
#include <util/system/thread.h>
#include <util/generic/bitops.h>
#include <util/generic/vector.h>
#include <util/generic/scope.h>
#include <util/stream/str.h>

#include <library/cpp/threading/bounded_queue/bounded_queue.h>
#include <library/cpp/yt/threading/event_count.h>

class TFastElasticQueue
    : public TThreadPoolBase
    , private IThreadFactory::IThreadAble
{
public:
    explicit TFastElasticQueue(const TParams& params = {})
        : TThreadPoolBase(params)
    {
        Y_ENSURE(!params.Blocking_);
    }

    ~TFastElasticQueue() {
        Stop();
    }

    void Start(size_t threadCount, size_t maxQueueSize) override {
        Y_ENSURE(Threads_.empty());
        Y_ENSURE(maxQueueSize > 0);

        Queue_.Reset(new NThreading::TBoundedQueue<IObjectInQueue*>(FastClp2(maxQueueSize + threadCount))); //threadCount is for stop events
        MaxQueueSize_ = maxQueueSize;
        CurrentMaxQueueSize_ = maxQueueSize;
        ActiveThreadCount_ = threadCount;

        try {
            for (size_t i = 0; i < threadCount; ++i) {
                Threads_.push_back(Pool()->Run(this));
            }
        } catch (...) {
            Stop();
            throw;
        }

        Stopped_ = false;
    }

    size_t ObjectCount() const {
        //May return extra +1 for real object count if near CurrentMaxQueueSize_
        return GuardCount_.load();
    }

    bool Add(IObjectInQueue* obj) override Y_WARN_UNUSED_RESULT {
        if (Stopped_ || !obj) {
            return false;
        }

        if (GuardCount_.fetch_add(1) >= CurrentMaxQueueSize_) {
            GuardCount_.fetch_sub(1);
            return false;
        }

        QueueSize_.fetch_add(1);

        if (!Queue_->Enqueue(obj)) {
            //Simultaneous Dequeue calls can return not in exact fifo order of items,
            //so there can be GuardCount_ < CurrentMaxQueueSize_ but Enqueue will fail because of
            //the oldest enqueued item is not actually dequeued and ring buffer can't proceed.
            GuardCount_.fetch_sub(1);
            QueueSize_.fetch_sub(1);
            return false;
        }


        Event_.NotifyOne();

        return true;
    }

    size_t Size() const noexcept override {
        return QueueSize_.load();
    }

    void Stop() noexcept override {
        Stopped_ = true;

        for (size_t i = 0; i < Threads_.size(); ++i) {
            while (!Queue_->Enqueue(nullptr)) {
                Sleep(TDuration::MilliSeconds(1));
            }

            Event_.NotifyOne();
        }

        while (!Threads_.empty()) {
            Threads_.back()->Join();
            Threads_.pop_back();
        }

        Queue_.Reset();
    }

    void DoExecute() override {
        TThread::SetCurrentThreadName(Params.ThreadName_.c_str());
        const size_t thisThreadId = ThreadCount_++;

        while (true) {
            IObjectInQueue* job = nullptr;

            while (thisThreadId >= ActiveThreadCount_ && !Stopped_) {
                Sleep(TDuration::Seconds(1));
            }

            Event_.Await([&]() {
                return Queue_->Dequeue(job);
            });

            if (!job) {
                break;
            }

            QueueSize_.fetch_sub(1);

            Y_DEFER {
                GuardCount_.fetch_sub(1);
            };

            if (Params.Catching_) {
                try {
                    try {
                        job->Process(nullptr);
                    } catch (...) {
                        Cdbg << "[mtp queue] " << CurrentExceptionMessage() << Endl;
                    }
                } catch (...) {
                    ;
                }
            } else {
                job->Process(nullptr);
            }
        }
    }

    void SetCurrentMaxQueueSize(size_t v) {
        Y_ENSURE(v <= MaxQueueSize_);
        CurrentMaxQueueSize_ = v;
    }

    void SetActiveThreadCount(size_t v) {
        Y_ENSURE(v <= ThreadCount_);
        ActiveThreadCount_ = v;
    }
private:
    std::atomic<bool> Stopped_ = false;
    size_t MaxQueueSize_ = 0;
    std::atomic<size_t> CurrentMaxQueueSize_ = 0;
    std::atomic<size_t> ThreadCount_ = 0;
    std::atomic<size_t> ActiveThreadCount_ = 0;

    alignas(64) std::atomic<size_t> GuardCount_ = 0;
    alignas(64) std::atomic<size_t> QueueSize_ = 0;

    TVector<THolder<IThreadFactory::IThread>> Threads_;

    THolder<NThreading::TBoundedQueue<IObjectInQueue*>> Queue_;
    NYT::NThreading::TEventCount Event_;
};

